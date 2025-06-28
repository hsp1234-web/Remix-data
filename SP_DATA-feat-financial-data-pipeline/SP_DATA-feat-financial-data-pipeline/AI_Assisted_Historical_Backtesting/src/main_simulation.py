import os
import sys
import json
from datetime import datetime, timedelta, timezone

# ---路徑修正，確保能導入項目內的模塊---
# 假設此文件在 project_root/src/main_simulation.py
# project_root = AI_Assisted_Historical_Backtesting
current_script_dir = os.path.dirname(os.path.abspath(__file__)) # .../src
project_root_dir = os.path.dirname(current_script_dir) # AI_Assisted_Historical_Backtesting
project_root_parent_dir = os.path.dirname(project_root_dir) # Parent of AI_Assisted_Historical_Backtesting

if project_root_parent_dir not in sys.path:
    sys.path.insert(0, project_root_parent_dir)
# ---路徑修正結束---

from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME, get_logger
from AI_Assisted_Historical_Backtesting.src.database.db_manager import DatabaseManager
from AI_Assisted_Historical_Backtesting.src.connectors.fred_connector import FredConnector
from AI_Assisted_Historical_Backtesting.src.connectors.yfinance_connector import YFinanceConnector
from AI_Assisted_Historical_Backtesting.src.connectors.finmind_connector import FinMindConnector
from AI_Assisted_Historical_Backtesting.src.data_processing.cleaners import (
    clean_yfinance_csv_row, clean_fred_observation, clean_finmind_data_item,
    standardize_datetime_str_to_iso_utc
)
from AI_Assisted_Historical_Backtesting.src.data_processing.aligners import (
    align_ohlcv_data, get_target_period_start_utc
)
from AI_Assisted_Historical_Backtesting.src.data_processing.feature_calculator import (
    calculate_all_features # 假設 OHLCV 鍵名為 'open', 'high', 'low', 'close', 'volume'
)
from AI_Assisted_Historical_Backtesting.src.ai_logic.prompt_generator import PromptGenerator
from AI_Assisted_Historical_Backtesting.src.ai_logic.llama_agent import LlamaOllamaAgent

import argparse # 用於解析命令行參數

# --- 日誌設置 ---
# setup_logger 在模塊被導入時，如果 main_simulation 是主執行文件，其 __main__ 會調用
logger = get_logger(__name__)

# --- 預設配置 (將被命令行參數覆蓋) ---
DEFAULT_DB_FILEPATH = os.path.join(project_root_dir, "data", "project_data.sqlite")
DEFAULT_SIM_STEP_HOURS = 12
DEFAULT_TARGET_SYMBOL_MAIN = "AAPL" # 避免與 HistoricalSimulation 類的 target_symbol 衝突
DEFAULT_HISTORICAL_CONTEXT_DAYS = 60
DEFAULT_OLLAMA_MODEL = "llama3:8b-instruct-q4_K_M" # 與 LlamaOllamaAgent 預設一致
DEFAULT_SIM_MODE = "simulate"


# FRED 和 FinMind 的配置可以保持為常量，或將來也參數化
FRED_SERIES_TO_FETCH = {"CPI": "CPIAUCSL", "UNRATE": "UNRATE", "FEDFUNDS": "FEDFUNDS"}
FINMIND_CHIP_DATASETS = {"InstitutionalInvestors": "InstitutionalInvestorsBuySell", "MarginTrading": "MarginPurchaseShortSale"}
FEATURE_CALC_HISTORY_WINDOW_SIZE = 60 # 以分析週期為單位


class HistoricalSimulation:
    def __init__(self, sim_latest_date_str: str,
                 sim_earliest_date_str: str,
                 target_symbol: str,
                 interval_hours: int,
                 historical_context_days: int,
                 mode: str,
                 db_path: str,
                 ollama_model: str):
        """
        初始化歷史回溯模擬。
        """
        self.sim_latest_dt_utc = self._parse_iso_date_to_datetime_utc(sim_latest_date_str, start_of_day=False) # 最新日期，取當天結束前回溯
        self.sim_earliest_dt_utc = self._parse_iso_date_to_datetime_utc(sim_earliest_date_str, start_of_day=True) # 最早日期，取當天開始
        self.target_symbol = target_symbol
        self.interval_hours = interval_hours
        self.historical_context_days = historical_context_days
        self.mode = mode # "simulate" 或 "dry-run"
        self.db_path = db_path
        self.ollama_model = ollama_model

        if not self.sim_latest_dt_utc or not self.sim_earliest_dt_utc:
            raise ValueError("無效的模擬開始或結束日期格式。請使用 YYYY-MM-DD。")
        if self.sim_latest_dt_utc <= self.sim_earliest_dt_utc: # 最新日期應晚於最早日期
            raise ValueError("模擬的最新日期 (--sim-latest-date) 必須晚於最早日期 (--sim-earliest-date)。")

        logger.info(f"歷史回溯模擬初始化:")
        logger.info(f"  標的                : {self.target_symbol}")
        logger.info(f"  回溯最新日期 (起點) : {self.sim_latest_dt_utc.isoformat()}")
        logger.info(f"  回溯最早日期 (終點) : {self.sim_earliest_dt_utc.isoformat()}")
        logger.info(f"  時間間隔 (小時)     : {self.interval_hours}")
        logger.info(f"  歷史上下文 (天)    : {self.historical_context_days}")
        logger.info(f"  模式                : {self.mode}")
        logger.info(f"  數據庫路徑          : {self.db_path}")
        logger.info(f"  Ollama 模型         : {self.ollama_model}")


        # 初始化核心組件
        self.db_manager = DatabaseManager(db_path=self.db_path)
        self.fred_connector = FredConnector()
        self.yf_connector = YFinanceConnector()
        self.fm_connector = FinMindConnector()
        self.prompt_generator = PromptGenerator()
        self.llama_agent = LlamaOllamaAgent(model_name=self.ollama_model)

        self._historical_features_cache = {}

    def _parse_iso_date_to_datetime_utc(self, date_str: str, start_of_day: bool = True) -> datetime | None:
        """將 YYYY-MM-DD 格式的日期字符串解析為 UTC 的 datetime 對象 (當天開始或結束)。"""
        try:
            dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
            if start_of_day:
                dt_obj = dt_obj.replace(hour=0, minute=0, second=0, microsecond=0)
            else: # end of day for latest_date means start of next day for loop condition
                dt_obj = dt_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            return dt_obj.replace(tzinfo=timezone.utc)
        except ValueError as e:
            logger.error(f"無法解析日期字符串: {date_str}. 錯誤: {e}")
            return None

    def _get_historical_data_for_features(self, symbol: str, current_period_start_dt: datetime, window_size: int) -> list[dict]:
        """
        從數據庫或內存快取中獲取計算特徵所需的歷史 OHLCV 數據。
        返回按時間升序排列的數據列表，最新的在末尾。
        """
        logger.info(f"正在為 {symbol} 從數據庫查詢過去 {window_size} 個週期的歷史特徵數據，"
                    f"當前週期開始於 {current_period_start_dt.isoformat()}")

        # 查詢的歷史窗口應截止到 current_period_start_dt 的上一個週期
        query_end_dt = current_period_start_dt - timedelta(microseconds=1) # 不包含當前週期的開始
        query_start_dt = query_end_dt - timedelta(hours=(window_size -1) * self.interval_hours) # -1 因為窗口包含 query_end_dt 所在的週期
                                                                                             # (需要 window_size 個點)

        query = f"""
        SELECT timestamp_period_start_utc, price_open, price_high, price_low, price_close, volume_total
        FROM processed_features_hourly
        WHERE symbol = ?
          AND timestamp_period_start_utc >= ?
          AND timestamp_period_start_utc <= ?
        ORDER BY timestamp_period_start_utc ASC;
        """
        # LIMIT ? (LIMIT window_size) - SQLite 在這裡的 LIMIT 可能不會如預期工作，因為我們需要最新的 N 條 *之前* 的數據
        # ORDER BY DESC LIMIT N 然後再反轉，或者直接用日期範圍篩選後再處理。
        # 目前的日期範圍篩選應該是主要的。

        params = [
            symbol,
            query_start_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            query_end_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            # window_size # 移除 LIMIT，依賴日期範圍
        ]

        results = self.db_manager.execute_query(query, params)
        historical_data = []
        if results:
            for row in results:
                historical_data.append({
                    "timestamp_utc": row[0],
                    "open": row[1], "high": row[2], "low": row[3],
                    "close": row[4], "volume": row[5]
                })
            logger.info(f"為 {symbol} 找到 {len(historical_data)} 條歷史特徵數據。")
        else:
            logger.warning(f"未能為 {symbol} 在 {current_period_start_dt.isoformat()} 之前找到歷史特徵數據。")
        return historical_data[-window_size:] # 確保最多返回 window_size 條，取最新的

    def _update_historical_features_cache(self, symbol: str, features_for_current_period: dict):
        # (目前未使用，因為 _get_historical_data_for_features 直接查庫)
        pass


    def run_simulation_period(self, current_period_start_dt: datetime, symbol: str):
        """
        為單個時間週期執行模擬步驟。
        current_period_start_dt: 當前要分析的12小時週期的開始時間 (UTC)。
        """
        current_period_start_iso = current_period_start_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        logger.info(f"--- [CYCLE START] 主模擬循環: 分析標的 {symbol}，週期開始於 {current_period_start_iso} ---")

        # --- 1. 數據獲取框架 ---
        #    獲取覆蓋 current_period_start_dt 及 historical_context_days 的原始數據
        #    例如，如果 current_period_start_dt 是 2023-10-26T00:00:00Z，interval=12h, context=60d
        #    則 YF/FinMind 數據需要從 (2023-10-26 - 60d) 到 2023-10-26。
        #    FRED 數據則獲取在 2023-10-26 之前的最新值。

        #    對 YFinance:
        yf_period_end_date_obj = current_period_start_dt + timedelta(hours=self.interval_hours) - timedelta.microseconds(1)
        yf_period_end_date_str = yf_period_end_date_obj.strftime("%Y-%m-%d")
        yf_hist_start_date_obj = current_period_start_dt - timedelta(days=self.historical_context_days)
        yf_hist_start_date_str = yf_hist_start_date_obj.strftime("%Y-%m-%d")

        logger.info(f"框架: 調用 YF Connector 獲取 {symbol} 從 {yf_hist_start_date_str} 到 {yf_period_end_date_str} 的日線數據。")
        # raw_yf_csv = self.yf_connector.get_historical_data_csv(symbol, yf_hist_start_date_str, yf_period_end_date_str)
        # if raw_yf_csv:
        #     logger.debug(f"YF 原始數據 (部分): {raw_yf_csv[:200]}")
        #     # TODO: 存儲到 raw_market_data
        # else:
        #     logger.warning(f"YF Connector 未能獲取 {symbol} 的數據。")

        #    對 FRED (獲取截至 current_period_start_dt 的最新數據)
        # for series_name, series_id in FRED_SERIES_TO_FETCH.items():
        #     logger.info(f"框架: 調用 FRED Connector 獲取 {series_name} ({series_id}) 數據。")
            # fred_data = self.fred_connector.get_series_observations(series_id, end_date_str=current_period_start_dt.strftime("%Y-%m-%d"))
            # if fred_data: logger.debug(f"FRED {series_name} 原始數據 (部分): {str(fred_data)[:200]}")
            # TODO: 存儲

        #    對 FinMind (籌碼數據通常是日度，財報是季度/年度)
        # fm_target_date_str = current_period_start_dt.strftime("%Y-%m-%d")
        # fm_hist_start_date_str_short = (current_period_start_dt - timedelta(days=7)).strftime("%Y-%m-%d") # 籌碼可能只需要幾天
        # for fm_dataset_name, fm_dataset_id in FINMIND_CHIP_DATASETS.items():
        #     logger.info(f"框架: 調用 FinMind Connector 獲取 {fm_dataset_name} ({fm_dataset_id}) for {symbol}。")
            # chip_data = self.fm_connector.get_chip_data(symbol, fm_hist_start_date_str_short, fm_target_date_str, chip_type=fm_dataset_id)
            # TODO: 存儲

        logger.info("框架: 數據獲取調用完成 (目前為 placeholder)。")

        # --- 2. 數據清洗與對齊框架 ---
        #    假設原始數據已獲取並存儲，現在需要讀取、清洗、對齊以生成當前週期的 OHLCV
        #    這一步的輸出應該是類似 aligned_ohlcv_period_data 的結構
        logger.info("框架: 數據清洗與對齊 (目前為 placeholder)...")
        # aligned_ohlcv_for_current_period = align_ohlcv_data(...)
        # 假設我們得到了當前週期的 OHLCV:
        aligned_ohlcv_for_current_period = { # 模擬數據
            "open": 150.0, "high": 152.0, "low": 149.5, "close": 151.5, "volume": 1000000
        }
        if not aligned_ohlcv_for_current_period:
            logger.warning(f"未能為週期 {current_period_start_iso} 對齊 OHLCV 數據，跳過此週期。")
            return

        # --- 3. 特徵計算框架 ---
        logger.info("框架: 計算技術指標和其他特徵...")
        #    獲取歷史已處理特徵 (OHLCV) 用於計算指標
        historical_aligned_ohlcv_for_calc = self._get_historical_data_for_features(
            symbol, current_period_start_dt, FEATURE_CALC_HISTORY_WINDOW_SIZE
        )
        # 將當前週期的 OHLCV 加入到歷史序列的末尾，以便 calculate_all_features 能計算當前週期的指標
        current_period_ohlcv_for_hist = aligned_ohlcv_for_current_period.copy()
        current_period_ohlcv_for_hist["timestamp_utc"] = current_period_start_iso # 添加時間戳

        # 確保 historical_aligned_ohlcv_for_calc 是列表，並且不包含當前週期的數據 (因為它是 "歷史")
        # calculate_all_features 的第二個參數期望是包含當前週期的完整序列
        full_series_for_calc = historical_aligned_ohlcv_for_calc + [current_period_ohlcv_for_hist]

        features_for_db = {}
        if len(full_series_for_calc) >= 1: # 至少要有當前數據
            # calculate_all_features 的第一個參數是當前對齊的 OHLCV，第二個是包含歷史的完整序列
            features_for_db = calculate_all_features(current_period_ohlcv_for_hist, full_series_for_calc)
            features_for_db["timestamp_period_start_utc"] = current_period_start_iso
            features_for_db["symbol"] = symbol
            # TODO: 添加 data_source_references, feature_generated_at_utc
            logger.info(f"框架: 特徵計算完成。SMA20: {features_for_db.get('sma_20')}")
        else:
            logger.warning(f"歷史數據不足，無法為 {symbol} @ {current_period_start_iso} 計算特徵。")
            # 即使特徵不全，也可能需要存儲基礎的 OHLCV
            features_for_db.update(aligned_ohlcv_for_current_period) # 存儲基礎OHLCV
            features_for_db["timestamp_period_start_utc"] = current_period_start_iso
            features_for_db["symbol"] = symbol


        # --- 4. 數據庫寫入框架 (processed_features_hourly) ---
        if self.mode == "simulate" and features_for_db:
            logger.info("框架: 寫入 processed_features_hourly 到數據庫...")
            cols = ', '.join(features_for_db.keys())
            placeholders = ', '.join(['?'] * len(features_for_db))
            vals = list(features_for_db.values())
            # self.db_manager.execute_modification(f"INSERT OR REPLACE INTO processed_features_hourly ({cols}) VALUES ({placeholders})", vals)
        elif features_for_db:
            logger.info(f"[DRY-RUN] 應寫入 processed_features_hourly: {str(features_for_db)[:200]}...")


        # --- 5. AI 決策框架 ---
        if features_for_db: # 只有在有特徵的情況下才進行 AI 決策
            logger.info("框架: 生成市場簡報並調用 AI...")
            # qual_info_placeholder = {"news_summary": "今日無重大新聞 (placeholder)"}
            # briefing_json, llm_prompt = self.prompt_generator.generate_market_briefing_json_and_prompt(
            #     current_period_start_iso, symbol, features_for_db, qual_info_placeholder
            # )
            # llm_response = self.llama_agent.send_prompt_to_ollama(llm_prompt)
            # if llm_response:
            #     judgment_data = self.llama_agent.parse_llm_response_to_judgment_fields(llm_response)
            #     # TODO: 補充 judgment_data 的其他字段
            #     judgment_data["judgment_timestamp_utc"] = current_period_start_iso
            #     # (或 current_period_start_dt + interval，表示在週期結束時做判斷)
            #     judgment_data["market_briefing_json"] = briefing_json
            #     # ...

            #     if self.mode == "simulate":
            #         logger.info("框架: 寫入 ai_historical_judgments 到數據庫...")
            #         # self.db_manager.execute_modification(...)
            #     else:
            #         logger.info(f"[DRY-RUN] 應寫入 ai_historical_judgments: {str(judgment_data)[:200]}...")
            # else:
            #     logger.warning(f"AI 未能為 {symbol} @ {current_period_start_iso} 生成決策。")
        else:
            logger.info("框架: 無有效特徵，跳過 AI 決策。")

        logger.info(f"--- [CYCLE END] 完成處理週期: {symbol} @ {current_period_start_iso} ---")


    def run(self):
        """
        執行完整的回溯模擬。
        """
        logger.info(f"===== 開始歷史回溯模擬 (模式: {self.mode}) =====")

        current_t_for_period_start = self.sim_latest_dt_utc # current_t 代表我們要分析的週期的開始時間
        # 循環條件：當前週期的開始時間 >= 最早允許的週期開始時間
        # sim_earliest_dt_utc 是 YYYY-MM-DD 00:00:00 UTC

        loop_count = 0 # 防止無限循環的保險絲
        max_loops = 365 * (24 // self.interval_hours) * 5 # 假設最多回溯5年

        while current_t_for_period_start >= self.sim_earliest_dt_utc:
            if loop_count >= max_loops:
                logger.warning("已達到最大循環次數，提前終止模擬以防止無限循環。")
                break

            self.run_simulation_period(current_t_for_period_start, self.target_symbol)

            current_t_for_period_start -= timedelta(hours=self.interval_hours)
            loop_count += 1

            # (可選) 短暫休眠，避免過於密集的日誌或IO (主要用於調試)
            # time.sleep(0.1)

        logger.info(f"===== 歷史回溯模擬結束 (共執行 {loop_count} 個週期) =====")
        self.db_manager.close_connection()


def main():
    parser = argparse.ArgumentParser(description="AI 輔助歷史回溯與交易策略生成系統 - 主模擬程序")

    # 日期參數：YYYY-MM-DD 格式
    default_latest_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d") # 預設為昨天
    default_earliest_date = (datetime.now(timezone.utc) - timedelta(days=8)).strftime("%Y-%m-%d") # 預設回溯7天

    parser.add_argument("--sim-latest-date", type=str, default=default_latest_date,
                        help="模擬開始時的最新數據日期 (回溯的起點, YYYY-MM-DD UTC)。預設: 昨天。")
    parser.add_argument("--sim-earliest-date", type=str, default=default_earliest_date,
                        help="模擬希望回溯到的最早日期 (回溯的歷史終點, YYYY-MM-DD UTC)。預設: 7天前。")
    parser.add_argument("--target-symbol", type=str, default=DEFAULT_TARGET_SYMBOL_MAIN,
                        help=f"要分析的主要股票/產品代碼。預設: {DEFAULT_TARGET_SYMBOL_MAIN}")
    parser.add_argument("--interval-hours", type=int, default=DEFAULT_SIM_STEP_HOURS,
                        help=f"回溯的時間間隔（小時）。預設: {DEFAULT_SIM_STEP_HOURS}")
    parser.add_argument("--historical-context-days", type=int, default=DEFAULT_HISTORICAL_CONTEXT_DAYS,
                        help=f"獲取歷史上下文原始數據的天數窗口。預設: {DEFAULT_HISTORICAL_CONTEXT_DAYS}")
    parser.add_argument("--mode", choices=["simulate", "dry-run"], default=DEFAULT_SIM_MODE,
                        help=f"運行模式：'simulate' (實際執行並寫入數據庫) 或 'dry-run' (僅打印日誌不寫入)。預設: {DEFAULT_SIM_MODE}")
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_FILEPATH,
                        help=f"數據庫文件的完整路徑。預設: {DEFAULT_DB_FILEPATH}")
    parser.add_argument("--ollama-model", type=str, default=DEFAULT_OLLAMA_MODEL,
                        help=f"Ollama 中使用的 LLM 模型名稱。預設: {DEFAULT_OLLAMA_MODEL}")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO",
                        help="設置日誌級別。預設: INFO")

    args = parser.parse_args()

    # 根據命令行參數設置日誌級別
    log_level_map = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    setup_logger(PROJECT_LOGGER_NAME, level=log_level_map.get(args.log_level.upper(), 20))

    logger.info("--- 主模擬程序 (main_simulation.py) 啟動 ---")
    logger.info(f"參數: {args}")

    # (可選) 在開始前確保數據庫已初始化
    # 這部分也可以由 run_full_simulation.sh 管理
    if args.mode == "simulate": # 只有在 simulate 模式下才需要檢查/初始化生產數據庫
        from AI_Assisted_Historical_Backtesting.src.database.db_manager import initialize_database_from_schema
        schema_path = os.path.join(project_root_dir, "config", "schema.sql")
        if not os.path.exists(args.db_path) or not os.path.getsize(args.db_path) > 0 : # 如果數據庫不存在或為空
            logger.info(f"數據庫 {args.db_path} 不存在或為空，嘗試初始化...")
            if not initialize_database_from_schema(args.db_path, schema_path):
                logger.error("主模擬開始前數據庫初始化失敗，程序退出。")
                sys.exit(1)
            logger.info("主模擬開始前數據庫已成功初始化。")
        else:
             logger.info(f"數據庫 {args.db_path} 已存在。將在此基礎上運行模擬。")


    simulation = HistoricalSimulation(
        sim_latest_date_str=args.sim_latest_date,
        sim_earliest_date_str=args.sim_earliest_date,
        target_symbol=args.target_symbol,
        interval_hours=args.interval_hours,
        historical_context_days=args.historical_context_days,
        mode=args.mode,
        db_path=args.db_path,
        ollama_model=args.ollama_model
    )

    try:
        simulation.run()
    except ValueError as ve: # 捕獲初始化時的參數驗證錯誤
        logger.error(f"初始化 HistoricalSimulation 失敗: {ve}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"主模擬運行期間發生頂層錯誤: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("--- 主模擬程序 (main_simulation.py) 結束 ---")
        # db_manager.close_connection() 已在 simulation.run() 的 finally 中調用

if __name__ == "__main__":
    main()
