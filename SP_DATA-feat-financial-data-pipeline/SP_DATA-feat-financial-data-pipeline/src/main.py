import yaml
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import logging
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional # Ensure Dict, Any, Optional are imported

# --- Early Path Setup & Pre-Init Logger ---
# This initial logger is basic and will be superseded by the global logger from initialize_global_log.py
# once that module is successfully imported and initialized.
if not logging.getLogger().hasHandlers(): # Check if any handlers are configured on the root logger
    logging.basicConfig(
        level=logging.DEBUG, # Capture all levels initially
        format='%(asctime)s [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s (main-pre-init)',
        handlers=[logging.StreamHandler(sys.stdout)] # Log to console
    )
pre_init_logger = logging.getLogger("MainPreInit")

# PROJECT_ROOT should point to the 'src' directory's parent for atomic script.
# When run_prototype.sh executes 'python src/main.py', __file__ will be 'src/main.py'.
# So, Path(__file__).resolve().parent.parent gives the actual project root where run_prototype.sh is.
try:
    PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
except NameError: # Fallback if __file__ is not defined (e.g. interactive, though unlikely for atomic script)
    PROJECT_ROOT = str(Path(".").resolve())
    pre_init_logger.warning(f"__file__ not defined in main.py, PROJECT_ROOT set to CWD: {PROJECT_ROOT}")


# DETAILED_LOG_FILENAME will be created in the PROJECT_ROOT (where run_prototype.sh is)
# This specific log file is for the detailed transcript as requested by the user.
# The global logger (initialize_global_log) will create its own timestamped logs in api_test_logs/.
DETAILED_LOG_FILENAME = os.path.join(PROJECT_ROOT, "market_briefing_log.txt")


# Add project root to sys.path to allow imports like 'from connectors.base import ...'
# This assumes that 'connectors', 'engine', 'scripts' are directories directly under PROJECT_ROOT/src/
# For the atomic script, main.py is in src/, so PROJECT_ROOT/src is effectively the "source root".
# To import 'from connectors.base...', 'PROJECT_ROOT/src' must be in sys.path.
# Path(__file__).resolve().parent gives the 'src' directory.
SOURCE_ROOT = str(Path(__file__).resolve().parent)
if SOURCE_ROOT not in sys.path:
    sys.path.insert(0, SOURCE_ROOT)
    pre_init_logger.info(f"Inserted SOURCE_ROOT ({SOURCE_ROOT}) into sys.path for relative imports.")

pre_init_logger.info(f"main.py: __file__ is {Path(__file__).resolve() if '__file__' in locals() else 'not_defined'}")
pre_init_logger.info(f"main.py: PROJECT_ROOT (parent of src): {PROJECT_ROOT}")
pre_init_logger.info(f"main.py: SOURCE_ROOT (src directory): {SOURCE_ROOT}")
pre_init_logger.info(f"main.py: sys.path for module import: {sys.path}")


# --- Module Imports ---
# These imports now assume that 'connectors', 'engine', 'scripts' are packages within 'src'.
# The 'src' directory itself is made available by SOURCE_ROOT in sys.path.
global_log = None
init_global_log_function = None
global_log_file_path_imported = None
get_taipei_time_func_imported = None

try:
    # Corrected imports based on the new structure where these are submodules of 'src'
    # from connectors.base import BaseConnector # 舊的 BaseConnector 不再直接使用
    # from connectors.fred_connector import FredConnector # 改由 DataMaster 處理
    # from connectors.nyfed_connector import NYFedConnector # 改由 DataMaster 處理
    # from connectors.yfinance_connector import YFinanceConnector # 改由 DataMaster 處理
    from database.database_manager import DatabaseManager
    from engine.indicator_engine import IndicatorEngine
    from data_master import DataMaster # 引入 DataMaster

    from scripts.initialize_global_log import log_message, get_taipei_time, LOG_FILE_PATH as GLOBAL_LOG_FILE_PATH_FROM_MODULE, initialize_log_file

    global_log = log_message
    init_global_log_function = initialize_log_file
    global_log_file_path_imported = GLOBAL_LOG_FILE_PATH_FROM_MODULE
    get_taipei_time_func_imported = get_taipei_time

    if init_global_log_function is not None:
        try:
            log_dir_for_global_logger = Path(PROJECT_ROOT) / "api_test_logs"
            actual_log_file = init_global_log_function(
                log_dir_override=str(log_dir_for_global_logger),
                force_reinit=True,
                project_root_path=Path(PROJECT_ROOT)
            )
            if actual_log_file:
                global_log(f"main.py: Global application logger initialized. Log file: {actual_log_file}", "INFO", logger_name="MainApp.Setup")
            else:
                global_log("main.py: Global logger init returned no path.", "ERROR", logger_name="MainApp.Setup")
        except Exception as e_log_init_main:
            pre_init_logger.error(f"main.py: Failed to init global logger: {e_log_init_main}", exc_info=True)
            if global_log is None:
                 global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback) {msg}")
            global_log("main.py: Using fallback logger.", "WARNING", logger_name="MainApp.Setup")
    else:
        pre_init_logger.error("main.py: initialize_global_log_file not imported.")
        if global_log is None:
            global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback_no_init) {msg}")

except ImportError as e_imp:
    pre_init_logger.error(f"Failed to import modules: {e_imp}. sys.path: {sys.path}", exc_info=True)
    print(f"CRITICAL IMPORT ERROR (main.py): {e_imp}.")
    sys.exit(1)
except Exception as e_general_imp:
    pre_init_logger.error(f"General error during imports: {e_general_imp}", exc_info=True)
    print(f"CRITICAL GENERAL IMPORT ERROR (main.py): {e_general_imp}.")
    sys.exit(1)

def load_config(config_path_relative_to_project_root="config.yaml") -> Dict[str, Any]: # 修改預設路徑
    """載入位於專案根目錄的 YAML 設定檔。"""
    full_config_path = Path(PROJECT_ROOT) / config_path_relative_to_project_root
    global_log(f"Loading project configuration from: {full_config_path}", "INFO", logger_name="MainApp.ConfigLoader")
    try:
        with open(full_config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        global_log(f"Project configuration loaded successfully from {full_config_path}.", "INFO", logger_name="MainApp.ConfigLoader")
        if not isinstance(config_data, dict):
            global_log(f"Config file {full_config_path} did not load as a dictionary.", "ERROR", logger_name="MainApp.ConfigLoader")
            raise ValueError(f"Configuration file {full_config_path} is not a valid YAML dictionary.")
        return config_data
    except FileNotFoundError:
        global_log(f"Config file not found: {full_config_path}. Using default or empty config.", "WARNING", logger_name="MainApp.ConfigLoader")
        # 返回一個最小化的預設設定，以允許 DataMaster 初始化 (雖然可能沒有 API)
        return {"apis": {}}
    except Exception as e_conf:
        global_log(f"Error loading or parsing config from {full_config_path}: {e_conf}", "CRITICAL", logger_name="MainApp.ConfigLoader", exc_info=True)
        raise

def main():
    detailed_run_log_handler = None
    try:
        detailed_run_log_handler = logging.FileHandler(DETAILED_LOG_FILENAME, mode='w', encoding='utf-8')
        detailed_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s')
        detailed_run_log_handler.setFormatter(detailed_formatter)
        detailed_run_log_handler.setLevel(logging.DEBUG)
        root_logger_for_detailed = logging.getLogger()
        root_logger_for_detailed.addHandler(detailed_run_log_handler)
        global_log(f"Detailed execution transcript will ALSO be saved to: {DETAILED_LOG_FILENAME}", "INFO", logger_name="MainApp.Setup")
    except Exception as e_detail_log:
        err_msg = f"Failed to set up detailed run log at {DETAILED_LOG_FILENAME}: {e_detail_log}"
        if global_log: global_log(err_msg, "ERROR", logger_name="MainApp.Setup", exc_info=True)
        else: pre_init_logger.error(err_msg, exc_info=True)

    global_log("--- 開始執行端到端金融數據處理原型 (DataMaster整合版) ---", "INFO", logger_name="MainApp.main_flow")

    config: Dict[str, Any] = {}
    db_manager = None # 初始化為 None
    try:
        # 載入設定檔 (config.yaml 應位於專案根目錄)
        config = load_config(config_path_relative_to_project_root="config.yaml")

        # 如果 load_config 因 FileNotFoundError 返回了預設空 config，這裡需要處理
        if not config or not config.get("apis"):
            global_log("Config.yaml 未找到或 'apis' 部分為空。DataMaster 可能無法運作。", "ERROR", logger_name="MainApp.Config")
            # 根據需求，這裡可以選擇退出或繼續 (DataMaster 會處理空 apis 設定)
            # return # 如果設定檔是必需的，則退出

        # 初始化 DataMaster (config.yaml 也應位於專案根目錄)
        data_master = DataMaster(config_path=str(Path(PROJECT_ROOT) / "config.yaml"))
        global_log("DataMaster 初始化完成。", "INFO", logger_name="MainApp.Setup")


        start_date_cfg = config.get('data_fetch_range', {}).get('start_date', "2020-01-01")
        end_date_cfg = config.get('data_fetch_range', {}).get('end_date')

        current_date_for_end_calc = ""
        try:
            current_date_for_end_calc = get_taipei_time_func_imported().strftime('%Y-%m-%d') if get_taipei_time_func_imported else datetime.now(timezone.utc).strftime('%Y-%m-%d')
        except Exception as e_time_local:
            current_date_for_end_calc = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            global_log(f"Using UTC for 'today's date' as get_taipei_time failed: {e_time_local}", "WARNING", logger_name="MainApp.main_flow")

        end_date_to_use = end_date_cfg if end_date_cfg else current_date_for_end_calc
        global_log(f"Data fetch range: Start='{start_date_cfg}', End='{end_date_to_use}'.", "INFO", logger_name="MainApp.main_flow")

        # API 金鑰現在由 DataMaster 內部的 Connector 從 config.yaml 讀取
        # 不再需要在 main.py 中手動設定環境變數 (除非 Connector 設計為依賴環境變數且 config 中未提供)
        # fred_api_key_env_name = config.get('api_endpoints', {}).get('fred', {}).get('api_key_env', 'FRED_API_KEY')
        # user_provided_fred_key = "78ea51fb13b546d89f1a683cb4ba26f5"
        # os.environ[fred_api_key_env_name] = user_provided_fred_key
        # global_log(f"FRED API Key handling is now managed by FredConnector via config.yaml.", "DEBUG", logger_name="MainApp.main_flow")

        db_logger = logging.getLogger("project_logger.DatabaseManager")
        engine_logger = logging.getLogger("project_logger.IndicatorEngine")

        db_manager = DatabaseManager(config, logger_instance=db_logger, project_root_dir=PROJECT_ROOT)
        db_manager.connect()

        data_fetch_status = {'fred': False, 'nyfed': False, 'yfinance_move': False, 'finmind_price': False, 'finmind_chips': False}
        macro_unique_cols = ['metric_date', 'security_id', 'source_api'] # security_id for FRED series_id
        stock_unique_cols = ['price_date', 'security_id', 'source_api']
        chip_unique_cols = ["transaction_date", "security_id", "metric_name", "metric_sub_category", "source_api"]


        global_log("\n--- 階段 1: 數據獲取 (使用 DataMaster) ---", "INFO", logger_name="MainApp.main_flow")

        # --- FRED Data Fetching via DataMaster ---
        fred_series_ids = config.get('target_metrics', {}).get('fred_series_ids', [])
        if fred_series_ids:
            fred_data_df = data_master.get_fred_series_data(series_ids=fred_series_ids, start_date=start_date_cfg, end_date=end_date_to_use)
            if not fred_data_df.empty:
                global_log(f"DataMaster: Fetched {len(fred_data_df)} FRED records.", "INFO", logger_name="MainApp.main_flow")
                db_manager.bulk_insert_or_replace('fact_macro_economic_data', fred_data_df, unique_cols=macro_unique_cols)
                data_fetch_status['fred'] = True
            else:
                global_log("DataMaster: FRED Connector returned no data or an empty DataFrame.", "WARNING", logger_name="MainApp.main_flow")
        else:
            global_log("DataMaster: No FRED series IDs configured. Skipping FRED data fetch.", "INFO", logger_name="MainApp.main_flow")


        # --- NYFed Data Fetching via DataMaster ---
        # NYFedConnector.get_configured_data() 通常不需要額外參數
        nyfed_data_df = data_master.get_nyfed_data()
        if not nyfed_data_df.empty:
            global_log(f"DataMaster: Fetched {len(nyfed_data_df)} NYFed records.", "INFO", logger_name="MainApp.main_flow")
            # NYFed 的 'metric_name' 在 Connector 中已設定為類似 'NYFED/PRIMARY_DEALER_NET_POSITION'
            # 'security_id' 也被設定為此 metric_name。這符合 fact_macro_economic_data 的 schema。
            db_manager.bulk_insert_or_replace('fact_macro_economic_data', nyfed_data_df, unique_cols=macro_unique_cols)
            data_fetch_status['nyfed'] = True
        else:
            global_log("DataMaster: NYFed Connector returned no data or an empty DataFrame.", "WARNING", logger_name="MainApp.main_flow")

        # --- YFinance Data Fetching via DataMaster ---
        yfinance_tickers_list = config.get('target_metrics', {}).get('yfinance_tickers', [])
        if yfinance_tickers_list:
            # 注意：DataMaster 的 get_historical_price 簽名是 (symbol, start, end, interval)
            # 我們需要遍歷 tickers
            all_yf_data = []
            for ticker in yfinance_tickers_list:
                yf_ticker_df = data_master.get_historical_price(symbol=ticker, start_date=start_date_cfg, end_date=end_date_to_use)
                if not yf_ticker_df.empty:
                    all_yf_data.append(yf_ticker_df)

            if all_yf_data:
                yf_data_df_combined = pd.concat(all_yf_data, ignore_index=True)
                global_log(f"DataMaster: Fetched {len(yf_data_df_combined)} YFinance records for {yfinance_tickers_list}.", "INFO", logger_name="MainApp.main_flow")
                db_manager.bulk_insert_or_replace('fact_stock_price', yf_data_df_combined, unique_cols=stock_unique_cols)
                data_fetch_status['yfinance_move'] = True # 假設 yfinance_tickers 包含 ^MOVE
            else:
                global_log(f"DataMaster: YFinance Connector returned no data for {yfinance_tickers_list}.", "WARNING", logger_name="MainApp.main_flow")
        else:
            global_log("DataMaster: No YFinance tickers configured. Skipping YFinance data fetch.", "INFO", logger_name="MainApp.main_flow")

        # --- FinMind Data Fetching (範例：股價和籌碼) ---
        finmind_stock_ids = config.get('target_metrics', {}).get('finmind_stock_ids', []) # 例如 ["2330.TW", "0050.TW"]
        if finmind_stock_ids:
            all_fm_stock_price_data = []
            for fm_stock_id in finmind_stock_ids:
                # 假設 DataMaster 有一個 get_finmind_stock_price 或通用 get_historical_price 能調到 FinMind
                # 這裡使用通用的 get_historical_price，DataMaster 內部會根據優先級選擇 FinMind (如果設定正確)
                fm_price_df = data_master.get_historical_price(symbol=fm_stock_id, start_date=start_date_cfg, end_date=end_date_to_use)
                if not fm_price_df.empty:
                    all_fm_stock_price_data.append(fm_price_df)

            if all_fm_stock_price_data:
                fm_price_combined_df = pd.concat(all_fm_stock_price_data, ignore_index=True)
                global_log(f"DataMaster: Fetched {len(fm_price_combined_df)} FinMind stock price records.", "INFO")
                db_manager.bulk_insert_or_replace('fact_stock_price', fm_price_combined_df, unique_cols=stock_unique_cols)
                data_fetch_status['finmind_price'] = True
            else:
                global_log("DataMaster: FinMind Connector returned no stock price data.", "WARNING")

            all_fm_chip_data = []
            chip_categories_to_fetch = ["institutional_trades", "margin_trading"] # 可以從 config 讀取
            for fm_stock_id in finmind_stock_ids:
                for category in chip_categories_to_fetch:
                    fm_chip_df = data_master.get_chip_data(symbol=fm_stock_id, data_category=category, start_date=start_date_cfg, end_date=end_date_to_use)
                    if not fm_chip_df.empty:
                        all_fm_chip_data.append(fm_chip_df)

            if all_fm_chip_data:
                fm_chip_combined_df = pd.concat(all_fm_chip_data, ignore_index=True)
                global_log(f"DataMaster: Fetched {len(fm_chip_combined_df)} FinMind chip data records.", "INFO")
                db_manager.bulk_insert_or_replace('fact_tw_chip_data', fm_chip_combined_df, unique_cols=chip_unique_cols)
                data_fetch_status['finmind_chips'] = True
            else:
                global_log("DataMaster: FinMind Connector returned no chip data.", "WARNING")
        else:
            global_log("DataMaster: No FinMind stock IDs configured. Skipping FinMind data fetch.", "INFO")


        global_log("\n--- 階段 2 & 3: 指標計算與市場簡報 (與原邏輯相似) ---", "INFO", logger_name="MainApp.main_flow")

        # Fetch data from DB for IndicatorEngine
        current_macro_data_for_engine = db_manager.fetch_all_for_engine('fact_macro_economic_data', start_date_cfg, end_date_to_use, date_column='metric_date')
        current_stock_data_for_engine = db_manager.fetch_all_for_engine('fact_stock_price', start_date_cfg, end_date_to_use, date_column='price_date')

        if (current_macro_data_for_engine is None or current_macro_data_for_engine.empty) and            (current_stock_data_for_engine is None or current_stock_data_for_engine.empty):
            global_log("IndicatorEngine: Insufficient data from DB (both macro and stock are empty/None). Skipping stress index calculation.", "ERROR", logger_name="MainApp.main_flow")
        else:
            # Ensure DataFrames are not None before use, default to empty if None
            current_macro_data_for_engine = current_macro_data_for_engine if current_macro_data_for_engine is not None else pd.DataFrame()
            current_stock_data_for_engine = current_stock_data_for_engine if current_stock_data_for_engine is not None else pd.DataFrame()

            move_data_for_engine = pd.DataFrame()
            if not current_stock_data_for_engine.empty and 'security_id' in current_stock_data_for_engine.columns:
                move_data_for_engine = current_stock_data_for_engine[current_stock_data_for_engine['security_id'] == '^MOVE']

            if move_data_for_engine.empty:
                global_log("IndicatorEngine: ^MOVE data not found in DB stock data or stock data was empty.", "WARNING", logger_name="MainApp.main_flow")

            engine_input_data = {'macro': current_macro_data_for_engine, 'move': move_data_for_engine}
            engine_params_from_config = config.get('indicator_engine_params', {})

            indicator_engine_instance = IndicatorEngine(engine_input_data, params=engine_params_from_config, logger_instance=engine_logger)
            stress_index_df = indicator_engine_instance.calculate_dealer_stress_index()

            if stress_index_df is None or stress_index_df.empty:
                global_log("Dealer Stress Index calculation resulted in no data or all NaN values.", "ERROR", logger_name="MainApp.main_flow")
            else:
                global_log(f"Dealer Stress Index calculated. Shape: {stress_index_df.shape}. Latest date: {stress_index_df.index[-1].strftime('%Y-%m-%d') if not stress_index_df.empty else 'N/A'}", "INFO", logger_name="MainApp.main_flow")
                global_log(f"Stress Index Tail:\n{stress_index_df.tail().to_string()}", "INFO", logger_name="MainApp.main_flow")

                # Market Briefing Generation
                briefing_date = stress_index_df.index[-1]
                briefing_date_str = briefing_date.strftime('%Y-%m-%d')
                latest_stress_value = stress_index_df['DealerStressIndex'].iloc[-1]

                stress_level_desc = "N/A"
                if pd.notna(latest_stress_value):
                    threshold_moderate = engine_params_from_config.get('stress_threshold_moderate', 40)
                    threshold_high = engine_params_from_config.get('stress_threshold_high', 60)
                    threshold_extreme = engine_params_from_config.get('stress_threshold_extreme', 80)
                    if latest_stress_value >= threshold_extreme: stress_level_desc = f"{latest_stress_value:.2f} (極度緊張)"
                    elif latest_stress_value >= threshold_high: stress_level_desc = f"{latest_stress_value:.2f} (高度緊張)"
                    elif latest_stress_value >= threshold_moderate: stress_level_desc = f"{latest_stress_value:.2f} (中度緊張)"
                    else: stress_level_desc = f"{latest_stress_value:.2f} (正常)"

                stress_trend_desc = "N/A"
                if len(stress_index_df['DealerStressIndex'].dropna()) >= 2: # Need at least two points for diff
                    change_in_stress = stress_index_df['DealerStressIndex'].diff().iloc[-1]
                    if pd.notna(change_in_stress):
                        stress_trend_desc = "上升" if change_in_stress > 0.1 else ("下降" if change_in_stress < -0.1 else "穩定")

                # Accessing prepared data from the engine for briefing components
                engine_prepared_full_df = indicator_engine_instance.df_prepared
                latest_briefing_components_data = None
                if engine_prepared_full_df is not None and not engine_prepared_full_df.empty:
                    if briefing_date in engine_prepared_full_df.index:
                        latest_briefing_components_data = engine_prepared_full_df.loc[briefing_date]
                    else: # Fallback if exact date match fails (e.g. different time components)
                        try:
                           latest_briefing_components_data = engine_prepared_full_df.loc[briefing_date_str] # Try matching by string date
                        except KeyError:
                           global_log(f"Could not find briefing_date {briefing_date_str} or {briefing_date} in engine_prepared_df. Using last available row for briefing components.", "WARNING", logger_name="MainApp.Briefing")
                           if not engine_prepared_full_df.empty: latest_briefing_components_data = engine_prepared_full_df.iloc[-1]

                def get_formatted_value(series_data, component_key, value_format="{:.2f}", not_available_str="N/A"):
                    if series_data is not None and component_key in series_data.index and pd.notna(series_data[component_key]):
                        val = series_data[component_key]
                        try:
                            return value_format.format(val) if isinstance(val, (int, float)) and pd.notna(val) else str(val)
                        except (ValueError, TypeError): # Handle cases where format might not apply
                            return str(val)
                    return not_available_str

                move_value_str = get_formatted_value(latest_briefing_components_data, '^MOVE')
                spread_10y2y_raw = latest_briefing_components_data['spread_10y2y'] if latest_briefing_components_data is not None and 'spread_10y2y' in latest_briefing_components_data else None
                spread_10y2y_str = f"{(spread_10y2y_raw * 100):.2f} bps" if pd.notna(spread_10y2y_raw) else "N/A"
                primary_dealer_pos_str = get_formatted_value(latest_briefing_components_data, 'NYFED/PRIMARY_DEALER_NET_POSITION', value_format="{:,.0f}") # Changed fmt to value_format
                vix_value_str = get_formatted_value(latest_briefing_components_data, 'FRED/VIXCLS')
                sofr_dev_str = get_formatted_value(latest_briefing_components_data, 'FRED/SOFR_Dev') # Assuming SOFR_Dev is already a deviation value

                market_briefing_output = {
                    "briefing_date": briefing_date_str,
                    "data_window_end_date": briefing_date_str, # Or actual end_date_to_use if different
                    "dealer_stress_index": {"current_value_description": stress_level_desc, "trend_approximation": stress_trend_desc},
                    "key_financial_components_latest": [
                        {"component_name": "MOVE Index (Bond Mkt Volatility)", "value_string": move_value_str},
                        {"component_name": "10Y-2Y Treasury Spread", "value_string": spread_10y2y_str},
                        {"component_name": "Primary Dealer Net Positions (Millions USD)", "value_string": primary_dealer_pos_str}
                    ],
                    "broader_market_context_latest": {
                        "vix_index (Equity Mkt Volatility)": vix_value_str,
                        "sofr_deviation_from_ma": sofr_dev_str
                    },
                    "summary_narrative": (
                        f"市場壓力指數 ({briefing_date_str}): {stress_level_desc}. "
                        f"主要影響因素包括債券市場波動率 (MOVE Index: {move_value_str}) 及 "
                        f"10年期與2年期公債利差 ({spread_10y2y_str}). "
                        f"一級交易商淨持倉部位為 {primary_dealer_pos_str} 百萬美元。"
                    )
                }
                global_log("\n--- 市場簡報 (Market Briefing - JSON) ---", "INFO", logger_name="MainApp.Briefing")
                # Print to console for run_prototype.sh to capture
                print("\n--- 市場簡報 (Market Briefing - JSON) ---")
                print(json.dumps(market_briefing_output, indent=2, ensure_ascii=False))
                # Also log it to the file
                global_log(json.dumps(market_briefing_output, indent=2, ensure_ascii=False), "INFO", logger_name="MainApp.BriefingOutput")

    except FileNotFoundError as e_fnf: # Specifically for config loading
        err_msg_fnf = f"CRITICAL FAILURE: Configuration file not found: {e_fnf}. Application cannot start."
        print(err_msg_fnf) # Print to console as logger might not be fully up
        if global_log: global_log(err_msg_fnf, "CRITICAL", logger_name="MainApp.main_flow", exc_info=False)
        else: pre_init_logger.critical(err_msg_fnf, exc_info=False)
    except Exception as e_main_runtime:
        err_msg_runtime = f"主流程 main() 發生嚴重執行期錯誤: {e_main_runtime}"
        print(err_msg_runtime)
        if global_log: global_log(err_msg_runtime, "CRITICAL", logger_name="MainApp.main_flow", exc_info=True)
        else: pre_init_logger.critical(err_msg_runtime, exc_info=True)
    finally:
        # --- Database Disconnection (Commented out) ---
        # if 'db_manager' in locals() and hasattr(db_manager, 'conn') and db_manager.conn is not None:
        #     if not db_manager.conn.isclosed(): db_manager.disconnect()
        # else:
        # --- Database Disconnection ---
        if 'db_manager' in locals() and db_manager is not None: # Check if db_manager was instantiated
            db_manager.disconnect()
        else:
            global_log("DB Manager was not instantiated, skipping disconnect.", "DEBUG", logger_name="MainApp.main_flow")

        global_log("\n--- 端到端原型執行完畢 (Atomic Script Version) ---", "INFO", logger_name="MainApp.main_flow")

        # --- Clean up detailed file logger (market_briefing_log.txt) ---
        if detailed_run_log_handler is not None and 'root_logger_for_detailed' in locals(): # Ensure handler was created
            global_log(f"Removing detailed run log handler. Transcript saved to {DETAILED_LOG_FILENAME}", "INFO", logger_name="MainApp.Cleanup")
            if hasattr(locals().get('root_logger_for_detailed'), 'removeHandler'): # Check if logger has removeHandler
                 root_logger_for_detailed.removeHandler(detailed_run_log_handler)
            detailed_run_log_handler.close()

if __name__ == "__main__":
    # This initial global_log check is for the very unlikely case that imports failed so badly
    # that global_log wasn't even assigned its fallback lambda.
    if global_log is None:
        pre_init_logger.critical("global_log function was not assigned its fallback. Logging will be severely limited.")
        # Define an ultra-fallback if pre_init_logger itself is somehow problematic (highly unlikely)
        global_log = lambda msg, level="INFO", **kwargs: print(f"ULTRA_FALLBACK_LOG [{level.upper()}] {msg}")

    # The global application logger (timestamped file in api_test_logs) should have been initialized
    # during the import phase. If not, log_message will use the bootstrap logger.
    # The detailed_run_log_handler (for market_briefing_log.txt) is set up inside main().

    # A final check on global logger initialization path from the module.
    if global_log_file_path_imported:
        global_log(f"Confirmed global application log file from module: {global_log_file_path_imported}", "DEBUG", logger_name="MainApp.InitCheck")
    else:
        global_log("Global application log file path from module was not set. Bootstrap logger might be active for app logs.", "WARNING", logger_name="MainApp.InitCheck")

    main()
