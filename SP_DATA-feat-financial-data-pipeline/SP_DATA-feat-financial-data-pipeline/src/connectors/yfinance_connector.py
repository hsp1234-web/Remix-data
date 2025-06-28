import yfinance as yf
import pandas as pd
import time
import logging
from datetime import datetime, timezone, date # Added date
import random # For jitter in backoff
from typing import List, Dict, Any, Tuple, Optional
import requests # For potential future session use, not strictly for yfinance Ticker
import os # For caching directory

# Use a module-level logger
logger = logging.getLogger(__name__)

# Define a conservative request interval for yfinance if not specified by RPM
# This is a guideline; yfinance itself doesn't publish hard limits.
_MIN_REQUEST_INTERVAL_YFINANCE = 0.5 # yfinance 的隱含速率限制，保守估計每秒最多2次請求

class YFinanceConnector:
    """
    使用 yfinance 獲取股價和指數數據的連接器。
    包含讀取設定檔、速率控制、本地快取、指數退避重試機制。
    """

    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 YFinanceConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典。
                                         yfinance 通常不需要 API 金鑰，但可以包含速率和快取設定。
                                         例如:
                                         {
                                             "requests_per_minute": 100,
                                             "cache_enabled": true,
                                             "cache_directory": "data/cache/yfinance", # Template uses "apis.yfinance.cache_dir"
                                             "cache_expire_after_seconds": 3600
                                         }
        """
        self.requests_per_minute = api_config.get("requests_per_minute", 100)
        self._last_request_time = 0

        rpm_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0
        self._min_request_interval = max(rpm_interval, _MIN_REQUEST_INTERVAL_YFINANCE)

        self.source_api_name = "yfinance"

        self.cache_enabled = api_config.get("cache_enabled", True) # Default to True if not specified
        # Use a more specific cache directory path from the config if available
        self.cache_dir = api_config.get("cache_directory", os.path.join("data", "cache", self.source_api_name))

        self.cache_expire_after_seconds = api_config.get("cache_expire_after_seconds", 3600)

        if self.cache_enabled:
            # Ensure the specific cache directory for yfinance exists
            if not os.path.exists(self.cache_dir):
                try:
                    os.makedirs(self.cache_dir, exist_ok=True)
                    logger.info(f"YFinanceConnector: 已建立快取目錄 {self.cache_dir}")
                except OSError as e:
                    logger.error(f"YFinanceConnector: 無法建立快取目錄 {self.cache_dir}: {e}。將禁用快取。")
                    self.cache_enabled = False
            elif not os.access(self.cache_dir, os.W_OK): # Check if directory exists but is not writable
                    logger.error(f"YFinanceConnector: 快取目錄 {self.cache_dir} 不可寫。將禁用快取。")
                    self.cache_enabled = False

        logger.info(f"YFinanceConnector 初始化完成。RPM Config: {self.requests_per_minute}, Effective Interval: {self._min_request_interval:.2f}s, Cache: {'啟用' if self.cache_enabled else '禁用'} at '{self.cache_dir}'")

    def _get_cache_filepath(self, ticker_symbol: str, start_date: str, end_date: Optional[str], interval: str) -> str:
        """產生快取檔案的路徑。"""
        # 標準化日期字串以避免因格式不同導致快取失效
        start_date_norm = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d') if end_date else "latest"

        # 清理 ticker_symbol 中的特殊字符，使其適用於檔名
        safe_ticker_symbol = "".join(c if c.isalnum() or c in ['.', '-', '^'] else '_' for c in ticker_symbol) # Allow ^ for indices

        filename = f"{safe_ticker_symbol}_{start_date_norm}_{end_date_str}_{interval}.parquet"
        return os.path.join(self.cache_dir, filename)

    def _read_from_cache(self, filepath: str) -> Optional[pd.DataFrame]:
        """從快取檔案讀取 DataFrame。"""
        if not self.cache_enabled or not os.path.exists(filepath):
            return None

        try:
            # 檢查檔案修改時間是否過期
            file_mod_time = os.path.getmtime(filepath)
            if (time.time() - file_mod_time) > self.cache_expire_after_seconds:
                logger.info(f"YFinance 快取檔案 {filepath} 已過期 (超過 {self.cache_expire_after_seconds} 秒)，將重新獲取。")
                try:
                    os.remove(filepath)
                except OSError as e_rem:
                    logger.error(f"YFinanceConnector: 無法刪除過期的快取檔案 {filepath}: {e_rem}")
                return None

            df = pd.read_parquet(filepath)
            logger.info(f"YFinanceConnector: 從快取成功讀取 {filepath}。")
            return df
        except Exception as e: # Catch broad exceptions like pyarrow.lib.ArrowInvalid
            logger.error(f"YFinanceConnector: 從快取讀取 {filepath} 失敗 (可能檔案損壞): {e}。將嘗試重新獲取。")
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except OSError as ose:
                    logger.error(f"YFinanceConnector: 無法刪除損壞的快取檔案 {filepath}: {ose}")
            return None

    def _write_to_cache(self, df: pd.DataFrame, filepath: str):
        """將 DataFrame 寫入快取檔案。"""
        if not self.cache_enabled or df.empty:
            if df.empty: logger.debug(f"YFinanceConnector: 不快取空的 DataFrame for {filepath}")
            return
        try:
            # 確保目標目錄存在 (雖然 init 時已檢查，但多一層防護)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            df.to_parquet(filepath, index=False)
            logger.info(f"YFinanceConnector: 已成功寫入快取 {filepath}。")
        except Exception as e:
            logger.error(f"YFinanceConnector: 寫入快取 {filepath} 失敗: {e}。")

    def _wait_for_rate_limit(self):
        """等待直到可以安全地發出下一個 API 請求。"""
        if self._min_request_interval == 0: return
        now = time.time()
        elapsed_time = now - self._last_request_time
        wait_time = self._min_request_interval - elapsed_time
        if wait_time > 0:
            logger.debug(f"yfinance 速率控制：等待 {wait_time:.2f} 秒。")
            time.sleep(wait_time)
        # self._last_request_time = time.time() # This should be updated AFTER the request is made or just before

    def get_historical_price(self, tickers: List[str], start_date: str, end_date: Optional[str] = None,
                             interval: str = "1d", max_retries: int = 3, initial_backoff: float = 2.0) -> pd.DataFrame:
        """
        獲取指定股票列表的歷史價格數據。
        包含速率控制、本地快取、指數退避重試。
        返回空 DataFrame 表示失敗或無數據。
        """
        logger.info(f"yfinance: 請求股票列表 {tickers} 從 {start_date} 到 {end_date} (間隔: {interval})。")

        if not tickers:
            logger.warning("yfinance: 未提供股票代碼。")
            return self._create_empty_standard_df()

        all_ticker_data_list = []

        for ticker_symbol in tickers:
            # 標準化日期參數以用於快取鍵
            norm_start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
            # yfinance end_date is exclusive for daily intervals.
            # If user provides '2023-12-15', yf fetches up to '2023-12-14'.
            # To include '2023-12-15', end_date for yf should be '2023-12-16'.
            # We will assume the user means inclusive end_date, so adjust if interval is daily.
            if end_date:
                user_end_date_obj = pd.to_datetime(end_date)
                if interval == "1d": # For daily, yfinance is exclusive
                    yf_end_date_obj = user_end_date_obj + pd.Timedelta(days=1)
                    norm_end_date_for_api = yf_end_date_obj.strftime('%Y-%m-%d')
                else: # For intraday, yfinance is usually inclusive
                    norm_end_date_for_api = user_end_date_obj.strftime('%Y-%m-%d')
                norm_end_date_for_cache = user_end_date_obj.strftime('%Y-%m-%d') # Cache key uses user's end_date
            else: # If no end_date, yfinance fetches up to the latest
                norm_end_date_for_api = None
                norm_end_date_for_cache = "latest"


            cache_filepath = self._get_cache_filepath(ticker_symbol, norm_start_date, norm_end_date_for_cache, interval)
            cached_df = self._read_from_cache(cache_filepath)
            if cached_df is not None:
                all_ticker_data_list.append(cached_df)
                logger.debug(f"yfinance: {ticker_symbol} 數據從快取載入。")
                continue

            # --- 如果未命中快取，則進行 API 調用 ---
            self._wait_for_rate_limit()
            self._last_request_time = time.time() # 更新時間戳為即將發送請求的時間

            current_retries = 0
            success_for_this_ticker = False
            while current_retries <= max_retries:
                try:
                    logger.debug(f"yfinance: 嘗試獲取 {ticker_symbol} (嘗試 {current_retries + 1}/{max_retries + 1})")

                    # yfinance Ticker.history() 可能會因網路問題或 Yahoo Finance 限制而失敗
                    # 增加 try-except 塊來捕獲這些潛在錯誤
                    ticker_obj = yf.Ticker(ticker_symbol)
                    hist_df = ticker_obj.history(
                        start=norm_start_date,
                        end=norm_end_date_for_api,
                        interval=interval,
                        auto_adjust=False, # 獲取未調整價格和調整後價格
                        actions=True       # 獲取股息和拆股數據
                    )

                    if hist_df.empty:
                        # Check if it's due to ticker not found vs. no data in range
                        # yfinance doesn't easily distinguish "ticker not found" from "no data in range"
                        # A common pattern for non-existent tickers is an empty df with no error.
                        # We can try a very short, recent period for the ticker to see if it has *any* info.
                        # This adds an extra call, so use cautiously or rely on other validation.
                        # For now, assume empty means no data for the period or invalid ticker.
                        logger.info(f"yfinance: {ticker_symbol} 在 {norm_start_date}-{norm_end_date_for_cache} (間隔 {interval}) 無數據 (或股票不存在)。")
                        success_for_this_ticker = True
                        break

                    hist_df.reset_index(inplace=True)

                    date_col_name = 'Datetime' if 'Datetime' in hist_df.columns else 'Date'
                    if date_col_name not in hist_df.columns:
                        logger.error(f"yfinance: {ticker_symbol} 數據中未找到日期欄位。欄位: {hist_df.columns.tolist()}")
                        break # 數據格式錯誤，不重試

                    rename_map = {
                        date_col_name: 'price_date', 'Open': 'open_price', 'High': 'high_price',
                        'Low': 'low_price', 'Close': 'close_price', 'Adj Close': 'adj_close_price',
                        'Volume': 'volume', 'Dividends': 'dividends', 'Stock Splits': 'stock_splits'
                    }
                    current_rename_map = {k: v for k, v in rename_map.items() if k in hist_df.columns}
                    df_renamed = hist_df.rename(columns=current_rename_map)

                    df_renamed['price_date'] = pd.to_datetime(df_renamed['price_date']).dt.tz_localize(None).dt.normalize().dt.date
                    df_renamed['security_id'] = ticker_symbol
                    df_renamed['source_api'] = self.source_api_name
                    df_renamed['last_updated_timestamp'] = datetime.now(timezone.utc)

                    final_cols_spec = self._get_standard_columns()
                    for fc_col in final_cols_spec:
                        if fc_col not in df_renamed.columns: # 填充缺失的標準欄位
                            df_renamed[fc_col] = 0.0 if fc_col in ['dividends', 'stock_splits'] else pd.NA

                    numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'dividends', 'stock_splits']
                    for col_to_num in numeric_cols:
                        if col_to_num in df_renamed.columns:
                            df_renamed[col_to_num] = pd.to_numeric(df_renamed[col_to_num], errors='coerce')
                    if 'volume' in df_renamed.columns:
                        df_renamed['volume'] = pd.to_numeric(df_renamed['volume'], errors='coerce').astype('Int64')

                    all_ticker_data_list.append(df_renamed[final_cols_spec])
                    self._write_to_cache(df_renamed[final_cols_spec], cache_filepath)
                    logger.debug(f"yfinance: 已處理 {ticker_symbol}, {len(df_renamed)} 筆記錄。")
                    success_for_this_ticker = True
                    break

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404: # 股票不存在
                        logger.warning(f"yfinance: 股票 {ticker_symbol} 可能不存在 (HTTP 404)。跳過。")
                        success_for_this_ticker = True # 視為已處理 (無數據)
                        break
                    if e.response.status_code == 429 or e.response.status_code >= 500:
                        logger.warning(f"yfinance: 獲取 {ticker_symbol} 時發生 HTTP 錯誤 {e.response.status_code}。嘗試 {current_retries + 1}/{max_retries + 1}。")
                        sleep_time = initial_backoff * (2 ** current_retries) + random.uniform(0, 0.5)
                        logger.info(f"yfinance: 指數退避，等待 {sleep_time:.2f} 秒後重試 {ticker_symbol}。")
                        time.sleep(sleep_time)
                        current_retries += 1
                    else:
                        logger.error(f"yfinance: 獲取 {ticker_symbol} 時發生不可重試的 HTTP 錯誤: {e}", exc_info=True)
                        break
                except Exception as e:
                    # yfinance can sometimes raise other errors for various reasons (e.g. data parsing issues for odd tickers)
                    logger.error(f"yfinance: 獲取或處理 {ticker_symbol} 時發生未知錯誤: {e}", exc_info=True)
                    # Depending on the error, might retry or break. For now, break on unknown.
                    # If it's a temporary issue, a retry might help.
                    # For example, if it's a random `KeyError` due to unexpected data format for a specific day.
                    # Let's allow one retry for generic errors too, but log them carefully.
                    if current_retries < max_retries:
                        logger.warning(f"yfinance: {ticker_symbol} 發生未知錯誤，將嘗試重試 (嘗試 {current_retries + 1})。")
                        sleep_time = initial_backoff * (2 ** current_retries) + random.uniform(0, 0.5) # Add jitter
                        time.sleep(sleep_time)
                        current_retries += 1
                    else:
                        break # Max retries for generic error reached

            if not success_for_this_ticker:
                logger.error(f"yfinance: 獲取 {ticker_symbol} 數據已達最大重試次數 ({current_retries}) 或因不可重試錯誤而放棄。")

        if not all_ticker_data_list:
            logger.warning(f"yfinance: 未能為任何請求的股票列表 {tickers} 獲取數據。")
            return self._create_empty_standard_df()

        final_df = pd.concat(all_ticker_data_list, ignore_index=True)

        if final_df.empty:
             logger.info(f"yfinance: 最終合併的 DataFrame 為空 (所有請求的股票可能確實無數據，或均獲取失敗)。")

        logger.info(f"yfinance: 共獲取並處理 {len(final_df)} 筆記錄 for {tickers}。")
        return final_df

    def _get_standard_columns(self) -> List[str]:
        """返回標準化的欄位列表。"""
        return [
            'price_date', 'security_id', 'open_price', 'high_price', 'low_price',
            'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
            'source_api', 'last_updated_timestamp'
        ]

    def _create_empty_standard_df(self) -> pd.DataFrame:
        """創建一個帶有標準欄位的空 DataFrame。"""
        df = pd.DataFrame(columns=self._get_standard_columns())
        return df


# 簡易測試 (如果直接運行此檔案)
if __name__ == '__main__':
    import sys
    import shutil

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])

    test_api_config_yf = {
        "requests_per_minute": 60,
        "cache_enabled": True,
        "cache_directory": os.path.join("temp_test_cache", "yfinance"), # Use a distinct name for testing
        "cache_expire_after_seconds": 60
    }

    # Ensure the test cache directory exists and is clean before test
    if os.path.exists(test_api_config_yf["cache_directory"]):
        try:
            shutil.rmtree(test_api_config_yf["cache_directory"])
            logger.info(f"已清理舊的測試快取目錄: {test_api_config_yf['cache_directory']}")
        except Exception as e:
            logger.error(f"清理測試快取目錄失敗: {e}")
    os.makedirs(test_api_config_yf["cache_directory"], exist_ok=True)


    yf_connector = YFinanceConnector(api_config=test_api_config_yf)

    logger.info("\n--- 測試 YFinanceConnector for ^MOVE, AAPL, NONEXISTENTTICKERXYZ ---")
    tickers_to_test = ["^MOVE", "AAPL", "NONEXISTENTTICKERXYZ"]
    df_result = yf_connector.get_historical_price(
        tickers=tickers_to_test,
        start_date="2023-12-01",
        end_date="2023-12-15" # User means inclusive, connector will handle yf's exclusive end for daily
    )

    if not df_result.empty:
        logger.info(f"測試結果 DataFrame shape: {df_result.shape}")
        logger.info(f"測試結果 DataFrame (前5筆):\n{df_result.head().to_string()}")

        if "^MOVE" in df_result['security_id'].unique(): logger.info("^MOVE 數據已獲取。")
        if "AAPL" in df_result['security_id'].unique(): logger.info("AAPL 數據已獲取。")
        if "NONEXISTENTTICKERXYZ" not in df_result['security_id'].unique():
            logger.info("NONEXISTENTTICKERXYZ 正確地未返回數據或被跳過。")

        logger.info("\n--- 再次調用以測試快取 (AAPL) ---")
        df_cached_result_aapl = yf_connector.get_historical_price(
            tickers=["AAPL"], start_date="2023-12-01", end_date="2023-12-15"
        )
        if not df_cached_result_aapl.empty and "AAPL" in df_cached_result_aapl['security_id'].unique():
            logger.info("AAPL 數據已從快取 (或重新) 獲取。")
            # Compare with non-cached version if needed (data should be same)
            # pd.testing.assert_frame_equal(df_result[df_result['security_id']=='AAPL'].reset_index(drop=True),
            #                               df_cached_result_aapl.reset_index(drop=True))
        else:
            logger.warning("從快取獲取 AAPL 數據失敗。")

    else:
        logger.warning("YFinanceConnector 測試未返回任何數據。")

    logger.info("\n--- 測試 YFinanceConnector (空股票列表) ---")
    df_empty_tickers = yf_connector.get_historical_price(tickers=[], start_date="2023-01-01")
    if df_empty_tickers.empty:
        logger.info("OK: 對於空股票列表，返回了空的 DataFrame。")
    else:
        logger.error(f"錯誤: 對於空股票列表，未返回空的 DataFrame。Shape: {df_empty_tickers.shape}")

    # Clean up test cache directory after test
    if os.path.exists(test_api_config_yf["cache_directory"]):
        try:
            shutil.rmtree(test_api_config_yf["cache_directory"])
            logger.info(f"已清理測試快取目錄: {test_api_config_yf['cache_directory']}")
        except Exception as e:
            logger.error(f"清理測試快取目錄失敗: {e}")

    logger.info("--- YFinanceConnector 測試完成 ---")
