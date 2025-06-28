from fredapi import Fred
import pandas as pd
import time
import logging
from datetime import datetime, timezone, date # Added date
from typing import List, Dict, Any, Tuple, Optional
import os

# Use a module-level logger
logger = logging.getLogger(__name__)

# Define a conservative request interval for FRED if not specified by RPM
_MIN_REQUEST_INTERVAL_FRED = 0.5 # fredapi 的隱含速率限制 (如果使用金鑰，通常是120RPM，即0.5s/req)

class FredConnector:
    """
    使用 fredapi 函式庫從 FRED (Federal Reserve Economic Data) 獲取經濟數據。
    包含讀取設定檔、速率控制、統一錯誤處理。
    """

    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 FredConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典。
                                         應包含 'api_key' (可選，但建議) 和 'requests_per_minute'。
                                         例如:
                                         {
                                             "api_key": "YOUR_FRED_API_KEY", // 或從環境變數讀取
                                             "requests_per_minute": 120
                                         }
        """
        # API 金鑰可以來自 config，或從環境變數 FRED_API_KEY (fredapi 預設行為)
        self.api_key = api_config.get("api_key", os.getenv("FRED_API_KEY"))

        if not self.api_key or self.api_key == "YOUR_FRED_API_KEY": # Check against template placeholder
            logger.warning("FredConnector: FRED API 金鑰未在設定中明確提供，將依賴 fredapi 的預設行為 (可能從 FRED_API_KEY 環境變數讀取或無金鑰訪問)。")
            # fredapi 即使沒有金鑰也能訪問某些數據，但有更嚴格的限制
            self.api_key = None # 確保如果未提供，則 fredapi 使用其預設

        self.requests_per_minute = api_config.get("requests_per_minute", 120) # Default from config.yaml.template
        self._last_request_time = 0

        rpm_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0
        self._min_request_interval = max(rpm_interval, _MIN_REQUEST_INTERVAL_FRED)

        self.source_api_name = "FRED"

        try:
            # 如果 self.api_key 為 None，Fred() 會嘗試無金鑰訪問或讀取環境變數
            self.fred_client = Fred(api_key=self.api_key)
            logger.info(f"FredConnector 初始化完成。API Key: {'提供' if self.api_key else '未提供/依賴環境變數'} , RPM Config: {self.requests_per_minute}, Effective Interval: {self._min_request_interval:.2f}s")
            # 可以嘗試一個簡單的測試調用來驗證金鑰 (如果提供的話)
            # self.fred_client.get_series_info('GNPCA') # Example series
        except Exception as e:
            logger.error(f"FredConnector: 初始化 Fred client 時發生錯誤: {e}", exc_info=True)
            self.fred_client = None # 標記 client 不可用

    def _wait_for_rate_limit(self):
        """等待直到可以安全地發出下一個 API 請求。"""
        if self._min_request_interval == 0 or not self.fred_client:
            return
        now = time.time()
        elapsed_time = now - self._last_request_time
        wait_time = self._min_request_interval - elapsed_time
        if wait_time > 0:
            logger.debug(f"FRED 速率控制：等待 {wait_time:.2f} 秒。")
            time.sleep(wait_time)
        # self._last_request_time = time.time() # Update after request is made

    def get_series_data(self, series_ids: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        獲取一個或多個 FRED 經濟數據序列。

        Args:
            series_ids (List[str]): FRED 序列 ID 列表 (例如 ["GDP", "UNRATE"])。
            start_date (Optional[str]): 開始日期 (YYYY-MM-DD)。
            end_date (Optional[str]): 結束日期 (YYYY-MM-DD)。

        Returns:
            pd.DataFrame: 包含請求序列數據的長格式 DataFrame，若失敗則為空 DataFrame。
                          欄位: 'metric_date', 'security_id' (序列ID), 'metric_name', 'metric_value',
                                'source_api', 'last_updated_timestamp'
        """
        if not self.fred_client:
            logger.error("FredConnector: Fred client 未初始化。無法獲取數據。")
            return self._create_empty_standard_df()

        if not series_ids:
            logger.warning("FredConnector: 未提供 series_ids。")
            return self._create_empty_standard_df()

        logger.info(f"FRED: 請求序列 {series_ids} 從 {start_date or '最早'} 到 {end_date or '最新'}。")

        all_series_data_list = []
        errors_encountered_log = []

        for series_id in series_ids:
            self._wait_for_rate_limit()
            self._last_request_time = time.time() # 更新時間戳

            try:
                logger.debug(f"FRED: 正在獲取序列 {series_id}")
                # fredapi 的 get_series 返回一個 Pandas Series，索引是日期，值是序列值
                series_data_raw = self.fred_client.get_series(
                    series_id=series_id,
                    observation_start=start_date,
                    observation_end=end_date
                )

                if series_data_raw is None or series_data_raw.empty:
                    logger.info(f"FRED: 序列 {series_id} 在指定日期範圍內無數據，或序列不存在。")
                    continue # 跳到下一個序列

                df_single_series = series_data_raw.reset_index()
                df_single_series.columns = ['metric_date', 'metric_value']

                df_single_series['metric_date'] = pd.to_datetime(df_single_series['metric_date']).dt.date
                df_single_series['security_id'] = series_id # 使用 series_id 作為 security_id
                df_single_series['metric_name'] = series_id # 指標名稱也用 series_id
                df_single_series['source_api'] = self.source_api_name
                df_single_series['last_updated_timestamp'] = datetime.now(timezone.utc)

                df_single_series['metric_value'] = pd.to_numeric(df_single_series['metric_value'], errors='coerce')

                if df_single_series['metric_value'].isnull().all() and not series_data_raw.empty : # If all values became NaN after conversion
                    logger.info(f"FRED: 序列 {series_id} 的所有值在轉換為數字後均為 NaN (原始數據可能非數字)。")
                    continue # Skip if no valid numeric data

                # Do not dropna for metric_value here, as NaN can be a valid state for economic data (e.g. not yet reported)
                # However, if metric_date itself is NaT after conversion, that's an issue.
                df_single_series.dropna(subset=['metric_date'], inplace=True)
                if df_single_series.empty:
                    logger.info(f"FRED: 序列 {series_id} 在日期轉換/清洗後變為空。")
                    continue

                all_series_data_list.append(df_single_series[self._get_standard_columns()])
                logger.debug(f"FRED: 成功處理序列 {series_id}, {len(df_single_series)} 筆記錄。")

            except Exception as e: # fredapi 可能拋出各種錯誤，例如請求錯誤或數據問題
                error_msg = f"FRED: 獲取或處理序列 {series_id} 時發生錯誤: {e}"
                logger.error(error_msg, exc_info=True)
                errors_encountered_log.append(f"{series_id}: {str(e)[:100]}") # Log a snippet of the error
                # 不因單個序列失敗而中止整個請求，但記錄錯誤

        if not all_series_data_list:
            log_msg = f"FRED: 未能為任何請求的序列 {series_ids} 成功獲取數據。"
            if errors_encountered_log:
                log_msg += f" 遇到的錯誤: {'; '.join(errors_encountered_log)}"
            logger.warning(log_msg)
            return self._create_empty_standard_df()

        final_df = pd.concat(all_series_data_list, ignore_index=True)

        if final_df.empty :
             if not errors_encountered_log:
                 logger.info(f"FRED: 最終合併的 DataFrame 為空 (所有請求的序列可能確實無數據)。")
             else:
                 logger.warning(f"FRED: 最終合併的 DataFrame 為空，且過程中發生錯誤: {'; '.join(errors_encountered_log)}")

        logger.info(f"FRED: 共獲取並處理 {len(final_df)} 筆記錄 for {series_ids}。")
        return final_df

    def _get_standard_columns(self) -> List[str]:
        """返回標準化的欄位列表。"""
        return [
            'metric_date', 'security_id', 'metric_name', 'metric_value',
            'source_api', 'last_updated_timestamp'
        ]

    def _create_empty_standard_df(self) -> pd.DataFrame:
        """創建一個帶有標準欄位的空 DataFrame。"""
        return pd.DataFrame(columns=self._get_standard_columns())


# 簡易測試 (如果直接運行此檔案)
if __name__ == '__main__':
    import sys # Added sys for stdout handler

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])

    # 測試用的設定檔 (通常會從 config.yaml 讀取)
    # 這裡假設 API 金鑰已設定在環境變數 FRED_API_KEY 中，或使用無金鑰訪問
    test_api_config_fred = {
        "api_key": os.getenv("FRED_API_KEY_TEST", os.getenv("FRED_API_KEY")), # 允許測試時用特定金鑰
        "requests_per_minute": 100 # 測試時可以設低一些
    }

    if not test_api_config_fred["api_key"]:
        logger.warning("FredConnector 測試：未提供 API 金鑰，將嘗試無金鑰訪問。某些數據可能受限。")


    fred_connector = FredConnector(api_config=test_api_config_fred)

    if fred_connector.fred_client is not None:
        logger.info("\n--- 測試 FredConnector get_series_data ---")
        series_to_test = ["GDP", "UNRATE", "DGS10", "NONEXISTENTFREDSERIES"] # 包含一個不存在的序列

        df_fred_result = fred_connector.get_series_data(
            series_ids=series_to_test,
            start_date="2022-01-01",
            end_date="2023-01-01"
        )

        if not df_fred_result.empty:
            logger.info(f"FRED 測試結果 DataFrame shape: {df_fred_result.shape}")
            logger.info(f"FRED 測試結果 DataFrame (前5筆):\n{df_fred_result.head().to_string()}")

            unique_series_found = df_fred_result['security_id'].unique()
            logger.info(f"獲取到的序列: {unique_series_found}")
            if "GDP" in unique_series_found: logger.info("GDP 數據已獲取。")
            if "UNRATE" in unique_series_found: logger.info("UNRATE 數據已獲取。")
            if "NONEXISTENTFREDSERIES" not in unique_series_found:
                logger.info("NONEXISTENTFREDSERIES 正確地未返回數據或被跳過。")
        else:
            logger.warning("FredConnector 測試未返回任何數據。")

        logger.info("\n--- 測試 FredConnector (空序列列表) ---")
        df_empty_series = fred_connector.get_series_data(series_ids=[])
        if df_empty_series.empty:
            logger.info("OK: 對於空序列列表，返回了空的 DataFrame。")
        else:
            logger.error(f"錯誤: 對於空序列列表，未返回空的 DataFrame。Shape: {df_empty_series.shape}")
    else:
        logger.error("FredConnector client (self.fred_client) 未初始化。測試無法繼續。")

    logger.info("--- FredConnector 測試完成 ---")
