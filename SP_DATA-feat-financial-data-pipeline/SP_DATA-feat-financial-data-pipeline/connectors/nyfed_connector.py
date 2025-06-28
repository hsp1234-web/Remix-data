import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List
from io import BytesIO
import logging
from datetime import datetime, timezone
import sys
import time
import random

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__':
        from base import BaseConnector
    else:
        raise

class NYFedConnector(BaseConnector):
    """從紐約聯儲網站獲取並解析一級交易商持倉數據。"""

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler.")

        super().__init__(config, source_api_name="NYFED")

        self.urls_config = self.config.get('nyfed_primary_dealer_urls', [])
        self.recipes = self.config.get('nyfed_format_recipes', {})
        # Get general requests_config for retry parameters
        self.requests_config = self.config.get('requests_config', {})


        if not self.urls_config:
            self.logger.warning("No URLs configured for NYFed (nyfed_primary_dealer_urls).")
        if not self.recipes:
            self.logger.warning("No recipes for NYFed formats (nyfed_format_recipes).")

    def _download_excel_with_retries(self, url:str) -> Optional[BytesIO]:
        retries = self.requests_config.get('max_retries', 3)
        base_backoff = self.requests_config.get('base_backoff_seconds', 1)
        timeout_sec = self.requests_config.get('timeout', 60) # Use a longer timeout for file downloads

        for attempt in range(retries):
            try:
                self.logger.debug(f"Attempt {attempt + 1}/{retries} to download NYFed Excel from {url}")
                response = requests.get(url, timeout=timeout_sec)
                response.raise_for_status()
                self.logger.info(f"Successfully downloaded NYFed Excel from {url} (status {response.status_code}).")
                return BytesIO(response.content)
            except requests.exceptions.HTTPError as e:
                self.logger.warning(f"HTTP error on attempt {attempt + 1}/{retries} for NYFed URL '{url}': {e.response.status_code} - {e.response.text[:100] if e.response else 'N/A'}")
                if 400 <= e.response.status_code < 500 and e.response.status_code != 429: # Non-retryable client errors
                    self.logger.error(f"Client error {e.response.status_code} for NYFed URL '{url}', not retrying this file.")
                    return None
                if attempt == retries - 1: # Last attempt
                    self.logger.error(f"Final attempt failed for NYFed URL '{url}' with HTTPError: {e}")
                    return None
            except requests.exceptions.RequestException as e: # Other request errors (timeout, connection)
                self.logger.warning(f"RequestException on attempt {attempt + 1}/{retries} for NYFed URL '{url}': {e}")
                if attempt == retries - 1:
                    self.logger.error(f"Final attempt failed for NYFed URL '{url}' with RequestException: {e}")
                    return None

            # Exponential backoff with jitter for retriable errors (429 or 5xx implied by not returning above)
            wait_time = (base_backoff * (2 ** attempt)) + random.uniform(0, 0.5 * base_backoff)
            self.logger.info(f"Retrying download from NYFed URL '{url}' in {wait_time:.2f} seconds...")
            time.sleep(wait_time)

        self.logger.error(f"All download attempts failed for NYFed URL '{url}'.")
        return None


    def fetch_data(self, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        all_positions_data_list = []
        self.logger.info(f"Fetching NYFed data from {len(self.urls_config)} configured URLs.")

        if not self.urls_config:
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "No NYFed URLs configured."

        for file_info in self.urls_config:
            url = file_info.get('url')
            format_type = file_info.get('format_type')
            file_log_name = file_info.get('file_pattern', url)

            if not url or not format_type:
                self.logger.warning(f"Skipping invalid NYFed URL config: {file_info}.")
                continue

            self.logger.info(f"Processing NYFed file: {file_log_name} from URL: {url} (format: {format_type})")

            excel_file_content = self._download_excel_with_retries(url)
            if not excel_file_content:
                # Error already logged by _download_excel_with_retries
                continue

            recipe = self.recipes.get(format_type)
            if not recipe:
                self.logger.warning(f"No recipe for '{format_type}' (file: {file_log_name}). Skipping.")
                continue

            try:
                header_idx = recipe.get('header_row', 1) - 1
                df = pd.read_excel(excel_file_content, header=header_idx, engine='openpyxl')

                date_col_name = recipe.get('date_column')
                if not date_col_name or date_col_name not in df.columns:
                    self.logger.error(f"Date column '{date_col_name}' not in {file_log_name}. Cols: {df.columns.tolist()}")
                    continue

                df.rename(columns={date_col_name: 'metric_date'}, inplace=True)
                df['metric_date'] = pd.to_datetime(df['metric_date'], errors='coerce')
                df.dropna(subset=['metric_date'], inplace=True)

                if df.empty: self.logger.warning(f"No valid dates in {file_log_name}."); continue

                sum_cols_cfg = recipe.get('columns_to_sum', [])
                actual_cols_to_sum = [col for col in sum_cols_cfg if col in df.columns]

                missing_cols = set(sum_cols_cfg) - set(actual_cols_to_sum)
                if missing_cols: self.logger.warning(f"Missing cols in {file_log_name} for recipe '{format_type}': {missing_cols}. Summing: {actual_cols_to_sum}")

                if not actual_cols_to_sum: self.logger.warning(f"No columns to sum in {file_log_name}."); continue

                for col in actual_cols_to_sum: df[col] = pd.to_numeric(df[col], errors='coerce')

                sum_series = df[actual_cols_to_sum].sum(axis=1, skipna=True)
                proc_df = df[['metric_date']].copy()
                proc_df['metric_value'] = sum_series

                multiplier = recipe.get('data_unit_multiplier', 1)
                proc_df['metric_value'] *= multiplier
                proc_df.dropna(subset=['metric_value'], inplace=True)

                if proc_df.empty: self.logger.warning(f"No valid summed data for {file_log_name}."); continue

                proc_df['metric_name'] = f"{self.source_api_name}/PRIMARY_DEALER_NET_POSITION"
                proc_df['source_api'] = self.source_api_name
                proc_df['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                all_positions_data_list.append(proc_df[['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']])
                self.logger.debug(f"Processed NYFed file: {file_log_name}, {len(proc_df)} rows.")
            except Exception as e:
                self.logger.error(f"Error processing Excel {file_log_name}: {e}", exc_info=True)
                continue

        if not all_positions_data_list:
            self.logger.warning("No data from any NYFed files.")
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "No data from NYFed."

        combo_df = pd.concat(all_positions_data_list, ignore_index=True)
        if combo_df.empty: self.logger.warning("NYFed data empty after concat."); return combo_df, "NYFed data empty post-concat."

        combo_df.sort_values('metric_date', inplace=True)
        combo_df.drop_duplicates(subset=['metric_date'], keep='last', inplace=True)

        if combo_df.empty: self.logger.warning("NYFed data empty after dedup."); return combo_df, "NYFed data empty post-dedup."

        combo_df.set_index('metric_date', inplace=True)
        if not combo_df.index.is_monotonic_increasing:
             self.logger.warning("NYFed index not monotonic, sorting."); combo_df.sort_index(inplace=True)

        if combo_df.empty: self.logger.warning("NYFed data empty after index sort."); return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "NYFed data empty post-sort."

        min_d, max_d = combo_df.index.min(), combo_df.index.max()
        if pd.isna(min_d) or pd.isna(max_d):
            self.logger.error(f"Invalid date range for NYFed. Min: {min_d}, Max: {max_d}")
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "Invalid date range."

        daily_idx = pd.date_range(start=min_d, end=max_d, freq='D')
        daily_df = combo_df.reindex(daily_idx).ffill()
        daily_df.index.name = 'metric_date'
        daily_df.reset_index(inplace=True)

        if not daily_df.empty:
            daily_df['metric_name'] = f"{self.source_api_name}/PRIMARY_DEALER_NET_POSITION"
            daily_df['source_api'] = self.source_api_name
            daily_df['data_snapshot_timestamp'] = datetime.now(timezone.utc)

        self.logger.info(f"Processed {len(daily_df)} total NYFed records after daily ffill.")
        return daily_df[['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']], None


if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger = logging.getLogger("NYFedConnectorTestRunV4") # Unique name for this test run
    if not test_logger.handlers:
        ch = logging.StreamHandler(sys.stdout); ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s')); test_logger.addHandler(ch); test_logger.propagate = False

    cfg = {
        'requests_config': {'max_retries': 1, 'base_backoff_seconds': 0.1, 'timeout': 20},
        'nyfed_primary_dealer_urls': [
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx", "file_pattern": "prideal2023.xlsx", "format_type": "SBP2013_TEST"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2022.xlsx", "file_pattern": "prideal2022.xlsx", "format_type": "SBP2013_TEST"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/bad_url_for_test_nyfed.xlsx", "file_pattern": "bad_url.xlsx", "format_type": "SBP2013_TEST"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2021.xlsx", "file_pattern": "prideal2021.xlsx", "format_type": "NONEXISTENT_RECIPE_TEST_NYFED"}
        ],
        'nyfed_format_recipes': {
            "SBP2013_TEST": { "header_row": 3, "date_column": "As of Date",
                              "columns_to_sum": ["U.S. Treasury coupons", "U.S. Treasury bills",
                                                 "U.S. Treasury floating rate notes (FRNs)", "NonExistentColumnForTest"],
                              "data_unit_multiplier": 1000 }}}

    conn = NYFedConnector(config=cfg, logger_instance=test_logger)
    df_res, err = conn.fetch_data()

    if err: test_logger.error(f"NYFed Test failed: {err}")
    elif df_res is not None:
        test_logger.info(f"NYFed Test successful. Fetched data shape: {df_res.shape}")
        if not df_res.empty:
            test_logger.info(f"NYFed Data head:\n{df_res.head().to_string()}")
            test_logger.info(f"NYFed Data tail:\n{df_res.tail().to_string()}")
            unique_dates = df_res['metric_date'].nunique()
            if not df_res['metric_date'].empty:
                expected_days = (df_res['metric_date'].max() - df_res['metric_date'].min()).days + 1
                if unique_dates == expected_days: test_logger.info("NYFed data frequency appears to be daily.")
                else: test_logger.warning(f"NYFed data freq not strictly daily: {unique_dates} unique for {expected_days} day span.")
            else: test_logger.warning("NYFed data has no dates to check frequency.")
        else: test_logger.info("NYFed Test: Returned DataFrame is empty.")
    else: test_logger.error("NYFed Test failed: data_df is None and no error message was returned.")

# **日誌記錄器初始化邏輯的微調（已更新）：**
# *   在 `NYFedConnector` 的 `__init__` 方法中，從 `self.config` 獲取 `requests_config` 以便 `_download_excel_with_retries` 可以使用它。
# *   `_download_excel_with_retries` 方法現在從 `self.requests_config` 獲取 `timeout`，並為其提供了一個更適合文件下載的預設值 (60秒)。
# *   `if __name__ == '__main__':` 塊中的測試配置 `cfg` 現在也包含了 `requests_config` 部分，以便可以測試到下載重試邏輯。
# *   其他日誌記錄和錯誤處理的微調與之前的版本類似。
#
# 這個版本的 `NYFedConnector` 在下載部分也加入了重試邏輯，使其更加穩健。
