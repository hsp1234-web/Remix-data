#!/bin/bash
# run_prototype.sh - 原子化執行腳本

# === 階段一：環境清理 (可選) ===
# 確保我們從一個乾淨的狀態開始
echo "Phase 1: Cleaning up previous artifacts..."
# 我們將在創建檔案時覆蓋，所以這裡可以暫時不執行實際的 rm 命令，
# 或者如果需要，可以取消註解下一行。
# rm -rf src/ data/ market_briefing_log.txt api_test_logs/

# === 階段二：專案建構 (核心步驟) ===
# 使用 `cat` 和 `EOF` 一次性、精確地創建所有檔案。
echo "Phase 2: Building project structure and files..."

# 2.1 創建目錄結構
echo "Creating directory structure..."
mkdir -p src/configs
mkdir -p src/connectors
mkdir -p src/database
mkdir -p src/engine
mkdir -p src/scripts
# data/ 目錄通常由應用程式在執行時創建，或者如果需要預先填充，則在此處創建。
# mkdir -p data/
# 日誌目錄也通常由日誌設定程式碼創建。
# mkdir -p api_test_logs/

echo "Creating configuration file (src/configs/project_config.yaml)..."
cat <<EOF > src/configs/project_config.yaml
# Configuration for the Financial Data Processing Prototype

database:
  path: "data/financial_data.duckdb" # Relative to project root for the atomic script
  # schema_file: "src/configs/database_schemas.json" # Optional, if we define schemas externally

data_fetch_range:
  start_date: "2020-01-01"
  # end_date: "YYYY-MM-DD" # Optional: If empty, main.py will use current date

api_endpoints:
  fred:
    api_key_env: "FRED_API_KEY" # Actual key is hardcoded in main.py for this task
    base_url: "https://api.stlouisfed.org/fred/"
  # nyfed: # URLs are handled directly in nyfed_primary_dealer_urls
  # yfinance: # No specific endpoint, yfinance library handles it

target_metrics:
  fred_series_ids:
    - "DGS10"    # 10-Year Treasury Constant Maturity Rate
    - "DGS2"     # 2-Year Treasury Constant Maturity Rate
    - "SOFR"     # Secured Overnight Financing Rate
    - "VIXCLS"   # CBOE Volatility Index
    - "WRESBAL"  # Reserves Balance with Federal Reserve Banks
  yfinance_tickers:
    - "^MOVE"    # ICE BofA MOVE Index (Treasury Volatility)
    # - "SPY"    # Example: S&P 500 ETF for broader market context if needed

# Configuration for NYFedConnector
nyfed_primary_dealer_urls:
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2024.xlsx"
    file_pattern: "prideal2024.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx"
    file_pattern: "prideal2023.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2022.xlsx"
    file_pattern: "prideal2022.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  # Add more historical files if needed, e.g.:
  # - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2021.xlsx"
  #   file_pattern: "prideal2021.xlsx"
  #   format_type: "PD_STATS_FORMAT_2013_ONWARDS"

nyfed_format_recipes:
  "PD_STATS_FORMAT_2013_ONWARDS":
    header_row: 3 # Row number in Excel where headers are (1-indexed)
    date_column: "As of Date"
    columns_to_sum:
      - "U.S. Treasury coupons"
      - "U.S. Treasury bills"
      - "U.S. Treasury floating rate notes (FRNs)"
      - "Federal agency debt securities (MBS)" # Mortgage-Backed Securities
      - "Federal agency debt securities (non-MBS)"
      - "Commercial paper"
      - "Certificates of deposit"
      - "Bankers acceptances"
      - "Equities"
      - "Corporate bonds (investment grade)"
      - "Corporate bonds (below investment grade)"
      - "Municipal securities"
      - "Other assets" # This can be a catch-all for various other positions
    data_unit_multiplier: 1000000 # Data is in millions, convert to actual value

# Configuration for IndicatorEngine
indicator_engine_params:
  rolling_window_days: 252 # Approximately 1 trading year
  stress_index_weights:
    sofr_deviation: 0.20
    spread_10y2y: 0.20
    primary_dealer_position: 0.15 # Based on NYFED data
    move_index: 0.25
    vix_index: 0.15
    pos_res_ratio: 0.05 # Primary Dealer Positions to Reserves Ratio
  stress_threshold_moderate: 40
  stress_threshold_high: 60
  stress_threshold_extreme: 80

# General configuration for requests made by connectors
requests_config:
  max_retries: 3
  base_backoff_seconds: 1
  timeout: 30 # Default timeout for most API calls
  download_timeout: 120 # Longer timeout for file downloads (like NYFed Excel)

# Configuration for AI Service (RemoteAIAgent)
ai_service:
  # API key for the AI service. Replace "YOUR_API_KEY_HERE" with the actual key.
  # If left as "YOUR_API_KEY_HERE" or empty, AI decision making will be skipped.
  api_key: "YOUR_API_KEY_HERE"

  # API endpoint for the AI service
  # Example for Anthropic Claude API:
  api_endpoint: "https://api.anthropic.com/v1/messages"
  # Example for OpenAI API:
  # api_endpoint: "https://api.openai.com/v1/chat/completions"

  # Default model to use if not overridden in the call
  # Example for Anthropic Claude:
  default_model: "claude-3-opus-20240229"
  # Example for OpenAI GPT-4 Turbo:
  # default_model: "gpt-4-turbo-preview"

  # API call settings
  max_retries: 3
  retry_delay_seconds: 5 # Delay between retries for API calls
  api_call_delay_seconds: 1.0 # Minimum delay between consecutive API calls (respect rate limits)
EOF

echo "Creating requirements.txt file..."
cat <<EOF > requirements.txt
pandas
pyyaml
duckdb
SQLAlchemy
fredapi
yfinance
requests
openpyxl
beautifulsoup4
# FinMind # Removed as it's not currently used
tqdm
# Add any other specific versions if necessary, e.g., pandas==2.0.3
EOF

echo "Creating src/connectors/base.py..."
cat <<EOF > src/connectors/base.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Tuple, Optional

class BaseConnector(ABC):
    """
    所有數據連接器的抽象基類。
    定義了標準接口，確保所有 Connector 的行為一致。
    """

    def __init__(self, config: Dict[str, Any], source_api_name: str = "Unknown"):
        self.config = config
        self.source_api_name = source_api_name
        # Logger can be passed by the child class or DataManager for better context
        # For now, child classes will initialize their own loggers or use a global one.

    @abstractmethod
    def fetch_data(self, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        從 API 或數據源獲取原始數據並進行初步轉換成 DataFrame (通常是長表格式)。

        Args:
            **kwargs: 特定 connector 需要的參數 (例如 series_ids, tickers, start_date, end_date).

        Returns:
            一個包含 (DataFrame, error_message) 的元組。
            成功時，DataFrame 包含獲取和初步標準化的數據，error_message 為 None。
            失敗時，DataFrame 為 None 或空的 DataFrame (帶有預期欄位)，error_message 包含錯誤信息。
            DataFrame 應包含 'source_api' 和 'data_snapshot_timestamp' (UTC) 欄位。
            對於時間序列數據，應有 'metric_date' 或 'price_date'。
            對於宏觀/因子數據，應有 'metric_name'。
            對於股價數據，應有 'security_id'。
        """
        pass

    def get_source_name(self) -> str:
        """返回數據源的名稱。"""
        return self.source_api_name

    # Common utility methods can be added here if needed, e.g.,
    # _make_request_with_retries (similar to what was in the old BaseConnector from user's Colab)
    # or a method to standardize date formats.
    # For now, keeping it lean as per the new design focusing on fetch_data.
    # The retry logic from user's previous BaseConnector (with jitter) is excellent
    # and ideally should be part of a shared HTTP request utility or within each connector's
    # implementation of how it calls external APIs if not using a library that handles it.
    # Given the "one-shot build" nature, detailed retry in Base might be over-engineering for now,
    # and each connector can implement its specific retry or rely on the robustness of the used library.
    # However, for FREDConnector which uses requests directly, that logic would be valuable.
    # Let's assume for now that retry logic is handled within each connector's specific requests.
    # Or, we can add a protected _make_request method here later if many connectors use raw requests.
EOF

echo "Creating src/connectors/nyfed_connector.py..."
cat <<EOF > src/connectors/nyfed_connector.py
import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List
from io import BytesIO
import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List
from io import BytesIO
import logging
from datetime import datetime, timezone
import sys
import time
import random
from bs4 import BeautifulSoup # Added for HTML parsing
from urllib.parse import urljoin # Added for joining relative URLs

try:
    from .base import BaseConnector
except ImportError:
    # This fallback might be useful if running the script directly for testing
    # For the atomic script, this should ideally not be hit if structure is correct.
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
            if not self.logger.handlers and not logging.getLogger().hasHandlers(): # Basic NullHandler setup
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        super().__init__(config, source_api_name="NYFED")

        self.urls_config = self.config.get('nyfed_primary_dealer_urls', [])
        self.recipes = self.config.get('nyfed_format_recipes', {})
        self.requests_config = self.config.get('requests_config', {})

        if not self.urls_config:
            self.logger.warning("No URLs configured for NYFed (nyfed_primary_dealer_urls).")
        if not self.recipes:
            self.logger.warning("No recipes for NYFed formats (nyfed_format_recipes).")

    def _download_excel_with_retries(self, url_config: Dict[str, str]) -> Optional[BytesIO]: # Changed signature
        url = url_config.get('url') # Get URL from url_config
        if not url:
            self.logger.error("URL not found in url_config for _download_excel_with_retries.")
            return None

        retries = self.requests_config.get('max_retries', 3)
        base_backoff = self.requests_config.get('base_backoff_seconds', 1)
        timeout_sec = self.requests_config.get('download_timeout', self.requests_config.get('timeout', 60))

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        for attempt in range(retries):
            try:
                self.logger.debug(f"Attempt {attempt + 1}/{retries} to access NYFed resource page: {url}")
                page_response = requests.get(url, timeout=timeout_sec, headers=headers)
                self.logger.info(f"NYFed Page URL: {url}, Attempt: {attempt + 1}, Status: {page_response.status_code}, Content-Type: {page_response.headers.get('Content-Type')}")
                page_response.raise_for_status()

                # If the page itself is HTML, try to find an Excel link
                if 'text/html' in page_response.headers.get('Content-Type', '').lower():
                    self.logger.info(f"Content from {url} is HTML. Attempting to find Excel link...")
                    soup = BeautifulSoup(page_response.content, 'html.parser')
                    file_pattern_hint = url_config.get('file_pattern', '') # Now uses passed url_config
                    year_hint = ''.join(filter(str.isdigit, file_pattern_hint))

                    excel_link_found = None
                    # Try a few patterns for finding the link
                    possible_links = soup.find_all('a', href=True)
                    self.logger.debug(f"Found {len(possible_links)} links on page {url}. Checking for Excel files related to '{file_pattern_hint}'.")

                    for link_tag in possible_links:
                        href = link_tag['href']
                        link_text = link_tag.get_text(strip=True)
                        # Prioritize links containing the file_pattern directly or parts of it
                        if file_pattern_hint and file_pattern_hint.lower() in href.lower():
                             excel_link_found = href
                             self.logger.info(f"Found strong match for Excel link by file_pattern in href: {excel_link_found} for {url}")
                             break
                        if year_hint and year_hint in href.lower() and '.xlsx' in href.lower():
                             excel_link_found = href
                             self.logger.info(f"Found year match for Excel link in href: {excel_link_found} for {url}")
                             break
                        if 'prideal' in href.lower() and '.xlsx' in href.lower(): # General fallback
                             excel_link_found = href
                             self.logger.info(f"Found general 'prideal' Excel link in href: {excel_link_found} for {url}")
                             break
                        if link_text and file_pattern_hint and file_pattern_hint.lower() in link_text.lower() and '.xlsx' in href.lower():
                            excel_link_found = href
                            self.logger.info(f"Found Excel link by text match '{link_text}': {excel_link_found} for {url}")
                            break

                    if excel_link_found:
                        # Ensure the link is absolute
                        if not excel_link_found.startswith('http'):
                            from urllib.parse import urljoin
                            excel_download_url = urljoin(url, excel_link_found) # url is the base page URL
                        else:
                            excel_download_url = excel_link_found

                        self.logger.info(f"Attempting to download actual Excel file from scraped URL: {excel_download_url}")
                        response = requests.get(excel_download_url, timeout=timeout_sec, headers=headers)
                        self.logger.info(f"Scraped Excel URL: {excel_download_url}, Status: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}, Size: {len(response.content)} bytes")
                        response.raise_for_status()
                        # Now check content type of the actual downloaded file
                        content_type = response.headers.get('Content-Type', '').lower()
                        if any(ct in content_type for ct in ['excel', 'spreadsheetml', 'officedocument', 'application/octet-stream']):
                            self.logger.info(f"Successfully downloaded and verified Excel from scraped link: {excel_download_url}")
                            return BytesIO(response.content)
                        else:
                            self.logger.error(f"Scraped link {excel_download_url} provided non-Excel Content-Type: '{content_type}'. Skipping.")
                            return None
                    else:
                        self.logger.error(f"Could not find a suitable Excel download link on HTML page: {url} for pattern '{file_pattern_hint}'.")
                        self.logger.debug(f"Page content sample (first 1000 bytes of HTML from {url}): {page_response.content[:1000].decode('utf-8', errors='replace')}")
                        return None # No link found

                # If original URL was not HTML, or if it was HTML but logic above failed to return:
                # This part handles direct downloads if the URL itself is supposed to be an Excel file
                # (original logic before scraping attempt)
                # For safety, let's assume if it wasn't HTML and wasn't handled, it might be a direct file.
                # This might be redundant if the HTML path is robust.
                # However, the previous logic had a direct check for excel content type.
                # Let's ensure if it's not HTML, we still check its content type directly.
                elif any(ct in page_response.headers.get('Content-Type', '').lower() for ct in ['excel', 'spreadsheetml', 'officedocument', 'application/octet-stream']):
                    self.logger.info(f"URL {url} seems to be a direct Excel link (Content-Type: {page_response.headers.get('Content-Type', '')}). Using its content.")
                    return BytesIO(page_response.content)
                else: # Neither HTML with a good link, nor a direct Excel link by Content-Type
                    self.logger.error(f"Content from {url} is not HTML with a usable Excel link, nor a direct Excel file. Content-Type: {page_response.headers.get('Content-Type', '')}. Skipping.")
                    self.logger.debug(f"Content sample (first 500 bytes from {url}): {page_response.content[:500].decode('utf-8', errors='replace') if page_response.content else 'No content'}")
                    return None

            except requests.exceptions.HTTPError as e:
                # This error is for the page_response or the subsequent excel_response
                response_for_error_log = e.response # Could be page_response or the actual excel download response
                self.logger.warning(f"HTTP error on attempt {attempt + 1}/{retries} for NYFed process (URL: {url}): {response_for_error_log.status_code if response_for_error_log else 'N/A'} - {response_for_error_log.text[:200] if response_for_error_log and response_for_error_log.text else 'N/A'}")
                if response_for_error_log is not None:
                    self.logger.warning(f"Headers for error response (URL: {url}): {response_for_error_log.headers}")
                if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                    self.logger.error(f"Client error {e.response.status_code} for NYFed URL '{url}', not retrying this file.")
                    return None
                if attempt == retries - 1:
                    self.logger.error(f"Final attempt failed for NYFed URL '{url}' with HTTPError: {e}")
                    return None
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"RequestException on attempt {attempt + 1}/{retries} for NYFed URL '{url}': {e}")
                if attempt == retries - 1:
                    self.logger.error(f"Final attempt failed for NYFed URL '{url}' with RequestException: {e}")
                    return None

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
            excel_file_content = self._download_excel_with_retries(file_info) # Pass the whole file_info dict
            if not excel_file_content:
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

                if df.empty:
                    self.logger.warning(f"No valid dates in {file_log_name} after processing."); continue

                sum_cols_cfg = recipe.get('columns_to_sum', [])
                actual_cols_to_sum = [col for col in sum_cols_cfg if col in df.columns]

                missing_cols = set(sum_cols_cfg) - set(actual_cols_to_sum)
                if missing_cols:
                    self.logger.warning(f"Missing cols in {file_log_name} for recipe '{format_type}': {missing_cols}. Summing available: {actual_cols_to_sum}")

                if not actual_cols_to_sum:
                    self.logger.warning(f"No columns to sum were found in {file_log_name} based on recipe."); continue

                for col in actual_cols_to_sum:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                sum_series = df[actual_cols_to_sum].sum(axis=1, skipna=True)
                proc_df = df[['metric_date']].copy()
                proc_df['metric_value'] = sum_series

                multiplier = recipe.get('data_unit_multiplier', 1)
                proc_df['metric_value'] *= multiplier
                proc_df.dropna(subset=['metric_value'], inplace=True)

                if proc_df.empty:
                    self.logger.warning(f"No valid summed data for {file_log_name} after processing."); continue

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
        if combo_df.empty:
            self.logger.warning("NYFed data empty after concat.");
            return combo_df, "NYFed data empty post-concat."

        combo_df.sort_values('metric_date', inplace=True)
        # For positions, taking the latest report for a given day seems reasonable.
        combo_df.drop_duplicates(subset=['metric_date'], keep='last', inplace=True)

        if combo_df.empty:
            self.logger.warning("NYFed data empty after dedup.");
            return combo_df, "NYFed data empty post-dedup."

        combo_df.set_index('metric_date', inplace=True)
        if not combo_df.index.is_monotonic_increasing:
             self.logger.warning("NYFed index not monotonic after initial sort/dedup, re-sorting.");
             combo_df.sort_index(inplace=True)

        if combo_df.empty:
            self.logger.warning("NYFed data empty after index operations.");
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "NYFed data empty post-index ops."

        min_d, max_d = combo_df.index.min(), combo_df.index.max()
        if pd.isna(min_d) or pd.isna(max_d): # Should not happen if df is not empty and dates are valid
            self.logger.error(f"Invalid date range for NYFed. Min: {min_d}, Max: {max_d}")
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "Invalid date range for NYFed data."

        daily_idx = pd.date_range(start=min_d, end=max_d, freq='D')
        daily_df = combo_df.reindex(daily_idx).ffill() # Forward fill missing daily values
        daily_df.index.name = 'metric_date'
        daily_df.reset_index(inplace=True)

        # Ensure required columns are present even if daily_df becomes empty after reindex/ffill (unlikely but safeguard)
        final_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
        for col in final_cols:
            if col not in daily_df.columns:
                daily_df[col] = pd.NA # Or appropriate default

        if not daily_df.empty: # Re-assign static values after ffill
            daily_df['metric_name'] = f"{self.source_api_name}/PRIMARY_DEALER_NET_POSITION"
            daily_df['source_api'] = self.source_api_name
            # Snapshot timestamp should ideally be per fetch, but for ffilled data, using current time is acceptable
            daily_df['data_snapshot_timestamp'] = datetime.now(timezone.utc)

        self.logger.info(f"Processed {len(daily_df)} total NYFed records after daily ffill.")
        return daily_df[final_cols], None

# Main block for testing if script is run directly
if __name__ == '__main__':
    # Setup basic logging for test execution
    if not logging.getLogger().hasHandlers(): # Ensure no duplicate handlers from other runs
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_ny = logging.getLogger("NYFedConnectorTestRun_Atomic") # Unique name for this test run
    if not test_logger_ny.handlers: # Avoid adding handlers multiple times
        ch_ny = logging.StreamHandler(sys.stdout)
        ch_ny.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_ny.addHandler(ch_ny)
        test_logger_ny.propagate = False # Prevent logging to root if it has other handlers

    # Test configuration
    test_cfg = {
        'requests_config': {'max_retries': 2, 'base_backoff_seconds': 0.5, 'timeout': 15, 'download_timeout': 45},
        'nyfed_primary_dealer_urls': [
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx", "file_pattern": "prideal2023.xlsx", "format_type": "TEST_PD_FORMAT"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2022.xlsx", "file_pattern": "prideal2022.xlsx", "format_type": "TEST_PD_FORMAT"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/non_existent_file_for_test.xlsx", "file_pattern": "non_existent.xlsx", "format_type": "TEST_PD_FORMAT"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2021.xlsx", "file_pattern": "prideal2021.xlsx", "format_type": "UNKNOWN_RECIPE"}
        ],
        'nyfed_format_recipes': {
            "TEST_PD_FORMAT": {
                "header_row": 3,
                "date_column": "As of Date",
                "columns_to_sum": [
                    "U.S. Treasury coupons", "U.S. Treasury bills",
                    "U.S. Treasury floating rate notes (FRNs)", "NonExistentColumnForTest" # Include a non-existent column for robustness testing
                ],
                "data_unit_multiplier": 1000 # Test with a different multiplier
            }
        }
    }

    test_logger_ny.info("--- Starting NYFedConnector Test ---")
    ny_conn = NYFedConnector(config=test_cfg, logger_instance=test_logger_ny)
    ny_df_res, ny_err = ny_conn.fetch_data()

    if ny_err:
        test_logger_ny.error(f"NYFed Test failed with error: {ny_err}")
    elif ny_df_res is not None:
        test_logger_ny.info(f"NYFed Test successful. Fetched data shape: {ny_df_res.shape}")
        if not ny_df_res.empty:
            test_logger_ny.info(f"NYFed Data head:\n{ny_df_res.head().to_string()}")
            test_logger_ny.info(f"NYFed Data tail:\n{ny_df_res.tail().to_string()}")
            unique_dates_ny = ny_df_res['metric_date'].nunique()
            if not ny_df_res['metric_date'].empty:
                expected_days_ny = (ny_df_res['metric_date'].max() - ny_df_res['metric_date'].min()).days + 1
                if unique_dates_ny == expected_days_ny:
                    test_logger_ny.info(f"NYFed data frequency appears to be daily ({unique_dates_ny} days).")
                else:
                    test_logger_ny.warning(f"NYFed data frequency not strictly daily: {unique_dates_ny} unique dates for {expected_days_ny} day span.")
            else:
                test_logger_ny.warning("NYFed data has no dates to check frequency.")
        else:
            test_logger_ny.info("NYFed Test: Returned DataFrame is empty, as might be expected if all sources failed or had no data.")
    else:
        test_logger_ny.error("NYFed Test failed: result DataFrame is None and no error message was returned (unexpected state).")
    test_logger_ny.info("--- NYFedConnector Test Finished ---")
EOF

echo "Creating src/connectors/fred_connector.py..."
cat <<EOF > src/connectors/fred_connector.py
from fredapi import Fred
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import logging
import os # For accessing API key from environment variable

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__': # For standalone testing
        from base import BaseConnector
    else:
        raise

class FredConnector(BaseConnector):
    """
    使用 fredapi 函式庫從 FRED (Federal Reserve Economic Data) 獲取經濟數據。
    """

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        super().__init__(config, source_api_name="FRED")

        self.api_key_env_var = self.config.get('api_endpoints', {}).get('fred', {}).get('api_key_env', 'FRED_API_KEY')
        self.api_key = os.getenv(self.api_key_env_var)

        if not self.api_key:
            self.logger.critical(f"FRED API key not found in environment variable '{self.api_key_env_var}'. FREDConnector will not be able to fetch data.")
            self.fred_client = None
        else:
            try:
                self.fred_client = Fred(api_key=self.api_key)
                self.logger.info("FredConnector initialized successfully with API key.")
            except Exception as e:
                self.logger.critical(f"Failed to initialize Fred client with API key: {e}", exc_info=True)
                self.fred_client = None

    def fetch_data(self, series_ids: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        if self.fred_client is None:
            # Return an empty DataFrame with the expected schema and an error message
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), "FRED client not initialized due to missing API key or initialization error."

        if not series_ids:
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), "No series_ids provided to FredConnector."

        self.logger.info(f"Fetching FRED data for series_ids: {series_ids} from {start_date} to {end_date}.")

        all_series_data_list = []
        error_messages = []

        for series_id in series_ids:
            try:
                self.logger.debug(f"Fetching data for FRED series_id: {series_id}")
                series_data = self.fred_client.get_series(series_id, observation_start=start_date, observation_end=end_date)

                if series_data.empty:
                    self.logger.warning(f"No data returned for FRED series_id: {series_id} for the given date range.")
                    continue

                df_series = series_data.reset_index()
                df_series.columns = ['metric_date', 'metric_value']

                df_series['metric_date'] = pd.to_datetime(df_series['metric_date']).dt.date
                df_series['metric_name'] = f"FRED/{series_id}"
                df_series['source_api'] = self.source_api_name
                df_series['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                df_series['metric_value'] = pd.to_numeric(df_series['metric_value'], errors='coerce')
                df_series.dropna(subset=['metric_value'], inplace=True)

                all_series_data_list.append(df_series[['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']])
                self.logger.debug(f"Successfully fetched and processed FRED series_id: {series_id}, {len(df_series)} rows.")

            except Exception as e:
                error_msg = f"Error fetching/processing FRED series_id {series_id}: {e}"
                self.logger.error(error_msg, exc_info=True)
                error_messages.append(error_msg)

        if not all_series_data_list:
            final_error_message = "No data successfully fetched for any FRED series_ids."
            if error_messages:
                final_error_message += " Errors encountered: " + "; ".join(error_messages)
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), final_error_message

        final_df = pd.concat(all_series_data_list, ignore_index=True)

        if final_df.empty: # Should be caught if all_series_data_list is empty
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), "Combined FRED data is empty after processing all series."

        self.logger.info(f"Successfully fetched and processed {len(final_df)} total records from FRED for series_ids: {series_ids}.")

        full_error_summary = "; ".join(error_messages) if error_messages else None
        return final_df, full_error_summary


if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_fred = logging.getLogger("FredConnectorTestRun_Atomic_Main")
    if not test_logger_fred.handlers:
        ch_fred = logging.StreamHandler(sys.stdout)
        ch_fred.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_fred.addHandler(ch_fred)
        test_logger_fred.propagate = False

    sample_fred_config = {
        "api_endpoints": { "fred": { "api_key_env": "FRED_API_KEY_TEST" } } # Use a distinct env var for testing
    }

    # For testing, explicitly set the API key if you have one, or mock Fred()
    # IMPORTANT: Do not commit real API keys.
    MOCK_API_KEY_FOR_TEST = "YOUR_TEST_API_KEY_OR_MOCK"
    # os.environ["FRED_API_KEY_TEST"] = MOCK_API_KEY_FOR_TEST # FredConnector will pick this up

    if not os.getenv(sample_fred_config['api_endpoints']['fred']['api_key_env']):
        # Fallback: Try the main key if test key not set (for user convenience during dev)
        main_api_key_env = "FRED_API_KEY" # As defined in project_config.yaml template
        if os.getenv(main_api_key_env):
            test_logger_fred.warning(f"Test-specific FRED API key env var '{sample_fred_config['api_endpoints']['fred']['api_key_env']}' not set. Falling back to main key '{main_api_key_env}' for this test run.")
            os.environ[sample_fred_config['api_endpoints']['fred']['api_key_env']] = os.getenv(main_api_key_env)
        else:
             test_logger_fred.error(f"Cannot run FredConnector test: Neither test env var '{sample_fred_config['api_endpoints']['fred']['api_key_env']}' nor main env var '{main_api_key_env}' for FRED API key is set.")
             sys.exit(1) # Exit if no key can be found for testing

    test_logger_fred.info("--- Starting FredConnector Test ---")
    fred_conn_test = FredConnector(config=sample_fred_config, logger_instance=test_logger_fred)

    if fred_conn_test.fred_client is not None:
        test_series_list = ["DGS10", "FEDFUNDS", "UNRATE", "NONEXISTENTSERIESXYZ"]
        test_start = "2023-01-01"
        test_end = "2023-02-28"

        test_logger_fred.info(f"Testing fetch_data for series: {test_series_list} from {test_start} to {test_end}")
        fred_df, fred_err = fred_conn_test.fetch_data(series_ids=test_series_list, start_date=test_start, end_date=test_end)

        if fred_err:
            test_logger_fred.warning(f"FredConnector test fetch_data completed with error(s): {fred_err}")

        if fred_df is not None and not fred_df.empty:
            test_logger_fred.info(f"FredConnector test fetch_data returned data. Shape: {fred_df.shape}")
            test_logger_fred.info(f"Result head:\\n{fred_df.head().to_string()}")
            test_logger_fred.info(f"Result tail:\\n{fred_df.tail().to_string()}")
            actual_metrics = set(fred_df['metric_name'].unique())
            test_logger_fred.info(f"Metrics returned: {actual_metrics}")
            if "FRED/NONEXISTENTSERIESXYZ" not in actual_metrics:
                test_logger_fred.info("Correctly did not return data for 'NONEXISTENTSERIESXYZ'.")
        elif fred_df is not None and fred_df.empty:
             test_logger_fred.warning("FredConnector test fetch_data returned an empty DataFrame. This might be due to all series failing or returning no data for the period.")
        else: # fred_df is None
             test_logger_fred.error(f"FredConnector test fetch_data returned None for data. Error was: {fred_err}")
    else:
        test_logger_fred.error("FredConnector client (self.fred_client) was not initialized in test. API key issue likely.")
    test_logger_fred.info("--- FredConnector Test Finished ---")
EOF

echo "Creating src/database/database_manager.py..."
cat <<EOF > src/database/database_manager.py
import duckdb
import pandas as pd
from typing import Dict, Any, Optional, List
import logging
from pathlib import Path # Ensure Path is imported
import os # Required for os.urandom

class DatabaseManager:
    """
    管理與 DuckDB 資料庫的連接和操作。
    """
    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, project_root_dir: Optional[str] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        self.db_config = config.get('database', {})
        db_path_str = self.db_config.get('path', 'data/default_financial_data.duckdb') # Default path

        # Ensure the database path is absolute or relative to a known project root
        if project_root_dir:
            self.db_file = Path(project_root_dir) / db_path_str
        else:
            # Fallback if project_root_dir is not provided (e.g. direct testing)
            # This might need adjustment based on where run_prototype.sh executes from.
            # For the atomic script, main.py should pass its PROJECT_ROOT.
            self.db_file = Path(db_path_str)
            self.logger.warning(f"project_root_dir not provided to DatabaseManager. Database path resolved to: {self.db_file.resolve()}")

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.logger.info(f"DatabaseManager initialized. DB file target: {self.db_file.resolve()}")

    def connect(self):
        """建立與 DuckDB 資料庫的連接。"""
        if self.conn is not None: # Check if connection object exists
            try:
                # Try a simple query to see if connection is active
                self.conn.execute("SELECT 1")
                self.logger.info("Database connection already active and valid.")
                return
            except Exception as e: # duckdb.duckdb.ConnectionException or similar if closed/broken
                self.logger.warning(f"Existing connection object found but it's not usable ({e}). Will try to reconnect.")
                self.conn = None # Reset to force reconnection

        try:
            # Ensure the parent directory for the database file exists
            self.db_file.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(database=str(self.db_file), read_only=False)
            self.logger.info(f"Successfully connected to DuckDB database: {self.db_file.resolve()}")
            self._create_tables_if_not_exist() # Create tables upon connection
        except Exception as e:
            self.logger.critical(f"Failed to connect to DuckDB database at {self.db_file.resolve()}: {e}", exc_info=True)
            self.conn = None # Ensure conn is None if connection fails
            raise # Re-raise the exception to signal failure to the caller

    def disconnect(self):
        """關閉資料庫連接。"""
        if self.conn is not None:
            try:
                self.logger.info(f"Attempting to CHECKPOINT database before disconnecting: {self.db_file.resolve()}")
                self.conn.execute("CHECKPOINT;")
                self.logger.info(f"Successfully CHECKPOINTED database: {self.db_file.resolve()}")

                # Perform a dummy read operation
                try:
                    count_after_checkpoint = self.conn.execute("SELECT COUNT(*) FROM log_ai_decision;").fetchone()
                    if count_after_checkpoint:
                        self.logger.info(f"Dummy read from log_ai_decision after CHECKPOINT returned count: {count_after_checkpoint[0]}")
                    else:
                        self.logger.warning("Dummy read from log_ai_decision after CHECKPOINT returned no result.")
                except Exception as e_read:
                    self.logger.error(f"Error during dummy read after CHECKPOINT: {e_read}", exc_info=True)

                self.conn.close()
                self.logger.info(f"Disconnected from DuckDB database: {self.db_file.resolve()}")
            except Exception as e:
                self.logger.error(f"Error during CHECKPOINT or closing DuckDB connection: {e}", exc_info=True)
        else:
            self.logger.info("Database connection already None or not established.")
        self.conn = None


    def _create_tables_if_not_exist(self):
        """如果表不存在，則創建它們。"""
        if self.conn is None:
            self.logger.error("Cannot create tables: Database connection is None.")
            return

        try:
            self.logger.info("Dropping and recreating tables to ensure fresh schema...")
            self.conn.execute("DROP TABLE IF EXISTS fact_macro_economic_data;")
            self.conn.execute("DROP TABLE IF EXISTS fact_stock_price;")
            self.logger.info("Old tables (if any) dropped.")

            # Schema for fact_macro_economic_data
            # metric_date: DATE, metric_name: VARCHAR, metric_value: DOUBLE,
            # source_api: VARCHAR, data_snapshot_timestamp: TIMESTAMP
            self.conn.execute("""
                CREATE TABLE fact_macro_economic_data (
                    metric_date DATE,
                    metric_name VARCHAR,
                    metric_value DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (metric_date, metric_name, source_api) -- Assuming this combination is unique
                );
            """)
            self.logger.info("Table 'fact_macro_economic_data' checked/created.")

            # Schema for fact_stock_price
            # price_date: DATE, security_id: VARCHAR, open_price: DOUBLE, high_price: DOUBLE,
            # low_price: DOUBLE, close_price: DOUBLE, adj_close_price: DOUBLE, volume: BIGINT,
            # dividends: DOUBLE, stock_splits: DOUBLE, source_api: VARCHAR, data_snapshot_timestamp: TIMESTAMP
            self.conn.execute("""
                CREATE TABLE fact_stock_price (
                    price_date DATE,
                    security_id VARCHAR,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    adj_close_price DOUBLE,
                    volume BIGINT,
                    dividends DOUBLE,
                    stock_splits DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (price_date, security_id, source_api) -- Assuming this combination is unique
                );
            """)
            self.logger.info("Table 'fact_stock_price' checked/created.")

            # Schema for log_ai_decision
            self.conn.execute("""
                CREATE TABLE log_ai_decision (
                    decision_id VARCHAR DEFAULT uuid(), -- Auto-generated unique ID
                    decision_date DATE,
                    stress_index_value DOUBLE,
                    stress_index_trend VARCHAR,
                    strategy_summary VARCHAR,
                    key_factors VARCHAR, -- Store as JSON string if it's a list
                    confidence_score DOUBLE,
                    raw_ai_response TEXT, -- Can store larger text like full JSON or error message
                    briefing_json TEXT, -- Store the market_snapshot JSON used for the decision
                    decision_timestamp TIMESTAMP DEFAULT current_timestamp,
                    PRIMARY KEY (decision_id),
                    UNIQUE (decision_date) -- Assuming one decision per date for this system
                );
            """)
            self.logger.info("Table 'log_ai_decision' checked/created.")
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}", exc_info=True)
            # Depending on severity, might want to raise this

    def bulk_insert_or_replace(self, table_name: str, df: pd.DataFrame, unique_cols: List[str]):
        """
        將 DataFrame 中的數據批量插入或替換到指定的表中。
        使用 DuckDB 的 INSERT ... ON CONFLICT DO UPDATE (upsert) 功能。
        """
        if self.conn is None:
            self.logger.error(f"Cannot insert into {table_name}: Database connection is None.")
            return False
        if df.empty:
            self.logger.info(f"DataFrame for table {table_name} is empty. Nothing to insert.")
            return True # Not an error, just nothing to do

        self.logger.debug(f"Attempting to bulk insert/replace into {table_name}, {len(df)} rows. Unique cols: {unique_cols}")

        try:
            # Ensure DataFrame columns match table schema and have correct types if necessary.
            # DuckDB is quite good at type inference from Pandas, but explicit casting might be needed for complex cases.
            # For date/timestamp, ensure they are in a compatible format.
            # Example: df['metric_date'] = pd.to_datetime(df['metric_date']).dt.date
            # This should ideally be handled by the connectors before this stage.

            # Register DataFrame as a temporary table
            temp_table_name = f"temp_{table_name}_{os.urandom(4).hex()}" # Unique temp table name
            self.conn.register(temp_table_name, df)

            # Build the ON CONFLICT part of the SQL query
            if not unique_cols:
                raise ValueError("unique_cols must be provided for upsert operation.")

            conflict_target = ", ".join(unique_cols)

            # Build the SET part for DO UPDATE
            # Exclude unique_cols from update as they are used for conflict resolution
            update_cols = [col for col in df.columns if col not in unique_cols]
            if not update_cols: # If all columns are part of unique_cols, it's effectively an INSERT OR IGNORE
                 set_clause = "NOTHING" # Placeholder for DO NOTHING, adjust if needed
                 # For DuckDB, if all columns are unique keys, an insert on conflict would just do nothing.
                 # A more explicit "DO NOTHING" might be:
                 # INSERT INTO target_table SELECT * FROM source_table ON CONFLICT (unique_cols) DO NOTHING;
                 # However, we'll try to update other columns if they exist.
                 # If no columns to update, then an insert that conflicts will do nothing.
                 # To be safe, if update_cols is empty, we can simply do an INSERT OR IGNORE.
                 # For now, let's assume there's always at least one column to update or this case is handled by table design.
                 self.logger.warning(f"No columns to update for table {table_name} as all columns are in unique_cols. Conflicting rows will be ignored.")
                 # Simple insert, relying on PK to prevent duplicates if that's the desired behavior without explicit update
                 # This part needs careful consideration based on exact "replace" semantics desired.
                 # A common approach for "replace" with DuckDB is to delete and insert.
                 # This part needs careful consideration based on exact "replace" semantics desired.
                 # A common approach for "replace" with DuckDB is to delete and insert.
                 # Let's use the upsert functionality.

                # The logic is: if no columns to update (all are unique keys), then DO NOTHING on conflict.
                # Otherwise, DO UPDATE the non-key columns.
                 sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO NOTHING;"
                 self.logger.debug(f"Executing SQL (INSERT OR IGNORE style as no update_cols): {sql}")
            else: # There are columns to update
                set_statements = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
                sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO UPDATE SET {set_statements};"
                self.logger.debug(f"Executing SQL (UPSERT style): {sql}")

            self.conn.execute(sql) # This should be at the same indentation level as the if/else that defines sql
            self.conn.unregister(temp_table_name) # Clean up temporary table
            self.logger.info(f"Successfully inserted/replaced {len(df)} rows into {table_name}.")
            self.conn.execute("CHECKPOINT;") # Force flush to disk
            self.logger.info(f"Executed CHECKPOINT after upsert into {table_name}.")
            return True
        except Exception as e:
            self.logger.error(f"Error during bulk insert/replace into {table_name}: {e}", exc_info=True)
            # Attempt to unregister temp table even on error
            if 'temp_table_name' in locals() and self.conn.table(temp_table_name) is not None: # Check if temp table exists
                try:
                    self.conn.unregister(temp_table_name)
                except Exception as e_unreg:
                    self.logger.error(f"Failed to unregister temp table {temp_table_name} on error: {e_unreg}")
            return False

    def fetch_all_for_engine(self, table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_column: str = 'metric_date') -> Optional[pd.DataFrame]:
        """
        從指定的表中獲取所有數據，可選地按日期範圍過濾。
        """
        if self.conn is None:
            self.logger.error(f"Cannot fetch from {table_name}: Database connection is None.")
            return None

        self.logger.debug(f"Fetching all data for engine from {table_name}, date_col: {date_column}, start: {start_date}, end: {end_date}")

        query = f"SELECT * FROM {table_name}"
        params = []
        conditions = []

        if start_date:
            conditions.append(f"{date_column} >= ?")
            params.append(start_date)
        if end_date:
            conditions.append(f"{date_column} <= ?")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY {date_column}"

        try:
            result_df = self.conn.execute(query, params).fetchdf()
            self.logger.info(f"Successfully fetched {len(result_df)} rows from {table_name}.")
            return result_df
        except Exception as e:
            self.logger.error(f"Error fetching data from {table_name}: {e}", exc_info=True)
            return None

    def execute_query(self, query: str, params: Optional[list] = None) -> Optional[pd.DataFrame]:
        """執行一個自定義的 SQL 查詢並返回結果為 DataFrame。"""
        if self.conn is None:
            self.logger.error("Cannot execute query: Database connection is None.")
            return None
        try:
            self.logger.debug(f"Executing custom query: {query} with params: {params}")
            return self.conn.execute(query, params).fetchdf()
        except Exception as e:
            self.logger.error(f"Error executing custom query '{query}': {e}", exc_info=True)
            return None

    def close(self): # Alias for disconnect for convenience
        self.disconnect()

if __name__ == '__main__':
    # This __main__ block is for basic, standalone testing of DatabaseManager.
    # It will create a DuckDB file in the current directory or a 'data' subdirectory.

    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_db = logging.getLogger("DatabaseManagerTestRun_Atomic")
    if not test_logger_db.handlers:
        ch_db = logging.StreamHandler(sys.stdout)
        ch_db.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_db.addHandler(ch_db)
        test_logger_db.propagate = False

    # Test configuration for the database
    # Assume this script is run from the project root where 'data/' would be created.
    # For atomic script, PROJECT_ROOT will be passed from main.py
    test_db_config = {
        "database": {
            "path": "data/test_financial_data_atomic.duckdb"
        }
    }
    # Determine project root for test (assuming this test is run from project root)
    test_project_root = str(Path(".").resolve())

    test_logger_db.info("--- Starting DatabaseManager Test ---")
    # Clean up old test DB if it exists
    old_db_file = Path(test_project_root) / test_db_config["database"]["path"]
    if old_db_file.exists():
        test_logger_db.info(f"Deleting old test database: {old_db_file}")
        old_db_file.unlink()

    db_man = DatabaseManager(config=test_db_config, logger_instance=test_logger_db, project_root_dir=test_project_root)

    try:
        db_man.connect()
        assert db_man.conn is not None, "Connection failed"
        test_logger_db.info("DB Connection successful.")

        # Test table creation (should happen in connect)
        test_logger_db.info("Checking if tables were created...")
        tables_df = db_man.execute_query("SHOW TABLES;")
        assert tables_df is not None, "SHOW TABLES query failed"
        test_logger_db.info(f"Tables in DB:\n{tables_df}")
        table_names = tables_df['name'].tolist()
        assert 'fact_macro_economic_data' in table_names, "fact_macro_economic_data not created"
        assert 'fact_stock_price' in table_names, "fact_stock_price not created"
        test_logger_db.info("Table creation check passed.")

        # Test bulk_insert_or_replace for macro data
        macro_sample_data = {
            'metric_date': [datetime(2023,1,1).date(), datetime(2023,1,2).date(), datetime(2023,1,1).date()],
            'metric_name': ['FRED/DGS10', 'FRED/DGS10', 'FRED/UNRATE'],
            'metric_value': [2.5, 2.6, 3.5],
            'source_api': ['FRED', 'FRED', 'FRED'],
            'data_snapshot_timestamp': [datetime.now(timezone.utc)] * 3
        }
        macro_df_test = pd.DataFrame(macro_sample_data)
        test_logger_db.info(f"\nInserting macro data (1st time):\n{macro_df_test}")
        success_macro_insert1 = db_man.bulk_insert_or_replace('fact_macro_economic_data', macro_df_test, unique_cols=['metric_date', 'metric_name', 'source_api'])
        assert success_macro_insert1, "First macro insert failed"

        fetched_macro1 = db_man.fetch_all_for_engine('fact_macro_economic_data')
        assert fetched_macro1 is not None and len(fetched_macro1) == 3, f"Expected 3 rows after 1st macro insert, got {len(fetched_macro1) if fetched_macro1 is not None else 'None'}"
        test_logger_db.info(f"Macro data after 1st insert ({len(fetched_macro1)} rows):\n{fetched_macro1}")

        # Test upsert: update one row, insert a new one
        macro_update_data = {
            'metric_date': [datetime(2023,1,1).date(), datetime(2023,1,3).date()], # Update DGS10 on 2023-01-01, new DGS10 on 2023-01-03
            'metric_name': ['FRED/DGS10', 'FRED/DGS10'],
            'metric_value': [2.55, 2.7], # Updated value, new value
            'source_api': ['FRED', 'FRED'],
            'data_snapshot_timestamp': [datetime.now(timezone.utc)] * 2
        }
        macro_df_update = pd.DataFrame(macro_update_data)
        test_logger_db.info(f"\nUpserting macro data (update 1, insert 1):\n{macro_df_update}")
        success_macro_upsert = db_man.bulk_insert_or_replace('fact_macro_economic_data', macro_df_update, unique_cols=['metric_date', 'metric_name', 'source_api'])
        assert success_macro_upsert, "Macro upsert failed"

        fetched_macro2 = db_man.fetch_all_for_engine('fact_macro_economic_data')
        assert fetched_macro2 is not None and len(fetched_macro2) == 4, f"Expected 4 rows after macro upsert, got {len(fetched_macro2) if fetched_macro2 is not None else 'None'}"
        test_logger_db.info(f"Macro data after upsert ({len(fetched_macro2)} rows):\n{fetched_macro2}")
        # Check updated value
        updated_val = fetched_macro2[(fetched_macro2['metric_date'] == datetime(2023,1,1).date()) & (fetched_macro2['metric_name'] == 'FRED/DGS10')]['metric_value'].iloc[0]
        assert updated_val == 2.55, f"Expected updated DGS10 value to be 2.55, got {updated_val}"
        test_logger_db.info("Macro data upsert successful.")

        # Test fetch_all_for_engine with date range
        fetched_macro_ranged = db_man.fetch_all_for_engine('fact_macro_economic_data', start_date='2023-01-02', end_date='2023-01-03')
        assert fetched_macro_ranged is not None and len(fetched_macro_ranged) == 2, f"Expected 2 rows in date range, got {len(fetched_macro_ranged) if fetched_macro_ranged is not None else 'None'}"
        test_logger_db.info(f"Macro data for 2023-01-02 to 2023-01-03 ({len(fetched_macro_ranged)} rows):\n{fetched_macro_ranged}")

        test_logger_db.info("DatabaseManager tests passed successfully.")

    except Exception as e_test:
        test_logger_db.error(f"DatabaseManager test failed: {e_test}", exc_info=True)
    finally:
        db_man.disconnect()
        test_logger_db.info("--- DatabaseManager Test Finished ---")
        # Optional: delete the test database file after test
        # if old_db_file.exists():
        #     test_logger_db.info(f"Deleting test database after run: {old_db_file}")
        #     old_db_file.unlink(missing_ok=True)
EOF

echo "Creating src/connectors/yfinance_connector.py..."
cat <<EOF > src/connectors/yfinance_connector.py
import yfinance as yf
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import logging
import sys
import requests # For session type hint, though not strictly used in __init__ here

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__':
        from base import BaseConnector
    else:
        raise

class YFinanceConnector(BaseConnector):
    """使用 yfinance 獲取股價和指數數據。"""

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, session: Optional[requests.Session] = None): # session param kept for interface consistency if needed later
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        super().__init__(config, source_api_name="yfinance")
        # self.requests_session = session # Not actively used by yfinance Ticker object directly in its constructor like some other libs

    def fetch_data(self, tickers: List[str], start_date: str, end_date: Optional[str] = None,
                   interval: str = "1d", **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        self.logger.info(f"Fetching yfinance data for tickers: {tickers} from {start_date} to {end_date} with interval {interval}.")

        if not tickers:
            self.logger.warning("No tickers provided to YFinanceConnector fetch_data.")
            # Return DataFrame with all expected columns for consistency
            final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                               'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                               'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=final_cols_spec), "No tickers provided."

        all_ticker_data_list = []
        # yfinance's Ticker object can accept a session for underlying requests,
        # but it's often managed internally or via its own mechanisms.
        # For this script, we'll let yfinance handle its session management unless a specific need arises.
        # session_to_use = kwargs.get('session', self.requests_session)

        for ticker_symbol in tickers:
            self.logger.debug(f"Fetching yfinance data for: {ticker_symbol}")
            try:
                ticker_obj = yf.Ticker(ticker_symbol) # Let yf.Ticker manage its session

                hist_df = ticker_obj.history(
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    auto_adjust=False, # Important to get 'Adj Close' and 'Close' separately if needed, and splits/dividends
                    actions=True,      # To get dividends and stock splits
                    # progress=False,    # Removed: yfinance 0.2.x no longer supports 'progress' arg here
                )

                if hist_df.empty:
                    self.logger.warning(f"yfinance returned no data for ticker: {ticker_symbol} (start: {start_date}, end: {end_date}, interval: {interval}).")
                    continue

                hist_df.reset_index(inplace=True)

                # Determine the correct date column name (yfinance can vary this)
                date_col_name = None
                if 'Datetime' in hist_df.columns: date_col_name = 'Datetime' # Usually for intraday
                elif 'Date' in hist_df.columns: date_col_name = 'Date'       # Usually for daily

                if not date_col_name:
                    self.logger.error(f"Date column ('Date' or 'Datetime') not found in yfinance data for {ticker_symbol}. Columns: {hist_df.columns.tolist()}")
                    continue

                # Standardize column names
                rename_map = {
                    date_col_name: 'price_date', 'Open': 'open_price', 'High': 'high_price',
                    'Low': 'low_price', 'Close': 'close_price', 'Adj Close': 'adj_close_price',
                    'Volume': 'volume', 'Dividends': 'dividends', 'Stock Splits': 'stock_splits'
                }
                # Only rename columns that exist in the DataFrame
                current_rename_map = {k: v for k, v in rename_map.items() if k in hist_df.columns}
                df_renamed = hist_df.rename(columns=current_rename_map)

                # Convert price_date to just date (YYYY-MM-DD), removing time and timezone
                df_renamed['price_date'] = pd.to_datetime(df_renamed['price_date'])
                if df_renamed['price_date'].dt.tz is not None: # If timezone-aware
                    df_renamed['price_date'] = df_renamed['price_date'].dt.tz_localize(None) # Make timezone-naive
                df_renamed['price_date'] = df_renamed['price_date'].dt.normalize().dt.date # Get date part

                # Add standard metadata columns
                df_renamed['security_id'] = ticker_symbol
                df_renamed['source_api'] = self.source_api_name
                df_renamed['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                # Ensure all expected final columns are present
                final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                                   'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                                   'source_api', 'data_snapshot_timestamp']

                for fc_col in final_cols_spec:
                    if fc_col not in df_renamed.columns:
                        # Default to 0.0 for dividends/splits, NA for others
                        default_val = 0.0 if fc_col in ['dividends', 'stock_splits'] else pd.NA
                        df_renamed[fc_col] = default_val

                all_ticker_data_list.append(df_renamed[final_cols_spec])
                self.logger.debug(f"Processed yfinance data for {ticker_symbol}, {len(df_renamed)} rows.")

            except Exception as e: # Catch broader exceptions from yfinance
                self.logger.error(f"Error fetching/processing yfinance for {ticker_symbol}: {e}", exc_info=True)

        if not all_ticker_data_list:
            self.logger.warning(f"No data successfully fetched for any yfinance tickers: {tickers}")
            final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                               'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                               'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=final_cols_spec), f"No data from yfinance for any of the tickers: {tickers}."

        final_df = pd.concat(all_ticker_data_list, ignore_index=True)

        if final_df.empty: # Should be caught by the above, but as a safeguard
             self.logger.warning("Final combined yfinance data is empty (all tickers failed or returned no data).")
             return final_df, "Final combined yfinance data is empty."

        # Ensure correct dtypes for numeric columns
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'dividends', 'stock_splits']
        for col_to_num in numeric_cols:
            if col_to_num in final_df.columns:
                final_df[col_to_num] = pd.to_numeric(final_df[col_to_num], errors='coerce')
        if 'volume' in final_df.columns: # Volume should be integer
            final_df['volume'] = pd.to_numeric(final_df['volume'], errors='coerce').astype('Int64') # Use nullable Int64

        self.logger.info(f"Successfully fetched and processed {len(final_df)} total records from yfinance for tickers: {tickers}.")
        return final_df, None


if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_yf = logging.getLogger("YFinanceConnectorTestRun_Atomic")
    if not test_logger_yf.handlers:
        ch_yf = logging.StreamHandler(sys.stdout)
        ch_yf.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_yf.addHandler(ch_yf)
        test_logger_yf.propagate = False

    sample_config_yf = {} # YFinanceConnector doesn't use much from config in this version
    yf_connector = YFinanceConnector(config=sample_config_yf, logger_instance=test_logger_yf)

    test_logger_yf.info("\n--- Testing YFinanceConnector for ^MOVE ---")
    move_df, move_err = yf_connector.fetch_data(tickers=["^MOVE"], start_date="2024-01-01", end_date="2024-01-15")
    if move_err:
        test_logger_yf.error(f"^MOVE Test Error: {move_err}")
    elif move_df is not None:
        test_logger_yf.info(f"^MOVE Test OK. Shape: {move_df.shape}")
        if not move_df.empty: test_logger_yf.info(f"^MOVE Head:\n{move_df.head().to_string()}")

    test_logger_yf.info("\n--- Testing YFinanceConnector for AAPL, NONEXISTENTTICKERXYZ ---")
    # Test with a mix of valid and potentially invalid tickers
    mixed_tickers = ["AAPL", "NONEXISTENTTICKERXYZ"]
    stocks_df, stocks_err = yf_connector.fetch_data(tickers=mixed_tickers, start_date="2024-01-01", end_date="2024-01-05")
    if stocks_err: # An error message might be returned if ALL fail, or partial data with warnings logged
        test_logger_yf.warning(f"Mixed Stocks Test potentially completed with issues: {stocks_err}")

    if stocks_df is not None:
        test_logger_yf.info(f"Mixed Stocks Test Data Shape: {stocks_df.shape}")
        if not stocks_df.empty:
            test_logger_yf.info(f"Mixed Stocks Data Head:\n{stocks_df.head().to_string()}")
            unique_tickers_found_mixed = stocks_df['security_id'].unique()
            test_logger_yf.info(f"Found data for tickers: {unique_tickers_found_mixed}")
            if "AAPL" in unique_tickers_found_mixed:
                test_logger_yf.info("AAPL data was found.")
            if "NONEXISTENTTICKERXYZ" not in unique_tickers_found_mixed:
                 test_logger_yf.info("NONEXISTENTTICKERXYZ correctly did not return data or was skipped.")
        else:
            test_logger_yf.info("Mixed Stocks Test returned an empty DataFrame (e.g., if AAPL also had no data for the period or all failed).")

    test_logger_yf.info("\n--- Testing YFinanceConnector with empty ticker list ---")
    empty_df, empty_err = yf_connector.fetch_data(tickers=[], start_date="2024-01-01")
    if empty_err == "No tickers provided." and (empty_df is not None and empty_df.empty):
        test_logger_yf.info(f"OK (empty ticker list): Error='{empty_err}', DataFrame is empty as expected.")
    else:
        test_logger_yf.error(f"Fail (empty ticker list): err='{empty_err}', df_empty={empty_df.empty if empty_df is not None else 'N/A'}")

    test_logger_yf.info("--- YFinanceConnector Test Finished ---")
EOF

echo "Creating src/engine/indicator_engine.py..."
cat <<EOF > src/engine/indicator_engine.py
import pandas as pd
from typing import Dict, Any, Optional # Ensure Optional is imported
import numpy as np
import logging
import sys # Not strictly necessary for this script's direct functionality but good practice if other parts use it

# Basic logger setup for the module, will be overridden if a logger_instance is passed to the class
# For atomic script, this logger will likely write to wherever the global logger (from main.py/initialize_global_log) is configured.
logger = logging.getLogger(f"project_logger.{__name__}")
if not logger.handlers and not logging.getLogger().hasHandlers(): # Check root logger too
    logger.addHandler(logging.NullHandler())
    logger.debug(f"Logger for {__name__} (IndicatorEngine module) configured with NullHandler for atomic script.")

class IndicatorEngine:
    """
    封裝計算衍生指標，特別是「債券壓力指標」的邏輯。
    """
    def __init__(self, data_frames: Dict[str, pd.DataFrame], params: Optional[Dict[str, Any]] = None, logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            # Fallback to a module-specific logger if no instance is provided
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                 self.logger.addHandler(logging.NullHandler())
                 self.logger.debug(f"Instance logger for {self.__class__.__name__} using NullHandler for atomic script.")

        self.raw_macro_df = data_frames.get('macro', pd.DataFrame()) # Default to empty DF
        self.raw_move_df = data_frames.get('move', pd.DataFrame())   # Default to empty DF
        self.params = params if params is not None else {}
        self.df_prepared: Optional[pd.DataFrame] = None

        if self.raw_macro_df.empty:
            self.logger.warning("IndicatorEngine initialized: 'macro' data is missing or empty.")
        if self.raw_move_df.empty:
            self.logger.warning("IndicatorEngine initialized: 'move' data (for ^MOVE) is missing or empty.")

    def _prepare_data(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Preparing data for stress index calculation...")

        if self.raw_macro_df.empty:
            self.logger.warning("IndicatorEngine: Macro data (raw_macro_df) is empty. Proceeding without macro indicators for pivot.")
            # Create an empty DataFrame with a DatetimeIndex if MOVE data might exist, to allow merging
            # However, if MOVE is also empty, this won't help much.
            # Consider the case where only MOVE data is present.
            if self.raw_move_df.empty:
                self.logger.error("IndicatorEngine: Both macro and MOVE data are empty. Cannot prepare data.")
                return None
            # If only MOVE data is present, macro_wide_df will be effectively empty or non-existent
            # and combined_df logic should handle it.
            macro_wide_df = pd.DataFrame()
        else:
            try:
                current_macro_df = self.raw_macro_df.copy()
                if 'metric_date' not in current_macro_df.columns:
                    self.logger.error("IndicatorEngine: 'metric_date' column missing in macro data.")
                    return None
                current_macro_df['metric_date'] = pd.to_datetime(current_macro_df['metric_date'], errors='coerce')
                current_macro_df.dropna(subset=['metric_date'], inplace=True)

                if current_macro_df.empty:
                    self.logger.error("IndicatorEngine: Macro data has no valid 'metric_date' entries after coercion.")
                    return None

                # Pivot macro data
                if not all(col in current_macro_df.columns for col in ['metric_name', 'metric_value']):
                    self.logger.error("IndicatorEngine: 'metric_name' or 'metric_value' missing for pivot.")
                    return None
                macro_wide_df = current_macro_df.pivot_table(
                    index='metric_date', columns='metric_name', values='metric_value'
                )
                macro_wide_df.index.name = 'date' # Standardize index name
                self.logger.debug(f"IndicatorEngine: Pivoted macro data shape: {macro_wide_df.shape}")
            except Exception as e:
                self.logger.error(f"IndicatorEngine: Failed to pivot macro_df: {e}", exc_info=True)
                return None

        # Prepare MOVE data
        move_wide_df = pd.DataFrame() # Initialize as empty
        if not self.raw_move_df.empty:
            if all(col in self.raw_move_df.columns for col in ['price_date', 'close_price', 'security_id']):
                move_df_filtered = self.raw_move_df[self.raw_move_df['security_id'] == '^MOVE'].copy()
                if not move_df_filtered.empty:
                    move_df_filtered['price_date'] = pd.to_datetime(move_df_filtered['price_date'], errors='coerce')
                    move_df_filtered.dropna(subset=['price_date'], inplace=True)
                    if not move_df_filtered.empty:
                        # Set index to price_date and rename close_price to ^MOVE
                        move_wide_df = move_df_filtered.set_index('price_date')[['close_price']].rename(columns={'close_price': '^MOVE'})
                        move_wide_df.index.name = 'date' # Standardize index name
                        self.logger.debug(f"IndicatorEngine: Prepared ^MOVE index data. Non-NaN count: {move_wide_df['^MOVE'].notna().sum()}")
                    else:
                        self.logger.warning("IndicatorEngine: ^MOVE data had no valid 'price_date' entries after coercion.")
                else:
                    self.logger.warning("IndicatorEngine: ^MOVE security_id not found in provided yfinance data (raw_move_df).")
            else:
                self.logger.warning("IndicatorEngine: ^MOVE DataFrame (raw_move_df) missing required columns (price_date, close_price, security_id).")
        else:
            self.logger.warning("IndicatorEngine: ^MOVE data (raw_move_df) is missing or empty. ^MOVE index will be NaN if not in macro_wide_df.")

        # Combine macro and MOVE data
        if macro_wide_df.empty and move_wide_df.empty:
            self.logger.error("IndicatorEngine: Both pivoted macro and MOVE data are empty. Cannot combine.")
            return None
        elif macro_wide_df.empty:
            combined_df = move_wide_df
            self.logger.warning("IndicatorEngine: Pivoted macro data was empty, using only MOVE data for combined_df.")
        elif move_wide_df.empty:
            combined_df = macro_wide_df
            if '^MOVE' not in combined_df.columns: # Ensure ^MOVE column exists if it's expected later
                combined_df['^MOVE'] = np.nan
            self.logger.warning("IndicatorEngine: MOVE data was empty, using only macro data for combined_df.")
        else:
            # Outer join to keep all dates, then decide on fill strategy
            combined_df = pd.merge(macro_wide_df, move_wide_df, left_index=True, right_index=True, how='outer')
            self.logger.debug(f"IndicatorEngine: Combined macro and MOVE data. Shape: {combined_df.shape}")

        if '^MOVE' not in combined_df.columns: # Ensure ^MOVE column exists after merge if it wasn't there
                combined_df['^MOVE'] = np.nan

        combined_df.sort_index(inplace=True)
        # Forward fill, then backward fill to handle NaNs robustly
        # Limit ffill/bfill to avoid excessive propagation if data is very sparse, e.g. 7 days
        combined_df = combined_df.ffill(limit=7).bfill(limit=7)
        combined_df.dropna(how='all', inplace=True) # Drop rows where all values are NaN after filling

        if combined_df.empty:
            self.logger.error("IndicatorEngine: Prepared data is empty after merge and fill operations.")
            return None

        self.logger.info(f"IndicatorEngine: Data preparation complete. Final shape: {combined_df.shape}")
        return combined_df

    def calculate_dealer_stress_index(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Calculating Dealer Stress Index...")
        # Always call _prepare_data to get the latest state based on inputs
        current_prepared_data = self._prepare_data()

        if current_prepared_data is None or current_prepared_data.empty:
            self.logger.error("IndicatorEngine: Prepared data is None or empty. Cannot calculate stress index.")
            self.df_prepared = current_prepared_data # Store the (empty) prepared data state
            return None

        # Store the successfully prepared data (potentially including ^MOVE from yfinance)
        # This df_prepared will be used for briefing if calculation is successful.
        self.df_prepared = current_prepared_data.copy()
        df = self.df_prepared.copy() # Work on a copy for calculations

        # Parameters for the index
        window = self.params.get('rolling_window_days', 252)
        weights_config = self.params.get('stress_index_weights', {})
        min_periods_ratio = self.params.get('min_periods_ratio_for_rolling', 0.5) # Ratio of window for min_periods

        # Define components and their expected column names in the prepared DataFrame
        component_map = {
            'sofr_deviation': 'FRED/SOFR_Dev',
            'spread_10y2y': 'spread_10y2y',
            'primary_dealer_position': 'NYFED/PRIMARY_DEALER_NET_POSITION', # This comes from NYFed data
            'move_index': '^MOVE',             # This comes from yfinance data
            'vix_index': 'FRED/VIXCLS',        # This comes from FRED data
            'pos_res_ratio': 'pos_res_ratio'   # Derived from FRED/WRESBAL and NYFED positions
        }
        self.logger.debug(f"IndicatorEngine: Stress Index Params: Window={window}, Weights={weights_config}, MinPeriodsRatio={min_periods_ratio}")

        # Calculate derived components first
        # 1. 10Y-2Y Spread
        if 'FRED/DGS10' in df.columns and 'FRED/DGS2' in df.columns:
            df['spread_10y2y'] = df['FRED/DGS10'] - df['FRED/DGS2']
        else:
            df['spread_10y2y'] = np.nan
            self.logger.warning("IndicatorEngine: FRED/DGS10 or FRED/DGS2 missing. 'spread_10y2y' will be NaN.")

        # 2. SOFR Deviation from its 20-day MA
        if 'FRED/SOFR' in df.columns and df['FRED/SOFR'].notna().sum() >= 20: # Need enough data for MA
             df['FRED/SOFR_MA20'] = df['FRED/SOFR'].rolling(window=20, min_periods=15).mean()
             df['FRED/SOFR_Dev'] = df['FRED/SOFR'] - df['FRED/SOFR_MA20']
        else:
            df['FRED/SOFR_Dev'] = np.nan
            self.logger.warning("IndicatorEngine: FRED/SOFR has insufficient data for 20-day MA or is missing. 'FRED/SOFR_Dev' will be NaN.")

        # 3. Primary Dealer Positions to Reserves Ratio
        if 'NYFED/PRIMARY_DEALER_NET_POSITION' in df.columns and 'FRED/WRESBAL' in df.columns:
            # Ensure WRESBAL (reserves) is not zero to avoid division by zero; replace 0 with NaN
            res_safe = df['FRED/WRESBAL'].replace(0, np.nan)
            df['pos_res_ratio'] = df['NYFED/PRIMARY_DEALER_NET_POSITION'] / res_safe
            df['pos_res_ratio'].replace([np.inf, -np.inf], np.nan, inplace=True) # Handle infinities if res_safe was NaN then became 0 through ops
        else:
            df['pos_res_ratio'] = np.nan
            self.logger.warning("IndicatorEngine: NYFED/PRIMARY_DEALER_NET_POSITION or FRED/WRESBAL missing. 'pos_res_ratio' will be NaN.")

        # Update self.df_prepared to include these newly derived columns before percentile ranking
        # This ensures that the briefing can access these intermediate calculations.
        self.df_prepared = df.copy()

        # Calculate rolling percentiles for each component
        percentiles_df = pd.DataFrame(index=df.index)
        active_component_weights = {} # Store weights of components that are actually used

        min_rolling_periods = max(2, int(window * min_periods_ratio)) # Ensure at least 2 periods

        for key, col_name in component_map.items():
            if weights_config.get(key, 0) == 0: # Skip if weight is zero
                self.logger.debug(f"IndicatorEngine: Skipping rank for {key} ({col_name}) due to zero weight.")
                percentiles_df[f"{key}_pct_rank"] = np.nan # Keep column for completeness if needed
                continue

            if col_name in df.columns and df[col_name].notna().any():
                series_to_rank = df[col_name]
                if series_to_rank.notna().sum() >= min_rolling_periods:
                    # Calculate rolling rank (percentile)
                    # rank(pct=True) gives percentile from 0 to 1. iloc[-1] takes the last value in the window.
                    rolling_percentile = series_to_rank.rolling(window=window, min_periods=min_rolling_periods).apply(
                        lambda x_window: pd.Series(x_window).rank(pct=True).iloc[-1] if pd.Series(x_window).notna().any() else np.nan,
                        raw=False # raw=False needed for DataFrames/Series with datetime index
                    )
                    # For 'spread_10y2y', lower is more stress (inverted yield curve), so invert percentile
                    percentiles_df[f"{key}_pct_rank"] = (1.0 - rolling_percentile) if key == 'spread_10y2y' else rolling_percentile
                    active_component_weights[key] = weights_config[key]
                    self.logger.debug(f"IndicatorEngine: Calculated rolling percentile for {key} ({col_name}).")
                else:
                    self.logger.warning(f"IndicatorEngine: Insufficient data for {col_name} (key: {key}) for rolling rank. Window: {window}, MinPeriods: {min_rolling_periods}, Available: {series_to_rank.notna().sum()}. Skipping rank.")
                    percentiles_df[f"{key}_pct_rank"] = np.nan
            else:
                self.logger.warning(f"IndicatorEngine: Component {key} ({col_name}) not found in prepared data or is all NaN. Skipping rank.")
                percentiles_df[f"{key}_pct_rank"] = np.nan

        if not active_component_weights:
            self.logger.error("IndicatorEngine: No active components with valid data and non-zero weights for stress index calculation.")
            return None # Or return df_prepared to show intermediate steps? For now, None if index fails.

        # Normalize active weights (so they sum to 1)
        total_active_weight = sum(active_component_weights.values())
        if total_active_weight == 0: # Should be caught by above, but safeguard
            self.logger.error("IndicatorEngine: Sum of active component weights is zero. Cannot normalize.")
            return None

        normalized_weights = {k: w / total_active_weight for k, w in active_component_weights.items()}
        self.logger.info(f"IndicatorEngine: Normalized Stress Index Weights (for active components): {normalized_weights}")

        # Calculate the weighted stress index
        # Initialize series for sum of weighted percentiles and sum of effective weights
        final_stress_index_series = pd.Series(0.0, index=df.index)
        sum_of_effective_weights = pd.Series(0.0, index=df.index)

        for component_key, weight in normalized_weights.items():
            percentile_col_name = f"{component_key}_pct_rank"
            if percentile_col_name in percentiles_df.columns and percentiles_df[percentile_col_name].notna().any():
                # Fill NaNs in percentile ranks with 0.5 (neutral) before weighting
                # This assumes that if a component's rank is missing, it contributes neutrally.
                component_contribution = percentiles_df[percentile_col_name].fillna(0.5) * weight
                final_stress_index_series = final_stress_index_series.add(component_contribution, fill_value=0)
                # Track sum of weights for rows where percentile rank was available (not NaN before fillna(0.5))
                sum_of_effective_weights = sum_of_effective_weights.add(percentiles_df[percentile_col_name].notna() * weight, fill_value=0)
            else:
                self.logger.warning(f"IndicatorEngine: Percentile rank column {percentile_col_name} for component {component_key} is missing or all NaN. This component will not contribute to the index.")

        # Adjust index for cases where some components were missing for certain dates
        # by dividing by the sum of effective weights for those dates.
        # Avoid division by zero if sum_of_effective_weights is 0 for some rows.
        adjusted_stress_index = final_stress_index_series.divide(sum_of_effective_weights.replace(0, np.nan))

        # Scale to 0-100 and clip
        final_stress_index_scaled = (adjusted_stress_index * 100).clip(0, 100)

        # Create result DataFrame
        result_df = pd.DataFrame({'DealerStressIndex': final_stress_index_scaled}, index=df.index)
        result_df = result_df.join(percentiles_df) # Join with individual percentile ranks for transparency

        # Drop rows where the final DealerStressIndex is NaN (e.g., if all components were NaN for that date)
        final_result_df = result_df.dropna(subset=['DealerStressIndex'])

        if final_result_df.empty:
            self.logger.warning("IndicatorEngine: Dealer Stress Index is all NaN after calculation and processing.")
            return None # Or an empty DataFrame with the columns?

        self.logger.info(f"IndicatorEngine: Dealer Stress Index calculated successfully. Final shape: {final_result_df.shape}")
        return final_result_df


# Test block for direct execution
if __name__ == '__main__':
    # Setup basic logging for test execution
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_eng_main = logging.getLogger("IndicatorEngineTestRun_Atomic")
    if not test_logger_eng_main.handlers:
        ch_eng_main = logging.StreamHandler(sys.stdout)
        ch_eng_main.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_eng_main.addHandler(ch_eng_main)
        test_logger_eng_main.propagate = False

    # Sample data for testing
    dates_sample = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
                                   '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10'])

    macro_data_test = {
        'metric_date': list(dates_sample) * 6, # Repeat dates for each metric
        'metric_name': (['FRED/DGS10'] * len(dates_sample) + ['FRED/DGS2'] * len(dates_sample) +
                        ['FRED/SOFR'] * len(dates_sample) + ['FRED/VIXCLS'] * len(dates_sample) +
                        ['NYFED/PRIMARY_DEALER_NET_POSITION'] * len(dates_sample) + ['FRED/WRESBAL'] * len(dates_sample)),
        'metric_value': (
            list(np.linspace(3.0, 3.5, len(dates_sample))) + # DGS10
            list(np.linspace(2.0, 2.5, len(dates_sample))) + # DGS2
            list(np.linspace(1.0, 1.2, len(dates_sample))) + # SOFR
            list(np.linspace(15, 25, len(dates_sample))) +   # VIXCLS
            list(np.linspace(1000e6, 1200e6, len(dates_sample))) + # Primary Dealer Positions (example: 1B to 1.2B)
            list(np.linspace(2.5e12, 2.7e12, len(dates_sample)))   # Reserves (example: 2.5T to 2.7T)
        )
    }
    sample_macro_df = pd.DataFrame(macro_data_test)

    move_data_test = {
        'price_date': dates_sample,
        'security_id': ['^MOVE'] * len(dates_sample),
        'close_price': np.linspace(80, 95, len(dates_sample)) # ^MOVE values
    }
    sample_move_df = pd.DataFrame(move_data_test)

    engine_params_config = {
        'rolling_window_days': 5, # Shorter window for test data
        'min_periods_ratio_for_rolling': 0.6, # Need 3 out of 5 days
        'stress_index_weights': {
            'sofr_deviation': 0.20,
            'spread_10y2y': 0.20,
            'primary_dealer_position': 0.15,
            'move_index': 0.25,
            'vix_index': 0.15,
            'pos_res_ratio': 0.05
        }
    }

    test_logger_eng_main.info("\n--- Test IndicatorEngine Full Calculation ---")
    engine_instance = IndicatorEngine(
        data_frames={'macro': sample_macro_df, 'move': sample_move_df},
        params=engine_params_config,
        logger_instance=test_logger_eng_main
    )

    stress_index_output = engine_instance.calculate_dealer_stress_index()

    if stress_index_output is not None and not stress_index_output.empty:
        test_logger_eng_main.info(f"Stress Index Output Shape: {stress_index_output.shape}")
        test_logger_eng_main.info(f"Stress Index Output Head:\n{stress_index_output.head().to_string()}")
        assert 'DealerStressIndex' in stress_index_output.columns, "Test Failed: DealerStressIndex column missing"

        # Check df_prepared for intermediate calculations
        if engine_instance.df_prepared is not None and not engine_instance.df_prepared.empty:
            test_logger_eng_main.info(f"Engine's df_prepared head (should include derived components like spread_10y2y, SOFR_Dev, pos_res_ratio):\n{engine_instance.df_prepared.head().to_string()}")
            assert 'spread_10y2y' in engine_instance.df_prepared.columns, "Test Failed: spread_10y2y missing in df_prepared"
            assert 'FRED/SOFR_Dev' in engine_instance.df_prepared.columns, "Test Failed: FRED/SOFR_Dev missing in df_prepared"
            assert 'pos_res_ratio' in engine_instance.df_prepared.columns, "Test Failed: pos_res_ratio missing in df_prepared"
            assert '^MOVE' in engine_instance.df_prepared.columns, "Test Failed: ^MOVE missing in df_prepared"
        else:
            test_logger_eng_main.error("Test Failed: engine_instance.df_prepared is None or empty after calculation.")
    elif stress_index_output is not None and stress_index_output.empty:
         test_logger_eng_main.warning("Stress Index calculation resulted in an empty DataFrame.")
    else: # stress_index_output is None
        test_logger_eng_main.error("Stress Index calculation failed and returned None.")

    test_logger_eng_main.info("\n--- Test with missing MOVE data ---")
    engine_no_move = IndicatorEngine(
        data_frames={'macro': sample_macro_df, 'move': pd.DataFrame()}, # Empty move DataFrame
        params=engine_params_config,
        logger_instance=test_logger_eng_main
    )
    stress_no_move_output = engine_no_move.calculate_dealer_stress_index()
    if stress_no_move_output is not None:
        test_logger_eng_main.info(f"Stress Index (no MOVE) Shape: {stress_no_move_output.shape}")
        if 'move_index_pct_rank' in stress_no_move_output.columns:
             assert stress_no_move_output['move_index_pct_rank'].isna().all(), "MOVE percentile should be all NaN if MOVE data missing"
        test_logger_eng_main.info(f"Stress Index (no MOVE) Head:\n{stress_no_move_output.head().to_string()}")
        if engine_no_move.df_prepared is not None:
             assert ('^MOVE' not in engine_no_move.df_prepared or engine_no_move.df_prepared['^MOVE'].isna().all()), "df_prepared should reflect missing MOVE"

    test_logger_eng_main.info("\n--- Test with completely empty input ---")
    engine_empty_all = IndicatorEngine(
        data_frames={'macro': pd.DataFrame(), 'move': pd.DataFrame()},
        params=engine_params_config,
        logger_instance=test_logger_eng_main
    )
    stress_empty_all_output = engine_empty_all.calculate_dealer_stress_index()
    assert stress_empty_all_output is None, "Expected None for completely empty input"
    test_logger_eng_main.info(f"Stress Index (empty input): {'None as expected' if stress_empty_all_output is None else 'FAIL: Unexpectedly got data'}")

    test_logger_eng_main.info("--- IndicatorEngine Test Finished ---")
EOF

echo "Creating src/scripts/initialize_global_log.py..."
cat <<EOF > src/scripts/initialize_global_log.py
import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import sys
from typing import Optional, Any # Added Any for potential use in kwargs

LOG_DIR_NAME = "api_test_logs" # Default log directory name within the project root
LOG_FILE_PATH: Optional[str] = None
_global_logger_initialized_flag = False # Flag to check if initialize_log_file has run successfully

# Bootstrap logger for issues during the logging setup itself or before it's fully set up.
_bootstrap_logger = logging.getLogger("BootstrapLogger")
if not _bootstrap_logger.handlers and not logging.getLogger().hasHandlers(): # Avoid adding handlers multiple times
    _ch_bootstrap = logging.StreamHandler(sys.stdout)
    _ch_bootstrap.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s (bootstrap)'))
    _bootstrap_logger.addHandler(_ch_bootstrap)
    _bootstrap_logger.setLevel(logging.INFO) # Bootstrap logs info and above to console
    _bootstrap_logger.propagate = False # Don't pass bootstrap messages to the root logger if it gets configured later

def get_taipei_time() -> datetime:
    """Returns the current time in Taipei timezone (UTC+8)."""
    return datetime.now(timezone.utc) + timedelta(hours=8)

class TaipeiTimeFormatter(logging.Formatter):
    """Custom formatter to add Taipei time to log records."""
    def format(self, record: logging.LogRecord) -> str:
        record.taipei_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        return super().format(record)

def initialize_log_file(
    log_dir_override: Optional[str] = None,
    force_reinit: bool = False,
    project_root_path: Optional[Path] = None
) -> Optional[str]:
    """
    Initializes a global file logger and a console logger for the application.
    The file logger will save all DEBUG level messages and above.
    The console logger will show INFO level messages and above.
    Logs are stored in a timestamped file within LOG_DIR_NAME (default 'api_test_logs').

    Args:
        log_dir_override: Optional path to a directory where logs should be stored.
                          If None, defaults to 'api_test_logs' under project_root_path.
        force_reinit: If True, removes existing handlers from the root logger and re-adds them.
                      Useful if settings need to change or in test environments.
        project_root_path: Optional Path object to the project's root directory.
                           If None, attempts to infer it (e.g., parent of this script's dir).

    Returns:
        The full path to the initialized log file if successful, otherwise None.
    """
    global LOG_FILE_PATH, _global_logger_initialized_flag

    # Determine the project root path if not provided
    current_project_root: Path
    if project_root_path:
        current_project_root = project_root_path
    else:
        try:
            # Assumes this script is in a 'scripts' subdirectory of the project root
            current_project_root = Path(__file__).resolve().parent.parent
        except NameError: # __file__ might not be defined in some execution contexts (e.g. interactive)
            current_project_root = Path(".").resolve() # Fallback to current working directory
            _bootstrap_logger.warning(f"__file__ not defined, using CWD '{current_project_root}' as project root for log path determination.")

    # Determine the log directory path
    current_log_dir_path: Path
    if log_dir_override:
        current_log_dir_path = Path(log_dir_override)
    else:
        current_log_dir_path = current_project_root / LOG_DIR_NAME

    # Check if logger is already initialized with the same log directory
    if _global_logger_initialized_flag and not force_reinit and LOG_FILE_PATH:
        # Check if the existing log file's parent directory matches the current target log directory
        if Path(LOG_FILE_PATH).parent == current_log_dir_path.resolve():
            _bootstrap_logger.debug(f"Global logger already initialized. Log file: {LOG_FILE_PATH}")
            return LOG_FILE_PATH
        else:
            _bootstrap_logger.warning(
                f"Log directory has changed or re-initialization is forced. "
                f"Old log dir: {Path(LOG_FILE_PATH).parent}, New log dir: {current_log_dir_path.resolve()}. Forcing re-init."
            )
            force_reinit = True # Force re-init if log directory changed

    try:
        current_log_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _bootstrap_logger.error(f"Failed to create log directory '{current_log_dir_path}': {e}", exc_info=True)
        return None

    # Create a timestamped log file name
    utc_now = datetime.now(timezone.utc)
    timestamp_filename_str = utc_now.strftime("%Y-%m-%dT%H%M%SZ") # ISO-like timestamp for filename
    log_filename = f"{timestamp_filename_str}_application_log.txt" # More descriptive name
    current_log_file_full_path = current_log_dir_path / log_filename

    try:
        # File Handler Setup (DEBUG and above)
        file_log_format_str = '%(asctime)s (Taipei: %(taipei_time_str)s) [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
        file_formatter = TaipeiTimeFormatter(file_log_format_str) # Use custom formatter for Taipei time
        file_handler = logging.FileHandler(current_log_file_full_path, mode='w', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)

        # Console Handler Setup (INFO and above)
        console_log_format_str = '[%(levelname)s] %(name)s: %(message)s' # Simpler format for console
        console_formatter = logging.Formatter(console_log_format_str)
        console_handler = logging.StreamHandler(sys.stdout) # Log to standard output
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        # Get the root logger
        root_logger = logging.getLogger()

        # If forcing re-initialization, remove existing handlers from the root logger
        if force_reinit and root_logger.hasHandlers():
            _bootstrap_logger.info("Forcing re-initialization of root logger handlers.")
            for handler_to_remove in root_logger.handlers[:]: # Iterate over a copy
                root_logger.removeHandler(handler_to_remove)
                handler_to_remove.close() # Close handler before removing

        # Add new handlers if no handlers exist or if re-init is forced
        if not root_logger.handlers or force_reinit:
            root_logger.addHandler(file_handler)
            root_logger.addHandler(console_handler)
            root_logger.setLevel(logging.DEBUG) # Root logger captures all messages from DEBUG up

            _global_logger_initialized_flag = True
            LOG_FILE_PATH = str(current_log_file_full_path)

            # Use a specific logger for setup messages to avoid confusion with bootstrap
            logging.getLogger("GlobalLogSetup").info(f"Global logger initialized. Log file: {LOG_FILE_PATH}")
        else:
            # This case should ideally be caught by the check at the beginning of the function
            _bootstrap_logger.info("Root logger already has handlers and not forcing re-init. Current setup maintained.")
            if LOG_FILE_PATH is None: # If somehow flag was true but path was not set
                 LOG_FILE_PATH = str(current_log_file_full_path) # Attempt to set it
                 _bootstrap_logger.warning(f"LOG_FILE_PATH was None but logger seemed initialized. Set to: {LOG_FILE_PATH}")

    except Exception as e:
        _bootstrap_logger.error(f"Failed to configure logging to file '{current_log_file_full_path}': {e}", exc_info=True)
        LOG_FILE_PATH = None # Ensure path is None on failure
        _global_logger_initialized_flag = False # Reset flag on failure
        return None

    return LOG_FILE_PATH

def log_message(
    message: str,
    level: str = "INFO",
    logger_name: Optional[str] = None,
    exc_info: bool = False,
    **kwargs: Any
):
    """
    Logs a message using the globally configured logger or a bootstrap logger if not initialized.

    Args:
        message: The message string to log.
        level: The logging level (e.g., "INFO", "WARNING", "ERROR", "DEBUG").
        logger_name: Optional name for the logger. Defaults to 'project_logger.general'.
        exc_info: If True and level is ERROR/CRITICAL, exception info is added to the log.
        **kwargs: Additional keyword arguments to pass as 'extra' to the logger.
    """
    effective_logger: logging.Logger
    if not _global_logger_initialized_flag or LOG_FILE_PATH is None:
        effective_logger = _bootstrap_logger
        # Log a warning about using bootstrap only once per session for general use
        if not hasattr(log_message, "_bootstrap_warning_issued_for_general_use"):
            effective_logger.warning(
                f"Global logger not fully initialized (Log file path: {LOG_FILE_PATH}). "
                f"Logging message ('{message[:50]}...') with bootstrap logger as fallback."
            )
            setattr(log_message, "_bootstrap_warning_issued_for_general_use", True)
    else:
        effective_logger = logging.getLogger(logger_name if logger_name else "project_logger.general")

    level_upper = level.upper()
    log_level_int = logging.getLevelName(level_upper) # Get integer value of log level

    # Determine the appropriate log method (e.g., logger.info, logger.error)
    log_method = getattr(effective_logger, level_upper.lower(), effective_logger.info) # Default to .info if level invalid

    # Only pass exc_info=True if the log level is ERROR or CRITICAL
    should_pass_exc_info = exc_info and (isinstance(log_level_int, int) and log_level_int >= logging.ERROR)

    try:
        log_method(message, exc_info=should_pass_exc_info, extra=kwargs if kwargs else None)
    except Exception as e:
        # Fallback to bootstrap if the chosen logger fails for some reason
        _bootstrap_logger.error(f"Failed to log message with '{effective_logger.name}'. Original message: '{message}'. Error: {e}", exc_info=True)

# Example usage when this script is run directly
if __name__ == "__main__":
    # When this script is run directly, it should attempt to set up its own project root
    # assuming it's in a 'scripts' folder under the main project directory.
    main_script_project_root_path = Path(__file__).resolve().parent.parent

    log_file_path_main = initialize_log_file(force_reinit=True, project_root_path=main_script_project_root_path)

    if log_file_path_main:
        log_message("Info message from __main__ of initialize_global_log.", "INFO", logger_name="TestInitializeGlobalLog")
        log_message("Warning message from __main__.", "WARNING", logger_name="TestInitializeGlobalLog")
        log_message("Debug message (should go to file, not console by default).", "DEBUG", logger_name="TestInitializeGlobalLog.DebugSub")

        try:
            x = 1 / 0
        except ZeroDivisionError as e:
            # Test logging with exception information
            log_message("A ZeroDivisionError occurred during test.", "ERROR", logger_name="TestInitializeGlobalLog.ErrorSub", exc_info=True)

        log_message(f"Global log file for this direct run is confirmed at: {LOG_FILE_PATH}", "CRITICAL", logger_name="TestInitializeGlobalLog.CriticalSub")
        print(f"Script execution finished. Log file should be at: {LOG_FILE_PATH}")
    else:
        # This means initialize_log_file returned None, indicating a setup failure.
        # _bootstrap_logger should have logged the specific error.
        print("Failed to initialize the log file in __main__ of initialize_global_log. Check console for bootstrap logger errors.")

EOF

echo "Creating src/main.py..."
cat <<EOF > src/main.py
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
    from connectors.base import BaseConnector
    from connectors.fred_connector import FredConnector
    from connectors.nyfed_connector import NYFedConnector
    from connectors.yfinance_connector import YFinanceConnector
    from database.database_manager import DatabaseManager
    from engine.indicator_engine import IndicatorEngine
    from ai_agent import RemoteAIAgent, AIDecisionModel # Import AI classes

    from scripts.initialize_global_log import log_message, get_taipei_time, LOG_FILE_PATH as GLOBAL_LOG_FILE_PATH_FROM_MODULE, initialize_log_file

    global_log = log_message
    init_global_log_function = initialize_log_file
    global_log_file_path_imported = GLOBAL_LOG_FILE_PATH_FROM_MODULE # Use the path from the module
    get_taipei_time_func_imported = get_taipei_time

    if init_global_log_function is not None:
        try:
            # The global logger will create its logs in <PROJECT_ROOT>/api_test_logs/
            # PROJECT_ROOT here is the actual root where run_prototype.sh resides.
            log_dir_for_global_logger = Path(PROJECT_ROOT) / "api_test_logs"

            actual_log_file = init_global_log_function(
                log_dir_override=str(log_dir_for_global_logger),
                force_reinit=True,
                project_root_path=Path(PROJECT_ROOT)
            )
            if actual_log_file:
                global_log(f"main.py: Global application logger (from initialize_global_log) explicitly initialized. Log file: {actual_log_file}", "INFO", logger_name="MainApp.Setup")
            else:
                global_log("main.py: Global application logger initialization returned no path. Check bootstrap logs.", "ERROR", logger_name="MainApp.Setup")
        except Exception as e_log_init_main:
            pre_init_logger.error(f"main.py: Failed to explicitly initialize global application logger: {e_log_init_main}", exc_info=True)
            if global_log is None: # Fallback if global_log assignment failed
                 global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback) {msg}")
            global_log("main.py: Using pre_init_logger or fallback due to global_log explicit init failure.", "WARNING", logger_name="MainApp.Setup")
    else:
        pre_init_logger.error("main.py: initialize_global_log_file function was not imported. Global application logging will be compromised.")
        if global_log is None: # Ensure global_log is callable
            global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback_no_init) {msg}")

except ImportError as e_imp:
    pre_init_logger.error(f"Failed to import custom modules: {e_imp}. Current sys.path: {sys.path}", exc_info=True)
    if global_log is None: print(f"CRITICAL IMPORT ERROR (main.py, global_log unavailable): {e_imp}.")
    else: global_log(f"CRITICAL: Failed to import custom modules in main.py: {e_imp}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1) # Critical failure
except Exception as e_general_imp: # Catch any other error during imports
    pre_init_logger.error(f"General error during import phase: {e_general_imp}", exc_info=True)
    if global_log is None: print(f"CRITICAL GENERAL IMPORT ERROR (main.py, global_log unavailable): {e_general_imp}.")
    else: global_log(f"CRITICAL: General error during import phase in main.py: {e_general_imp}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1) # Critical failure

def load_config(config_path_relative_to_project_root="src/configs/project_config.yaml") -> Dict[str, Any]:
    """Loads the YAML configuration file."""
    # Path is now relative to PROJECT_ROOT (where run_prototype.sh is)
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
        global_log(f"Config file not found: {full_config_path}. Exiting.", "CRITICAL", logger_name="MainApp.ConfigLoader")
        raise # Re-raise to be caught by main's try-except
    except Exception as e_conf:
        global_log(f"Error loading or parsing config from {full_config_path}: {e_conf}", "CRITICAL", logger_name="MainApp.ConfigLoader", exc_info=True)
        raise # Re-raise

def main():
    # --- Setup detailed file logger for this specific run (market_briefing_log.txt) ---
    # This is separate from the application's global, timestamped logger.
    detailed_run_log_handler = None
    try:
        detailed_run_log_handler = logging.FileHandler(DETAILED_LOG_FILENAME, mode='w', encoding='utf-8')
        detailed_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s')
        detailed_run_log_handler.setFormatter(detailed_formatter)
        detailed_run_log_handler.setLevel(logging.DEBUG) # Capture all levels for this detailed log

        # Add this handler to the root logger to capture logs from all modules
        root_logger_for_detailed = logging.getLogger()
        root_logger_for_detailed.addHandler(detailed_run_log_handler)

        # Ensure console output from the global logger still works if it was set up
        global_log(f"Detailed execution transcript for this run will ALSO be saved to: {DETAILED_LOG_FILENAME}", "INFO", logger_name="MainApp.Setup")
    except Exception as e_detail_log:
        # If detailed log setup fails, use pre_init_logger or global_log if available
        err_msg = f"Failed to set up detailed run log at {DETAILED_LOG_FILENAME}: {e_detail_log}"
        if global_log: global_log(err_msg, "ERROR", logger_name="MainApp.Setup", exc_info=True)
        else: pre_init_logger.error(err_msg, exc_info=True)
        # Continue execution even if this specific log fails. The global logger should still work.

    global_log("--- 開始執行端到端金融數據處理原型 (Atomic Script Version) ---", "INFO", logger_name="MainApp.main_flow")

    config: Dict[str, Any] = {}
    try:
        # Load configuration using the path relative to project root
        config = load_config(config_path_relative_to_project_root="src/configs/project_config.yaml")

        start_date_cfg = config.get('data_fetch_range', {}).get('start_date', "2020-01-01")
        end_date_cfg = config.get('data_fetch_range', {}).get('end_date') # Can be None

        current_date_for_end_calc = ""
        try:
            # Use the imported get_taipei_time function
            current_date_for_end_calc = get_taipei_time_func_imported().strftime('%Y-%m-%d') if get_taipei_time_func_imported else datetime.now(timezone.utc).strftime('%Y-%m-%d')
        except Exception as e_time_local: # Catch error if get_taipei_time_func_imported fails
            current_date_for_end_calc = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            global_log(f"Using UTC for 'today's date' as get_taipei_time function failed or was unavailable: {e_time_local}", "WARNING", logger_name="MainApp.main_flow")

        end_date_to_use = end_date_cfg if end_date_cfg else current_date_for_end_calc
        global_log(f"Data fetch range: Start='{start_date_cfg}', End='{end_date_to_use}'.", "INFO", logger_name="MainApp.main_flow")

        # FRED API Key Handling (as per original logic, user provided key is hardcoded for this task)
        fred_api_key_env_name = config.get('api_endpoints', {}).get('fred', {}).get('api_key_env', 'FRED_API_KEY') # Default if not in config
        user_provided_fred_key = "78ea51fb13b546d89f1a683cb4ba26f5" # User-provided key for the task
        os.environ[fred_api_key_env_name] = user_provided_fred_key
        global_log(f"Temporarily set environment variable '{fred_api_key_env_name}' for FRED API access.", "DEBUG", logger_name="MainApp.main_flow")

        # Instantiate loggers for different components
        db_logger = logging.getLogger("project_logger.DatabaseManager")
        fred_logger = logging.getLogger("project_logger.FredConnector")
        nyfed_logger = logging.getLogger("project_logger.NYFedConnector")
        yf_logger = logging.getLogger("project_logger.YFinanceConnector")
        engine_logger = logging.getLogger("project_logger.IndicatorEngine")
        ai_agent_logger = logging.getLogger("project_logger.RemoteAIAgent") # Logger for AI Agent

        # Initialize DatabaseManager
        # Pass PROJECT_ROOT so DatabaseManager can resolve relative db path correctly
        db_manager = DatabaseManager(config, logger_instance=db_logger, project_root_dir=PROJECT_ROOT)
        db_manager.connect() # This will also create tables if they don't exist

        # Initialize AI Agent
        ai_agent = RemoteAIAgent(config=config, logger_instance=ai_agent_logger)

        data_fetch_status = {'fred': False, 'nyfed': False, 'yfinance_move': False}
        # Define unique columns for upsert operations
        macro_unique_cols = ['metric_date', 'metric_name', 'source_api']
        stock_unique_cols = ['price_date', 'security_id', 'source_api']

        global_log("\n--- 階段 1: 數據獲取 ---", "INFO", logger_name="MainApp.main_flow")

        # --- FRED Data Fetching ---
        fred_conn = FredConnector(config, logger_instance=fred_logger)
        fred_series_ids = config.get('target_metrics', {}).get('fred_series_ids', [])
        fred_data_df, fred_error_msg = fred_conn.fetch_data(series_ids=fred_series_ids, start_date=start_date_cfg, end_date=end_date_to_use)
        if fred_error_msg and (fred_data_df is None or fred_data_df.empty): # If error and no data, it's a failure
            global_log(f"FRED Data Fetching Error: {fred_error_msg}", "ERROR", logger_name="MainApp.main_flow")
            data_fetch_status['fred'] = False
        elif fred_data_df is not None and not fred_data_df.empty:
            global_log(f"Fetched {len(fred_data_df)} FRED records.", "INFO", logger_name="MainApp.main_flow")
            if fred_error_msg: # Partial success with some errors
                 global_log(f"FRED Data Fetching completed with some errors: {fred_error_msg}", "WARNING", logger_name="MainApp.main_flow")
            db_manager.bulk_insert_or_replace('fact_macro_economic_data', fred_data_df, unique_cols=macro_unique_cols)
            data_fetch_status['fred'] = True
        else: # No data, no specific error message from connector (might have been logged internally)
            global_log("FRED Connector returned no data or an empty DataFrame.", "WARNING", logger_name="MainApp.main_flow")
            data_fetch_status['fred'] = False

        # --- NYFed Data Fetching ---
        nyfed_conn = NYFedConnector(config, logger_instance=nyfed_logger)
        nyfed_data_df, nyfed_error_msg = nyfed_conn.fetch_data()
        if nyfed_error_msg and (nyfed_data_df is None or nyfed_data_df.empty):
            global_log(f"NYFed Data Fetching Error: {nyfed_error_msg}", "ERROR", logger_name="MainApp.main_flow")
            data_fetch_status['nyfed'] = False
        elif nyfed_data_df is not None and not nyfed_data_df.empty:
            global_log(f"Fetched {len(nyfed_data_df)} NYFed records.", "INFO", logger_name="MainApp.main_flow")
            if nyfed_error_msg:
                 global_log(f"NYFed Data Fetching completed with some errors: {nyfed_error_msg}", "WARNING", logger_name="MainApp.main_flow")
            db_manager.bulk_insert_or_replace('fact_macro_economic_data', nyfed_data_df, unique_cols=macro_unique_cols)
            data_fetch_status['nyfed'] = True
        else:
            global_log("NYFed Connector returned no data or an empty DataFrame.", "WARNING", logger_name="MainApp.main_flow")
            data_fetch_status['nyfed'] = False

        # --- YFinance Data Fetching ---
        yf_conn = YFinanceConnector(config, logger_instance=yf_logger)
        yfinance_tickers_list = config.get('target_metrics', {}).get('yfinance_tickers', [])
        yf_data_df, yf_error_msg = yf_conn.fetch_data(tickers=yfinance_tickers_list, start_date=start_date_cfg, end_date=end_date_to_use)
        if yf_error_msg and (yf_data_df is None or yf_data_df.empty):
            global_log(f"YFinance Data Fetching Error for {yfinance_tickers_list}: {yf_error_msg}", "ERROR", logger_name="MainApp.main_flow")
            data_fetch_status['yfinance_move'] = False
        elif yf_data_df is not None and not yf_data_df.empty:
            global_log(f"Fetched {len(yf_data_df)} YFinance records for {yfinance_tickers_list}.", "INFO", logger_name="MainApp.main_flow")
            if yf_error_msg:
                global_log(f"YFinance Data Fetching for {yfinance_tickers_list} completed with some errors: {yf_error_msg}", "WARNING", logger_name="MainApp.main_flow")
            db_manager.bulk_insert_or_replace('fact_stock_price', yf_data_df, unique_cols=stock_unique_cols)
            data_fetch_status['yfinance_move'] = True
        else:
            global_log(f"YFinance Connector returned no data for {yfinance_tickers_list}.", "WARNING", logger_name="MainApp.main_flow")
            data_fetch_status['yfinance_move'] = False

        global_log("\n--- 階段 2 & 3: 指標計算與市場簡報 ---", "INFO", logger_name="MainApp.main_flow")

        # Fetch data from DB for IndicatorEngine
        current_macro_data_for_engine = db_manager.fetch_all_for_engine('fact_macro_economic_data', start_date_cfg, end_date_to_use, date_column='metric_date')
        current_stock_data_for_engine = db_manager.fetch_all_for_engine('fact_stock_price', start_date_cfg, end_date_to_use, date_column='price_date')

        if (current_macro_data_for_engine is None or current_macro_data_for_engine.empty) and \
           (current_stock_data_for_engine is None or current_stock_data_for_engine.empty):
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

                # --- AI Decision Making Loop ---
                global_log("\n--- 階段 4: AI 歷史決策生成 ---", "INFO", logger_name="MainApp.HistoricalLoop")
                if stress_index_df is not None and not stress_index_df.empty:
                    if not ai_agent.is_configured:
                        global_log("AI Agent not configured (API key or endpoint missing/placeholder). AI decisions will be logged as SKIPPED/FAILED.", "WARNING", logger_name="MainApp.HistoricalLoop")

                    num_historical_dates = len(stress_index_df.index.unique())
                    global_log(f"Starting AI decision generation process for {num_historical_dates} historical dates.", "INFO", logger_name="MainApp.HistoricalLoop")

                    ai_decision_log_entries = []
                    for decision_date_dt in stress_index_df.index:
                        decision_date_str = decision_date_dt.strftime('%Y-%m-%d')
                        current_stress_value = stress_index_df.loc[decision_date_dt, 'DealerStressIndex']

                        current_stress_trend = "N/A"
                        date_loc = stress_index_df.index.get_loc(decision_date_dt)
                        if date_loc > 0:
                            prev_stress_value = stress_index_df.iloc[date_loc - 1]['DealerStressIndex']
                            change = current_stress_value - prev_stress_value
                            if pd.notna(change):
                                current_stress_trend = "上升" if change > 0.1 else ("下降" if change < -0.1 else "穩定")

                        historical_snapshot_data = None
                        if engine_instance.df_prepared is not None and decision_date_dt in engine_instance.df_prepared.index:
                            historical_snapshot_data = engine_instance.df_prepared.loc[decision_date_dt]

                        historical_market_snapshot_for_ai = {
                            "briefing_date": decision_date_str,
                            "dealer_stress_index": {
                                "current_value_description": f"{current_stress_value:.2f}",
                                "trend_approximation": current_stress_trend
                            },
                            "key_financial_components_latest": [
                                {"component_name": "MOVE Index (Bond Mkt Volatility)", "value_string": get_formatted_value(historical_snapshot_data, '^MOVE')},
                                {"component_name": "10Y-2Y Treasury Spread", "value_string": f"{(historical_snapshot_data['spread_10y2y'] * 100):.2f} bps" if historical_snapshot_data is not None and 'spread_10y2y' in historical_snapshot_data and pd.notna(historical_snapshot_data['spread_10y2y']) else "N/A"},
                                {"component_name": "Primary Dealer Net Positions (Millions USD)", "value_string": get_formatted_value(historical_snapshot_data, 'NYFED/PRIMARY_DEALER_NET_POSITION', value_format="{:,.0f}")}
                            ],
                             "broader_market_context_latest": {
                                "vix_index (Equity Mkt Volatility)": get_formatted_value(historical_snapshot_data, 'FRED/VIXCLS'),
                                "sofr_deviation_from_ma": get_formatted_value(historical_snapshot_data, 'FRED/SOFR_Dev')
                            }
                        }
                        briefing_json_for_log = json.dumps(historical_market_snapshot_for_ai)

                        # ai_agent.get_decision() will return None if not configured or if API call fails
                        ai_decision_model = ai_agent.get_decision(
                            decision_date=decision_date_str,
                            market_snapshot=historical_market_snapshot_for_ai,
                            stress_index_value=current_stress_value,
                            stress_index_trend=current_stress_trend
                        )

                        log_entry = {
                            "decision_date": decision_date_str,
                            "stress_index_value": current_stress_value if pd.notna(current_stress_value) else None,
                            "stress_index_trend": current_stress_trend,
                            "briefing_json": briefing_json_for_log
                        }

                        if ai_decision_model:
                            log_entry.update({
                                "strategy_summary": ai_decision_model.strategy_summary,
                                "key_factors": json.dumps(ai_decision_model.key_factors, ensure_ascii=False),
                                "confidence_score": ai_decision_model.confidence_score,
                                "raw_ai_response": ai_decision_model.raw_ai_response
                            })
                            global_log(f"AI decision for {decision_date_str}: Strategy='{ai_decision_model.strategy_summary}', Confidence={ai_decision_model.confidence_score:.2f}", "INFO", logger_name="MainApp.AIDecision")
                        else:
                            # This block executes if get_decision returns None (agent not configured or error)
                            log_entry.update({
                                "strategy_summary": "AI_CALL_SKIPPED_OR_FAILED",
                                "key_factors": json.dumps(["AI agent not configured or call failed."]), # More generic message
                                "confidence_score": 0.0,
                                "raw_ai_response": "N/A - Check AI Agent logs for details (e.g., not configured, API error)."
                            })
                            global_log(f"AI decision skipped or failed for {decision_date_str}. Check AI Agent logs.", "WARNING", logger_name="MainApp.AIDecision")

                        ai_decision_log_entries.append(log_entry)

                    if ai_decision_log_entries:
                        ai_log_df = pd.DataFrame(ai_decision_log_entries)
                        expected_db_cols = ['decision_date', 'stress_index_value', 'stress_index_trend',
                                            'strategy_summary', 'key_factors', 'confidence_score',
                                            'raw_ai_response', 'briefing_json']
                        for col in expected_db_cols:
                            if col not in ai_log_df.columns:
                                ai_log_df[col] = None
                        db_manager.bulk_insert_or_replace('log_ai_decision', ai_log_df, unique_cols=['decision_date'])
                        global_log(f"Logged {len(ai_log_df)} AI decision entries to database.", "INFO", logger_name="MainApp.HistoricalLoop")

                else: # stress_index_df is None or empty
                    global_log("Stress index data is not available. Skipping AI decision loop.", "WARNING", logger_name="MainApp.HistoricalLoop")

                total_decisions_attempted = len(stress_index_df.index.unique()) if stress_index_df is not None else 0
                global_log(f"Finished historical AI decision loop. Total decision points processed: {total_decisions_attempted}", "INFO", logger_name="MainApp.HistoricalLoop")


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
EOF

echo "Creating __init__.py files..."
cat <<EOF > src/__init__.py
# This file makes 'src' a package.
EOF

cat <<EOF > src/configs/__init__.py
# This file makes 'src/configs' a package.
EOF

cat <<EOF > src/connectors/__init__.py
# This file makes 'src/connectors' a package.
from .base import BaseConnector
from .nyfed_connector import NYFedConnector
from .yfinance_connector import YFinanceConnector
from .fred_connector import FredConnector
EOF

cat <<EOF > src/database/__init__.py
# This file makes 'src/database' a package.
from .database_manager import DatabaseManager
EOF

cat <<EOF > src/engine/__init__.py
# This file makes 'src/engine' a package.
from .indicator_engine import IndicatorEngine
EOF

cat <<EOF > src/scripts/__init__.py
# This file makes 'src/scripts' a package.
from .initialize_global_log import initialize_log_file, log_message, get_taipei_time
EOF

echo "Creating src/ai_agent.py..."
cat <<EOF > src/ai_agent.py
import requests
import json
import logging
import time
import os
from typing import Dict, Any, Optional, Tuple, List # Added List
from pydantic import BaseModel, ValidationError, field_validator # Assuming pydantic is used for models

# Logger setup
logger = logging.getLogger(f"project_logger.{__name__}")
if not logger.handlers and not logging.getLogger().hasHandlers():
    logger.addHandler(logging.NullHandler())
    logger.debug(f"Logger for {__name__} (ai_agent module) configured with NullHandler.")

class AIDecisionModel(BaseModel):
    """Pydantic model for the AI's decision output."""
    decision_date: str # YYYY-MM-DD
    strategy_summary: str # e.g., "積極買入美國長天期公債"
    key_factors: List[str]
    confidence_score: float # 0.0 to 1.0
    raw_ai_response: Optional[str] = None # Store the full raw response for audit/debug

    @field_validator('decision_date')
    def validate_date_format(cls, value):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError("decision_date must be in YYYY-MM-DD format")
        return value

    @field_validator('confidence_score')
    def validate_confidence_score(cls, value):
        if not (0.0 <= value <= 1.0):
            raise ValueError("confidence_score must be between 0.0 and 1.0")
        return value

class RemoteAIAgent:
    """Handles communication with a remote AI service for decision making."""

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                 self.logger.addHandler(logging.NullHandler())

        self.ai_config = config.get('ai_service', {})
        self.api_key = self.ai_config.get('api_key', "YOUR_API_KEY_HERE")
        self.api_endpoint = self.ai_config.get('api_endpoint')
        self.default_model = self.ai_config.get('default_model')
        self.max_retries = self.ai_config.get('max_retries', 3)
        self.retry_delay = self.ai_config.get('retry_delay_seconds', 5)
        self.api_call_delay = self.ai_config.get('api_call_delay_seconds', 1.0) # Not used in get_decision yet, but good for future

        self.is_configured = True
        if not self.api_key or self.api_key == "YOUR_API_KEY_HERE" or not self.api_endpoint:
            self.logger.warning(
                "AI Agent is not fully configured. API key or endpoint is missing or placeholder. "
                "AI decision making will be skipped. "
                "【待處理：AI API 金鑰未在 project_config.yaml 中提供或端點未設定】"
            )
            self.is_configured = False
        else:
            self.logger.info(f"RemoteAIAgent initialized. Endpoint: {self.api_endpoint}, Model: {self.default_model}")

    def get_decision_prompt_template(self) -> str:
        # This is a simplified prompt template.
        # In a real scenario, this would be more complex and configurable.
        return """
        基於以下市場數據和壓力指標，請為日期 {decision_date} 提供一個美國公債市場的交易策略。
        你的回答必須嚴格遵循以下 JSON 格式，不得包含任何額外的解釋或文字。

        市場數據摘要:
        {market_data_summary}

        交易商壓力指標 (Dealer Stress Index): {stress_index_value:.2f} (越高越緊張)
        壓力指標趨勢: {stress_index_trend}

        請輸出 JSON 格式的決策:
        {{
          "decision_date": "{decision_date}",
          "strategy_summary": "你的策略摘要 (例如：買入/賣出/持有，短期/中期/長期，特定券種)",
          "key_factors": ["影響你決策的1-3個最關鍵因素"],
          "confidence_score": 0.0到1.0之間你的信心評分
        }}
        """

    def format_market_data_for_prompt(self, market_snapshot: Dict[str, Any]) -> str:
        """Formats the market snapshot into a string for the AI prompt."""
        # market_snapshot is expected to be a dictionary like the one generated for briefing
        # For simplicity, we'll just use a few key items.
        # A more robust implementation would carefully select and format more data.
        if not market_snapshot: return "市場數據不可用。"

        sds_val = market_snapshot.get('dealer_stress_index', {}).get('current_value_description', "N/A")
        key_components_str = "; ".join([f"{c['component_name']}: {c['value_string']}" for c in market_snapshot.get('key_financial_components_latest', [])])

        return (
            f"- 交易商壓力指數: {sds_val}\n"
            f"- 關鍵金融組件: {key_components_str}\n"
            f"- 更廣泛市場背景: VIX={market_snapshot.get('broader_market_context_latest',{}).get('vix_index (Equity Mkt Volatility)', 'N/A')}, "
            f"SOFR離差={market_snapshot.get('broader_market_context_latest',{}).get('sofr_deviation_from_ma', 'N/A')}"
        )

    def get_decision(self, decision_date: str, market_snapshot: Dict[str, Any], stress_index_value: float, stress_index_trend: str) -> Optional[AIDecisionModel]:
        if not self.is_configured:
            self.logger.debug("AI Agent not configured, skipping get_decision call.")
            return None

        prompt_template = self.get_decision_prompt_template()
        market_data_summary_str = self.format_market_data_for_prompt(market_snapshot)

        prompt = prompt_template.format(
            decision_date=decision_date,
            market_data_summary=market_data_summary_str,
            stress_index_value=stress_index_value,
            stress_index_trend=stress_index_trend
        )
        self.logger.debug(f"Generated AI prompt for date {decision_date}:\n{prompt}")

        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key, # Common for many APIs
            "Authorization": f"Bearer {self.api_key}", # Common for OpenAI, Anthropic might use x-api-key
            # Anthropic specific headers (if using Anthropic)
            "anthropic-version": "2023-06-01"
        }
        # Construct payload based on common LLM API structures (e.g., OpenAI, Anthropic)
        # This is a generic example, might need adjustment for specific API
        payload = {
            "model": self.default_model,
            "max_tokens": 500, # Max tokens for the response
            "messages": [
                {"role": "user", "content": prompt}
            ],
            # "temperature": 0.7, # Optional: for creativity vs determinism
        }
        # If using Anthropic, the payload structure for messages is slightly different for system prompts,
        # but for a user message like this, it's similar.
        # Anthropic payload example:
        # payload = {
        #     "model": self.default_model,
        #     "max_tokens": 500,
        #     "messages": [{"role": "user", "content": prompt}],
        #     "system": "You are a financial analyst providing bond trading strategies." # Optional system prompt
        # }


        raw_response_text = None
        for attempt in range(self.max_retries):
            try:
                response = requests.post(self.api_endpoint, headers=headers, json=payload, timeout=30)
                response.raise_for_status()

                response_data = response.json()
                self.logger.debug(f"Raw AI JSON response: {response_data}")

                # Extract content based on common API response patterns (OpenAI/Anthropic)
                # This part is highly dependent on the specific AI provider's response structure.
                # Assuming a structure where the main text is in response_data['choices'][0]['message']['content'] (OpenAI)
                # or response_data['content'][0]['text'] (Anthropic)

                ai_response_content = None
                if 'choices' in response_data and response_data['choices']: # OpenAI like
                    ai_response_content = response_data['choices'][0].get('message', {}).get('content')
                elif 'content' in response_data and isinstance(response_data['content'], list) and response_data['content']: # Anthropic like
                    ai_response_content = response_data['content'][0].get('text')

                if not ai_response_content:
                    self.logger.error(f"Could not extract AI content from response for date {decision_date}. Response structure not recognized. Full response: {response_data}")
                    raw_response_text = json.dumps(response_data) # Store raw response if extraction fails
                    return None # Or attempt retry if appropriate

                raw_response_text = ai_response_content # Store the extracted text part
                self.logger.info(f"Successfully received AI response for date {decision_date}.")
                self.logger.debug(f"Extracted AI response content for parsing: {raw_response_text}")

                # Attempt to parse the AI's response content (which should be JSON)
                try:
                    # The AI is asked to output JSON directly.
                    # We need to find the JSON block if there's any surrounding text.
                    # A simple heuristic: find first '{' and last '}'
                    json_start_index = raw_response_text.find('{')
                    json_end_index = raw_response_text.rfind('}')
                    if json_start_index != -1 and json_end_index != -1 and json_start_index < json_end_index:
                        json_str_to_parse = raw_response_text[json_start_index : json_end_index+1]
                        self.logger.debug(f"Attempting to parse JSON from AI response: {json_str_to_parse}")
                        decision_data = json.loads(json_str_to_parse)

                        # Add raw_ai_response to the data before validation
                        decision_data['raw_ai_response'] = raw_response_text

                        validated_decision = AIDecisionModel(**decision_data)
                        return validated_decision
                    else:
                        self.logger.error(f"Could not find valid JSON block in AI response for date {decision_date}. Response: {raw_response_text}")
                        # Store raw response text in a way that indicates parsing failure
                        # This might be done by returning a special error object or logging appropriately.
                        # For now, returning None after logging.
                        return None


                except json.JSONDecodeError as e_json:
                    self.logger.error(f"Failed to decode AI response JSON for date {decision_date}: {e_json}. Response text: {raw_response_text}", exc_info=True)
                    return None
                except ValidationError as e_val:
                    self.logger.error(f"AI response validation failed for date {decision_date}: {e_val}. Data: {decision_data if 'decision_data' in locals() else 'N/A'}", exc_info=True)
                    return None

            except requests.exceptions.HTTPError as e_http:
                self.logger.warning(f"HTTP error from AI service on attempt {attempt + 1}/{self.max_retries} for date {decision_date}: {e_http.response.status_code if e_http.response else 'N/A'}")
                if e_http.response is not None:
                    self.logger.warning(f"AI service error response content: {e_http.response.text[:500]}") # Log part of error response
                    raw_response_text = e_http.response.text # Store error response
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Final attempt failed for AI service call for date {decision_date} with HTTPError: {e_http}")
                    # Optionally, return a dummy/error AIDecisionModel with raw_ai_response populated
                    return None
            except Exception as e_gen:
                self.logger.error(f"Generic error during AI service call on attempt {attempt + 1}/{self.max_retries} for date {decision_date}: {e_gen}", exc_info=True)
                raw_response_text = str(e_gen) # Store error string
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Final attempt failed for AI service call for date {decision_date} with generic error.")
                    return None

            self.logger.info(f"Retrying AI service call for date {decision_date} in {self.retry_delay} seconds...")
            time.sleep(self.retry_delay)

        self.logger.error(f"All retries failed for AI service call for date {decision_date}.")
        # Return a model with the last raw response if available, or just None
        if raw_response_text:
             # This path might not be hit if all retries fail before raw_response_text is set.
             # Consider how to signal "retries exhausted" vs "parsing failed".
             # For now, if we reach here, it implies all retries failed to get a *successful* parseable response.
             # We could create a dummy AIDecisionModel indicating failure.
             # Example: return AIDecisionModel(decision_date=decision_date, strategy_summary="AI_CALL_FAILED", key_factors=["Retries exhausted"], confidence_score=0.0, raw_ai_response=raw_response_text)
             # But the current design expects Optional[AIDecisionModel], so None is appropriate for failure.
             pass # Fall through to return None
        return None

if __name__ == '__main__':
    # Basic test for RemoteAIAgent
    # This requires a valid project_config.yaml structure in the same directory or accessible path.
    # For atomic script, config is created by run_prototype.sh.

    # Setup basic logging for test execution
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_ai = logging.getLogger("AIAgentTestRun_Atomic")
    if not test_logger_ai.handlers:
        ch_ai = logging.StreamHandler(sys.stdout)
        ch_ai.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_ai.addHandler(ch_ai)
        test_logger_ai.propagate = False

    # Create a dummy config for testing
    # In a real scenario, this would be loaded from src/configs/project_config.yaml by main.py
    # For this direct test, we simulate it.
    # IMPORTANT: Replace with a real (test) API key and endpoint if you want to make actual calls.
    # DO NOT COMMIT REAL PRODUCTION KEYS.
    dummy_config_for_test = {
        "ai_service": {
            "api_key": os.getenv("TEST_ANTHROPIC_API_KEY", "YOUR_API_KEY_HERE"), # Use env var for testing if available
            "api_endpoint": os.getenv("TEST_ANTHROPIC_API_ENDPOINT", "https://api.anthropic.com/v1/messages"),
            "default_model": "claude-3-haiku-20240307", # Use a cheaper/faster model for testing
            "max_retries": 1,
            "retry_delay_seconds": 1,
            "api_call_delay_seconds": 0.1
        }
        # ... other config sections like database, etc., if RemoteAIAgent needed them directly
    }

    # Check if a test key is actually provided via environment, otherwise tests will be skipped or use placeholder.
    if dummy_config_for_test["ai_service"]["api_key"] == "YOUR_API_KEY_HERE":
        test_logger_ai.warning("Test AI API key not found in TEST_ANTHROPIC_API_KEY env var. AI calls will use placeholder and likely fail if not mocked.")
        # To run a real test, set TEST_ANTHROPIC_API_KEY and optionally TEST_ANTHROPIC_API_ENDPOINT in your environment.
        # For CI/CD or automated tests, these should be mocked.

    test_logger_ai.info("--- Starting RemoteAIAgent Test ---")
    ai_agent_test = RemoteAIAgent(config=dummy_config_for_test, logger_instance=test_logger_ai)

    if not ai_agent_test.is_configured:
        test_logger_ai.warning("AI Agent not configured (API key likely missing/placeholder). Skipping actual API call test.")
    else:
        test_logger_ai.info("AI Agent is configured. Attempting a test API call.")
        sample_market_snapshot = {
            "briefing_date": "2023-10-26",
            "dealer_stress_index": {"current_value_description": "35.00 (正常)", "trend_approximation": "穩定"},
            "key_financial_components_latest": [
                {"component_name": "MOVE Index", "value_string": "120.0"},
                {"component_name": "10Y-2Y Spread", "value_string": "-50 bps"}
            ],
            "broader_market_context_latest": {"vix_index (Equity Mkt Volatility)": "18.0", "sofr_deviation_from_ma": "0.01"}
        }
        decision_result = ai_agent_test.get_decision(
            decision_date="2023-10-26",
            market_snapshot=sample_market_snapshot,
            stress_index_value=35.00,
            stress_index_trend="穩定"
        )

        if decision_result:
            test_logger_ai.info(f"AI Decision Received: {decision_result.model_dump_json(indent=2)}")
            assert decision_result.decision_date == "2023-10-26"
            assert 0.0 <= decision_result.confidence_score <= 1.0
        else:
            test_logger_ai.error("AI Agent test call failed to return a valid decision.")
            test_logger_ai.info("This is expected if API key is a placeholder or invalid, or if the test endpoint/model is not reachable/correct.")

    test_logger_ai.info("--- RemoteAIAgent Test Finished ---")
EOF

# === 階段三：依賴安裝 ===
echo ""
echo "Phase 3: Installing dependencies from requirements.txt..."
pip install -r requirements.txt

# === 階段四：執行主流程 ===
echo ""
echo "Phase 4: Running the main application (src/main.py)..."
# 執行 src 目錄下的 main.py。
# 由於我們在 main.py 內部通過 __file__ 和 Path().parent 正確設定了 PROJECT_ROOT 和 SOURCE_ROOT，
# 並且 src 目錄已通過 __init__.py 成為一個套件的根目錄（相對於執行時的 sys.path），
# 因此 python src/main.py 應該能正確找到其子模組。
python src/main.py

echo ""
echo "Execution finished."
echo "The detailed transcript log should be in market_briefing_log.txt"
echo "Application-specific logs (timestamped) should be in api_test_logs/"
