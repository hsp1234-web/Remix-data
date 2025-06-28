#!/bin/bash
set -euo pipefail

echo "INFO: Protocol Terminus engaged. Creating a scorched-earth environment."
echo "INFO: Current working directory: $(pwd)"

# 步驟一：創建一個絕對乾淨的執行目錄
MISSION_DIR="mission_critical"
echo "INFO: Preparing mission directory: $MISSION_DIR"
rm -rf "$MISSION_DIR"
mkdir -p "$MISSION_DIR/src/connectors" "$MISSION_DIR/logs" # analytics 目錄暫不需要
echo "INFO: Mission directory created."

# 步驟二：直接寫入最精簡的、必要的程式碼檔案

# --- NYFedConnector 的程式碼 ---
# 注意：這裡直接嵌入 Python 程式碼。在真實場景中，如果程式碼很長，
# 可能會考慮用其他方式（如 git checkout 特定檔案到此目錄）。
# 但根據「焦土契約」，我們直接寫入。
# 確保這裡的 NYFedConnector 是最新的、經過驗證可以獨立運行的版本。
# （Jules註：我將使用先前已讀取的 src/connectors/nyfed_connector.py 內容）
cat <<'EOF' > "$MISSION_DIR/src/connectors/nyfed_connector.py"
import requests
import pandas as pd
from io import BytesIO
import logging
import time
import random
from datetime import datetime, timezone, date # Added date
from typing import Dict, Any, Tuple, Optional, List
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os # For path joining

# Use a module-level logger
# In this isolated script, logging might be simpler or directed to stdout
# For now, let's keep it but ensure it doesn't break if not configured externally.
logger = logging.getLogger(__name__)
# Basic config for the logger if no handlers are present
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout) # Output to stdout for mission script
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Default to INFO for less verbosity in this context

class NYFedConnector:
    def __init__(self, api_config: Dict[str, Any]):
        self.requests_per_minute = api_config.get("requests_per_minute", 30)
        self._last_request_time = 0
        self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

        self.source_api_name = "NYFED_MISSION" # Distinguish from regular NYFED
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.download_configs = api_config.get("download_configs", [])
        self.parser_recipes = api_config.get("parser_recipes", {})
        self.global_requests_config = api_config.get("requests_config", {})
        logger.info(f"NYFedConnector (Mission) Initialized. RPM: {self.requests_per_minute}, Found {len(self.download_configs)} download configs.")

    def _wait_for_rate_limit(self):
        if self._min_request_interval == 0: return
        now = time.time()
        elapsed_time = now - self._last_request_time
        wait_time = self._min_request_interval - elapsed_time
        if wait_time > 0:
            logger.debug(f"NYFed (Mission) Rate Limit: Waiting {wait_time:.2f}s")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _download_file_content(self, url: str, file_pattern_hint: Optional[str] = None) -> Optional[BytesIO]:
        self._wait_for_rate_limit()
        self._last_request_time = time.time()
        retries = self.global_requests_config.get('max_retries', 2) # Reduced retries for mission
        base_backoff = self.global_requests_config.get('base_backoff_seconds', 1)
        timeout_sec = self.global_requests_config.get('download_timeout', 30) # Reduced timeout

        for attempt in range(retries):
            try:
                logger.info(f"NYFed (Mission) Download Attempt {attempt + 1}/{retries} for URL: {url}")
                page_response = self.session.get(url, timeout=timeout_sec)
                page_response.raise_for_status()
                content_type = page_response.headers.get('Content-Type', '').lower()
                logger.info(f"NYFed (Mission) Response: Status {page_response.status_code}, Content-Type: {content_type}")

                if 'text/html' in content_type:
                    logger.info(f"NYFed (Mission) URL is HTML. Searching for pattern: '{file_pattern_hint}'")
                    soup = BeautifulSoup(page_response.content, 'html.parser')
                    link_found = None
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag['href']
                        if file_pattern_hint and file_pattern_hint.lower() in href.lower() and (href.lower().endswith('.xlsx') or href.lower().endswith('.csv')):
                            link_found = href; break
                    if link_found:
                        download_url = urljoin(url, link_found)
                        logger.info(f"NYFed (Mission) Found file link on page: {download_url}. Downloading...")
                        self._wait_for_rate_limit(); self._last_request_time = time.time()
                        file_response = self.session.get(download_url, timeout=timeout_sec)
                        file_response.raise_for_status()
                        logger.info(f"NYFed (Mission) Downloaded linked file: Status {file_response.status_code}, Size: {len(file_response.content)} bytes")
                        return BytesIO(file_response.content)
                    else:
                        logger.warning(f"NYFed (Mission) No link matching pattern '{file_pattern_hint}' found on HTML page {url}.")
                        return None
                elif any(ct_part in content_type for ct_part in ['excel', 'spreadsheetml', 'officedocument', 'csv', 'application/octet-stream']):
                    logger.info(f"NYFed (Mission) URL is a direct file. Downloaded {len(page_response.content)} bytes.")
                    return BytesIO(page_response.content)
                else:
                    logger.warning(f"NYFed (Mission) Unexpected Content-Type '{content_type}' for URL {url}.")
                    return None
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"NYFed (Mission) HTTP 404 for {url}. File likely not found."); return None
                logger.warning(f"NYFed (Mission) HTTP Error on attempt {attempt + 1} for {url}: {e}")
                if attempt == retries - 1: return None
            except requests.exceptions.RequestException as e:
                logger.warning(f"NYFed (Mission) RequestException on attempt {attempt + 1} for {url}: {e}")
                if attempt == retries - 1: return None
            sleep_time = base_backoff * (2 ** attempt) + random.uniform(0, 0.5)
            logger.info(f"NYFed (Mission) Retrying in {sleep_time:.2f}s for {url}")
            time.sleep(sleep_time)
        logger.error(f"NYFed (Mission) All download attempts failed for {url}.")
        return None

    def get_configured_data(self) -> pd.DataFrame:
        if not self.download_configs:
            logger.warning("NYFedConnector (Mission): No download_configs provided."); return self._create_empty_standard_df()
        all_data_frames = []
        current_year = datetime.now().year
        for dl_config in self.download_configs:
            config_name = dl_config.get("name", "Unnamed NYFed Data")
            url_template = dl_config.get("url_template")
            file_pattern = dl_config.get("file_pattern_on_page")
            recipe_name = dl_config.get("parser_recipe_name")
            metric_name = dl_config.get("metric_name_override", f"{self.source_api_name}/{config_name.upper()}")

            url_to_fetch = url_template.replace("{YYYY}", str(current_year)) if url_template and "{YYYY}" in url_template else url_template
            pattern_to_find = file_pattern.replace("{YYYY}", str(current_year)) if file_pattern and "{YYYY}" in file_pattern else file_pattern

            logger.info(f"NYFed (Mission) Processing: '{config_name}' from URL template: {url_template}")
            content_io = self._download_file_content(url_to_fetch, pattern_to_find)
            if not content_io: continue

            recipe = self.parser_recipes.get(recipe_name)
            if not recipe: logger.warning(f"NYFed (Mission) No parser recipe for '{recipe_name}'. Skipping."); continue

            try:
                is_csv = url_to_fetch.lower().endswith('.csv') or recipe.get("file_type") == "csv"
                df_raw = pd.read_csv(content_io, header=recipe.get('header_row', 1)-1, encoding=recipe.get('encoding','utf-8')) if is_csv \
                    else pd.read_excel(content_io, header=recipe.get('header_row', 1)-1, sheet_name=recipe.get('sheet_name', 0), engine='openpyxl')
                if df_raw.empty: logger.info(f"NYFed (Mission) Parsed file for '{config_name}' is empty."); continue

                date_col = recipe.get('date_column')
                if not date_col or date_col not in df_raw.columns:
                    logger.error(f"NYFed (Mission) Date column '{date_col}' not in '{config_name}'. Cols: {df_raw.columns.tolist()}"); continue

                df_transformed = df_raw[[date_col]].copy(); df_transformed.rename(columns={date_col: 'metric_date'}, inplace=True)
                df_transformed['metric_date'] = pd.to_datetime(df_transformed['metric_date'], errors='coerce'); df_transformed.dropna(subset=['metric_date'], inplace=True)
                if df_transformed.empty: continue

                cols_to_sum = recipe.get('columns_to_sum', [])
                val_col_direct = recipe.get('value_column')
                if cols_to_sum:
                    present_cols = [c for c in cols_to_sum if c in df_raw.columns]
                    if not present_cols: logger.warning(f"NYFed (Mission) Sum columns {cols_to_sum} not in '{config_name}'."); continue
                    for c in present_cols: df_raw[c] = pd.to_numeric(df_raw[c], errors='coerce')
                    df_transformed['metric_value'] = df_raw[present_cols].sum(axis=1, skipna=True)
                elif val_col_direct and val_col_direct in df_raw.columns:
                    df_transformed['metric_value'] = pd.to_numeric(df_raw[val_col_direct], errors='coerce')
                else: logger.warning(f"NYFed (Mission) No value source in recipe for '{config_name}'."); continue

                df_transformed['metric_value'] *= recipe.get('data_unit_multiplier', 1); df_transformed.dropna(subset=['metric_value'], inplace=True)
                if df_transformed.empty: continue

                df_transformed['security_id'] = metric_name; df_transformed['metric_name'] = metric_name
                df_transformed['source_api'] = self.source_api_name; df_transformed['last_updated_timestamp'] = datetime.now(timezone.utc)
                all_data_frames.append(df_transformed[self._get_standard_columns()])
            except Exception as e: logger.error(f"NYFed (Mission) Error parsing '{config_name}': {e}", exc_info=True)

        if not all_data_frames: return self._create_empty_standard_df()
        final_df = pd.concat(all_data_frames, ignore_index=True)
        if not final_df.empty:
            final_df.sort_values(by=['security_id', 'metric_date'], inplace=True)
            final_df.drop_duplicates(subset=['security_id', 'metric_date'], keep='last', inplace=True)
        return final_df

    def _get_standard_columns(self) -> List[str]:
        return ['metric_date', 'security_id', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp']
    def _create_empty_standard_df(self) -> pd.DataFrame:
        return pd.DataFrame(columns=self._get_standard_columns())

# --- End of NYFedConnector code ---
EOF
echo "INFO: NYFedConnector code written to $MISSION_DIR/src/connectors/nyfed_connector.py"

# --- __init__.py for connectors package ---
cat <<EOF > "$MISSION_DIR/src/connectors/__init__.py"
# Mission critical __init__.py
# Only NYFedConnector is needed for this mission.
from .nyfed_connector import NYFedConnector

__all__ = ["NYFedConnector"]
EOF
echo "INFO: Connectors __init__.py written to $MISSION_DIR/src/connectors/__init__.py"

# --- main_logic.py ---
cat <<'EOF' > "$MISSION_DIR/src/main_logic.py"
print("main_logic.py: [STATUS] Script started.", flush=True)
import sys
import os
from datetime import datetime # For current year in config

# --- Path setup for mission_critical structure ---
# When this script (main_logic.py) is run from $MISSION_DIR,
# and nyfed_connector.py is in $MISSION_DIR/src/connectors/,
# we need to add $MISSION_DIR/src to sys.path.
# The 'cd "$MISSION_DIR"' in the bash script handles the CWD.
# So, 'src' should be directly accessible.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # Add $MISSION_DIR to path for 'src'
print(f"main_logic.py: [INFO] Current sys.path: {sys.path}", flush=True)
print(f"main_logic.py: [INFO] Current working directory: {os.getcwd()}", flush=True)


# --- Dynamic Dependency Check ---
# Minimal logging for this critical phase
def _ensure_pkg(pkg_name, imp_name=None):
    module_to_import = imp_name if imp_name else pkg_name
    try:
        print(f"main_logic.py: [DEP_ATTEMPT] Importing '{module_to_import}' (for {pkg_name})...", flush=True)
        __import__(module_to_import)
        print(f"main_logic.py: [DEP_SUCCESS] '{module_to_import}' is available.", flush=True)
        return True
    except ImportError:
        print(f"main_logic.py: [DEP_FAILURE] Critical module '{module_to_import}' (for {pkg_name}) NOT FOUND.", flush=True)
        return False

# These are the absolute minimum for NYFedConnector and this script
# PyYAML is not needed if config is hardcoded as per plan
# Pandas, openpyxl, beautifulsoup4 are needed by NYFedConnector
# requests is needed by NYFedConnector
core_deps = [
    ("requests", "requests"),
    ("pandas", "pandas"),
    ("openpyxl", "openpyxl"),
    ("beautifulsoup4", "bs4"),
    ("yaml", "yaml") # For PyYAML, if loading config from file
]
all_core_deps_ok = True
for pkg, imp in core_deps:
    if not _ensure_pkg(pkg, imp):
        all_core_deps_ok = False

if not all_core_deps_ok:
    print("main_logic.py: [CRITICAL_ABORT] Essential dependencies missing after pip install. Aborting.", flush=True)
    sys.exit(1)
print("main_logic.py: [INFO] All core dependencies seem to be available.", flush=True)

# Now safe to import NYFedConnector and others
try:
    from src.connectors.nyfed_connector import NYFedConnector
    import yaml
    import pandas as pd
    import logging # Import for NYFedConnector's internal logger

    # Configure a basic logger for NYFedConnector if it uses one
    # This ensures its logger.info etc. calls don't fail.
    # It will print to stdout if StreamHandler is added.
    connector_logger = logging.getLogger("src.connectors.nyfed_connector")
    if not connector_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - NYFedConnector(Mission) [%(levelname)s] - %(message)s'))
        connector_logger.addHandler(handler)
        connector_logger.setLevel(logging.INFO) # Or DEBUG for more verbosity from connector

    print("main_logic.py: [SUCCESS] Successfully imported NYFedConnector, yaml, pandas.", flush=True)
except ImportError as e_imp:
    print(f"main_logic.py: [CRITICAL_IMPORT_POST_CHECK_FAIL] Failed to import after dep check: {e_imp}", flush=True)
    sys.exit(1)


# --- Minimal Config for NYFedConnector ---
# This config should match what your NYFedConnector expects.
# Using the AMBS URL as a test case, assuming it's a direct Excel link or a page.
# The actual URL from your config.yaml.template for 'ambs_operations_url' was complex.
# Let's use a simpler, known NYFed page that usually has Excel links for Primary Dealer stats.
# This is more robust for a "does it work at all" test.

# Using the Primary Dealer Statistics page, which should have links to Excel files.
# The actual file name changes weekly/monthly.
# We'll use a general pattern that should match one of the Excel files.
current_year_str = str(datetime.now().year)

# Minimal config for one specific, typically available NYFed data source (Primary Dealer Stats)
# This is an example, you might need to adjust based on the actual NYFed page and file naming.
# The goal is to test the download and parsing for *a* file.
minimal_nyfed_config = {
    "requests_per_minute": 10, # Be very conservative
    "download_configs": [
        {
            "name": "primary_dealer_stats_current",
            # This is a page where Excel files are listed
            "url_template": "https://www.newyorkfed.org/markets/primarydealer_statistics/financial_condition",
            # Pattern to find on the HTML page. Look for "Financial Condition Data" and ".xlsx"
            "file_pattern_on_page": f"Primary Dealer Financial Condition Data – {current_year_str}.xlsx",
            "parser_recipe_name": "primary_dealer_default_recipe",
            "metric_name_override": "NYFED_MISSION/PRIMARY_DEALER_TOTAL_ASSETS"
        }
    ],
    "parser_recipes": {
        "primary_dealer_default_recipe": {
            # These are common values for such reports, but might need adjustment
            # based on the actual current file format from NYFed.
            "header_row": 3,
            "date_column": "As of Date", # Common date column name
            "value_column": "Total assets", # Try to extract "Total assets"
            # "columns_to_sum": ["Net outright par positions U.S. Treasury coupons"], # Example if summing
            "data_unit_multiplier": 1000000, # Assuming millions
            "sheet_name": 0 # Try the first sheet
        }
    },
    "requests_config": {"max_retries": 1, "base_backoff_seconds": 1, "download_timeout": 60}
}
print(f"main_logic.py: [INFO] Using minimal_nyfed_config: {minimal_nyfed_config}", flush=True)


# --- Execute NYFedConnector ---
try:
    print("main_logic.py: [ATTEMPT] Initializing NYFedConnector with minimal config...", flush=True)
    connector = NYFedConnector(api_config=minimal_nyfed_config)
    print("main_logic.py: [SUCCESS] NYFedConnector initialized.", flush=True)

    print("main_logic.py: [ATTEMPT] Calling connector.get_configured_data()...", flush=True)
    data_df = connector.get_configured_data()

    if data_df is not None and not data_df.empty:
        print(f"main_logic.py: [SUCCESS] NYFedConnector.get_configured_data() returned {len(data_df)} rows.", flush=True)
        print("main_logic.py: [DATA_SAMPLE] First 3 rows:", flush=True)
        print(data_df.head(3).to_string(), flush=True)
    elif data_df is not None and data_df.empty:
        print("main_logic.py: [INFO_EMPTY] NYFedConnector.get_configured_data() returned an EMPTY DataFrame. This could be due to no data for the period, download/parsing issues, or the target file not found on the NYFed page. Check connector logs.", flush=True)
    else: # Should not happen if connector adheres to returning empty DF on failure
        print("main_logic.py: [WARNING] NYFedConnector.get_configured_data() returned None. This is unexpected.", flush=True)

except Exception as e:
    print(f"main_logic.py: [CRITICAL_FAILURE] An error occurred during NYFedConnector operation: {e}", flush=True)
    import traceback
    print("main_logic.py: [TRACEBACK]", flush=True)
    traceback.print_exc() # This will print to stderr, which bash script should capture

print("main_logic.py: [STATUS] Script finished.", flush=True)
EOF
echo "INFO: Main logic written to $MISSION_DIR/src/main_logic.py"

# 步驟三：創建最精簡的依賴列表 (只包含 requests)
cat <<EOF > "$MISSION_DIR/requirements.txt"
requests
EOF
echo "INFO: Minimized requirements.txt (only requests) created in $MISSION_DIR"

# 步驟四：使用最原始、最可靠的方式安裝依賴，並強化日誌和檢查
echo "INFO: Attempting final, minimal dependency installation (requests only)..."
# 使用 python3 -m pip 確保使用的是與 python3 命令關聯的 pip
# 將 pip 的詳細輸出保存到日誌檔案
python3 -m pip install -vvv -r "$MISSION_DIR/requirements.txt" --no-cache-dir > "$MISSION_DIR/logs/pip_install.log" 2>&1
PIP_EXIT_CODE=$? # 捕獲 pip install 的退出碼

echo "INFO: Displaying pip installation log from $MISSION_DIR/logs/pip_install.log:"
cat "$MISSION_DIR/logs/pip_install.log" # 將 pip 日誌打印到主輸出

# 檢查 pip install 是否真的成功 (檢查日誌內容比單純依賴退出碼更可靠一點)
if grep -q -i "Successfully installed requests" "$MISSION_DIR/logs/pip_install.log"; then
    echo "SUCCESS: 'requests' package appears to be successfully installed based on pip log."
elif [ $PIP_EXIT_CODE -eq 0 ]; then
    echo "WARNING: pip install exited with 0, but 'Successfully installed requests' not found in log. Assuming success with caution."
else
    echo "CRITICAL FAILURE: Minimal dependency (requests) installation failed. pip exit code: $PIP_EXIT_CODE. The sandbox environment may be actively blocking pip."
    echo "INFO: Protocol Terminus indicates potential fundamental pip block. Further Python execution might fail due to missing dependencies."
    # 根據指令，如果連 requests 都裝不上，後續步驟很可能失敗。
    # 這裡可以選擇 exit 1，或者讓後續的 Python 腳本自己去發現依賴缺失。
    # 為了獲取 Python 層面的 ImportError (如果有的話)，我們暫時不 exit 1，讓它繼續。
    # 但如果指揮官指示在此處終止，則應 exit 1。
    # exit 1
fi

# 步驟五：執行主診斷邏輯
echo "INFO: Executing the main diagnostic logic (cd $MISSION_DIR && python3 src/main_logic.py)."
# 進入 mission_critical 目錄執行，以確保 src.connectors 的相對導入能正確工作
# (因為 main_logic.py 中 sys.path.insert(0, '.') 依賴於 CWD)
cd "$MISSION_DIR"
python3 src/main_logic.py # Python 腳本的輸出會直接顯示

EXECUTION_EXIT_CODE=$?
if [ $EXECUTION_EXIT_CODE -ne 0 ]; then
    echo "ERROR: python3 src/main_logic.py failed with exit code $EXECUTION_EXIT_CODE."
else
    echo "INFO: python3 src/main_logic.py completed."
fi

cd .. # 返回到原始目錄 (PROJECT_ROOT)
echo "INFO: Protocol Terminus concluded. Exit code from main_logic.py: $EXECUTION_EXIT_CODE."
exit $EXECUTION_EXIT_CODE
