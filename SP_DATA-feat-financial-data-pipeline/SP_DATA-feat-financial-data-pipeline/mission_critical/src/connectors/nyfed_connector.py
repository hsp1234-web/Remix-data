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
