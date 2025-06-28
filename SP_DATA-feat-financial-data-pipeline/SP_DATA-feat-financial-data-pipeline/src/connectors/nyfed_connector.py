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
logger = logging.getLogger(__name__)

class NYFedConnector:
    """
    從紐約聯儲網站獲取並解析一級交易商持倉數據 (或其他 Excel/CSV 數據)。
    包含讀取設定檔、速率控制、統一錯誤處理。
    """

    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 NYFedConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典。
                                         主要包含 'base_url' (或特定檔案的完整 URL) 和 'requests_per_minute'。
                                         也可能包含解析 Excel/CSV 的 "recipes"。
                                         例如:
                                         {
                                             "requests_per_minute": 30,
                                             "base_url": "https://www.newyorkfed.org/markets/desk-operations/ambs", // 範例
                                             "download_configs": [
                                                 {
                                                     "name": "primary_dealer_positions",
                                                     "url_template": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal{YYYY}.xlsx",
                                                     "file_pattern_on_page": "prideal{YYYY}.xlsx", // 用於在 HTML 頁面中尋找連結
                                                     "parser_recipe_name": "primary_dealer_positions_recipe",
                                                     "metric_name_override": "NYFED/PRIMARY_DEALER_NET_POSITION"
                                                 }
                                             ],
                                             "parser_recipes": {
                                                 "primary_dealer_positions_recipe": {
                                                     "header_row": 3, // Excel 中的標頭行號 (1-based)
                                                     "date_column": "As of Date",
                                                     "columns_to_sum": ["U.S. Treasury coupons", "U.S. Treasury bills"],
                                                     "data_unit_multiplier": 1000000 // 例如，數據單位是百萬
                                                 }
                                             }
                                         }
        """
        self.requests_per_minute = api_config.get("requests_per_minute", 30)
        self._last_request_time = 0
        self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

        self.source_api_name = "NYFED"
        self.session = requests.Session()
        # 設定標準的 User-Agent
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        # 儲存下載設定和解析配方
        self.download_configs = api_config.get("download_configs", [])
        self.parser_recipes = api_config.get("parser_recipes", {})

        # 全局請求配置 (如果有的話)
        self.global_requests_config = api_config.get("requests_config", {})


        logger.info(f"NYFedConnector 初始化完成。RPM: {self.requests_per_minute}, Interval: {self._min_request_interval:.2f}s")

    def _wait_for_rate_limit(self):
        """等待直到可以安全地發出下一個 API 請求。"""
        if self._min_request_interval == 0: return
        now = time.time()
        elapsed_time = now - self._last_request_time
        wait_time = self._min_request_interval - elapsed_time
        if wait_time > 0:
            logger.debug(f"NYFed 速率控制：等待 {wait_time:.2f} 秒。")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _download_file_content(self, url: str, file_pattern_hint: Optional[str] = None) -> Optional[BytesIO]:
        """
        下載指定 URL 的檔案內容。如果 URL 指向 HTML 頁面，則嘗試從頁面中尋找符合 file_pattern_hint 的檔案連結。
        包含重試邏輯。
        """
        self._wait_for_rate_limit()
        self._last_request_time = time.time()

        retries = self.global_requests_config.get('max_retries', 3)
        base_backoff = self.global_requests_config.get('base_backoff_seconds', 2) # 增加基礎退避時間
        timeout_sec = self.global_requests_config.get('download_timeout', 60)

        for attempt in range(retries):
            try:
                logger.debug(f"NYFed: 嘗試 {attempt + 1}/{retries} 訪問/下載資源: {url}")
                page_response = self.session.get(url, timeout=timeout_sec)
                page_response.raise_for_status() # 檢查 HTTP 錯誤

                content_type = page_response.headers.get('Content-Type', '').lower()
                logger.info(f"NYFed: URL: {url}, 狀態: {page_response.status_code}, Content-Type: {content_type}")

                if 'text/html' in content_type:
                    logger.info(f"NYFed: {url} 是 HTML 頁面。嘗試尋找檔案連結 (提示: '{file_pattern_hint}')。")
                    soup = BeautifulSoup(page_response.content, 'html.parser')

                    excel_link_found = None
                    possible_links = soup.find_all('a', href=True)

                    for link_tag in possible_links:
                        href = link_tag['href']
                        # 優先尋找包含 file_pattern_hint 且以 .xlsx 或 .csv 結尾的連結
                        if file_pattern_hint and file_pattern_hint.lower() in href.lower() and (href.lower().endswith('.xlsx') or href.lower().endswith('.csv')):
                            excel_link_found = href; break
                        # 次要：尋找包含 "prideal" (一級交易商數據常見關鍵字) 且為 Excel 的連結
                        if not excel_link_found and 'prideal' in href.lower() and href.lower().endswith('.xlsx'):
                            excel_link_found = href; break
                        # 次要：尋找包含年份 (如果提示中有) 且為 Excel 的連結
                        if not excel_link_found and file_pattern_hint:
                            year_in_hint = "".join(filter(str.isdigit, file_pattern_hint))
                            if year_in_hint and year_in_hint in href and href.lower().endswith('.xlsx'):
                                excel_link_found = href; break

                    if excel_link_found:
                        download_url = urljoin(url, excel_link_found) # 確保是絕對 URL
                        logger.info(f"NYFed: 在 HTML 頁面找到檔案連結: {download_url}。正在下載...")
                        self._wait_for_rate_limit() # 為下載連結本身也做一次速率控制
                        self._last_request_time = time.time()
                        file_response = self.session.get(download_url, timeout=timeout_sec)
                        file_response.raise_for_status()
                        logger.info(f"NYFed: 已下載檔案 {download_url}, 狀態: {file_response.status_code}, 大小: {len(file_response.content)} bytes.")
                        return BytesIO(file_response.content)
                    else:
                        logger.warning(f"NYFed: 在 HTML 頁面 {url} 未找到符合 '{file_pattern_hint}' 的檔案連結。")
                        return None # 未找到連結，不再重試此 URL

                # 如果不是 HTML，則假定是直接的檔案下載
                elif any(ct_part in content_type for ct_part in ['excel', 'spreadsheetml', 'officedocument', 'csv', 'application/octet-stream']):
                    logger.info(f"NYFed: {url} 似乎是直接的檔案連結。已下載 {len(page_response.content)} bytes。")
                    return BytesIO(page_response.content)
                else:
                    logger.warning(f"NYFed: {url} 的 Content-Type ('{content_type}') 不是預期的 HTML 或檔案類型。")
                    return None # 類型不符，不再重試此 URL

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404: # 找不到檔案
                    logger.warning(f"NYFed: HTTP 404 錯誤 for {url} (嘗試 {attempt + 1})。檔案可能不存在。")
                    return None # 404 通常不應重試
                logger.warning(f"NYFed: HTTP 錯誤 on attempt {attempt + 1}/{retries} for {url}: {e}")
                if attempt == retries - 1: return None # 最後一次嘗試失敗
            except requests.exceptions.RequestException as e:
                logger.warning(f"NYFed: RequestException on attempt {attempt + 1}/{retries} for {url}: {e}")
                if attempt == retries - 1: return None

            # 指數退避
            sleep_time = base_backoff * (2 ** attempt) + random.uniform(0, 0.5)
            logger.info(f"NYFed: 等待 {sleep_time:.2f} 秒後重試 {url}...")
            time.sleep(sleep_time)

        logger.error(f"NYFed: 所有下載嘗試均失敗 for {url}。")
        return None

    def get_configured_data(self) -> pd.DataFrame:
        """
        根據設定檔中的 `download_configs` 遍歷並獲取所有設定的數據。
        返回一個合併了所有成功獲取並解析的數據的 DataFrame。
        """
        if not self.download_configs:
            logger.warning("NYFedConnector: 設定檔中未找到 'download_configs'。")
            return self._create_empty_standard_df()

        all_data_frames = []
        current_year = datetime.now().year

        for dl_config in self.download_configs:
            config_name = dl_config.get("name", "未命名設定")
            url_template = dl_config.get("url_template")
            file_pattern_on_page = dl_config.get("file_pattern_on_page") # 用於在 HTML 頁面中定位連結
            parser_recipe_name = dl_config.get("parser_recipe_name")
            metric_name_override = dl_config.get("metric_name_override", f"{self.source_api_name}/{config_name.upper()}")

            # 處理 URL 模板中的年份 (如果存在)
            # 假設我們只需要當前年份的數據，或者模板不含年份
            # 更複雜的邏輯可以遍歷年份範圍
            url_to_fetch = url_template.replace("{YYYY}", str(current_year)) if "{YYYY}" in url_template else url_template
            pattern_to_find = file_pattern_on_page.replace("{YYYY}", str(current_year)) if file_pattern_on_page and "{YYYY}" in file_pattern_on_page else file_pattern_on_page


            logger.info(f"NYFed: 開始處理設定 '{config_name}' (URL: {url_to_fetch}, Recipe: {parser_recipe_name})")

            file_content_bytesio = self._download_file_content(url_to_fetch, pattern_to_find)
            if not file_content_bytesio:
                logger.warning(f"NYFed: 未能下載設定 '{config_name}' 的檔案內容。")
                continue

            recipe = self.parser_recipes.get(parser_recipe_name)
            if not recipe:
                logger.warning(f"NYFed: 未找到名為 '{parser_recipe_name}' 的解析配方 for '{config_name}'。")
                continue

            try:
                # 根據檔案類型 (從 URL 或 recipe 暗示) 選擇解析器
                is_csv = url_to_fetch.lower().endswith('.csv') or recipe.get("file_type") == "csv"

                if is_csv:
                    # CSV 解析邏輯 (可以做得更細緻，例如處理分隔符、編碼等)
                    header_row_csv = recipe.get('header_row', 1) -1 # header=0 is first line
                    df_raw = pd.read_csv(file_content_bytesio, header=header_row_csv, encoding=recipe.get('encoding', 'utf-8'))
                else: # 預設為 Excel
                    header_row_excel = recipe.get('header_row', 1) - 1 # openpyxl header is 0-indexed
                    sheet_name = recipe.get('sheet_name', 0) # 預設第一個 sheet
                    df_raw = pd.read_excel(file_content_bytesio, header=header_row_excel, sheet_name=sheet_name, engine='openpyxl')

                if df_raw.empty:
                    logger.info(f"NYFed: 解析後檔案 '{config_name}' 為空。"); continue

                date_col = recipe.get('date_column')
                if not date_col or date_col not in df_raw.columns:
                    logger.error(f"NYFed: 日期欄位 '{date_col}' 不在檔案 '{config_name}' 中。可用欄位: {df_raw.columns.tolist()}")
                    continue

                df_transformed = df_raw[[date_col]].copy()
                df_transformed.rename(columns={date_col: 'metric_date'}, inplace=True)
                df_transformed['metric_date'] = pd.to_datetime(df_transformed['metric_date'], errors='coerce')
                df_transformed.dropna(subset=['metric_date'], inplace=True)

                if df_transformed.empty:
                    logger.info(f"NYFed: 檔案 '{config_name}' 在日期轉換後無有效數據。"); continue

                # 處理需要加總的欄位
                cols_to_sum = recipe.get('columns_to_sum', [])
                value_col_direct = recipe.get('value_column') # 如果是直接取某一列的值

                if cols_to_sum:
                    actual_cols_present = [col for col in cols_to_sum if col in df_raw.columns]
                    if not actual_cols_present:
                        logger.warning(f"NYFed: 配方中指定的加總欄位 {cols_to_sum} 均不在檔案 '{config_name}' 中。")
                        continue
                    for col in actual_cols_present: # 確保是數字類型
                        df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce')
                    df_transformed['metric_value'] = df_raw[actual_cols_present].sum(axis=1, skipna=True)
                elif value_col_direct and value_col_direct in df_raw.columns:
                    df_transformed['metric_value'] = pd.to_numeric(df_raw[value_col_direct], errors='coerce')
                else:
                    logger.warning(f"NYFed: 配方 '{parser_recipe_name}' 未指定 'columns_to_sum' 或有效的 'value_column'。")
                    continue

                multiplier = recipe.get('data_unit_multiplier', 1)
                df_transformed['metric_value'] *= multiplier
                df_transformed.dropna(subset=['metric_value'], inplace=True)

                if df_transformed.empty:
                    logger.info(f"NYFed: 檔案 '{config_name}' 在計算指標值後無有效數據。"); continue

                df_transformed['security_id'] = metric_name_override # 將指標名作為 security_id
                df_transformed['metric_name'] = metric_name_override
                df_transformed['source_api'] = self.source_api_name
                df_transformed['last_updated_timestamp'] = datetime.now(timezone.utc)

                all_data_frames.append(df_transformed[self._get_standard_columns()])
                logger.info(f"NYFed: 成功處理設定 '{config_name}', {len(df_transformed)} 筆記錄。")

            except Exception as e:
                logger.error(f"NYFed: 解析設定 '{config_name}' 的檔案時發生錯誤: {e}", exc_info=True)
                continue

        if not all_data_frames:
            logger.warning("NYFed: 未能從任何設定的來源成功獲取和解析數據。")
            return self._create_empty_standard_df()

        final_df_combined = pd.concat(all_data_frames, ignore_index=True)

        # 數據後處理 (例如排序、去重、填充缺失日)
        if not final_df_combined.empty:
            final_df_combined.sort_values(by=['security_id', 'metric_date'], inplace=True)
            # 根據 security_id 和 metric_date 去重，保留最新的 (儘管這裡的 timestamp 都一樣)
            final_df_combined.drop_duplicates(subset=['security_id', 'metric_date'], keep='last', inplace=True)

            # 如果需要，可以對每個 security_id 進行日期填充 (ffill)
            # 這取決於數據的性質。對於某些每日報告的位置數據，ffill 是合理的。
            # grouped = final_df_combined.groupby('security_id')
            # filled_dfs = []
            # for name, group in grouped:
            #     group = group.set_index('metric_date').asfreq('D').ffill()
            #     group['security_id'] = name # ffill 可能會清除這個
            #     group.reset_index(inplace=True)
            #     filled_dfs.append(group)
            # if filled_dfs:
            #     final_df_combined = pd.concat(filled_dfs, ignore_index=True)
            #     # 重新填充 ffill 可能清除的靜態列
            #     final_df_combined['source_api'] = self.source_api_name
            #     # metric_name 可能也需要重新填充，如果它在 ffill 過程中丟失
            #     # last_updated_timestamp 也應是原始獲取時間，ffill 後的也應一致

        logger.info(f"NYFed: 所有設定處理完成，共合併 {len(final_df_combined)} 筆記錄。")
        return final_df_combined

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
    import sys
    import shutil

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])

    # 模擬的 config.yaml 內容
    # 注意：此處的 URL 和解析配方僅為範例，可能需要根據實際 NYFed 網站結構調整
    mock_api_config_nyfed = {
        "requests_per_minute": 20, # 測試時降低速率
        "download_configs": [
            {
                "name": "primary_dealer_stats_current_year", # 主要交易商統計 (當年)
                # 假設 NYFed 將每年的報告放在類似這樣的 URL 結構下
                "url_template": "https://www.newyorkfed.org/markets/primarydealer_statistics/financial_condition",
                # file_pattern_on_page 需要包含年份和 .xlsx，例如 "Primary Dealer Financial Condition Data – YYYY.xlsx"
                # 或者更具體，如 "Primary Dealer Financial Condition Data – 2023.xlsx" (如果模板中不含 {YYYY})
                # 這裡假設 HTML 頁面上有一個連結文字或 href 包含 "Primary Dealer Financial Condition Data" 和 ".xlsx"
                "file_pattern_on_page": "Dealer Financial Condition Data", # 簡化匹配模式
                "parser_recipe_name": "nyfed_dealer_condition_recipe",
                "metric_name_override": "NYFED/DEALER_FINANCIAL_CONDITION_TOTAL_ASSETS" # 假設我們只關心總資產
            },
            # 可以添加更多 download_configs，例如不同報告或歷史年份
        ],
        "parser_recipes": {
            "nyfed_dealer_condition_recipe": {
                # 這些值需要根據實際 Excel 檔案的結構來確定
                "header_row": 5,  # 假設數據從第5行開始 (Excel中行號)
                "date_column": "Reporting Date", # 假設日期欄位名
                # "columns_to_sum": ["Total assets", "Another Metric"], # 如果需要加總多列
                "value_column": "Total assets", # 如果直接取某一列的值
                "data_unit_multiplier": 1000000, # 假設數據單位是百萬美元
                "sheet_name": "Table 1" # 假設數據在名為 "Table 1" 的工作表
            }
        },
        "requests_config": { # 全局請求設定 (如果 download_file_content 使用)
             "max_retries": 2,
             "base_backoff_seconds": 1,
             "download_timeout": 45
        }
    }

    logger.info("--- 開始 NYFedConnector 測試 ---")
    nyfed_connector = NYFedConnector(api_config=mock_api_config_nyfed)

    # 執行 get_configured_data
    df_nyfed_result = nyfed_connector.get_configured_data()

    if not df_nyfed_result.empty:
        logger.info(f"NYFed 測試成功。獲取數據 shape: {df_nyfed_result.shape}")
        logger.info(f"NYFed 數據 (前5筆):\n{df_nyfed_result.head().to_string()}")
        logger.info(f"NYFed 數據 (後5筆):\n{df_nyfed_result.tail().to_string()}")

        unique_metrics_ny = df_nyfed_result['metric_name'].unique()
        logger.info(f"獲取到的指標: {unique_metrics_ny}")
    else:
        logger.warning("NYFedConnector 測試未返回任何數據。請檢查 URL、檔案模式和解析配方是否與 NYFed 網站當前結構一致。")

    logger.info("--- NYFedConnector 測試完成 ---")
