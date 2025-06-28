import abc
import pandas as pd
import requests
import time
import logging
from typing import Optional, Dict, Any, Tuple
import random # Added for jitter

# 建議將日誌記錄器實例化在模組級別
logger = logging.getLogger(__name__)

class BaseConnector(abc.ABC):
    """
    抽象基類，用於定義所有 API 連接器的通用接口。
    """

    def __init__(self, api_key: Optional[str] = None, source_name: str = "UnknownSource", config: Optional[Dict[str, Any]] = None):
        """
        初始化 BaseConnector。

        Args:
            api_key (Optional[str]): 特定 API 的金鑰。
            source_name (str): 此連接器的數據源名稱 (例如 "fmp", "finmind")。
            config (Optional[Dict[str, Any]]): 全局配置字典，通常從 config.yaml 加載。
                                                用於獲取如請求超時、重試次數等通用設置。
        """
        self.api_key = api_key
        self.source_name = source_name
        self.config = config if config else {}

        # 從配置中獲取請求相關參數，或使用預設值
        self.timeout = self.config.get("requests_config", {}).get("timeout", 30)
        self.max_retries = self.config.get("requests_config", {}).get("max_retries", 3)

        # 創建一個 requests.Session 對象以實現連接池和共享配置
        self.session = requests.Session()
        base_headers = self.config.get("requests_config", {}).get("base_headers", {})
        if base_headers:
            self.session.headers.update(base_headers)

        # 如果子類需要特定的 headers，可以在其 __init__ 中進一步更新 self.session.headers

    def _make_request(self, url: str, method: str = "GET", params: Optional[Dict] = None, data: Optional[Dict] = None, headers: Optional[Dict] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        一個帶有重試邏輯的通用請求方法。

        Args:
            url (str): 請求的 URL。
            method (str): HTTP 方法 (GET, POST, etc.)。
            params (Optional[Dict]): URL 查詢參數。
            data (Optional[Dict]): POST 請求的 body 數據。
            headers (Optional[Dict]): 自定義請求頭，會覆蓋 session 的 headers。

        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[str]]:
                成功時返回 (解析後的 JSON 數據, None)，
                失敗時返回 (None, 錯誤訊息)。
        """
        attempt = 0
        effective_headers = self.session.headers.copy()
        if headers:
            effective_headers.update(headers)

        while attempt < self.max_retries:
            try:
                response = self.session.request(method, url, params=params, json=data, headers=effective_headers, timeout=self.timeout)
                response.raise_for_status()  # 如果 HTTP 狀態碼是 4xx 或 5xx，則拋出異常

                # 嘗試解析 JSON，如果響應為空或非 JSON，則優雅處理
                try:
                    json_response = response.json()
                    logger.debug(f"Request to {url} successful. Status: {response.status_code}. Response snippet: {str(json_response)[:200]}...")
                    return json_response, None
                except requests.exceptions.JSONDecodeError:
                    logger.warning(f"Failed to decode JSON from {url}. Status: {response.status_code}. Response text: {response.text[:200]}...")
                    if response.text: # 如果有文本內容但不是JSON，也許也該返回
                         return {"raw_text": response.text}, f"Non-JSON response with status {response.status_code}" # 或者定義一個特定的返回格式
                    return None, f"Failed to decode JSON from {url}. Status: {response.status_code}"


            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error on attempt {attempt + 1}/{self.max_retries} for {url}: {e}")
                # 對於某些特定的 HTTP 錯誤（如 429 Too Many Requests），可以實現更精細的退避策略
                if response.status_code == 429: # Too Many Requests
                    retry_after = int(response.headers.get("Retry-After", "10")) # 預設等待10秒
                    logger.info(f"Rate limit hit for {url}. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                # 其他 4xx 錯誤通常不應重試，因為請求本身可能有問題
                elif 400 <= response.status_code < 500 and response.status_code != 429:
                    return None, f"Client error {response.status_code} for {url}: {e}. Response: {response.text[:200]}"
                # 5xx 服務器錯誤可以重試
                # No specific action needed here, loop will continue for retries

            except requests.exceptions.RequestException as e: # 包括 ConnectionError, Timeout, etc.
                logger.warning(f"Request exception on attempt {attempt + 1}/{self.max_retries} for {url}: {e}")

            attempt += 1
            if attempt < self.max_retries:
                backoff_delay = 2 ** attempt # 指數退避
                jitter = random.uniform(0, 0.5) # 抖動因子 (0到0.5秒之間，避免過長)
                total_sleep_time = backoff_delay + jitter

                logger.info(f"Retrying in {total_sleep_time:.2f} seconds... (Attempt {attempt}/{self.max_retries})")
                time.sleep(total_sleep_time)

        logger.error(f"Failed to fetch data from {url} after {self.max_retries} attempts.")
        return None, f"Failed to fetch data from {url} after {self.max_retries} attempts."

    @abc.abstractmethod
    def fetch_data(self, **kwargs) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        從 API 抓取原始數據。
        子類必須實現此方法。

        Args:
            **kwargs: 特定 API 端點所需的參數 (例如 symbol, start_date, end_date)。

        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[str]]:
                成功時返回 (原始 API 響應數據 (通常是 dict), None)，
                失敗時返回 (None, 錯誤訊息)。
        """
        pass

    @abc.abstractmethod
    def transform_to_canonical(self, raw_data: Dict[str, Any], **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        將從 API 獲取的原始數據轉換為我們內部統一的 Pandas DataFrame 格式。
        子類必須實現此方法。

        Args:
            raw_data (Dict[str, Any]): `fetch_data` 方法返回的原始數據。
            **kwargs: 可能需要的額外轉換參數 (例如 target_schema_name)。

        Returns:
            Tuple[Optional[pd.DataFrame], Optional[str]]:
                成功時返回 (轉換後的 DataFrame, None)，
                數據無效或轉換失敗時返回 (None, 錯誤訊息)。
        """
        pass

    def get_data(self, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        一個完整的流程方法，用於抓取並轉換數據。
        子類通常不需要覆蓋此方法，除非有非常特殊的流程。

        Args:
            **kwargs: 傳遞給 `fetch_data` 和 `transform_to_canonical` 的參數。

        Returns:
            Tuple[Optional[pd.DataFrame], Optional[str]]:
                成功時返回 (標準化的 DataFrame, None)，
                失敗時返回 (None, 錯誤訊息)。
        """
        logger.info(f"[{self.source_name}] Starting data retrieval with params: {kwargs}")

        raw_data, error = self.fetch_data(**kwargs)
        if error:
            logger.error(f"[{self.source_name}] Failed during fetch_data: {error}")
            return None, error
        if raw_data is None: # fetch_data 可能因某些原因返回 None 但沒有明確 error message
            logger.error(f"[{self.source_name}] Failed during fetch_data: No data returned and no explicit error message.")
            return None, "No data returned from API and no explicit error message."

        logger.info(f"[{self.source_name}] Data fetched successfully. Starting transformation.")

        # 從 kwargs 中分離出 transform_to_canonical 可能需要的特定參數
        # 例如，如果 fetch_data 和 transform_to_canonical 都接受 'symbol'，
        # 則不需要特別處理。但如果 transform_to_canonical 需要 'target_table_name'
        # 而 fetch_data 不需要，則需要確保它被正確傳遞。
        # 這裡假設 kwargs 包含了兩者所需的所有參數。
        transformed_df, error = self.transform_to_canonical(raw_data, **kwargs)

        if error:
            logger.error(f"[{self.source_name}] Failed during transform_to_canonical: {error}")
            return None, error
        if transformed_df is None:
             logger.error(f"[{self.source_name}] Failed during transform_to_canonical: No data returned and no explicit error message.")
             return None, "No data transformed and no explicit error message."


        if transformed_df.empty:
            logger.warning(f"[{self.source_name}] Transformation resulted in an empty DataFrame.")
            # 根據業務邏輯，空 DataFrame 可能是一個有效的結果，也可能不是。
            # 這裡我們將其視為一個警告，但仍返回它。
            # 如果空 DataFrame 總是錯誤的，可以在這裡返回錯誤。

        logger.info(f"[{self.source_name}] Data transformed successfully. Shape: {transformed_df.shape}")
        return transformed_df, None

    def _get_config_value(self, key_path: str, default: Any = None) -> Any:
        """
        輔助方法，用於從嵌套的配置字典中安全地獲取值。
        例如: _get_config_value("api.fmp.url", "http://default.url")
        """
        keys = key_path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
