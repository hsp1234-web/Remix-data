import requests
import pandas as pd
import time
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple

# 預設日誌記錄器
logger = logging.getLogger(__name__)

class PolygonIOConnector:
    """
    用於從 Polygon.io API 獲取金融數據的連接器。
    """
    BASE_URL = "https://api.polygon.io"

    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 PolygonIOConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典，
                                         應包含 'api_key' 和 'requests_per_minute'。
                                         例如:
                                         {
                                             "api_key": "YOUR_POLYGON_API_KEY",
                                             "requests_per_minute": 5,
                                             "base_url": "https://api.polygon.io" # 可選，如果想覆蓋預設
                                         }
        """
        self.api_key = api_config.get("api_key")
        if not self.api_key:
            logger.error("Polygon.io API 金鑰未在設定中提供。")
            raise ValueError("Polygon.io API 金鑰未設定。")

        self.requests_per_minute = api_config.get("requests_per_minute", 5) # 預設為免費方案的 5 RPM
        self.base_url = api_config.get("base_url", self.BASE_URL)
        self._last_request_time = 0
        self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

        logger.info(f"PolygonIOConnector 初始化完成。RPM: {self.requests_per_minute}, Interval: {self._min_request_interval:.2f}s")

    def _wait_for_rate_limit(self):
        """
        等待直到可以安全地發出下一個 API 請求，以符合速率限制。
        """
        if self._min_request_interval == 0:
            return

        now = time.time()
        elapsed_time = now - self._last_request_time
        wait_time = self._min_request_interval - elapsed_time

        if wait_time > 0:
            logger.debug(f"Polygon.io 速率控制：等待 {wait_time:.2f} 秒。")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        向 Polygon.io API 發出請求並處理基本錯誤。

        Args:
            endpoint (str): API 端點路徑 (例如 "/v2/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-01-10")。
            params (Optional[Dict[str, Any]]): 請求的查詢參數。

        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[str]]:
                成功時返回 (解析後的 JSON 數據, None)，
                失敗時返回 (None, 錯誤訊息字串)。
        """
        self._wait_for_rate_limit()
        url = f"{self.base_url}{endpoint}"
        common_params = {'apiKey': self.api_key} # 有些端點可能仍需 apiKey 在 params
        if params:
            params.update(common_params)
        else:
            params = common_params

        request_params_log = {k: v for k, v in params.items() if k != 'apiKey'} # 不記錄金鑰
        logger.debug(f"Polygon.io 請求：URL='{url}', Params='{request_params_log}'")

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status() # 如果 HTTP 狀態碼是 4xx 或 5xx，則拋出異常

            # 檢查請求是否成功但沒有內容 (例如 204 No Content)
            if response.status_code == 204:
                logger.info(f"Polygon.io API 請求成功 (狀態碼 204)，但無內容返回：{url}")
                return {}, None # 返回空字典代表成功但無數據

            # 嘗試解析 JSON
            try:
                data = response.json()
                # Polygon.io 錯誤通常在 JSON 內，例如 {"status": "ERROR", "error": "..."}
                if data.get("status") == "ERROR" or data.get("error"):
                    error_message = data.get("error", data.get("message", "Polygon.io API 返回錯誤狀態。"))
                    logger.error(f"Polygon.io API 錯誤：{error_message} (URL: {url}, Params: {request_params_log})")
                    return None, error_message
                if data.get("status") == "DELAYED":
                    logger.warning(f"Polygon.io API 數據延遲：(URL: {url}, Params: {request_params_log})")

                logger.debug(f"Polygon.io API 請求成功：URL='{url}', 結果狀態='{data.get('status', 'N/A')}'")
                return data, None
            except requests.exceptions.JSONDecodeError:
                logger.error(f"Polygon.io API：無法解析 JSON 回應。URL: {url}, 狀態碼: {response.status_code}, 回應文本: {response.text[:200]}...")
                return None, f"無法解析 JSON 回應 (狀態碼 {response.status_code})"

        except requests.exceptions.HTTPError as e:
            error_message = f"Polygon.io API HTTP 錯誤：{e.response.status_code} - {e.response.text[:200]}"
            logger.error(f"{error_message} (URL: {url}, Params: {request_params_log})")
            return None, error_message
        except requests.exceptions.RequestException as e: # 例如 Timeout, ConnectionError
            error_message = f"Polygon.io API 請求錯誤：{e}"
            logger.error(f"{error_message} (URL: {url}, Params: {request_params_log})")
            return None, error_message
        except Exception as e:
            error_message = f"處理 Polygon.io API 請求時發生未知錯誤：{e}"
            logger.error(f"{error_message} (URL: {url}, Params: {request_params_log})", exc_info=True)
            return None, error_message

    def get_historical_price(self, symbol: str, start_date: str, end_date: str, adjusted: bool = True, limit: int = 5000) -> pd.DataFrame:
        """
        獲取指定股票的歷史日線價格數據 (Aggregates/Bars)。

        Args:
            symbol (str): 股票代碼 (例如 "AAPL")。
            start_date (str): 開始日期 (YYYY-MM-DD)。
            end_date (str): 結束日期 (YYYY-MM-DD)。
            adjusted (bool): 是否獲取調整後的價格。預設為 True。
            limit (int): 返回的最大數據點數量。預設為 5000。 Polygon 免費方案對歷史數據範圍有限制。

        Returns:
            pd.DataFrame: 包含 OHLCV 數據的 DataFrame，若失敗則為空 DataFrame。
                          欄位將標準化為：'price_date', 'security_id', 'open_price', 'high_price',
                                         'low_price', 'close_price', 'volume', 'source_api',
                                         'last_updated_timestamp'。
        """
        endpoint = f"/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": "asc",
            "limit": limit
        }

        raw_data, error = self._make_request(endpoint, params)

        if error or not raw_data or not raw_data.get("results"):
            logger.warning(f"Polygon.io: 無法獲取 {symbol} 的歷史價格數據。錯誤：{error if error else '無結果返回'}")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data["results"])
        if df.empty:
            logger.info(f"Polygon.io: {symbol} 在 {start_date} 到 {end_date} 期間無歷史價格數據。")
            return pd.DataFrame()

        # Polygon.io 't' 是 Unix毫秒時間戳
        df['price_date'] = pd.to_datetime(df['t'], unit='ms').dt.date
        df.rename(columns={
            'o': 'open_price', 'h': 'high_price',
            'l': 'low_price', 'c': 'close_price', 'v': 'volume'
        }, inplace=True)

        df['security_id'] = symbol
        df['source_api'] = 'polygon.io'
        df['last_updated_timestamp'] = datetime.now()

        # 確保所有標準欄位存在
        standard_columns = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'source_api', 'last_updated_timestamp']
        for col in standard_columns:
            if col not in df.columns:
                df[col] = None # 或 pd.NA

        return df[standard_columns]

    def get_company_profile(self, symbol: str) -> pd.DataFrame:
        """
        獲取指定公司的基本資料 (Ticker Details)。

        Args:
            symbol (str): 股票代碼 (例如 "AAPL")。

        Returns:
            pd.DataFrame: 包含公司基本資料的 DataFrame (單行)，若失敗則為空 DataFrame。
                          欄位將包含 'security_id', 'name', 'description', 'sector', 'industry', 'country', 'logo_url', 'source_api', 'last_updated_timestamp' 等。
        """
        endpoint = f"/v3/reference/tickers/{symbol}"

        raw_data, error = self._make_request(endpoint)

        if error or not raw_data or not raw_data.get("results"):
            logger.warning(f"Polygon.io: 無法獲取 {symbol} 的公司資料。錯誤：{error if error else '無結果返回'}")
            return pd.DataFrame()

        profile = raw_data["results"]

        data_dict = {
            'security_id': profile.get('ticker'),
            'name': profile.get('name'),
            'description': profile.get('description'),
            'sector': profile.get('sector'), # Polygon v3 Ticker Details 可能沒有 'sector'，但有 'sic_description' 或 'industry'
            'industry': profile.get('industry', profile.get('sic_description')), # sic_description 可能更接近行業
            'country': profile.get('locale', '').upper() if profile.get('locale') else None, # 'locale' is country code
            'exchange': profile.get('primary_exchange'),
            'market_cap': profile.get('market_cap'),
            'employees': profile.get('total_employees'),
            'list_date': profile.get('list_date'),
            'website_url': profile.get('homepage_url'),
            'logo_url': profile.get('branding', {}).get('logo_url'),
            'icon_url': profile.get('branding', {}).get('icon_url'),
            'source_api': 'polygon.io',
            'last_updated_timestamp': datetime.now()
        }

        df = pd.DataFrame([data_dict])

        # 標準欄位列表 (可以根據需求擴充)
        standard_columns = [
            'security_id', 'name', 'description', 'sector', 'industry', 'country', 'exchange',
            'market_cap', 'employees', 'list_date', 'website_url', 'logo_url', 'icon_url',
            'source_api', 'last_updated_timestamp'
        ]
        # 確保所有標準欄位存在
        for col in standard_columns:
            if col not in df.columns:
                df[col] = None

        return df[standard_columns]

    # 可以根據需要添加更多 Polygon.io API 端點的對應方法，例如：
    # get_financial_statements, get_news, get_dividends, get_splits 等。
    # 每個方法都應包含錯誤處理和速率控制。

if __name__ == '__main__':
    # 簡易測試 (需要您在環境中設定 POLYGON_API_KEY)
    import os
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api_key_from_env = os.getenv("POLYGON_API_KEY")
    if not api_key_from_env:
        logger.error("測試 PolygonIOConnector 需要設定環境變數 POLYGON_API_KEY。")
    else:
        test_config = {
            "api_key": api_key_from_env,
            "requests_per_minute": 5 # 遵守免費方案限制
        }
        connector = PolygonIOConnector(api_config=test_config)

        logger.info("--- 測試 Polygon.io get_historical_price ---")
        price_df = connector.get_historical_price("AAPL", "2023-01-01", "2023-01-10")
        if not price_df.empty:
            logger.info(f"AAPL 歷史價格 (前5筆):\n{price_df.head().to_string()}")
        else:
            logger.warning("未能獲取 AAPL 歷史價格。")

        time.sleep(12) # 等待一下，避免立即觸發速率限制

        logger.info("--- 測試 Polygon.io get_company_profile ---")
        profile_df = connector.get_company_profile("MSFT")
        if not profile_df.empty:
            logger.info(f"MSFT 公司資料:\n{profile_df.iloc[0].to_string()}")
        else:
            logger.warning("未能獲取 MSFT 公司資料。")

        # 測試不存在的股票
        time.sleep(12)
        logger.info("--- 測試 Polygon.io get_historical_price (不存在的股票) ---")
        non_existent_price_df = connector.get_historical_price("NONEXISTENTTICKERXYZ", "2023-01-01", "2023-01-10")
        if non_existent_price_df.empty:
            logger.info("成功：對於不存在的股票，返回了空的 DataFrame。")
        else:
            logger.error(f"錯誤：對於不存在的股票，未返回空的 DataFrame。\n{non_existent_price_df.head().to_string()}")

        time.sleep(12)
        logger.info("--- 測試 Polygon.io get_company_profile (不存在的股票) ---")
        non_existent_profile_df = connector.get_company_profile("NONEXISTENTTICKERXYZ")
        if non_existent_profile_df.empty:
            logger.info("成功：對於不存在的股票，公司資料返回了空的 DataFrame。")
        else:
            logger.error(f"錯誤：對於不存在的股票，公司資料未返回空的 DataFrame。\n{non_existent_profile_df.head().to_string()}")

        logger.info("PolygonIOConnector 測試完成。")
