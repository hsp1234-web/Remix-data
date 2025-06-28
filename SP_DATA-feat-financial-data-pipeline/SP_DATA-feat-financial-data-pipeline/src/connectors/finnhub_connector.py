import requests
import pandas as pd
import time
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple

# 預設日誌記錄器
logger = logging.getLogger(__name__)

class FinnhubConnector:
    """
    用於從 Finnhub API 獲取金融數據的連接器。
    """
    BASE_URL = "https://finnhub.io/api/v1"

    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 FinnhubConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典，
                                         應包含 'api_key' 和 'requests_per_minute'。
                                         例如:
                                         {
                                             "api_key": "YOUR_FINNHUB_API_KEY",
                                             "requests_per_minute": 60, # 免費方案限制
                                             "base_url": "https://finnhub.io/api/v1" # 可選
                                         }
        """
        self.api_key = api_config.get("api_key")
        if not self.api_key:
            logger.error("Finnhub API 金鑰未在設定中提供。")
            raise ValueError("Finnhub API 金鑰未設定。")

        self.requests_per_minute = api_config.get("requests_per_minute", 60)
        self.base_url = api_config.get("base_url", self.BASE_URL)

        self._last_request_time = 0
        # Finnhub 免費方案速率限制約為 60 次/分鐘
        self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

        self.session = requests.Session()
        self.session.headers.update({'X-Finnhub-Token': self.api_key})
        logger.info(f"FinnhubConnector 初始化完成。RPM: {self.requests_per_minute}, Interval: {self._min_request_interval:.2f}s")

    def _wait_for_rate_limit(self):
        """
        等待直到可以安全地發出下一個 API 請求。
        """
        if self._min_request_interval == 0:
            return
        now = time.time()
        elapsed_time = now - self._last_request_time
        wait_time = self._min_request_interval - elapsed_time
        if wait_time > 0:
            logger.debug(f"Finnhub 速率控制：等待 {wait_time:.2f} 秒。")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _make_request(self, endpoint_path: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Any], Optional[str]]:
        """
        向 Finnhub API 發出請求並處理基本錯誤。

        Args:
            endpoint_path (str): API 端點路徑 (例如 "/stock/profile2")。
            params (Optional[Dict[str, Any]]): 請求的查詢參數。

        Returns:
            Tuple[Optional[Any], Optional[str]]:
                成功時返回 (解析後的 JSON 數據 (通常是 dict 或 list), None)，
                失敗時返回 (None, 錯誤訊息字串)。
        """
        self._wait_for_rate_limit()

        url = f"{self.base_url}{endpoint_path}"
        # Finnhub API 金鑰在 header 中，但有些端點可能也接受 token 參數
        request_params = params.copy() if params else {}
        # request_params['token'] = self.api_key # 通常不需要，已在 header 設定

        param_log = {k:v for k,v in request_params.items() if k != 'token'}
        logger.debug(f"Finnhub API 請求：URL='{url}', Params='{param_log}'")

        try:
            response = self.session.get(url, params=request_params, timeout=30)
            response.raise_for_status()

            if response.status_code == 204:
                logger.info(f"Finnhub API 請求成功 (狀態碼 204)，但無內容返回：{url}")
                # 根據端點預期，可能返回 {} 或 []
                return {}, None

            try:
                data = response.json()
                # Finnhub 錯誤有時是 {"error": "message"}
                if isinstance(data, dict) and "error" in data:
                    error_message = data["error"]
                    logger.error(f"Finnhub API 錯誤訊息：{error_message} (URL: {url})")
                    return None, error_message

                logger.debug(f"Finnhub API 請求成功：URL='{url}'")
                return data, None
            except requests.exceptions.JSONDecodeError:
                logger.error(f"Finnhub API：無法解析 JSON 回應。URL: {url}, 狀態碼: {response.status_code}, 回應文本: {response.text[:200]}...")
                return None, f"無法解析 JSON 回應 (狀態碼 {response.status_code})"

        except requests.exceptions.HTTPError as e:
            error_message = f"Finnhub API HTTP 錯誤：{e.response.status_code} - {e.response.text[:200]}"
            if e.response.status_code == 401: # Unauthorized
                error_message = "Finnhub API 金鑰無效或未提供。"
            elif e.response.status_code == 403: # Forbidden
                 error_message = "Finnhub API 權限不足 (可能是金鑰級別不支援此端點，或已達速率/使用上限)。"
            elif e.response.status_code == 429: # Too Many Requests
                error_message = "Finnhub API 已達請求速率上限。"

            logger.error(f"{error_message} (URL: {url})")
            return None, error_message
        except requests.exceptions.RequestException as e:
            error_message = f"Finnhub API 請求錯誤：{e}"
            logger.error(f"{error_message} (URL: {url})")
            return None, error_message
        except Exception as e:
            error_message = f"處理 Finnhub API 請求時發生未知錯誤：{e}"
            logger.error(f"{error_message} (URL: {url})", exc_info=True)
            return None, error_message

    def get_company_profile(self, symbol: Optional[str] = None, isin: Optional[str] = None, cusip: Optional[str] = None) -> pd.DataFrame:
        """
        獲取公司基本資料 (Company Profile 2)。
        至少需要 symbol, isin, 或 cusip 中的一個。

        Args:
            symbol (Optional[str]): 股票代碼。
            isin (Optional[str]): ISIN。
            cusip (Optional[str]): CUSIP。

        Returns:
            pd.DataFrame: 包含公司基本資料的 DataFrame (單行)，若失敗則為空 DataFrame。
        """
        params = {}
        if symbol: params['symbol'] = symbol.upper()
        elif isin: params['isin'] = isin
        elif cusip: params['cusip'] = cusip
        else:
            logger.error("Finnhub get_company_profile: 必須提供 symbol, isin, 或 cusip。")
            return pd.DataFrame()

        profile_data, error = self._make_request("/stock/profile2", params)

        query_param_log = symbol or isin or cusip # 用於日誌
        if error or not profile_data or not isinstance(profile_data, dict) or not profile_data.get('ticker'):
            logger.warning(f"Finnhub: 無法獲取 {query_param_log} 的公司資料。錯誤：{error if error else '回應格式不符或 ticker 為空'}")
            return pd.DataFrame()

        data_dict = {
            'security_id': profile_data.get('ticker'), # Finnhub 'ticker' 通常是我們用的 symbol
            'name': profile_data.get('name'),
            'country': profile_data.get('country'),
            'currency': profile_data.get('currency'),
            'exchange': profile_data.get('exchange'),
            'ipo_date': profile_data.get('ipo'), # 日期格式 YYYY-MM-DD
            'market_cap': profile_data.get('marketCapitalization'), # 單位是百萬 (million)
            'shares_outstanding': profile_data.get('shareOutstanding'), # 單位是百萬 (million)
            'logo_url': profile_data.get('logo'),
            'website_url': profile_data.get('weburl'),
            'industry_group': profile_data.get('ggroup'), # GICS Industry Group
            'industry_sector': profile_data.get('gsector'), # GICS Sector
            'industry_subgroup': profile_data.get('gsubind'), # GICS Sub-Industry
            'industry_main': profile_data.get('gind'), # GICS Industry
            'finnhub_industry_classification': profile_data.get('finnhubIndustry'), # Finnhub's own classification
            'isin': profile_data.get('isin'), # 可能與查詢參數 isin 相同
            'cusip': profile_data.get('cusip'), # 可能與查詢參數 cusip 相同
            'source_api': 'finnhub',
            'last_updated_timestamp': datetime.now()
        }

        df = pd.DataFrame([data_dict])

        # 確保 market_cap 和 shares_outstanding 乘以百萬
        if 'market_cap' in df.columns and pd.notna(df['market_cap'].iloc[0]):
            df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce') * 1_000_000
        if 'shares_outstanding' in df.columns and pd.notna(df['shares_outstanding'].iloc[0]):
            df['shares_outstanding'] = pd.to_numeric(df['shares_outstanding'], errors='coerce') * 1_000_000

        return df

    def get_stock_candles(self, symbol: str, resolution: str, from_timestamp: int, to_timestamp: int) -> pd.DataFrame:
        """
        獲取股票 K 線數據 (Stock Candles)。
        注意：免費方案通常無法訪問此端點，或有嚴格限制。

        Args:
            symbol (str): 股票代碼。
            resolution (str): K 線週期。1, 5, 15, 30, 60, D, W, M。
            from_timestamp (int): Unix 時間戳 (秒)，開始時間。
            to_timestamp (int): Unix 時間戳 (秒)，結束時間。

        Returns:
            pd.DataFrame: 包含 OHLCV 數據的 DataFrame，若失敗則為空 DataFrame。
                          欄位：'price_date', 'security_id', 'open_price', 'high_price',
                                'low_price', 'close_price', 'volume', 'status_flag',
                                'source_api', 'last_updated_timestamp'
        """
        params = {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "from": from_timestamp,
            "to": to_timestamp
        }

        candle_data, error = self._make_request("/stock/candle", params)

        if error or not candle_data or not isinstance(candle_data, dict):
            logger.warning(f"Finnhub: 無法獲取 {symbol} 的 K 線數據 ({resolution})。錯誤：{error if error else '回應格式不符'}")
            return pd.DataFrame()

        if candle_data.get('s') == 'no_data': # Finnhub 表示無數據的方式
            logger.info(f"Finnhub: {symbol} 在指定期間無 K 線數據 ({resolution})。")
            return pd.DataFrame()

        if candle_data.get('s') != 'ok': # 其他錯誤狀態
            logger.warning(f"Finnhub: 獲取 {symbol} K 線數據時 API 狀態非 'ok': {candle_data.get('s')}")
            return pd.DataFrame()

        # 檢查是否有數據欄位
        required_keys = ['t', 'o', 'h', 'l', 'c', 'v']
        if not all(key in candle_data for key in required_keys):
            logger.warning(f"Finnhub: {symbol} K 線數據缺少必要欄位。返回數據: {str(candle_data)[:200]}")
            return pd.DataFrame()

        # 確保所有列表長度一致
        list_lengths = [len(candle_data[key]) for key in required_keys if isinstance(candle_data[key], list)]
        if not list_lengths or len(set(list_lengths)) != 1:
            logger.warning(f"Finnhub: {symbol} K 線數據列表長度不一致。Lengths: {list_lengths}")
            return pd.DataFrame()


        df = pd.DataFrame({
            'price_timestamp': candle_data['t'], # Unix timestamp (seconds)
            'open_price': candle_data['o'],
            'high_price': candle_data['h'],
            'low_price': candle_data['l'],
            'close_price': candle_data['c'],
            'volume': candle_data['v']
        })

        if df.empty:
            return pd.DataFrame()

        df['price_date'] = pd.to_datetime(df['price_timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.date # 假設美股數據
        # 或者，如果不需要轉換時區，直接 .dt.date (取決於數據定義)
        # df['price_date'] = pd.to_datetime(df['price_timestamp'], unit='s').dt.date

        df['security_id'] = symbol.upper()
        df['status_flag'] = candle_data.get('s', 'unknown') # 's' 欄位表示狀態 (ok, no_data)
        df['source_api'] = 'finnhub'
        df['last_updated_timestamp'] = datetime.now()

        standard_columns = [
            'price_date', 'security_id', 'open_price', 'high_price', 'low_price',
            'close_price', 'volume', 'status_flag',
            'source_api', 'last_updated_timestamp'
        ]
        return df[standard_columns]

    def get_basic_financials(self, symbol: str, metric_type: str = "all") -> pd.DataFrame:
        """
        獲取公司的基本財務數據 (Basic Financials)。
        包含多種指標，如 P/E, P/S, ROE, EPS 等。

        Args:
            symbol (str): 股票代碼。
            metric_type (str): "all", "price", "valuation", "margin", "management", "growth". 預設 "all".

        Returns:
            pd.DataFrame: 包含基本財務指標的 DataFrame，若失敗則為空 DataFrame。
                          格式為長表：'security_id', 'metric_period', 'metric_name', 'metric_value', ...
        """
        params = {"symbol": symbol.upper(), "metric": metric_type}
        data, error = self._make_request("/stock/metric", params) # Finnhub 端點是 /stock/metric

        if error or not data or not isinstance(data, dict) or 'metric' not in data or not data['metric']:
            logger.warning(f"Finnhub: 無法獲取 {symbol} 的基本財務數據 (類型: {metric_type})。錯誤：{error if error else '回應格式不符或無 metric 鍵'}")
            return pd.DataFrame()

        metrics = data['metric']
        if not isinstance(metrics, dict): # metrics 應該是字典
            logger.warning(f"Finnhub: {symbol} 的基本財務數據 metric 部分不是字典。")
            return pd.DataFrame()

        series_data = data.get('series', {}) # series 包含如 'annual', 'quarterly' 的時間序列數據
                                            # 但 /stock/metric 的主要數據在 'metric' 中，是點狀數據

        # metrics 字典的鍵是指標名稱，值是該指標的值
        all_metrics_long = []
        for metric_name, metric_value in metrics.items():
            record = {
                'security_id': symbol.upper(),
                'metric_period': 'current', # Basic financials 通常是當前或TTM值
                'metric_name': metric_name,
                'metric_value': pd.to_numeric(metric_value, errors='coerce') if metric_value is not None else pd.NA,
                'source_api': 'finnhub',
                'last_updated_timestamp': datetime.now()
            }
            all_metrics_long.append(record)

        if not all_metrics_long:
            return pd.DataFrame()

        df_long = pd.DataFrame(all_metrics_long)

        standard_columns = [
            'security_id', 'metric_period', 'metric_name', 'metric_value',
            'source_api', 'last_updated_timestamp'
        ]
        return df_long[standard_columns]

    # 可以根據 Finnhub API 文件添加更多方法...

if __name__ == '__main__':
    import os
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api_key_from_env = os.getenv("FINNHUB_API_KEY") # 請設定環境變數
    if not api_key_from_env:
        logger.error("測試 FinnhubConnector 需要設定環境變數 FINNHUB_API_KEY。")
    else:
        test_config = {
            "api_key": api_key_from_env,
            "requests_per_minute": 30 # 免費方案通常 60 RPM，測試時保守一點
        }
        connector = FinnhubConnector(api_config=test_config)

        logger.info("--- 測試 Finnhub get_company_profile ---")
        profile_df = connector.get_company_profile(symbol="AAPL")
        if not profile_df.empty:
            logger.info(f"AAPL Finnhub 公司資料 (部分欄位):\n{profile_df[['security_id', 'name', 'exchange', 'market_cap', 'finnhub_industry_classification']].iloc[0].to_string()}")
        else:
            logger.warning("未能獲取 AAPL Finnhub 公司資料。")

        time.sleep(2) # 等待 (60s / 30 RPM = 2s)

        logger.info("--- 測試 Finnhub get_basic_financials ---")
        financials_df = connector.get_basic_financials(symbol="MSFT", metric_type="all")
        if not financials_df.empty:
            # 篩選幾個指標來顯示
            display_metrics = ['peNormalizedAnnual', 'priceToSalesAnnual', 'roeTTM', 'epsGrowth5Y']
            logger.info(f"MSFT Finnhub 基本財務數據 (部分指標):\n{financials_df[financials_df['metric_name'].isin(display_metrics)].to_string()}")
        else:
            logger.warning("未能獲取 MSFT Finnhub 基本財務數據。")

        time.sleep(2)

        logger.info("--- 測試 Finnhub get_stock_candles (可能因權限失敗) ---")
        # 將時間戳轉換為整數秒
        # 例如，從 2023-01-01 00:00:00 UTC 到 2023-01-02 00:00:00 UTC
        from_ts = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=None).timestamp())
        to_ts = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=None).timestamp())

        candles_df = connector.get_stock_candles(symbol="GOOGL", resolution="D", from_timestamp=from_ts, to_timestamp=to_ts)
        if not candles_df.empty:
            logger.info(f"GOOGL Finnhub K 線數據 (日線):\n{candles_df.head().to_string()}")
        else:
            logger.warning("未能獲取 GOOGL Finnhub K 線數據。免費金鑰通常對此端點有限制。")

        logger.info("FinnhubConnector 測試完成。")
