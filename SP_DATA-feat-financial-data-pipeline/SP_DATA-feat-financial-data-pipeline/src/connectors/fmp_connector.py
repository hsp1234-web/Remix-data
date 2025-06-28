import requests
import pandas as pd
import time
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple

# 預設日誌記錄器
logger = logging.getLogger(__name__)

class FMPConnector:
    """
    用於從 Financial Modeling Prep (FMP) API 獲取金融數據的連接器。
    """
    BASE_URL_V3 = "https://financialmodelingprep.com/api/v3"
    BASE_URL_V4 = "https://financialmodelingprep.com/api/v4" # 部分端點可能在 v4

    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 FMPConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典，
                                         應包含 'api_key' 和 'requests_per_minute'。
                                         例如:
                                         {
                                             "api_key": "YOUR_FMP_API_KEY",
                                             "requests_per_minute": 200, # 免費方案每日限制 250
                                             "base_url_v3": "https://financialmodelingprep.com/api/v3", // 可選
                                             "base_url_v4": "https://financialmodelingprep.com/api/v4"  // 可選
                                         }
        """
        self.api_key = api_config.get("api_key")
        if not self.api_key:
            logger.error("FMP API 金鑰未在設定中提供。")
            raise ValueError("FMP API 金鑰未設定。")

        self.requests_per_minute = api_config.get("requests_per_minute", 200)
        self.base_url_v3 = api_config.get("base_url_v3", self.BASE_URL_V3)
        self.base_url_v4 = api_config.get("base_url_v4", self.BASE_URL_V4)

        self._last_request_time = 0
        self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

        self.session = requests.Session()
        # FMP 通常將 API 金鑰作為查詢參數 'apikey'

        logger.info(f"FMPConnector 初始化完成。RPM: {self.requests_per_minute}, Interval: {self._min_request_interval:.2f}s")

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
            logger.debug(f"FMP 速率控制：等待 {wait_time:.2f} 秒。")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None, api_version: str = "v3") -> Tuple[Optional[Any], Optional[str]]:
        """
        向 FMP API 發出請求並處理基本錯誤。

        Args:
            endpoint (str): API 端點路徑 (例如 "/historical-price-full/AAPL")。
            params (Optional[Dict[str, Any]]): 請求的查詢參數。
            api_version (str): 使用的 API 版本 ('v3' 或 'v4')。

        Returns:
            Tuple[Optional[Any], Optional[str]]:
                成功時返回 (解析後的 JSON 數據 (通常是 list 或 dict), None)，
                失敗時返回 (None, 錯誤訊息字串)。
        """
        self._wait_for_rate_limit()

        base_url = self.base_url_v3 if api_version == "v3" else self.base_url_v4
        url = f"{base_url}{endpoint}"

        request_params = params.copy() if params else {}
        request_params['apikey'] = self.api_key

        logger.debug(f"FMP API 請求：URL='{url}', Params (不含金鑰)='{params if params else {}}', Version='{api_version}'")

        try:
            response = self.session.get(url, params=request_params, timeout=30)
            response.raise_for_status()

            if response.status_code == 204:
                logger.info(f"FMP API 請求成功 (狀態碼 204)，但無內容返回：{url}")
                return [], None # FMP 通常返回列表，空列表表示成功但無數據

            try:
                data = response.json()
                # FMP 的錯誤有時是一個包含 "Error Message" 的字典
                if isinstance(data, dict) and "Error Message" in data:
                    error_message = data["Error Message"]
                    logger.error(f"FMP API 錯誤訊息：{error_message} (URL: {url})")
                    return None, error_message
                # 有些端點可能在列表的第一個元素返回錯誤
                if isinstance(data, list) and data and isinstance(data[0], dict) and "Error Message" in data[0]:
                    error_message = data[0]["Error Message"]
                    logger.error(f"FMP API 錯誤訊息 (來自列表)：{error_message} (URL: {url})")
                    return None, error_message

                logger.debug(f"FMP API 請求成功：URL='{url}'")
                return data, None
            except requests.exceptions.JSONDecodeError:
                logger.error(f"FMP API：無法解析 JSON 回應。URL: {url}, 狀態碼: {response.status_code}, 回應文本: {response.text[:200]}...")
                return None, f"無法解析 JSON 回應 (狀態碼 {response.status_code})"

        except requests.exceptions.HTTPError as e:
            error_message = f"FMP API HTTP 錯誤：{e.response.status_code} - {e.response.text[:200]}"
            # 特別處理 401 (無效金鑰) 和 403 (權限不足或超出限制)
            if e.response.status_code == 401:
                error_message = "FMP API 金鑰無效或未授權。"
            elif e.response.status_code == 403:
                 error_message = "FMP API 權限不足或已達請求上限。"
            logger.error(f"{error_message} (URL: {url})")
            return None, error_message
        except requests.exceptions.RequestException as e:
            error_message = f"FMP API 請求錯誤：{e}"
            logger.error(f"{error_message} (URL: {url})")
            return None, error_message
        except Exception as e:
            error_message = f"處理 FMP API 請求時發生未知錯誤：{e}"
            logger.error(f"{error_message} (URL: {url})", exc_info=True)
            return None, error_message

    def get_historical_price(self, symbol: str, from_date: Optional[str] = None, to_date: Optional[str] = None, timeseries: Optional[int] = None) -> pd.DataFrame:
        """
        獲取指定股票的歷史日線價格數據。
        FMP 的 /historical-price-full/{ticker} 端點。

        Args:
            symbol (str): 股票代碼 (例如 "AAPL")。
            from_date (Optional[str]): 開始日期 (YYYY-MM-DD)。
            to_date (Optional[str]): 結束日期 (YYYY-MM-DD)。
            timeseries (Optional[int]): 如果提供，表示獲取最近 N 天的數據 (優先於 from/to date)。

        Returns:
            pd.DataFrame: 包含 OHLCV 數據的 DataFrame，若失敗則為空 DataFrame。
                          欄位將標準化為：'price_date', 'security_id', 'open_price', 'high_price',
                                         'low_price', 'close_price', 'adj_close_price', 'volume',
                                         'unadjusted_volume', 'change_over_time', 'change_percent',
                                         'vwap', 'label', 'source_api', 'last_updated_timestamp'。
        """
        endpoint = f"/historical-price-full/{symbol.upper()}"
        params = {}
        if timeseries:
            params["timeseries"] = timeseries
        else:
            if from_date:
                params["from"] = from_date
            if to_date:
                params["to"] = to_date

        raw_response, error = self._make_request(endpoint, params, api_version="v3")

        if error or not raw_response or not isinstance(raw_response, dict) or "historical" not in raw_response:
            logger.warning(f"FMP: 無法獲取 {symbol} 的歷史價格數據。錯誤：{error if error else '回應格式不符或無歷史數據'}")
            return pd.DataFrame()

        historical_data = raw_response["historical"]
        if not historical_data or not isinstance(historical_data, list):
            logger.info(f"FMP: {symbol} 在指定期間內無歷史價格數據。")
            return pd.DataFrame()

        df = pd.DataFrame(historical_data)
        if df.empty:
            return pd.DataFrame()

        df.rename(columns={
            'date': 'price_date', 'open': 'open_price', 'high': 'high_price',
            'low': 'low_price', 'close': 'close_price', 'adjClose': 'adj_close_price',
            'volume': 'volume', 'unadjustedVolume': 'unadjusted_volume',
            'change': 'change_over_time', 'changePercent': 'change_percent',
            'vwap': 'vwap', 'label': 'label' # 'label' is HTML string like "January 01, 2020"
        }, inplace=True)

        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['security_id'] = symbol.upper()
        df['source_api'] = 'fmp'
        df['last_updated_timestamp'] = datetime.now()

        standard_columns = [
            'price_date', 'security_id', 'open_price', 'high_price', 'low_price',
            'close_price', 'adj_close_price', 'volume', 'unadjusted_volume',
            'change_over_time', 'change_percent', 'vwap', 'label',
            'source_api', 'last_updated_timestamp'
        ]
        for col in standard_columns:
            if col not in df.columns:
                df[col] = None

        return df[standard_columns]

    def get_company_profile(self, symbol: str) -> pd.DataFrame:
        """
        獲取指定公司的基本資料。
        FMP 端點: /profile/{ticker}

        Args:
            symbol (str): 股票代碼 (例如 "AAPL")。

        Returns:
            pd.DataFrame: 包含公司基本資料的 DataFrame (單行)，若失敗則為空 DataFrame。
        """
        endpoint = f"/profile/{symbol.upper()}"

        raw_data_list, error = self._make_request(endpoint, api_version="v3")

        if error or not raw_data_list or not isinstance(raw_data_list, list) or not raw_data_list[0]:
            logger.warning(f"FMP: 無法獲取 {symbol} 的公司資料。錯誤：{error if error else '回應格式不符或無資料'}")
            return pd.DataFrame()

        profile = raw_data_list[0] # Profile data is usually a list with one item

        data_dict = {
            'security_id': profile.get('symbol'),
            'name': profile.get('companyName'),
            'description': profile.get('description'),
            'sector': profile.get('sector'),
            'industry': profile.get('industry'),
            'ceo': profile.get('ceo'),
            'employees': profile.get('fullTimeEmployees'), # FMP uses 'fullTimeEmployees'
            'country': profile.get('country'),
            'exchange': profile.get('exchangeShortName'), # 'exchange' or 'exchangeShortName'
            'currency': profile.get('currency'),
            'ipo_date': profile.get('ipoDate'),
            'website_url': profile.get('website'),
            'image_url': profile.get('image'), # Logo URL
            'market_cap': profile.get('mktCap'),
            'cik': profile.get('cik'),
            'isin': profile.get('isin'),
            'cusip': profile.get('cusip'),
            'source_api': 'fmp',
            'last_updated_timestamp': datetime.now()
        }

        df = pd.DataFrame([data_dict])

        standard_columns = [
            'security_id', 'name', 'description', 'sector', 'industry', 'ceo', 'employees',
            'country', 'exchange', 'currency', 'ipo_date', 'website_url', 'image_url',
            'market_cap', 'cik', 'isin', 'cusip', 'source_api', 'last_updated_timestamp'
        ]
        for col in standard_columns:
            if col not in df.columns:
                df[col] = None

        return df[standard_columns]

    def get_income_statement(self, symbol: str, period: str = "annual", limit: int = 20) -> pd.DataFrame:
        """
        獲取損益表。
        FMP 端點: /income-statement/{ticker}

        Args:
            symbol (str): 股票代碼。
            period (str): 'annual' 或 'quarter'。
            limit (int): 返回的財報期數。

        Returns:
            pd.DataFrame: 損益表數據，若失敗則為空 DataFrame。
                          欄位包含 'report_date', 'security_id', 'period', 'metric_name', 'metric_value', ...
        """
        endpoint = f"/income-statement/{symbol.upper()}"
        params = {"period": period, "limit": limit}

        raw_data_list, error = self._make_request(endpoint, params, api_version="v3")

        if error or not raw_data_list or not isinstance(raw_data_list, list):
            logger.warning(f"FMP: 無法獲取 {symbol} 的損益表 ({period})。錯誤：{error if error else '回應格式不符或無資料'}")
            return pd.DataFrame()

        if not raw_data_list: # API 可能返回空列表
            logger.info(f"FMP: {symbol} 無可用的損益表數據 ({period})。")
            return pd.DataFrame()

        # 將寬表轉換為長表
        all_statements_long = []
        for statement_dict in raw_data_list:
            report_date = statement_dict.get('date')
            fiscal_year = statement_dict.get('calendarYear') # 'calendarYear' or 'fillingDate' for year context
            # period_from_data = statement_dict.get('period') # e.g., "FY", "Q1", "Q2"

            # FMP 的 period ('FY', 'Q1', 'Q2', 'Q3', 'Q4')
            # 我們可以將 'FY' 對應到 'annual', Qx 對應到 'quarterly'
            # 但由於我們是按 period 參數請求的，這裡的 period_from_data 主要是驗證或記錄

            id_vars_map = {
                'security_id': symbol.upper(),
                'report_date': pd.to_datetime(report_date).date() if report_date else None,
                'fiscal_year': fiscal_year,
                'period_type': period, # 來自請求參數
                'fmp_period_reported': statement_dict.get('period'), # FMP 報告的期間 (FY, Q1 etc)
                'currency': statement_dict.get('reportedCurrency'),
                'source_api': 'fmp',
                'last_updated_timestamp': datetime.now()
            }

            for metric_name, metric_value in statement_dict.items():
                # 跳過非指標的元數據欄位
                if metric_name in ['date', 'symbol', 'reportedCurrency', 'cik', 'fillingDate',
                                   'acceptedDate', 'calendarYear', 'period', 'link', 'finalLink']:
                    continue

                record = id_vars_map.copy()
                record['metric_name'] = metric_name
                record['metric_value'] = metric_value if metric_value is not None else pd.NA
                all_statements_long.append(record)

        if not all_statements_long:
            return pd.DataFrame()

        df_long = pd.DataFrame(all_statements_long)

        # 標準化欄位 (可以根據您的 'fact_financial_statement' schema 調整)
        standard_financial_columns = [
            'report_date', 'security_id', 'fiscal_year', 'period_type', 'fmp_period_reported',
            'currency', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp'
        ]
        for col in standard_financial_columns:
            if col not in df_long.columns:
                 df_long[col] = pd.NA

        # 確保 metric_value 是數字
        df_long['metric_value'] = pd.to_numeric(df_long['metric_value'], errors='coerce')

        return df_long[standard_financial_columns]

    # 可以根據需要添加更多 FMP API 端點的方法，例如：
    # get_balance_sheet, get_cash_flow_statement, get_dividends, etc.

if __name__ == '__main__':
    import os
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api_key_from_env = os.getenv("FMP_API_KEY") # 請確保您已設定此環境變數
    if not api_key_from_env:
        logger.error("測試 FMPConnector 需要設定環境變數 FMP_API_KEY。")
    else:
        test_config = {
            "api_key": api_key_from_env,
            "requests_per_minute": 10 # 測試時使用較低的速率
        }
        connector = FMPConnector(api_config=test_config)

        logger.info("--- 測試 FMP get_historical_price ---")
        price_df = connector.get_historical_price("AAPL", from_date="2023-01-01", to_date="2023-01-10")
        if not price_df.empty:
            logger.info(f"AAPL FMP 歷史價格 (前5筆):\n{price_df.head().to_string()}")
        else:
            logger.warning("未能獲取 AAPL FMP 歷史價格。")

        time.sleep(6) # 等待 60s / 10 RPM = 6s

        logger.info("--- 測試 FMP get_company_profile ---")
        profile_df = connector.get_company_profile("MSFT")
        if not profile_df.empty:
            logger.info(f"MSFT FMP 公司資料:\n{profile_df.iloc[0].to_string()}")
        else:
            logger.warning("未能獲取 MSFT FMP 公司資料。")

        time.sleep(6)

        logger.info("--- 測試 FMP get_income_statement (annual) ---")
        income_annual_df = connector.get_income_statement("NVDA", period="annual", limit=2)
        if not income_annual_df.empty:
            logger.info(f"NVDA FMP 年度損益表 (部分指標，前5筆):\n{income_annual_df[income_annual_df['metric_name'].isin(['revenue', 'netIncome'])].head().to_string()}")
        else:
            logger.warning("未能獲取 NVDA FMP 年度損益表。")

        time.sleep(6)

        logger.info("--- 測試 FMP get_income_statement (quarter) ---")
        income_quarter_df = connector.get_income_statement("GOOG", period="quarter", limit=1)
        if not income_quarter_df.empty:
            logger.info(f"GOOG FMP 季度損益表 (部分指標，前5筆):\n{income_quarter_df[income_quarter_df['metric_name'].isin(['revenue', 'grossProfit'])].head().to_string()}")
        else:
            logger.warning("未能獲取 GOOG FMP 季度損益表。")

        logger.info("FMPConnector 測試完成。")
