import requests
import pandas as pd
import time
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple

# 預設日誌記錄器
logger = logging.getLogger(__name__)

class AlphaVantageConnector:
    """
    用於從 Alpha Vantage API 獲取金融數據的連接器。
    """
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 AlphaVantageConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典，
                                         應包含 'api_key' 和 'requests_per_minute'。
                                         例如:
                                         {
                                             "api_key": "YOUR_AV_API_KEY",
                                             "requests_per_minute": 5, # 免費方案限制
                                             "base_url": "https://www.alphavantage.co/query" # 可選
                                         }
        """
        self.api_key = api_config.get("api_key")
        if not self.api_key:
            logger.error("Alpha Vantage API 金鑰未在設定中提供。")
            raise ValueError("Alpha Vantage API 金鑰未設定。")

        self.requests_per_minute = api_config.get("requests_per_minute", 5)
        self.base_url = api_config.get("base_url", self.BASE_URL)

        self._last_request_time = 0
        # Alpha Vantage 免費方案有每日和每分鐘限制，例如 25 calls/day, 5 calls/minute
        # 我們主要基於每分鐘限制來控制速率
        self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

        self.session = requests.Session()
        logger.info(f"AlphaVantageConnector 初始化完成。RPM: {self.requests_per_minute}, Interval: {self._min_request_interval:.2f}s")

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
            logger.debug(f"Alpha Vantage 速率控制：等待 {wait_time:.2f} 秒。")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _make_request(self, params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        向 Alpha Vantage API 發出請求並處理基本錯誤。

        Args:
            params (Dict[str, Any]): 請求的查詢參數，必須包含 'function'。

        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[str]]:
                成功時返回 (解析後的 JSON 數據, None)，
                失敗時返回 (None, 錯誤訊息字串)。
        """
        self._wait_for_rate_limit()

        request_params = params.copy()
        request_params['apikey'] = self.api_key

        function_name = request_params.get("function", "UNKNOWN_FUNCTION")
        symbol_name = request_params.get("symbol", request_params.get("symbols", "N/A"))
        log_params = {k:v for k,v in request_params.items() if k != 'apikey'}
        logger.debug(f"Alpha Vantage API 請求：Function='{function_name}', Symbol='{symbol_name}', Params(no key)='{log_params}'")

        try:
            response = self.session.get(self.base_url, params=request_params, timeout=30)
            response.raise_for_status()

            if response.status_code == 204:
                logger.info(f"Alpha Vantage API 請求成功 (狀態碼 204)，但無內容返回：Function {function_name}")
                return {}, None

            try:
                data = response.json()
                # 檢查 API 返回的常見錯誤/資訊訊息
                if "Error Message" in data:
                    error_message = data["Error Message"]
                    logger.error(f"Alpha Vantage API 錯誤訊息：{error_message} (Function: {function_name}, Symbol: {symbol_name})")
                    return None, error_message
                if "Information" in data: # 例如達到速率限制的訊息
                    info_message = data["Information"]
                    logger.warning(f"Alpha Vantage API 資訊：{info_message} (Function: {function_name}, Symbol: {symbol_name})")
                    # 這可能表示請求失敗或數據不完整，視為一種錯誤
                    return None, info_message
                if not data: # 有時成功但返回空字典
                    logger.info(f"Alpha Vantage API: Function {function_name} for {symbol_name} 返回了空的 JSON 資料。")
                    return {}, None


                logger.debug(f"Alpha Vantage API 請求成功：Function='{function_name}', Symbol='{symbol_name}'")
                return data, None
            except requests.exceptions.JSONDecodeError:
                logger.error(f"Alpha Vantage API：無法解析 JSON 回應。Function: {function_name}, 狀態碼: {response.status_code}, 回應文本: {response.text[:200]}...")
                return None, f"無法解析 JSON 回應 (狀態碼 {response.status_code})"

        except requests.exceptions.HTTPError as e:
            error_message = f"Alpha Vantage API HTTP 錯誤：{e.response.status_code} - {e.response.text[:200]}"
            logger.error(f"{error_message} (Function: {function_name}, Symbol: {symbol_name})")
            return None, error_message
        except requests.exceptions.RequestException as e:
            error_message = f"Alpha Vantage API 請求錯誤：{e}"
            logger.error(f"{error_message} (Function: {function_name}, Symbol: {symbol_name})")
            return None, error_message
        except Exception as e:
            error_message = f"處理 Alpha Vantage API 請求時發生未知錯誤：{e}"
            logger.error(f"{error_message} (Function: {function_name}, Symbol: {symbol_name})", exc_info=True)
            return None, error_message

    def get_historical_price_daily_adjusted(self, symbol: str, outputsize: str = "compact") -> pd.DataFrame:
        """
        獲取日調整後的時間序列數據 (TIME_SERIES_DAILY_ADJUSTED)。
        免費方案對此端點限制較多，可能很快達到上限。

        Args:
            symbol (str): 股票代碼 (例如 "IBM")。
            outputsize (str): "compact" (最近100天) 或 "full" (最多20年)。預設 "compact"。

        Returns:
            pd.DataFrame: 包含調整後 OHLCV 數據的 DataFrame，若失敗則為空 DataFrame。
                          欄位將標準化為：'price_date', 'security_id', 'open_price', 'high_price',
                                         'low_price', 'close_price', 'adj_close_price', 'volume',
                                         'dividend_amount', 'split_coefficient',
                                         'source_api', 'last_updated_timestamp'。
        """
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol.upper(),
            "outputsize": outputsize,
            "datatype": "json" # 預設是 json，但明確指定
        }

        raw_data, error = self._make_request(params)

        if error or not raw_data or "Time Series (Daily)" not in raw_data:
            logger.warning(f"Alpha Vantage: 無法獲取 {symbol} 的日調整後價格。錯誤：{error if error else '回應格式不符或無時間序列數據'}")
            return pd.DataFrame()

        time_series_data = raw_data["Time Series (Daily)"]
        if not time_series_data:
            logger.info(f"Alpha Vantage: {symbol} 無可用的日調整後價格數據。")
            return pd.DataFrame()

        df = pd.DataFrame.from_dict(time_series_data, orient='index')
        df.index.name = 'price_date'
        df.reset_index(inplace=True)

        df.rename(columns={
            '1. open': 'open_price', '2. high': 'high_price',
            '3. low': 'low_price', '4. close': 'close_price',
            '5. adjusted close': 'adj_close_price', '6. volume': 'volume',
            '7. dividend amount': 'dividend_amount', '8. split coefficient': 'split_coefficient'
        }, inplace=True)

        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume', 'dividend_amount', 'split_coefficient']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['security_id'] = symbol.upper()
        df['source_api'] = 'alphavantage'
        df['last_updated_timestamp'] = datetime.now()

        standard_columns = [
            'price_date', 'security_id', 'open_price', 'high_price', 'low_price', 'close_price',
            'adj_close_price', 'volume', 'dividend_amount', 'split_coefficient',
            'source_api', 'last_updated_timestamp'
        ]
        for col in standard_columns:
            if col not in df.columns:
                df[col] = None

        return df[standard_columns].sort_values(by='price_date', ascending=True)

    def get_company_overview(self, symbol: str) -> pd.DataFrame:
        """
        獲取公司概覽數據 (OVERVIEW)。

        Args:
            symbol (str): 股票代碼 (例如 "IBM")。

        Returns:
            pd.DataFrame: 包含公司概覽的 DataFrame (單行)，若失敗則為空 DataFrame。
        """
        params = {"function": "OVERVIEW", "symbol": symbol.upper()}
        profile_data, error = self._make_request(params)

        if error or not profile_data or 'Symbol' not in profile_data: # 'Symbol' 是概覽數據中的一個關鍵欄位
            logger.warning(f"Alpha Vantage: 無法獲取 {symbol} 的公司概覽。錯誤：{error if error else '回應格式不符或無 Symbol 鍵'}")
            return pd.DataFrame()

        # Alpha Vantage 的欄位名通常是駝峰式或首字母大寫，我們需要映射到蛇形小寫
        data_dict = {
            'security_id': profile_data.get('Symbol'),
            'name': profile_data.get('Name'),
            'description': profile_data.get('Description'),
            'asset_type': profile_data.get('AssetType'),
            'exchange': profile_data.get('Exchange'),
            'currency': profile_data.get('Currency'),
            'country': profile_data.get('Country'),
            'sector': profile_data.get('Sector'),
            'industry': profile_data.get('Industry'),
            'address': profile_data.get('Address'),
            'fiscal_year_end': profile_data.get('FiscalYearEnd'),
            'latest_quarter': profile_data.get('LatestQuarter'), # 日期格式
            'market_cap': pd.to_numeric(profile_data.get('MarketCapitalization'), errors='coerce'),
            'ebitda': pd.to_numeric(profile_data.get('EBITDA'), errors='coerce'),
            'pe_ratio': pd.to_numeric(profile_data.get('PERatio'), errors='coerce'),
            'peg_ratio': pd.to_numeric(profile_data.get('PEGRatio'), errors='coerce'),
            'book_value': pd.to_numeric(profile_data.get('BookValue'), errors='coerce'),
            'dividend_per_share': pd.to_numeric(profile_data.get('DividendPerShare'), errors='coerce'),
            'dividend_yield': pd.to_numeric(profile_data.get('DividendYield'), errors='coerce'),
            'eps': pd.to_numeric(profile_data.get('EPS'), errors='coerce'),
            'revenue_per_share_ttm': pd.to_numeric(profile_data.get('RevenuePerShareTTM'), errors='coerce'),
            'profit_margin': pd.to_numeric(profile_data.get('ProfitMargin'), errors='coerce'),
            'operating_margin_ttm': pd.to_numeric(profile_data.get('OperatingMarginTTM'), errors='coerce'),
            'return_on_assets_ttm': pd.to_numeric(profile_data.get('ReturnOnAssetsTTM'), errors='coerce'),
            'return_on_equity_ttm': pd.to_numeric(profile_data.get('ReturnOnEquityTTM'), errors='coerce'),
            'revenue_ttm': pd.to_numeric(profile_data.get('RevenueTTM'), errors='coerce'),
            'gross_profit_ttm': pd.to_numeric(profile_data.get('GrossProfitTTM'), errors='coerce'),
            'beta': pd.to_numeric(profile_data.get('Beta'), errors='coerce'),
            '52_week_high': pd.to_numeric(profile_data.get('52WeekHigh'), errors='coerce'),
            '52_week_low': pd.to_numeric(profile_data.get('52WeekLow'), errors='coerce'),
            '50_day_moving_average': pd.to_numeric(profile_data.get('50DayMovingAverage'), errors='coerce'),
            '200_day_moving_average': pd.to_numeric(profile_data.get('200DayMovingAverage'), errors='coerce'),
            'shares_outstanding': pd.to_numeric(profile_data.get('SharesOutstanding'), errors='coerce'),
            'dividend_date': profile_data.get('DividendDate'), # 日期格式
            'ex_dividend_date': profile_data.get('ExDividendDate'), # 日期格式
            'source_api': 'alphavantage',
            'last_updated_timestamp': datetime.now()
        }

        df = pd.DataFrame([data_dict])
        return df

    def get_income_statement_annual(self, symbol: str) -> pd.DataFrame:
        """
        獲取年度損益表 (INCOME_STATEMENT, 假設年度報告)。
        Alpha Vantage 的財報數據通常在 'annualReports' 和 'quarterlyReports' 鍵下。

        Args:
            symbol (str): 股票代碼。

        Returns:
            pd.DataFrame: 包含年度損益表數據的長格式 DataFrame，若失敗則為空 DataFrame。
        """
        params = {"function": "INCOME_STATEMENT", "symbol": symbol.upper()}
        raw_data, error = self._make_request(params)

        if error or not raw_data or "annualReports" not in raw_data:
            logger.warning(f"Alpha Vantage: 無法獲取 {symbol} 的年度損益表。錯誤：{error if error else '回應格式不符或無 annualReports'}")
            return pd.DataFrame()

        annual_reports = raw_data["annualReports"]
        if not annual_reports or not isinstance(annual_reports, list):
            logger.info(f"Alpha Vantage: {symbol} 無可用的年度損益表數據。")
            return pd.DataFrame()

        all_statements_long = []
        for report_dict in annual_reports:
            fiscal_date_ending = report_dict.get('fiscalDateEnding')
            reported_currency = report_dict.get('reportedCurrency')

            id_vars_map = {
                'security_id': symbol.upper(),
                'report_date': pd.to_datetime(fiscal_date_ending).date() if fiscal_date_ending else None,
                'period_type': 'annual',
                'currency': reported_currency,
                'source_api': 'alphavantage',
                'last_updated_timestamp': datetime.now()
            }

            for metric_name, metric_value_str in report_dict.items():
                if metric_name in ['fiscalDateEnding', 'reportedCurrency']:
                    continue

                # AV 的財報指標值可能是 "None" (字串) 或數字
                metric_value = pd.to_numeric(metric_value_str, errors='coerce') if metric_value_str != "None" else pd.NA

                record = id_vars_map.copy()
                record['metric_name'] = metric_name # AV 的指標名稱通常是駝峰式
                record['metric_value'] = metric_value
                all_statements_long.append(record)

        if not all_statements_long:
            return pd.DataFrame()

        df_long = pd.DataFrame(all_statements_long)

        standard_financial_columns = [
            'report_date', 'security_id', 'period_type', 'currency',
            'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp'
        ]
        for col in standard_financial_columns:
            if col not in df_long.columns:
                 df_long[col] = pd.NA

        return df_long[standard_financial_columns].sort_values(by=['report_date', 'metric_name'])

    # 可以根據 Alpha Vantage 的 API 文件添加更多方法，例如：
    # get_balance_sheet, get_cash_flow, get_earnings, get_news_sentiment, etc.

if __name__ == '__main__':
    import os
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api_key_from_env = os.getenv("ALPHA_VANTAGE_API_KEY") # 請設定環境變數
    if not api_key_from_env:
        logger.error("測試 AlphaVantageConnector 需要設定環境變數 ALPHA_VANTAGE_API_KEY。")
    else:
        test_config = {
            "api_key": api_key_from_env,
            "requests_per_minute": 5 # 免費方案嚴格限制
        }
        connector = AlphaVantageConnector(api_config=test_config)

        # 由於免費 API 的嚴格限制，這裡只測試一個端點，並加入足夠的延遲
        logger.info("--- 測試 Alpha Vantage get_company_overview ---")
        profile_df = connector.get_company_overview("IBM")
        if not profile_df.empty:
            logger.info(f"IBM Alpha Vantage 公司概覽 (部分欄位):\n{profile_df[['security_id', 'name', 'sector', 'market_cap']].iloc[0].to_string()}")
        else:
            logger.warning("未能獲取 IBM Alpha Vantage 公司概覽。可能是 API 限制或金鑰問題。")

        # 如果要測試其他端點，請確保遵守速率限制，例如在兩次調用間等待至少 12-15 秒
        logger.info("等待 15 秒以遵守 Alpha Vantage 速率限制...")
        time.sleep(15)

        logger.info("--- 測試 Alpha Vantage get_historical_price_daily_adjusted (compact) ---")
        # 注意：免費 API 金鑰對 TIME_SERIES_DAILY_ADJUSTED 的訪問可能非常有限或不穩定
        price_df = connector.get_historical_price_daily_adjusted("TSLA", outputsize="compact")
        if not price_df.empty:
            logger.info(f"TSLA Alpha Vantage 日調整後價格 (前3筆):\n{price_df.head(3).to_string()}")
        else:
            logger.warning("未能獲取 TSLA Alpha Vantage 日調整後價格。免費金鑰對此端點限制較多。")

        logger.info("等待 15 秒...")
        time.sleep(15)

        logger.info("--- 測試 Alpha Vantage get_income_statement_annual ---")
        income_df = connector.get_income_statement_annual("MSFT")
        if not income_df.empty:
            logger.info(f"MSFT Alpha Vantage 年度損益表 (部分指標，前5筆):\n{income_df[income_df['metric_name'] == 'netIncome'].head().to_string()}")
        else:
            logger.warning("未能獲取 MSFT Alpha Vantage 年度損益表。")


        logger.info("AlphaVantageConnector 測試完成。請注意 API 速率限制。")
