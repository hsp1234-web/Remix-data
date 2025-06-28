import pandas as pd
import logging
import time # For rate limiting
import re # For snake_case conversion
from typing import Dict, Any, Tuple, Optional, List
from FinMind.data import DataLoader
from datetime import datetime, timezone

# Use a module-level logger
logger = logging.getLogger(__name__)
# Removed: from .base_connector import BaseConnector
# This connector will be self-contained as per the new request structure.

class FinMindConnector:
    """
    用於從 FinMind API (透過其 SDK) 獲取台灣市場金融數據的連接器。
    包含讀取設定檔、速率控制、統一錯誤處理。
    """
    def __init__(self, api_config: Dict[str, Any]):
        """
        初始化 FinMindConnector。

        Args:
            api_config (Dict[str, Any]): 包含此 API 設定的字典，
                                         應包含 'api_key' (FinMind Token) 和 'requests_per_minute'。
                                         例如:
                                         {
                                             "api_key": "YOUR_FINMIND_TOKEN",
                                             "requests_per_minute": 500
                                         }
        """
        self.api_token = api_config.get("api_key")
        if not self.api_token or self.api_token == "YOUR_FINMIND_API_TOKEN": # Check against template placeholder
            logger.error("FinMind API Token (api_key) 未在設定中正確提供。")
            raise ValueError("FinMind API Token 未設定或仍為預設值。")

        self.requests_per_minute = api_config.get("requests_per_minute", 500) # Default from config.yaml.template
        self._last_request_time = 0
        self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

        self.source_name = "finmind" # For standardized logging or data tagging

        try:
            self.data_loader = DataLoader()
            # FinMind's login_by_token typically prints to stdout on success/failure
            # and doesn't raise an exception for invalid token immediately on login,
            # but rather when an API call is made that requires a valid token.
            # We'll assume login call itself doesn't fail catastrophically here.
            self.data_loader.login_by_token(api_token=self.api_token)
            # It's hard to programmatically verify login success from login_by_token alone.
            # A test API call might be needed for true verification, but for __init__, this is a start.
            self.logger.info(f"FinMindConnector: 已使用 API token '{self.api_token[:4]}...' 嘗試初始化 DataLoader 並登入。")
        except Exception as e:
            self.logger.error(f"FinMindConnector: DataLoader 初始化或登入時發生嚴重錯誤: {e}", exc_info=True)
            # If DataLoader() or login_by_token itself raises an unexpected critical error
            raise RuntimeError(f"FinMind DataLoader 初始化或登入失敗: {e}")


    def _fetch_data_internal(self, api_method_name: str, **kwargs) -> pd.DataFrame:
        """
        通用的數據獲取內部方法，直接調用 FinMind 的 DataLoader。
        Args:
            api_method_name (str): FinMind DataLoader 中的方法名稱 (例如 'taiwan_stock_daily')。
            **kwargs: 傳遞給 DataLoader 方法的參數。
        Returns:
            pd.DataFrame: 獲取的數據，如果失敗或無數據則為空 DataFrame。

        備註:
            此方法目前直接依賴 FinMind SDK (DataLoader) 內部的錯誤處理和可能的重試機制。
            如果 SDK 本身沒有健壯的 API 速率限制處理或網路錯誤重試邏輯，
            在高頻調用或網路不穩定時，此方法可能直接返回空 DataFrame 或拋出未被捕獲的異常。
            未來可考慮為此方法增加更上層的、類似 BaseConnector._make_request 中的
            指數退避與抖動重試邏輯，特別是針對已知的可重試錯誤類型。
            使用者應自行注意調用頻率，避免超出 FinMind API 的限制。
        """
        if not self.data_loader:
            self.logger.error(f"FinMindConnector ({api_method_name}): DataLoader 未初始化 (可能 token 未設定或登入失敗)。")
            return pd.DataFrame()

        try:
            fetch_func = getattr(self.data_loader, api_method_name)
            self.logger.debug(f"FinMindConnector: Calling DataLoader.{api_method_name} with params: {kwargs}")
            df = fetch_func(**kwargs)

            if df is None:
                self.logger.warning(f"FinMindConnector ({api_method_name}): 未找到數據 (API 返回 None) for {kwargs}")
                return pd.DataFrame()

            if df.empty:
                self.logger.info(f"FinMindConnector ({api_method_name}): 未找到數據 (API 返回空 DataFrame) for {kwargs}")
                return pd.DataFrame()

            if 'error_message' in df.columns and not df['error_message'].isnull().all():
                 error_msg_series = df['error_message'].dropna()
                 if not error_msg_series.empty:
                    error_msg = f"FinMind API 返回錯誤: {error_msg_series.iloc[0]}"
                    self.logger.error(f"FinMindConnector ({api_method_name}): {error_msg} for {kwargs}")
                    return pd.DataFrame()
                 else:
                    self.logger.debug(f"FinMindConnector ({api_method_name}): 'error_message' column exists but all values are null.")


            self.logger.info(f"FinMindConnector ({api_method_name}): 成功獲取 {len(df)} 筆數據 for {kwargs}。")
            return df

        except AttributeError:
            self.logger.error(f"FinMindConnector: DataLoader 中不存在名為 '{api_method_name}' 的方法。")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"FinMindConnector ({api_method_name}): 獲取數據時發生異常：{e} for {kwargs}", exc_info=True)
            return pd.DataFrame()

    # --- 股價數據獲取與轉換 ---
    def get_stock_price(self, stock_id: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """獲取台股日線價格並進行標準化轉換"""
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 到 {end_date} 的價格數據。")
        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_daily',
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date
        )

        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} 的價格數據，或返回數據為空。")
            return pd.DataFrame(columns=self._get_canonical_price_columns()), None

        return self.transform_stock_price_to_canonical(raw_df, stock_id)

    def transform_stock_price_to_canonical(self, raw_df: pd.DataFrame, stock_id: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        將股價數據轉換為我們預定義的 'fact_stock_price' 標準格式。
        """
        self.logger.debug(f"FinMindConnector: 開始轉換股票 {stock_id} 的 {len(raw_df)} 筆價格數據。")
        try:
            if raw_df.empty:
                self.logger.info(f"FinMindConnector: 原始股價數據為空 for {stock_id}，無需轉換。")
                return pd.DataFrame(columns=self._get_canonical_price_columns()), None

            canonical_df = raw_df.copy()

            rename_map = {
                'date': 'price_date',
                'stock_id': 'security_id',
                'open': 'open_price',
                'max': 'high_price',
                'min': 'low_price',
                'close': 'close_price',
                'Trading_Volume': 'volume',
                'Trading_money': 'turnover'
            }
            canonical_df.rename(columns=rename_map, inplace=True)

            canonical_df['price_date'] = pd.to_datetime(canonical_df['price_date']).dt.date

            numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover']
            for col in numeric_cols:
                if col in canonical_df.columns:
                    canonical_df[col] = pd.to_numeric(canonical_df[col], errors='coerce')

            canonical_df['source_api'] = self.source_name
            canonical_df['last_updated_timestamp'] = datetime.now(timezone.utc)

            final_columns = self._get_canonical_price_columns()

            df_to_return = pd.DataFrame(columns=final_columns)
            for col in final_columns:
                if col in canonical_df.columns:
                    df_to_return[col] = canonical_df[col]
                else:
                    df_to_return[col] = None

            critical_cols_for_dropna = ['price_date', 'security_id', 'close_price']
            # Ensure columns exist before trying to dropna on them
            cols_to_dropna_on = [col for col in critical_cols_for_dropna if col in df_to_return.columns]
            if cols_to_dropna_on:
                 df_to_return.dropna(subset=cols_to_dropna_on, inplace=True)

            if df_to_return.empty and not raw_df.empty :
                 self.logger.warning(f"FinMindConnector: 股票 {stock_id} 的價格數據在清洗後變為空。")

            self.logger.info(f"FinMindConnector: 成功轉換股票 {stock_id} 的 {len(df_to_return)} 筆價格數據。")
            return df_to_return, None

        except Exception as e:
            error_msg = f"FinMindConnector: 轉換股票代碼 {stock_id} 的價格數據時失敗: {e}"
            self.logger.error(error_msg, exc_info=True)
            return None, error_msg

    def _get_canonical_price_columns(self) -> List[str]:
        """返回股價標準模型的欄位列表，供多處使用。
           應與 config/schemas.json 中的 fact_stock_price 定義一致。
        """
        return [
            'price_date', 'security_id', 'open_price', 'high_price',
            'low_price', 'close_price', 'volume', 'turnover',
            'source_api', 'last_updated_timestamp'
        ]

    # --- BaseConnector abstract methods (if we choose to fully implement them) ---

    # As per plan, BaseConnector's fetch_data and transform_to_canonical are abstract.
    # FinMindConnector should implement them if it's to be a concrete BaseConnector.
    # The current get_stock_price etc. are specific.
    # We'll provide generic implementations for fetch_data and transform_to_canonical
    # that dispatch to specific methods based on a 'data_type' kwarg.

    # --- 新增獲取綜合損益表的功能 ---

    def get_income_statement(self, stock_id: str, start_date: str, end_date: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        獲取台股綜合損益表 (Income Statement) 並進行標準化轉換。
        FinMind 的 'taiwan_stock_income_statement' API 似乎是按季度/年度返回特定時間點的報表，
        start_date 可能用來指定最早的財報日期，end_date 可能不太適用或有不同含義。
        根據 FinMindPy/FinMind/data/finmind_api.py, taiwan_stock_income_statement 接受 start_date。
        它似乎會返回該 start_date 之後的所有可用季度/年度數據。
        如果 FinMind SDK 更新，參數可能會改變。
        """
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 開始的綜合損益表數據。")
        # FinMind 的 taiwan_stock_income_statement 通常不需要 end_date，它會獲取指定股票和日期之後的所有數據
        # 或者，如果 start_date 是 YYYY-MM-DD，它可能只獲取該日期所屬的季度/年度的數據。
        # 假設 start_date 是用來過濾報告日期的下限。
        fetch_params = {'stock_id': stock_id, 'start_date': start_date}
        # if end_date: # 如果 FinMind API 支持 end_date for income statements in future
        #     fetch_params['end_date'] = end_date

        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_income_statement',
            **fetch_params
        )

        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} (自 {start_date}) 的綜合損益表數據，或返回數據為空。")
            # 返回符合 fact_financial_statement schema 的空 DataFrame
            return pd.DataFrame(columns=self._get_canonical_financials_columns()), None

        return self.transform_financials_to_canonical(raw_df=raw_df, stock_id=stock_id, statement_type="income_statement")

    def transform_financials_to_canonical(self, raw_df: pd.DataFrame, stock_id: str, statement_type: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        將來自 FinMind 的「寬格式」財報 DataFrame (如綜合損益表、資產負債表、現金流量表)
        轉換為符合 'fact_financial_statement' schema 的「長格式」。

        Args:
            raw_df (pd.DataFrame): FinMind API 返回的原始財報 DataFrame。
            stock_id (str): 股票代碼。
            statement_type (str): 報表類型 (例如 "income_statement", "balance_sheet", "cash_flow_statement")
                                  這將用於填充 fact_financial_statement 中的 statement_type 欄位。

        Returns:
            Tuple[Optional[pd.DataFrame], Optional[str]]: 標準化後的 DataFrame 及錯誤信息。
        """
        self.logger.debug(f"FinMindConnector: 開始轉換股票 {stock_id} 的 {len(raw_df)} 筆 {statement_type} 數據。")
        try:
            if raw_df.empty:
                self.logger.info(f"FinMindConnector: 原始 {statement_type} 數據為空 for {stock_id}，無需轉換。")
                return pd.DataFrame(columns=self._get_canonical_financials_columns()), None

            canonical_df = raw_df.copy()

            # 1a. 欄位重命名 (初步)
            # FinMind 的財報欄位：date, stock_id, type (e.g. "Q1"), ... (各種財報指標)
            # 我們 schema: security_id, fiscal_period, announcement_date, data_snapshot_date, metric_name, metric_value, currency, source_api, last_updated_in_db_timestamp

            rename_map_initial = {
                'date': 'report_date', # FinMind 的 'date' 是財報期末日期
                'stock_id': 'security_id'
                # 'type' 欄位將用於 fiscal_period
            }
            canonical_df.rename(columns=rename_map_initial, inplace=True)

            # 處理財報期間相關欄位
            if 'type' not in canonical_df.columns:
                return None, f"FinMindConnector: 財報數據缺少 'type' 欄位 (用於 fiscal_period) for {stock_id}."

            canonical_df['fiscal_period'] = canonical_df['type'] # e.g., Q1, Q2, Q3, Q4
            canonical_df['report_date'] = pd.to_datetime(canonical_df['report_date']).dt.date

            # 從 report_date 提取年份作為 fiscal_year
            # 注意：對於跨年度的財報期（例如，公司財年結束於1月），這可能需要更複雜的邏輯。
            # FinMind 的台股財報 'date' 通常是該季/年的結束日，所以直接取年份是合理的。
            canonical_df['fiscal_year'] = pd.to_datetime(canonical_df['report_date']).dt.year


            # 1b. 數據透視 (Unpivot/Melt)
            # 確定ID變量 (不被 melt 的列)
            # 這些是每個財報記錄的唯一標識符，除了具體的指標本身。
            id_vars = ['security_id', 'report_date', 'fiscal_year', 'fiscal_period', 'type'] # 'type' is kept temporarily if needed, or used for fiscal_period

            # 確定值變量 (需要被 melt 成 metric_name 和 metric_value 的列)
            # 這些是除了 id_vars 和我們不想要的原始欄位之外的所有列。
            # FinMind 的財報 API 返回的列中，除了 date, stock_id, type 之外，其他基本都是財報指標。
            # 我們需要排除任何非指標的元數據列，如果有的話。
            # 'origin_url' 是 FinMind 可能返回的一個元數據列，我們不需要它作為指標。
            # 'currency' 也可能出現，但我們 schema 中有單獨的 currency 欄位。
            value_vars = [col for col in canonical_df.columns if col not in id_vars and col not in ['origin_url', 'currency']]

            if not value_vars:
                return None, f"FinMindConnector: 未找到可用於 melt 的財報指標欄位 for {stock_id}."

            melted_df = canonical_df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name='metric_name', # 新的指標名稱列
                value_name='metric_value' # 新的指標值列
            )

            # 類型轉換 for metric_value
            melted_df['metric_value'] = pd.to_numeric(melted_df['metric_value'], errors='coerce')
            # 可以選擇在此處 dropna(subset=['metric_value'])，或者在 DatabaseWriter 層面處理
            # 根據 schema，metric_value 可以為 NULL，所以暫不 dropna

            # 1c. 添加元數據
            melted_df['source_api'] = self.source_name
            melted_df['last_updated_in_db_timestamp'] = datetime.now(timezone.utc) # Renamed to match schema
            melted_df['statement_type'] = statement_type # 從參數傳入

            # 處理 schema 中定義但 FinMind 不直接提供的欄位
            melted_df['announcement_date'] = pd.NaT # FinMind income_statement 不直接提供公告日
            melted_df['data_snapshot_date'] = datetime.now(timezone.utc).date() # 使用當前日期作為快照日期
            melted_df['currency'] = 'TWD' # 台股財報通常為新台幣

            # 1d. 最終格式化：篩選並排序欄位
            final_columns = self._get_canonical_financials_columns()

            # 確保所有期望的欄位都存在，並按正確順序排列
            df_to_return = pd.DataFrame(columns=final_columns)
            for col in final_columns:
                if col in melted_df.columns:
                    df_to_return[col] = melted_df[col]
                else: # Schema 中有但 melted_df 中沒有的，設為 None (Pandas 會處理為 NaT/NaN)
                    df_to_return[col] = None

            # 根據 schema，announcement_date 和 data_snapshot_date 是 NOT NULL
            # 但由於 FinMind 不提供 announcement_date，我們需要在 schema 中調整或有其他來源
            # 目前暫時允許 announcement_date 為 NaT，data_snapshot_date 設為今天
            # fiscal_period 已經從 'type' 賦值

            # 關鍵欄位清洗
            # fiscal_period 是主鍵的一部分，不能為空
            # metric_name 也是主鍵一部分
            critical_cols_for_dropna = ['security_id', 'report_date', 'fiscal_year', 'fiscal_period', 'metric_name', 'statement_type']
            cols_to_dropna_on = [col for col in critical_cols_for_dropna if col in df_to_return.columns]
            if cols_to_dropna_on:
                 df_to_return.dropna(subset=cols_to_dropna_on, inplace=True)


            if df_to_return.empty and not raw_df.empty:
                 self.logger.warning(f"FinMindConnector: 股票 {stock_id} 的 {statement_type} 數據在轉換/清洗後變為空。")

            self.logger.info(f"FinMindConnector: 成功轉換股票 {stock_id} 的 {len(df_to_return)} 筆 {statement_type} 標準化記錄。")
            return df_to_return, None

        except Exception as e:
            error_msg = f"FinMindConnector: 轉換股票代碼 {stock_id} 的 {statement_type} 數據時失敗: {e}"
            self.logger.error(error_msg, exc_info=True)
            return None, error_msg

    def _get_canonical_financials_columns(self) -> List[str]:
        """返回財報標準模型 (fact_financial_statement) 的欄位列表。"""
        # 順序應與 schemas.json 中 fact_financial_statement 的定義一致
        return [
            "security_id", "fiscal_period", "announcement_date", "data_snapshot_date",
            "metric_name", "metric_value", "currency", "source_api",
            "last_updated_in_db_timestamp",
            # 以下是輔助轉換或從原始數據中提取的，確保它們在 melt 之前被正確處理或在之後添加
            "report_date", "fiscal_year", "statement_type"
            # 注意： 'type' from FinMind is mapped to 'fiscal_period'.
            # 'filing_date' is in schema but not directly from this FinMind API.
            # We added 'statement_type' to record if it's income_statement, balance_sheet etc.
            # 'report_date' is the FinMind 'date' field.
            # 'fiscal_year' is derived from 'report_date'.
        ]


    def fetch_data(self, data_type: str = "stock_price", **kwargs) -> Tuple[Optional[Any], Optional[str]]:
        """
        通用 fetch_data 實現，根據 data_type 調用特定獲取方法。
        覆寫 BaseConnector 的抽象方法。
        For FinMind, the 'raw_data' returned will be a pandas DataFrame.
        """
        self.logger.debug(f"FinMindConnector: 通用 fetch_data 被調用，data_type='{data_type}', params={kwargs}")
        if data_type == "stock_price":
            stock_id = kwargs.get("stock_id")
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")
            if not all([stock_id, start_date, end_date]):
                err_msg = "獲取股價需提供 stock_id, start_date, end_date"
                self.logger.error(f"FinMindConnector: {err_msg}")
                return None, err_msg

            # _fetch_data_internal returns pd.DataFrame.
            # BaseConnector's fetch_data signature hints at Dict, but Any allows DataFrame.
            raw_df = self._fetch_data_internal(
                api_method_name='taiwan_stock_daily',
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )
            # _fetch_data_internal already logs if df is empty or has error_message
            if raw_df.empty :
                # This might indicate either no data or an API error handled in _fetch_data_internal
                # Return None, and an error message indicating no data or specific error if available from logs
                # However, _fetch_data_internal itself returns an empty DF in such cases.
                # To align with Tuple[Optional[Any], Optional[str]], if empty means "no data successfully fetched"
                # it might be better to return (None, "message")
                # For now, let's return the empty DataFrame as success with no data.
                # The contract is (data, error_string). So if df is empty but no error, it's (empty_df, None)
                # This is handled by get_stock_price which returns (empty_df_with_cols, None)
                 return raw_df, None # Successfully fetched (potentially empty) data
            return raw_df, None

        # TODO: Add other data_types like 'financial_statement'
        elif data_type == "income_statement":
            # This case is handled by get_income_statement directly calling transform_financials_to_canonical
            # However, if DataManager were to call fetch_data(data_type="income_statement", ...)
            # then transform_to_canonical(raw_df, data_type="income_statement", ...)
            # this path would be taken.
            stock_id = kwargs.get("stock_id")
            # ... (similar logic as stock_price to get necessary params for transform_financials_to_canonical)
            # For now, this generic fetch_data might not be the primary way to get financials.
            # The specific get_income_statement is clearer.
            # This is more of a placeholder for strict BaseConnector compliance.
            # We assume raw_df is already fetched.
            # The main purpose of this generic fetch_data is to return the raw_df for the specific data_type.
            # The raw_df for financials is fetched by _fetch_data_internal.
            # So, if we are here, it implies _fetch_data_internal was already called.
            # This part needs careful design if we want DataManager to use these generic methods for all types.
            # For now, let's assume if data_type is 'income_statement', the raw_df is already what we need.
            # This method's primary role is to return the raw data (which is already a DataFrame for FinMind).
            # The error checking for parameters should ideally be in the specific get_X methods.
            # This is simplified for now.
            if not isinstance(kwargs.get("raw_df_for_transform"), pd.DataFrame): # A bit of a hack for this example
                 return None, "通用 fetch_data for financials 期望 raw_df_for_transform (DataFrame) in kwargs"
            return kwargs.get("raw_df_for_transform"), None


        err_msg = f"FinMindConnector: 不支持的 data_type '{data_type}' in fetch_data"
        self.logger.error(err_msg)
        return None, err_msg

    def transform_to_canonical(self, raw_data: Any, data_type: str = "stock_price", **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        通用 transform_to_canonical 實現，根據 data_type 調用特定轉換方法。
        覆寫 BaseConnector 的抽象方法。
        For FinMind, raw_data is expected to be a pandas DataFrame.
        """
        self.logger.debug(f"FinMindConnector: 通用 transform_to_canonical 被調用，data_type='{data_type}', params={kwargs}")
        if not isinstance(raw_data, pd.DataFrame):
            err_msg = f"FinMindConnector: transform_to_canonical 期望 raw_data 是 pandas DataFrame，但收到 {type(raw_data)}"
            self.logger.error(err_msg)
            return None, err_msg

        stock_id = kwargs.get("stock_id")
        if not stock_id: # Try to infer from DataFrame if possible
            if 'security_id' in raw_data.columns and not raw_data.empty:
                 stock_id = raw_data['security_id'].iloc[0]
            elif 'stock_id' in raw_data.columns and not raw_data.empty: # FinMind specific before rename
                 stock_id = raw_data['stock_id'].iloc[0]

        # If raw_data is empty, stock_id might not be inferable, but transform methods handle empty raw_df.
        if not stock_id and not raw_data.empty:
            err_msg = f"轉換 {data_type} 數據需要 stock_id，但未在參數中提供或從原始數據 DataFrame 中獲取。"
            self.logger.error(f"FinMindConnector: {err_msg}")
            return None, err_msg
        elif not stock_id and raw_data.empty: # If df is empty and no stock_id, use placeholder
             stock_id = "Unknown"


        if data_type == "stock_price":
            return self.transform_stock_price_to_canonical(raw_df=raw_data, stock_id=stock_id)
        elif data_type == "income_statement":
            return self.transform_financials_to_canonical(raw_df=raw_data, stock_id=stock_id, statement_type="income_statement")
        elif data_type == "balance_sheet":
            return self.transform_financials_to_canonical(raw_df=raw_data, stock_id=stock_id, statement_type="balance_sheet")
        elif data_type == "cash_flow_statement":
            return self.transform_financials_to_canonical(raw_df=raw_data, stock_id=stock_id, statement_type="cash_flow_statement")
        elif data_type == "institutional_trades":
            return self.transform_chip_data_to_canonical(raw_df=raw_data, stock_id=stock_id, data_category="institutional_trades")
        elif data_type == "margin_trading":
            return self.transform_chip_data_to_canonical(raw_df=raw_data, stock_id=stock_id, data_category="margin_trading")
        elif data_type == "shareholding":
            return self.transform_chip_data_to_canonical(raw_df=raw_data, stock_id=stock_id, data_category="shareholding")

        err_msg = f"FinMindConnector: 不支持的 data_type '{data_type}' 用於通用轉換 transform_to_canonical"
        self.logger.error(err_msg)
        return None, err_msg

    # --- 新增獲取資產負債表的功能 ---
    def get_balance_sheet(self, stock_id: str, start_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        獲取台股資產負債表 (Balance Sheet) 並進行標準化轉換。
        FinMind API (taiwan_stock_balance_sheet) 通常按 start_date 獲取該日期之後的所有數據。
        """
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 開始的資產負債表數據。")
        fetch_params = {'stock_id': stock_id, 'start_date': start_date}

        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_balance_sheet',
            **fetch_params
        )

        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} (自 {start_date}) 的資產負債表數據，或返回數據為空。")
            return pd.DataFrame(columns=self._get_canonical_financials_columns()), None

        return self.transform_financials_to_canonical(raw_df=raw_df, stock_id=stock_id, statement_type="balance_sheet")

    # --- 新增獲取現金流量表的功能 ---
    def get_cash_flow_statement(self, stock_id: str, start_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        獲取台股現金流量表 (Cash Flow Statement) 並進行標準化轉換。
        FinMind API (taiwan_stock_cash_flows_statement) 通常按 start_date 獲取該日期之後的所有數據。
        """
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 開始的現金流量表數據。")
        fetch_params = {'stock_id': stock_id, 'start_date': start_date}

        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_cash_flows_statement',
            **fetch_params
        )

        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} (自 {start_date}) 的現金流量表數據，或返回數據為空。")
            return pd.DataFrame(columns=self._get_canonical_financials_columns()), None

        return self.transform_financials_to_canonical(raw_df=raw_df, stock_id=stock_id, statement_type="cash_flow_statement")

    # --- 新增獲取現金流量表的功能 ---
    def get_cash_flow_statement(self, stock_id: str, start_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        獲取台股現金流量表 (Cash Flow Statement) 並進行標準化轉換。
        FinMind API (taiwan_stock_cash_flows_statement) 通常按 start_date 獲取該日期之後的所有數據。
        """
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 開始的現金流量表數據。")
        fetch_params = {'stock_id': stock_id, 'start_date': start_date}

        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_cash_flows_statement', # Correct API method name
            **fetch_params
        )

        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} (自 {start_date}) 的現金流量表數據，或返回數據為空。")
            return pd.DataFrame(columns=self._get_canonical_financials_columns()), None

        return self.transform_financials_to_canonical(raw_df=raw_df, stock_id=stock_id, statement_type="cash_flow_statement")

    # --- 籌碼面數據獲取與轉換 ---

    def _get_canonical_chip_columns(self) -> List[str]:
        """返回籌碼數據標準模型 (fact_tw_chip_data) 的欄位列表。"""
        return [
            "transaction_date", "security_id", "metric_name",
            "metric_sub_category", "metric_value", "source_api",
            "last_updated_in_db_timestamp"
        ]

    def transform_chip_data_to_canonical(self, raw_df: pd.DataFrame, stock_id: str, data_category: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        將來自 FinMind 的「寬格式」籌碼 DataFrame 轉換為符合 'fact_tw_chip_data' schema 的「長格式」。

        Args:
            raw_df (pd.DataFrame): FinMind API 返回的原始籌碼 DataFrame。
            stock_id (str): 股票代碼。 (注意: FinMind籌碼數據通常已有stock_id列)
            data_category (str): 籌碼數據類別，例如 'institutional_trades', 'margin_trading', 'shareholding'。
                                 用於指導如何解析和 melt 不同的原始 DataFrame 結構。
        Returns:
            Tuple[Optional[pd.DataFrame], Optional[str]]: 標準化後的 DataFrame 及錯誤信息。
        """
        self.logger.debug(f"FinMindConnector: 開始轉換股票 {stock_id} 的 {len(raw_df)} 筆 {data_category} 籌碼數據。")
        if raw_df.empty:
            self.logger.info(f"FinMindConnector: 原始 {data_category} 數據為空 for {stock_id}，無需轉換。")
            return pd.DataFrame(columns=self._get_canonical_chip_columns()), None

        try:
            canonical_df = raw_df.copy()

            # 通用欄位重命名
            if 'date' in canonical_df.columns:
                canonical_df.rename(columns={'date': 'transaction_date'}, inplace=True)
            if 'stock_id' in canonical_df.columns:
                 canonical_df.rename(columns={'stock_id': 'security_id'}, inplace=True)

            canonical_df['transaction_date'] = pd.to_datetime(canonical_df['transaction_date']).dt.date

            melted_dfs = []

            if data_category == 'institutional_trades':
                # FinMind taiwan_stock_institutional_investors_buy_sell returns:
                # date, stock_id, name (法人名稱), buy (股), sell (股)
                # We need to map 'name' to 'metric_sub_category' and 'buy'/'sell' to 'metric_name' parts.
                if 'name' not in canonical_df.columns or not all(col in canonical_df.columns for col in ['buy', 'sell']):
                    return None, f"institutional_trades 數據缺少 'name', 'buy', 或 'sell' 欄位 for {stock_id}"

                # 計算淨買賣超 (股數)
                canonical_df['net_shares'] = pd.to_numeric(canonical_df['buy'], errors='coerce') - pd.to_numeric(canonical_df['sell'], errors='coerce')

                id_vars = ['transaction_date', 'security_id', 'name']
                value_vars_map = { # map original column to canonical metric_name stem
                    'buy': 'institutional_buy_shares',
                    'sell': 'institutional_sell_shares',
                    'net_shares': 'institutional_net_shares'
                }
                # FinMind 的 name 通常是英文，例如 Foreign_Investor, Investment_Trust, Dealer_Proprietary, Dealer_Hedging
                # 我們可以直接使用這些作為 metric_sub_category，或進行映射
                sub_category_col = 'name'

                for finmind_col, metric_name_stem in value_vars_map.items():
                    if finmind_col in canonical_df.columns:
                        temp_df = canonical_df[id_vars + [finmind_col]].copy()
                        temp_df.rename(columns={finmind_col: 'metric_value', sub_category_col: 'metric_sub_category'}, inplace=True)
                        temp_df['metric_name'] = metric_name_stem
                        melted_dfs.append(temp_df)

            elif data_category == 'margin_trading':
                # FinMind taiwan_stock_margin_purchase_short_sale returns columns like:
                # MarginPurchaseLimit, MarginPurchaseTodayBalance, MarginPurchaseUsedAmount,
                # ShortSaleLimit, ShortSaleTodayBalance, ShortSaleUsedAmount, etc.
                id_vars = ['transaction_date', 'security_id']
                value_vars = [col for col in canonical_df.columns if col not in id_vars + ['stock_name', 'Note', 'type']] # type is sometimes present

                if not value_vars:
                     return None, f"margin_trading 數據未找到可 melt 的指標欄位 for {stock_id}"

                melted_df = canonical_df.melt(
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name='metric_name',
                    value_name='metric_value'
                )
                # metric_name 會是 MarginPurchaseLimit 等，可以考慮轉換為蛇形命名
                melted_df['metric_name'] = melted_df['metric_name'].apply(lambda x: re.sub(r'(?<!^)(?=[A-Z])', '_', x).lower())
                melted_df['metric_sub_category'] = '' # No sub-category for these metrics typically
                melted_dfs.append(melted_df)

            elif data_category == 'shareholding':
                # FinMind taiwan_stock_shareholding returns:
                # date, stock_id, stock_name, ForeignInvestmentSharesRatio, DomesticInstitutionSharesRatio etc.
                # Also InternationalStrategicTraditional, ForeignInvestmentShares, DomesticInstitutionShares etc.
                id_vars = ['transaction_date', 'security_id']
                # Example value_vars, need to confirm actual columns from FinMind and decide which ones to keep
                value_vars_candidates = [
                    'ForeignInvestmentSharesRatio', 'DomesticInstitutionSharesRatio',
                    'ForeignInvestmentShares', 'DomesticInstitutionShares',
                    'ForeignNaturalPersonSharesRatio', 'ForeignNaturalPersonShares',
                    'TrustSharesRatio', 'TrustShares',
                    'DealerSharesRatio', 'DealerShares',
                    'DomesticStrategicTraditionalSharesRatio', 'DomesticStrategicTraditionalShares',
                    'GovernmentAgencySharesRatio', 'GovernmentAgencyShares',
                    'FinancialInstitutionSharesRatio', 'FinancialInstitutionShares'
                    # Add more as needed
                ]
                value_vars = [col for col in value_vars_candidates if col in canonical_df.columns]

                if not value_vars:
                     return None, f"shareholding 數據未找到可 melt 的指標欄位 for {stock_id}"

                melted_df = canonical_df.melt(
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name='metric_name',
                    value_name='metric_value'
                )
                melted_df['metric_name'] = melted_df['metric_name'].apply(lambda x: re.sub(r'(?<!^)(?=[A-Z])', '_', x).lower())
                melted_df['metric_sub_category'] = ''
                melted_dfs.append(melted_df)
            else:
                return None, f"未知的籌碼數據類別: {data_category}"

            if not melted_dfs: # If no data was processed for any category
                 self.logger.warning(f"FinMindConnector: 未能從 {data_category} 類別的原始數據中提取任何指標 for {stock_id}.")
                 return pd.DataFrame(columns=self._get_canonical_chip_columns()), None

            final_df = pd.concat(melted_dfs, ignore_index=True)

            # 通用後處理
            final_df['metric_value'] = pd.to_numeric(final_df['metric_value'], errors='coerce')
            final_df['source_api'] = self.source_name
            final_df['last_updated_in_db_timestamp'] = datetime.now(timezone.utc)

            # 確保欄位和清洗
            output_columns = self._get_canonical_chip_columns()
            df_to_return = pd.DataFrame(columns=output_columns)
            for col in output_columns:
                if col in final_df.columns:
                    df_to_return[col] = final_df[col]
                elif col == 'metric_sub_category': # Ensure it exists with default if not created by specific logic
                    df_to_return[col] = ''
                else:
                    df_to_return[col] = None

            # 關鍵欄位清洗 (metric_sub_category 允許為空字符串，但主鍵中不能為純粹的NULL)
            critical_cols_for_dropna = ['transaction_date', 'security_id', 'metric_name']
            df_to_return.dropna(subset=critical_cols_for_dropna, inplace=True)

            # Fill NaN in metric_sub_category with default empty string if it's part of PK and was not set
            if 'metric_sub_category' in df_to_return.columns:
                 df_to_return['metric_sub_category'].fillna('', inplace=True)


            if df_to_return.empty and not raw_df.empty:
                 self.logger.warning(f"FinMindConnector: 股票 {stock_id} 的 {data_category} 數據在轉換/清洗後變為空。")

            self.logger.info(f"FinMindConnector: 成功轉換股票 {stock_id} 的 {len(df_to_return)} 筆 {data_category} 標準化籌碼記錄。")
            return df_to_return, None

        except Exception as e:
            error_msg = f"FinMindConnector: 轉換股票代碼 {stock_id} 的 {data_category} 籌碼數據時失敗: {e}"
            self.logger.error(error_msg, exc_info=True)
            return None, error_msg

    def get_institutional_trades(self, stock_id: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 到 {end_date} 的三大法人買賣超數據。")
        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_institutional_investors_buy_sell',
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date
        )
        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} 的三大法人買賣超數據。")
            return pd.DataFrame(columns=self._get_canonical_chip_columns()), None
        return self.transform_chip_data_to_canonical(raw_df, stock_id, data_category='institutional_trades')

    def get_margin_trading(self, stock_id: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 到 {end_date} 的融資融券餘額數據。")
        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_margin_purchase_short_sale',
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date
        )
        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} 的融資融券餘額數據。")
            return pd.DataFrame(columns=self._get_canonical_chip_columns()), None
        return self.transform_chip_data_to_canonical(raw_df, stock_id, data_category='margin_trading')

    def get_shareholding(self, stock_id: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 到 {end_date} 的股權分散數據。")
        # FinMind API for shareholding might be taiwan_stock_shareholding or other, need to verify exact name
        # Assuming 'taiwan_stock_shareholding' for now based on previous context
        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_shareholding', # Verify this API method name
            stock_id=stock_id,
            start_date=start_date, # taiwan_stock_shareholding might take 'date' instead of 'start_date'/'end_date'
            end_date=end_date     # Or it might return all historical data for a given date query
        )
        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} 的股權分散數據。")
            return pd.DataFrame(columns=self._get_canonical_chip_columns()), None
        return self.transform_chip_data_to_canonical(raw_df, stock_id, data_category='shareholding')

    # 未來可以繼續在此擴充其他類型財報的獲取方法...
# ``` # Removed backticks and non-Python text below

# # Một vài chỉnh sửa nhỏ trong quá trình tạo file:
# # *   Sửa `api_method` thành `api_method_name` trong `_fetch_data_internal` để提高可讀性。
# # *   Trong `transform_stock_price_to_canonical`, thêm kiểm tra `if col in canonical_df.columns:` trước khi thực hiện `pd.to_numeric` để避免 `turnover` 不存在時出錯。
# # *   Trong `transform_stock_price_to_canonical`, tạo `df_to_return` 時先以 `final_columns` 初始化，再用 `canonical_df` 中的數據填充，確保即使來源數據缺少某些 schema 中的可選列，最終 DataFrame 結構也完整。
# # *   Trong `transform_stock_price_to_canonical` 的 `dropna` 步驟，增加檢查 `cols_to_dropna_on` 是否為空。
# # *   Trong `fetch_data` (BaseConnector override), 對於 `raw_df.empty` 的情況，直接返回 `(raw_df, None)`，因為空 DataFrame 本身不是一個 fetch 錯誤。
# # *   Trong `transform_to_canonical` (BaseConnector override), 增加了對 `raw_data` 類型的檢查，並嘗試從 `raw_data`（如果是 DataFrame）中推斷 `stock_id`。

# # File đã được tạo.
