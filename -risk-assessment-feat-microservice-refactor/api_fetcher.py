# api_fetcher.py

import yaml
import os
import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import requests # 用於通用的 requests 異常
import time # 用於快取時間戳

logger = logging.getLogger(__name__)

# --- 全局常量 ---
CACHE_DIR_DEFAULT = "CACHE_Market_Data"
CACHE_EXPIRE_AFTER_HOURS_DEFAULT = 24
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 5

# --- 異常類 ---
class APIFetchError(Exception):
    """自定義 API 獲取錯誤基類。"""
    pass

class APIAuthenticationError(APIFetchError):
    """API 認證失敗錯誤。"""
    pass

class APIRateLimitError(APIFetchError):
    """API 速率限制錯誤。"""
    pass

class APIDataNotFoundError(APIFetchError):
    """API 未找到請求的數據。"""
    pass

# --- 適配器基類 ---
class BaseAdapter(ABC):
    def __init__(self, api_name: str, api_key: Optional[str] = None,
                 retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
                 retry_delay: int = DEFAULT_RETRY_DELAY_SECONDS):
        self.api_name = api_name
        self.api_key = api_key
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def _fetch_data(self, indicator_config: Dict[str, Any],
                    start_date_str: str, end_date_str: str) -> pd.DataFrame:
        raise NotImplementedError

    @retry(
        stop=stop_after_attempt(DEFAULT_RETRY_ATTEMPTS),
        wait=wait_fixed(DEFAULT_RETRY_DELAY_SECONDS),
        retry=retry_if_exception_type((requests.exceptions.RequestException, APIRateLimitError, APIFetchError)),
        reraise=True
    )
    def get_data(self, indicator_config: Dict[str, Any],
                 start_date_str: str, end_date_str: str) -> pd.DataFrame:
        self.logger.info(f"嘗試從 {self.api_name} 獲取指標數據，配置: {indicator_config.get('series_id') or indicator_config.get('ticker')}")
        try:
            df = self._fetch_data(indicator_config, start_date_str, end_date_str)
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, errors='coerce')
                df = df[pd.notna(df.index)]
            if 'value' not in df.columns:
                potential_value_cols = [col for col in ['Close', 'close', 'VALUE', indicator_config.get('series_id')] if col in df.columns]
                if potential_value_cols:
                    df.rename(columns={potential_value_cols[0]: 'value'}, inplace=True)
                    self.logger.debug(f"已將列 '{potential_value_cols[0]}' 重命名為 'value'")
                else:
                    raise APIFetchError(f"{self.api_name} 返回的數據中缺少 'value' 列，也未能從其他列推斷。")

            if not pd.api.types.is_numeric_dtype(df['value']):
                df['value'] = pd.to_numeric(df['value'], errors='coerce')

            start_dt_naive = pd.to_datetime(start_date_str).tz_localize(None)
            end_dt_naive = pd.to_datetime(end_date_str).tz_localize(None)

            if df.index.tz is not None: # 確保比較前 df.index 是 tz-naive
                df.index = df.index.tz_localize(None)

            df_filtered = df[(df.index >= start_dt_naive) & (df.index <= end_dt_naive)]

            self.logger.info(f"成功從 {self.api_name} 獲取並標準化了 {len(df_filtered)} 條數據。")
            return df_filtered[['value']]
        except APIFetchError:
            raise
        except Exception as e:
            self.logger.error(f"從 {self.api_name} 獲取數據時發生未預期錯誤: {e}", exc_info=True)
            raise APIFetchError(f"從 {self.api_name} 獲取數據時發生未預期錯誤: {e}") from e

# --- 具體 API 適配器實現 ---
class FredAdapter(BaseAdapter):
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        super().__init__(api_name="fred", api_key=api_key, **kwargs)
        self.fred_client = None # 預設為無金鑰模式

        if self.api_key: # 如果 api_key 不是 None 或空字串
            if self.api_key == "TEST_FRED_API_KEY_1234567890abcdef":
                self.logger.info("FredAdapter 檢測到測試佔位符 API Key，將使用無金鑰模式。")
                # self.fred_client 保持 None
            else:
                # 嘗試使用真實 API Key 初始化
                try:
                    from fredapi import Fred
                    self.fred_client = Fred(api_key=self.api_key)
                    self.logger.info("FredAdapter 已使用真實 API Key 初始化 fredapi 客戶端。")
                except ImportError:
                    self.logger.error("fredapi 庫未安裝。FredAdapter 在金鑰模式下將無法工作，回退到無金鑰模式。")
                    self.fred_client = None # 確保回退
                except Exception as e:
                    self.logger.error(f"使用 API Key 初始化 fredapi 客戶端失敗: {e}，回退到無金鑰模式。")
                    self.fred_client = None # 確保回退
        else: # api_key is None or empty
            self.logger.info("FredAdapter 未提供 API Key，將使用無金鑰模式。")
            # self.fred_client 保持 None

        if self.fred_client is None: # 再次檢查，確保如果上面 try-except 中失敗設為 None，這裡會記錄
             self.logger.info("FredAdapter (當前為無金鑰模式) 將依賴 market_data_fred.py。")

    def _fetch_data(self, indicator_config: Dict[str, Any],
                    start_date_str: str, end_date_str: str) -> pd.DataFrame:
        series_id = indicator_config.get('series_id')
        if not series_id:
            raise ValueError("FredAdapter 需要 'series_id' 在指標配置中。")

        if self.fred_client: # 優先使用 fredapi 庫 (金鑰模式)
            self.logger.info(f"FredAdapter (金鑰模式) 正在獲取 {series_id}...")
            try:
                series_data = self.fred_client.get_series(series_id,
                                                          observation_start=start_date_str,
                                                          observation_end=end_date_str)
                if series_data.empty:
                    raise APIDataNotFoundError(f"FRED API (金鑰模式) 未找到序列 '{series_id}' 在指定日期範圍的數據，或數據為空。")
                df = series_data.to_frame(name='value')
                df.index.name = 'date'
                if df.index.tz is not None: # fredapi 返回的索引通常是 tz-naive，但以防萬一
                    df.index = df.index.tz_localize(None)
                return df.reset_index()
            except Exception as e:
                if "API key is invalid" in str(e).lower() or "No API key" in str(e).lower():
                    self.logger.error(f"FRED API Key 問題: {e}。")
                    raise APIAuthenticationError(f"FRED API Key 問題: {e}") from e
                if "Rate limit exceeded" in str(e).lower():
                    raise APIRateLimitError(f"FRED API 速率限制: {e}") from e
                self.logger.warning(f"使用 fredapi 獲取 '{series_id}' 失敗: {e}")
                raise APIFetchError(f"使用 fredapi 獲取 '{series_id}' 失敗: {e}") from e
        else: # 無金鑰模式
            self.logger.info(f"FredAdapter (無金鑰模式) 正在獲取 {series_id}...")
            try:
                import market_data_fred
                df_raw = market_data_fred.fetch_fred_series_no_key(series_id, start_date_str, end_date_str)
                if df_raw is None or df_raw.empty:
                    raise APIDataNotFoundError(f"FRED (無金鑰模式) 未找到序列 '{series_id}' 在指定日期範圍的數據。")
                df = df_raw.rename(columns={series_id: 'value'})
                df.index.name = 'date'
                if df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                return df.reset_index()
            except ImportError:
                msg = "market_data_fred.py 未找到或無法導入，FredAdapter (無金鑰模式) 無法工作。"
                self.logger.error(msg)
                raise APIFetchError(msg)
            except APIDataNotFoundError:
                raise
            except Exception as e:
                self.logger.warning(f"FredAdapter (無金鑰模式) 獲取 '{series_id}' 失敗: {e}")
                raise APIFetchError(f"FredAdapter (無金鑰模式) 獲取 '{series_id}' 失敗: {e}") from e

class YFinanceAdapter(BaseAdapter):
    def __init__(self, **kwargs):
        super().__init__(api_name="yfinance", **kwargs)
        try:
            import yfinance as yf
            self.yf = yf
            self.logger.info("YFinanceAdapter 初始化完成。")
        except ImportError:
            self.logger.error("yfinance 庫未安裝。YFinanceAdapter 將無法工作。")
            self.yf = None

    def _fetch_data(self, indicator_config: Dict[str, Any],
                    start_date_str: str, end_date_str: str) -> pd.DataFrame:
        if not self.yf:
            raise APIFetchError("yfinance 庫未成功初始化。")

        ticker_symbol = indicator_config.get('ticker')
        if not ticker_symbol:
            raise ValueError("YFinanceAdapter 需要 'ticker' 在指標配置中。")

        interval = indicator_config.get('interval', '1d')
        data_column = indicator_config.get('data_column', 'Close')

        try:
            self.logger.info(f"YFinanceAdapter 正在獲取 {ticker_symbol} (列: {data_column})...")
            ticker_obj = self.yf.Ticker(ticker_symbol)
            end_date_yf = (pd.to_datetime(end_date_str) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            history_df = ticker_obj.history(start=start_date_str, end=end_date_yf, interval=interval)

            if history_df.empty:
                raise APIDataNotFoundError(f"yfinance 未找到 Ticker '{ticker_symbol}' 在指定日期範圍的數據。")
            if data_column not in history_df.columns:
                raise APIFetchError(f"yfinance 返回的數據中缺少列 '{data_column}' (Ticker: {ticker_symbol})。可用列: {history_df.columns.tolist()}")

            df = history_df[[data_column]].rename(columns={data_column: 'value'})
            df.index.name = 'date'
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            return df.reset_index()
        except Exception as e:
            if "No data found" in str(e) or "404" in str(e):
                raise APIDataNotFoundError(f"yfinance 獲取 '{ticker_symbol}' 數據失敗 (未找到或無數據): {e}") from e
            self.logger.warning(f"yfinance 獲取 '{ticker_symbol}' 失敗: {e}")
            raise APIFetchError(f"yfinance 獲取 '{ticker_symbol}' 失敗: {e}") from e

# --- 統一 API 請求器 ---
class UnifiedAPIFetcher:
    def __init__(self,
                 project_config: Dict[str, Any],
                 endpoints_config_path: str = "config/api_endpoints.yaml"):
        self.project_config = project_config
        self.endpoints_config_path = endpoints_config_path
        self.endpoints_config: Dict[str, Any] = self._load_endpoints_config()
        self.adapters: Dict[str, BaseAdapter] = {}
        self.cache_settings = project_config.get("data_fetching", {}).get("unified_api_fetcher", {})
        self.cache_dir = self.cache_settings.get("cache_dir", CACHE_DIR_DEFAULT)
        self.cache_expire_hours = self.cache_settings.get("cache_expire_after_hours", CACHE_EXPIRE_AFTER_HOURS_DEFAULT)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        os.makedirs(self.cache_dir, exist_ok=True)
        self.logger.info(f"UnifiedAPIFetcher 初始化完成。端點配置: {len(self.endpoints_config.get('indicators', {}))} 個指標。快取目錄: {self.cache_dir}")

    def _load_endpoints_config(self) -> Dict[str, Any]:
        if not os.path.exists(self.endpoints_config_path):
            err_msg = f"API 端點設定檔未找到: {self.endpoints_config_path}"
            self.logger.error(err_msg)
            raise FileNotFoundError(err_msg)
        try:
            with open(self.endpoints_config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            err_msg = f"解析 API 端點設定檔 {self.endpoints_config_path} 失敗: {e}"
            logger.error(err_msg, exc_info=True)
            raise

    def _get_adapter(self, api_provider_name: str) -> Optional[BaseAdapter]:
        if api_provider_name not in self.adapters:
            api_key_env_var_map = self.project_config.get("api_keys_env_vars", {})
            api_key_env_name = api_key_env_var_map.get(api_provider_name)
            api_key = None
            if api_key_env_name:
                api_key = self.project_config.get("runtime_api_keys", {}).get(api_provider_name)
                if not api_key and api_provider_name != 'yfinance':
                     logger.warning(f"適配器 '{api_provider_name}' 的 API Key 未在環境中設定 (透過 '{api_key_env_name}')。")

            retry_attempts = self.cache_settings.get("default_retry_attempts", DEFAULT_RETRY_ATTEMPTS)
            retry_delay = self.cache_settings.get("default_retry_delay_seconds", DEFAULT_RETRY_DELAY_SECONDS)

            if api_provider_name == "fred":
                self.adapters[api_provider_name] = FredAdapter(api_key=api_key, retry_attempts=retry_attempts, retry_delay=retry_delay)
            elif api_provider_name == "yfinance":
                self.adapters[api_provider_name] = YFinanceAdapter(retry_attempts=retry_attempts, retry_delay=retry_delay)
            else:
                logger.error(f"未知的 API Provider: {api_provider_name}")
                return None
        return self.adapters.get(api_provider_name)

    def _get_cache_filepath(self, indicator_name: str, start_date_str: str, end_date_str: str) -> str:
        filename = f"{indicator_name}_{start_date_str}_{end_date_str}.parquet"
        return os.path.join(self.cache_dir, filename)

    def _read_from_cache(self, filepath: str) -> Optional[pd.DataFrame]:
        if os.path.exists(filepath):
            try:
                file_mod_time = os.path.getmtime(filepath)
                if (time.time() - file_mod_time) / 3600 < self.cache_expire_hours:
                    df = pd.read_parquet(filepath)
                    self.logger.info(f"從快取檔案 {filepath} 成功載入數據。")
                    return df
                else:
                    self.logger.info(f"快取檔案 {filepath} 已過期。")
            except Exception as e:
                self.logger.warning(f"讀取快取檔案 {filepath} 失敗: {e}。將嘗試重新獲取。")
        return None

    def _write_to_cache(self, df: pd.DataFrame, filepath: str):
        try:
            df.to_parquet(filepath, index=True)
            self.logger.info(f"數據已成功寫入快取檔案 {filepath}。")
        except Exception as e:
            self.logger.error(f"寫入快取檔案 {filepath} 失敗: {e}", exc_info=True)

    def get_data(self, indicator_name: str, start_date_str: str, end_date_str: str) -> Optional[pd.DataFrame]:
        cache_filepath = self._get_cache_filepath(indicator_name, start_date_str, end_date_str)
        cached_df = self._read_from_cache(cache_filepath)
        if cached_df is not None:
            return cached_df

        indicator_sources_config = self.endpoints_config.get("indicators", {}).get(indicator_name)
        if not indicator_sources_config:
            self.logger.warning(f"指標 '{indicator_name}' 在 API 端點設定檔中未定義。")
            return None

        if not isinstance(indicator_sources_config, list):
            self.logger.error(f"指標 '{indicator_name}' 的源配置不是列表格式。")
            return None

        for source_config in indicator_sources_config:
            api_provider = source_config.get('api_provider')
            if not api_provider:
                self.logger.warning(f"指標 '{indicator_name}' 的一個源配置缺少 'api_provider'。跳過此源。")
                continue

            adapter = self._get_adapter(api_provider)
            if not adapter:
                continue

            try:
                df = adapter.get_data(source_config, start_date_str, end_date_str)
                if df is not None and not df.empty:
                    if not isinstance(df.index, pd.DatetimeIndex):
                        if 'date' in df.columns:
                             df['date'] = pd.to_datetime(df['date'], errors='coerce')
                             df = df.set_index('date').dropna(subset=['value'])
                        else:
                             raise APIFetchError(f"Adapter {api_provider} 未返回帶有 'date' 索引或列的 DataFrame。")

                    if df.index.tz is not None: # 確保從 adapter 返回的 df 索引是 tz-naive
                        df.index = df.index.tz_localize(None)

                    if 'value' not in df.columns:
                         raise APIFetchError(f"Adapter {api_provider} 未返回帶有 'value' 列的 DataFrame。")

                    start_dt_naive = pd.to_datetime(start_date_str).tz_localize(None)
                    end_dt_naive = pd.to_datetime(end_date_str).tz_localize(None)
                    df_filtered = df[(df.index >= start_dt_naive) &
                                     (df.index <= end_dt_naive)][['value']]

                    if not df_filtered.empty:
                        self._write_to_cache(df_filtered, cache_filepath)
                        return df_filtered
                    else:
                        self.logger.info(f"從 {api_provider} 獲取指標 '{indicator_name}' 成功，但在指定日期範圍內無數據。")
                else:
                    self.logger.info(f"從 {api_provider} 獲取指標 '{indicator_name}' 返回空或無效數據。")

            except APIFetchError as e:
                self.logger.warning(f"嘗試從 {api_provider} 獲取指標 '{indicator_name}' 失敗: {e}")
            except Exception as e:
                self.logger.error(f"嘗試從 {api_provider} 獲取指標 '{indicator_name}' 時發生未預期錯誤: {e}", exc_info=True)

        self.logger.error(f"未能從任何已配置的源成功獲取指標 '{indicator_name}'。")
        return None

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    mock_project_config = {
        "api_keys_env_vars": {
            "fred": "FRED_API_KEY",
        },
        "runtime_api_keys": {
            "fred": "TEST_FRED_API_KEY_1234567890abcdef"
        },
        "data_fetching": {
            "unified_api_fetcher": {
                "cache_dir": "TEMP_CACHE_Market_Data",
                "cache_expire_after_hours": 1,
                "default_retry_attempts": 2,
                "default_retry_delay_seconds": 1
            }
        }
    }
    if os.path.exists("TEMP_CACHE_Market_Data"):
        import shutil
        shutil.rmtree("TEMP_CACHE_Market_Data")
    os.makedirs("TEMP_CACHE_Market_Data", exist_ok=True)

    mock_endpoints_content = """
indicators:
  DGS10_FRED: # 使用 DGS10 進行測試
    - api_provider: "fred"
      series_id: "DGS10"
  VIX_YF:
    - api_provider: "yfinance"
      ticker: "^VIX"
      data_column: "Close"
  SPY_YF: # 添加 SPY 進行 yfinance 測試
    - api_provider: "yfinance"
      ticker: "SPY"
      data_column: "Close"
  NON_EXISTENT_INDICATOR:
    - api_provider: "non_existent_provider"
      param: "test"
  FRED_INVALID_SERIES:
    - api_provider: "fred"
      series_id: "INVALIDFREDSERIESXYZ"
  YF_INVALID_TICKER:
    - api_provider: "yfinance"
      ticker: "INVALIDYFTICKERXYZ"
"""
    mock_endpoints_path = "config/temp_api_endpoints.yaml"
    # os.makedirs("config", exist_ok=True) # config_loader.py 的測試區塊可能會創建，這裡避免衝突
    if not os.path.exists("config"): # 確保 config 目錄存在
        os.makedirs("config")
    with open(mock_endpoints_path, "w") as f:
        f.write(mock_endpoints_content)

    fetcher = UnifiedAPIFetcher(project_config=mock_project_config, endpoints_config_path=mock_endpoints_path)

    start_date_short = "2023-01-01"
    end_date_short = "2023-01-15"

    start_date_long_yf = "2022-10-01"
    end_date_long_yf = "2023-01-15"

    logger.info("\n--- 測試獲取 DGS10_FRED (FRED) ---") # 更新測試指標名稱
    dgs10_df = fetcher.get_data("DGS10_FRED", start_date_short, end_date_short)
    if dgs10_df is not None and not dgs10_df.empty: # 檢查是否為空
        print(f"DGS10_FRED Data ( முதல் 5 行):\n{dgs10_df.head()}\n")
    else:
        print("未能獲取 DGS10_FRED 數據。\n")

    logger.info("\n--- 再次獲取 DGS10_FRED (應從快取讀取，如果第一次成功) ---")
    dgs10_df_cached = fetcher.get_data("DGS10_FRED", start_date_short, end_date_short)
    if dgs10_df_cached is not None and not dgs10_df_cached.empty: # 檢查是否為空
        print(f"DGS10_FRED Data (來自快取， முதல் 5 行):\n{dgs10_df_cached.head()}\n")
    else:
        print("未能從快取獲取 DGS10_FRED 數據 (可能因為首次獲取失敗或數據為空)。\n")

    logger.info("\n--- 測試獲取 VIX_YF (Yahoo Finance, 擴大日期範圍) ---")
    vix_df = fetcher.get_data("VIX_YF", start_date_long_yf, end_date_long_yf)
    if vix_df is not None and not vix_df.empty:
        print(f"VIX_YF Data ( முதல் 5 行):\n{vix_df.head()}\n")
        logger.info(f"成功獲取 {len(vix_df)} 條 VIX_YF 數據。")
        logger.info("\n--- 再次獲取 VIX_YF (應從快取讀取) ---")
        vix_df_cached = fetcher.get_data("VIX_YF", start_date_long_yf, end_date_long_yf)
        if vix_df_cached is not None and not vix_df_cached.empty:
            print(f"VIX_YF Data (來自快取， முதல் 5 行):\n{vix_df_cached.head()}\n")
        else:
            print("未能從快取獲取 VIX_YF 數據。\n")
    else:
        print("未能獲取 VIX_YF 數據。\n")

    logger.info("\n--- 測試獲取 SPY_YF (Yahoo Finance) ---") # 新增 SPY 測試
    spy_df = fetcher.get_data("SPY_YF", start_date_long_yf, end_date_long_yf)
    if spy_df is not None and not spy_df.empty:
        print(f"SPY_YF Data ( முதல் 5 行):\n{spy_df.head()}\n")
        logger.info(f"成功獲取 {len(spy_df)} 條 SPY_YF 數據。")
    else:
        print("未能獲取 SPY_YF 數據。\n")


    logger.info("\n--- 測試獲取不存在的指標 ---")
    non_existent_df = fetcher.get_data("DOES_NOT_EXIST", start_date_short, end_date_short)
    if non_existent_df is None:
        print("成功處理不存在的指標 (返回 None)。\n")
    else:
        print("錯誤：不存在的指標應返回 None。\n")

    logger.info("\n--- 測試獲取使用不存在的 provider 的指標 ---")
    non_existent_provider_df = fetcher.get_data("NON_EXISTENT_INDICATOR", start_date_short, end_date_short)
    if non_existent_provider_df is None:
        print("成功處理使用不存在的 provider 的指標 (返回 None)。\n")
    else:
        print("錯誤：使用不存在的 provider 的指標應返回 None。\n")

    logger.info("\n--- 測試獲取 FRED 無效序列 ---")
    fred_invalid_df = fetcher.get_data("FRED_INVALID_SERIES", start_date_short, end_date_short)
    if fred_invalid_df is None:
        print("成功處理 FRED 無效序列 (返回 None)。\n")
    else:
        print(f"錯誤：FRED 無效序列應返回 None。得到:\n{fred_invalid_df}\n")

    logger.info("\n--- 測試獲取 yfinance 無效 Ticker ---")
    yf_invalid_df = fetcher.get_data("YF_INVALID_TICKER", start_date_short, end_date_short)
    if yf_invalid_df is None:
        print("成功處理 yfinance 無效 Ticker (返回 None)。\n")
    else:
        print(f"錯誤：yfinance 無效 Ticker 應返回 None。得到:\n{yf_invalid_df}\n")

    if os.path.exists(mock_endpoints_path):
        os.remove(mock_endpoints_path)
    if os.path.exists("TEMP_CACHE_Market_Data"):
        import shutil
        shutil.rmtree("TEMP_CACHE_Market_Data")
    if os.path.exists("config") and not os.listdir("config"):
        try: os.rmdir("config")
        except OSError: pass

    logger.info("UnifiedAPIFetcher 測試演示完成。")

# ... (後續程式碼)
