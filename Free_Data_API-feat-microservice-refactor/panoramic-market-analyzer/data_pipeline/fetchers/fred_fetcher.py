# fetchers/fred_fetcher.py
import pandas as pd
import logging
import time
import random
from typing import Optional
from fredapi import Fred
from ..interfaces.data_fetcher_interface import DataFetcherInterface

class FredFetcher(DataFetcherInterface):
    """
    使用 fredapi 獲取 FRED (Federal Reserve Economic Data) 數據的穩健實現。
    需要設定 FRED_API_KEY 環境變數，或者在 config.yaml 中提供。
    """

    def __init__(self, robustness_config: dict, api_key: Optional[str] = None):
        self.config = robustness_config
        self.logger = logging.getLogger(self.__class__.__name__)

        # API 金鑰的處理: 優先使用傳入的 api_key，其次是環境變數，最後是設定檔 (若有)
        # 為了簡化，此處假設 api_key 會由 Commander 傳入，或者 fredapi 能自動從環境變數讀取
        if api_key:
            self.fred = Fred(api_key=api_key)
        else:
            # fredapi 會自動嘗試從 FRED_API_KEY 環境變數讀取
            try:
                self.fred = Fred()
                # 測試一下 API key 是否有效 (雖然 fredapi 本身不直接提供此功能)
                # 可以嘗試獲取一個常見序列來間接測試
                self.fred.get_series_info('GNPCA')
                self.logger.info("Fred API key loaded successfully (likely from environment variable FRED_API_KEY).")
            except ValueError as ve: # Fredapi 在沒有key時拋出 ValueError
                 self.logger.error(f"Fred API key not found or invalid. Please set FRED_API_KEY environment variable or pass it during initialization: {ve}")
                 raise ValueError("Fred API key not found or invalid.") from ve
            except Exception as e:
                self.logger.error(f"Failed to initialize Fred client: {e}", exc_info=True)
                raise

    def fetch(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        獲取指定 FRED 序列的歷史數據。

        Args:
            symbol (str): FRED 序列代碼 (e.g., 'DGS10', 'CPIAUCSL').
            start_date (str): 開始日期 'YYYY-MM-DD'.
            end_date (str): 結束日期 'YYYY-MM-DD'.

        Returns:
            Optional[pd.DataFrame]: 包含數據的 DataFrame，若獲取失敗則返回 None。
                                     DataFrame 將包含 'date' 和 'value' 兩列。
        """
        current_delay = self.config['delay_min_seconds']
        for attempt in range(self.config['retries']):
            try:
                self.logger.info(f"Attempt {attempt + 1}/{self.config['retries']} to fetch FRED series {symbol}...")

                series_data = self.fred.get_series(
                    series_id=symbol,
                    observation_start=start_date,
                    observation_end=end_date
                )

                if series_data is None or series_data.empty:
                    self.logger.warning(f"No data found for FRED series {symbol} for the given period.")
                    return None

                # 將 Series 轉換為 DataFrame 並標準化
                data_df = series_data.reset_index()
                data_df.columns = ['date', 'value'] # FRED數據通常只有日期和值

                # 確保 'date' 列是 datetime64[ns] 類型
                data_df['date'] = pd.to_datetime(data_df['date'])

                # FRED數據通常不直接對應OHLCV，所以我們只返回原始值
                # 若要整合進 ohlcv_daily 表，需要在 Commander 層進行轉換或映射
                # 例如，將 'value' 視為 'close' 價格，其他 OHLC 設為相同值或 NaN

                self.logger.info(f"Successfully fetched {len(data_df)} points for FRED series {symbol}.")
                return data_df

            except Exception as e:
                # fredapi 可能會拋出 requests.exceptions.HTTPError 等錯誤
                self.logger.error(f"Error fetching FRED series {symbol} on attempt {attempt + 1}: {e}", exc_info=True)
                if attempt < self.config['retries'] - 1:
                    sleep_time = current_delay + random.uniform(0, current_delay * 0.1)
                    self.logger.info(f"Waiting {sleep_time:.2f} seconds before retrying...")
                    time.sleep(sleep_time)
                    current_delay = min(current_delay * self.config.get('backoff_factor', 2), self.config['delay_max_seconds'])
                else:
                    self.logger.critical(f"Failed to fetch FRED series {symbol} after all {self.config['retries']} retries.")
                    return None
        return None
