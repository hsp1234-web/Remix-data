# fetchers/yfinance_fetcher.py
import yfinance as yf
import pandas as pd
import time
import random
import logging
from typing import Optional
from ..interfaces.data_fetcher_interface import DataFetcherInterface

class YFinanceFetcher(DataFetcherInterface):
    """使用 yfinance 獲取金融數據的穩健實現。"""

    def __init__(self, robustness_config: dict):
        self.config = robustness_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        current_delay = self.config['delay_min_seconds']
        for attempt in range(self.config['retries']):
            try:
                self.logger.info(f"Attempt {attempt + 1}/{self.config['retries']} to fetch {symbol} from yfinance...")
                # 確保下載時段的包容性，yfinance 的 end_date 是不包含的
                # 但通常使用者期望的是包含，所以這裡可以考慮將 end_date + 1 天
                # 不過，為了與原始草圖一致，暫時不修改
                data = yf.download(symbol, start=start_date, end=end_date, progress=False)

                if data.empty:
                    self.logger.warning(f"No data found for {symbol} on yfinance for the given period: {start_date} to {end_date}.")
                    return None # 優雅地返回 None

                # 標準化數據格式
                data.reset_index(inplace=True)
                # 將所有列名轉為小寫並替換空格為下劃線
                data.columns = [str(col).lower().replace(' ', '_') for col in data.columns]

                # 確保 'date' 列是主要的日期時間列
                if 'datetime' in data.columns and 'date' not in data.columns:
                    data.rename(columns={'datetime': 'date'}, inplace=True)
                elif 'timestamp' in data.columns and 'date' not in data.columns:
                     data.rename(columns={'timestamp': 'date'}, inplace=True)


                # 檢查必要的 OHLCV 欄位是否存在，並確保它們是數值類型
                # yfinance 通常返回 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'
                # 標準化後是 'open', 'high', 'low', 'close', 'adj_close', 'volume'
                expected_cols = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
                for col in expected_cols:
                    if col not in data.columns:
                        self.logger.warning(f"Column '{col}' not found in data for {symbol}. Filling with NaN.")
                        data[col] = pd.NA # 或者 0，視情況而定

                # 確保 'date' 列是 datetime64[ns] 類型
                if 'date' in data.columns:
                    data['date'] = pd.to_datetime(data['date'])
                else:
                    self.logger.error(f"Critical: 'date' column not found after attempting to standardize for {symbol}.")
                    return None

                return data

            except Exception as e:
                self.logger.error(f"Error fetching {symbol} from yfinance on attempt {attempt + 1}: {e}", exc_info=True)
                if attempt < self.config['retries'] - 1:
                    # 指數退避邏輯
                    sleep_time = current_delay + random.uniform(0, current_delay * 0.1) # 增加一點抖動
                    self.logger.info(f"Waiting {sleep_time:.2f} seconds before retrying...")
                    time.sleep(sleep_time)
                    current_delay = min(current_delay * self.config.get('backoff_factor', 2), self.config['delay_max_seconds'])
                else:
                    self.logger.critical(f"Failed to fetch {symbol} from yfinance after all {self.config['retries']} retries.")
                    return None # 所有重試失敗後，返回 None
        return None # 理論上不會執行到這裡，除非 retries 為 0
