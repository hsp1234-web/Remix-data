# data_fetchers/yfinance_fetcher.py

import yfinance as yf
import pandas as pd
import time
import random
import logging
from interfaces.data_fetcher_interface import DataFetcherInterface

logger = logging.getLogger(__name__)

class YFinanceFetcher(DataFetcherInterface):
    """
    使用 yfinance 庫獲取金融數據的具體實現。
    內建了延遲和基本的重試概念（更強大的重試由 commander 層的並行邏輯處理）。
    """
    def __init__(self, config: dict):
        # 簡單地從組態中獲取延遲參數
        self.delay_min = config.get('delay_min_seconds', 1)
        self.delay_max = config.get('delay_max_seconds', 3)
        logger.info(f"YFinanceFetcher 初始化，請求延遲區間: [{self.delay_min}, {self.delay_max}] 秒。")

    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """
        實現從 yfinance 獲取數據的邏輯。
        """
        try:
            # 加入隨機延遲 (抖動)，以避免同時對 API 發起過多請求
            delay = random.uniform(self.delay_min, self.delay_max)
            logger.info(f"等待 {delay:.2f} 秒後，開始獲取 {symbol} 數據...")
            time.sleep(delay)

            ticker = yf.Ticker(symbol)
            # 獲取日線數據
            data = ticker.history(start=start_date, end=end_date, interval="1d")

            if data.empty:
                logger.warning(f"未能為 {symbol} 在指定日期範圍內找到數據。")
                return None

            # 清理數據：重設索引，將日期作為列；將列名轉為小寫
            data.reset_index(inplace=True)
            data.columns = [col.lower() for col in data.columns]

            # 確保 'date' 列是 datetime64[ns] 類型
            data['date'] = pd.to_datetime(data['date'])


            logger.info(f"成功獲取 {symbol} 的 {len(data)} 筆數據。")
            return data

        except Exception as e:
            # yfinance 可能拋出多種錯誤，這裡統一捕獲
            logger.error(f"使用 yfinance 獲取 {symbol} 數據時出錯: {e}", exc_info=True)
            return None
