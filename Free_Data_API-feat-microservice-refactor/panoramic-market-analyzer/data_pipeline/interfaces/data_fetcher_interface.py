# interfaces/data_fetcher_interface.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional

class DataFetcherInterface(ABC):
    """
    定義數據獲取器的契約 (Interface)。
    所有具體的數據獲取器都必須實現此接口。
    """

    @abstractmethod
    def fetch(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        獲取指定金融商品的歷史數據。

        Args:
            symbol (str): 商品代碼 (e.g., 'AAPL', 'DGS10', 'BTC-USD').
            start_date (str): 開始日期 'YYYY-MM-DD'.
            end_date (str): 結束日期 'YYYY-MM-DD'.

        Returns:
            Optional[pd.DataFrame]: 包含數據的 DataFrame，若獲取失敗則返回 None。
        """
        pass
