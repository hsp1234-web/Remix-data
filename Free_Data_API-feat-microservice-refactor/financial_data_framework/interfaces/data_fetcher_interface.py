# interfaces/data_fetcher_interface.py

from abc import ABC, abstractmethod
import pandas as pd

class DataFetcherInterface(ABC):
    """
    定義數據獲取器的契約 (Interface)。
    所有具體的數據獲取器都必須實現此接口定義的方法。
    """

    @abstractmethod
    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """
        獲取指定金融商品的歷史 OCHLV 數據。

        Args:
            symbol (str): 商品代碼 (e.g., 'AAPL', '^TWII').
            start_date (str): 開始日期 'YYYY-MM-DD'.
            end_date (str): 結束日期 'YYYY-MM-DD'.

        Returns:
            pd.DataFrame | None: 包含 OCHLV 數據的 DataFrame，若失敗則返回 None。
        """
        pass
