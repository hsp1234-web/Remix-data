# interfaces/database_interface.py

from abc import ABC, abstractmethod
import pandas as pd

class DatabaseInterface(ABC):
    """
    定義數據庫服務層的契約 (Interface)。
    所有具體的數據庫倉儲類 (Repository) 都必須實現此接口。
    """

    @abstractmethod
    def connect(self):
        """建立數據庫連接。"""
        pass

    @abstractmethod
    def disconnect(self):
        """斷開數據庫連接。"""
        pass

    @abstractmethod
    def save_ohlcv(self, data: pd.DataFrame, table_name: str):
        """
        將 OCHLV DataFrame 數據保存到指定的表中。
        實現時必須包含 UPSERT (更新或插入) 邏輯以避免重複。

        Args:
            data (pd.DataFrame): 包含 OCHLV 數據的 DataFrame。
            table_name (str): 目標數據表的名稱。
        """
        pass

    @abstractmethod
    def get_ohlcv(self, table_name: str, symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """
        從指定表中獲取特定時間範圍的 OCHLV 數據。

        Args:
            table_name (str): 來源數據表的名稱。
            symbol (str): 商品代碼。
            start_date (str): 開始日期 'YYYY-MM-DD'.
            end_date (str): 結束日期 'YYYY-MM-DD'.

        Returns:
            pd.DataFrame | None: 包含所請求數據的 DataFrame，若無則返回 None。
        """
        pass
