# interfaces/database_interface.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional

class DatabaseInterface(ABC):
    """
    定義數據庫服務層的契約 (Interface)，即倉儲模式的接口。
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
    def upsert_ohlcv(self, data: pd.DataFrame, table_name: str):
        """
        將 OHLCV DataFrame 數據保存到指定的表中 (更新或插入)。

        Args:
            data (pd.DataFrame): 包含 OHLCV 數據的 DataFrame。
            table_name (str): 目標數據表的名稱。
        """
        pass

    @abstractmethod
    def get_ohlcv(self, symbol: str, table_name: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """從指定表中獲取特定時間範圍的 OHLCV 數據。"""
        pass
