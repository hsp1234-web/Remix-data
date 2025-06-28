# modules/database/interface.py
# ----------------------------------------------------
from abc import ABC, abstractmethod
import pandas as pd

class DatabaseInterface(ABC):
    """
    數據庫倉儲的抽象基底類別 (契約)。
    """
    @abstractmethod
    def save_timeseries_data(self, identifier: str, data: pd.DataFrame):
        """將時間序列數據儲存到數據庫"""
        pass

    @abstractmethod
    def get_timeseries_data(self, identifier: str) -> pd.DataFrame:
        """從數據庫讀取時間序列數據"""
        pass
