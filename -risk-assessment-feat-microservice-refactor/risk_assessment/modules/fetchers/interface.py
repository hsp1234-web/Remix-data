# modules/fetchers/interface.py
# ----------------------------------------------------
from abc import ABC, abstractmethod
import pandas as pd

class DataFetcherInterface(ABC):
    """
    數據獲取器的抽象基底類別 (契約)。
    """
    @abstractmethod
    def fetch_data(self, identifier: str) -> pd.DataFrame:
        """
        根據給定的標識符，獲取時間序列數據。
        :param identifier: 數據的唯一標識，例如 'AAPL' 或 'FRED/GDP'
        :return: 一個包含 DatetimeIndex 的 Pandas DataFrame
        """
        pass
