from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Tuple, Optional

class BaseConnector(ABC):
    """
    所有數據連接器的抽象基類。
    定義了標準接口，確保所有 Connector 的行為一致。
    """

    def __init__(self, config: Dict[str, Any], source_api_name: str = "Unknown"):
        self.config = config
        self.source_api_name = source_api_name
        # Logger can be passed by the child class or DataManager for better context
        # For now, child classes will initialize their own loggers or use a global one.

    @abstractmethod
    def fetch_data(self, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        從 API 或數據源獲取原始數據並進行初步轉換成 DataFrame (通常是長表格式)。

        Args:
            **kwargs: 特定 connector 需要的參數 (例如 series_ids, tickers, start_date, end_date).

        Returns:
            一個包含 (DataFrame, error_message) 的元組。
            成功時，DataFrame 包含獲取和初步標準化的數據，error_message 為 None。
            失敗時，DataFrame 為 None 或空的 DataFrame (帶有預期欄位)，error_message 包含錯誤信息。
            DataFrame 應包含 'source_api' 和 'data_snapshot_timestamp' (UTC) 欄位。
            對於時間序列數據，應有 'metric_date' 或 'price_date'。
            對於宏觀/因子數據，應有 'metric_name'。
            對於股價數據，應有 'security_id'。
        """
        pass

    def get_source_name(self) -> str:
        """返回數據源的名稱。"""
        return self.source_api_name

    # Common utility methods can be added here if needed, e.g.,
    # _make_request_with_retries (similar to what was in the old BaseConnector from user's Colab)
    # or a method to standardize date formats.
    # For now, keeping it lean as per the new design focusing on fetch_data.
    # The retry logic from user's previous BaseConnector (with jitter) is excellent
    # and ideally should be part of a shared HTTP request utility or within each connector's
    # implementation of how it calls external APIs if not using a library that handles it.
    # Given the "one-shot build" nature, detailed retry in Base might be over-engineering for now,
    # and each connector can implement its specific retry or rely on the robustness of the used library.
    # However, for FREDConnector which uses requests directly, that logic would be valuable.
    # Let's assume for now that retry logic is handled within each connector's specific requests.
    # Or, we can add a protected _make_request method here later if many connectors use raw requests.
