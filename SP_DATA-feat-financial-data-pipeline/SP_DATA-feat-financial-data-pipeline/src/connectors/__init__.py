# src/connectors/__init__.py
import logging

# --- 戰術模組停用 (第二次嘗試 - 確保NYFed隔離) ---
# 僅保留 NYFedConnector 的導入。
# diagnose_nyfed_logic.py 中直接 `from connectors.nyfed_connector import NYFedConnector`
# 因此，嚴格來說，這個 __init__.py 在該診斷腳本執行時，如果只導入NYFedConnector，
# 其內容對 diagnose_nyfed_logic.py 的影響不大。
# 但是，為了防止其他可能的間接導入或測試框架掃描路徑時引發問題，
# 我們還是將其清理乾淨，只明確導出 NYFedConnector。

# from .alpha_vantage_connector import AlphaVantageConnector
# from .finlab_connector import FinLabConnector
# from .finmind_connector import FinMindConnector
# from .finnhub_connector import FinnhubConnector
# from .fmp_connector import FMPConnector
# from .fred_connector import FredConnector
from .nyfed_connector import NYFedConnector # <<< 唯一實際導入的 Connector
# from .polygon_io_connector import PolygonIOConnector
# from .yfinance_connector import YFinanceConnector

__all__ = [
    "NYFedConnector",
]

# 在此診斷模式下，DataMaster 用到的輔助函數和映射表可以被簡化或移除，
# 因為我們的主要目標是 diagnose_nyfed_logic.py 的直接執行，它不依賴 DataMaster。
# 如果其他測試(非本次診斷目標)意外運行並嘗試初始化 DataMaster，
# 以下簡化的部分會讓 DataMaster 只能找到 NYFed。

SUPPORTED_CONNECTORS_MAP = {
    "nyfed": NYFedConnector,
}

def get_connector_class(connector_name: str):
    logger = logging.getLogger(__name__) # 在函數內部獲取 logger 實例
    connector_name_lower = connector_name.lower().replace('-', '_')
    if connector_name_lower in SUPPORTED_CONNECTORS_MAP:
        return SUPPORTED_CONNECTORS_MAP[connector_name_lower]
    else:
        logger.error(f"Unsupported connector name during NYFed-focused diagnosis: {connector_name}. Only NYFed is effectively available via get_connector_class.")
        raise ValueError(f"Unsupported connector: {connector_name}. Only NYFed is configured in __init__.py for diagnosis.")

logger = logging.getLogger(__name__)
logger.info(f"Connectors package initialized (NYFed Diagnosis Mode - Strict Isolation). Effective __all__: {__all__}")
logger.info(f"Supported connectors map (NYFed Diagnosis Mode - Strict Isolation) contains: {list(SUPPORTED_CONNECTORS_MAP.keys())}")
