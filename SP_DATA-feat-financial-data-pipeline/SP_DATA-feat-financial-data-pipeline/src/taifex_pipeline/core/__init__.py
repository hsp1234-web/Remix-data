# src/taifex_pipeline/core/__init__.py
# This file makes Python treat the 'core' directory as a package.

# 匯出核心功能，方便外部使用
from .logger_setup import setup_logger
from .config_loader import load_format_catalog, clear_config_cache

__all__ = [
    "setup_logger",
    "load_format_catalog",
    "clear_config_cache",
]

# 您也可以在這裡初始化一個預設的 logger，如果整個 core 套件或其子模組需要共用一個 logger 實例
# import logging
# logger = logging.getLogger(__name__)
# logger.addHandler(logging.NullHandler()) # 建議為函式庫加入 NullHandler
                                        # 除非應用程式明確設定 handler，否則不會有輸出
                                        # 或者，依賴於應用程式在啟動時呼叫 setup_logger
