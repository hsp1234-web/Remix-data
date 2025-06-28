# -*- coding: utf-8 -*-
"""
核心共用模組：日誌設定 (Logger Setup)

本模組負責設定和管理專案的日誌系統。它實現了一個雙軌制日誌系統：
1.  **主控台即時報告 (Console Output)**: 輸出人類易讀、帶有時間戳和級別的簡潔狀態更新，
    主要用於操作者即時了解管線運行狀況。
2.  **結構化日誌檔案 (Structured Log File)**: 以 JSON 格式記錄詳細的日誌事件，
    包含時間戳、執行ID、日誌級別、來源模組、函式名稱、行號、檔案雜湊（若適用）以及訊息內容。
    這種格式便於後續的機器分析、日誌管理系統集成和問題追蹤。

主要功能：
- `setup_global_logger()`: 初始化全域的根 logger (`taifex_pipeline`)，配置上述兩種 handler。
- `get_logger()`: 供其他模組獲取 logger 實例的便捷函式。
- `JsonFormatter`: 自訂的 logging Formatter，用於產生 JSON 格式的日誌。
- `EXECUTION_ID`: 在 logger 首次初始化時生成一個全域唯一的 UUID，用於標識單次管線運行，
  並會包含在每條 JSON 日誌記錄中。
"""
import logging
import sys
import json
import datetime
import pytz
import uuid
import os
from pathlib import Path
from typing import Dict, Any, Optional # 從 typing 導入 Optional

# --- 全域設定 ---
TAIPEI_TZ: pytz.BaseTzInfo = pytz.timezone('Asia/Taipei')
"""台北時區物件，用於日誌時間戳。"""

LOG_FORMAT_CONSOLE: str = "%(asctime)s [%(levelname)-8s] [%(name)-25s:%(lineno)4d] %(message)s"
"""主控台日誌的格式字串。顯示時間、級別、模組名、行號和訊息。"""

LOG_DATE_FORMAT_CONSOLE: str = "%Y-%m-%d %H:%M:%S"
"""主控台日誌的時間格式。"""

EXECUTION_ID: str = str(uuid.uuid4())
"""
全局唯一的管線執行 ID。
在 logger 首次初始化時（即模組加載時）設定，用於追蹤單次管線執行的所有相關日誌。
"""

# --- 結構化日誌 JSON 格式化器 ---
class JsonFormatter(logging.Formatter):
    """
    自訂 JSON 格式化器 (Custom JSON Formatter)。

    將 Python `logging.LogRecord` 物件轉換為 JSON 字串，以便進行結構化日誌記錄。
    包含標準日誌欄位以及自訂的 `execution_id` 和 `file_hash` (如果提供)。
    """
    def format(self, record: logging.LogRecord) -> str:
        """
        將 LogRecord 格式化為 JSON 字串。

        Args:
            record (logging.LogRecord): Python logging 模組的日誌記錄物件。

        Returns:
            str: 代表該日誌記錄的 JSON 字串。
        """
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.datetime.fromtimestamp(record.created, tz=TAIPEI_TZ).isoformat(),
            "execution_id": EXECUTION_ID,
            "level": record.levelname,
            "logger_name": record.name, # logger 的名稱，通常是模組路徑
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "file_hash": getattr(record, "file_hash", "N/A"), # 允許透過 extra 參數傳遞 file_hash
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info) # 格式化異常堆疊追蹤

        # 處理 extra 參數中除了 'file_hash' 之外的其他自訂欄位
        if hasattr(record, "extra_info") and isinstance(getattr(record, "extra_info"), dict):
            custom_extras = {k: v for k, v in getattr(record, "extra_info").items()}
            log_entry.update(custom_extras)

        return json.dumps(log_entry, ensure_ascii=False, default=str) # default=str 處理無法序列化的物件

# --- 日誌設定函式 ---
_logger_initialized: bool = False
_root_logger: logging.Logger = logging.getLogger("taifex_pipeline") # 專案的根 logger

def setup_global_logger(
    log_level_console: int = logging.INFO,
    log_level_file: int = logging.DEBUG,
    log_dir: Path = Path("logs")
) -> logging.Logger:
    """
    設定並初始化全域的 `taifex_pipeline` logger。

    此函式應在應用程式啟動時儘早調用一次。
    它會配置一個根 logger，並為其添加兩個 handler：
    一個用於輸出到主控台，另一個用於輸出到結構化的 JSON 日誌檔案。

    Args:
        log_level_console (int): 主控台輸出的最低日誌級別 (例如 `logging.INFO`, `logging.DEBUG`)。
        log_level_file (int): 結構化日誌檔案輸出的最低日誌級別。
        log_dir (Path): 結構化日誌檔案的存放目錄。路徑應相對於專案根目錄。

    Returns:
        logging.Logger: 設定完成的 `taifex_pipeline` 根 logger 實例。
    """
    global _logger_initialized
    global _root_logger
    # EXECUTION_ID 已在模組加載時設定，此處不再重新賦值

    if _logger_initialized:
        _root_logger.debug("Logger 已初始化，跳過重複設定。")
        return _root_logger

    _root_logger.setLevel(min(log_level_console, log_level_file)) # 設定 logger 的最低處理級別
    _root_logger.handlers = [] # 清除任何已有的 handlers，避免重複添加

    # 1. 主控台 Handler (Console Output)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level_console)
    console_formatter = logging.Formatter(LOG_FORMAT_CONSOLE, datefmt=LOG_DATE_FORMAT_CONSOLE)
    # 若要使用 RichHandler 以獲得更美觀的控制台輸出:
    # from rich.logging import RichHandler
    # console_handler = RichHandler(rich_tracebacks=True, markup=True, log_time_format="[%Y-%m-%d %H:%M:%S]")
    # console_handler.setFormatter(logging.Formatter("%(message)s")) # RichHandler 通常自己處理格式
    console_handler.setFormatter(console_formatter)
    _root_logger.addHandler(console_handler)

    # 2. 結構化日誌檔案 Handler (Structured Log File)
    try:
        # 確定專案根目錄的路徑
        # 此檔案位於 src/taifex_pipeline/core/logger_setup.py
        # 所以專案根目錄是 Path(__file__).resolve().parent.parent.parent.parent
        project_root = Path(__file__).resolve().parents[3]
        actual_log_dir = project_root / log_dir
        actual_log_dir.mkdir(parents=True, exist_ok=True)

        current_time_str = datetime.datetime.now(TAIPEI_TZ).strftime("%Y%m%d_%H%M%S")
        # 使用 EXECUTION_ID 的前8位以保持檔名簡潔
        log_file_name = f"pipeline_run_{current_time_str}_{EXECUTION_ID[:8]}.log.json"
        log_file_path = actual_log_dir / log_file_name

        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(log_level_file)
        json_formatter = JsonFormatter()
        file_handler.setFormatter(json_formatter)
        _root_logger.addHandler(file_handler)

        _root_logger.info(f"結構化日誌將寫入: {log_file_path.relative_to(project_root)}")
    except Exception as e:
        # 如果檔案日誌設定失敗，至少主控台日誌還能工作
        _root_logger.error(f"設定檔案日誌 Handler 失敗: {e}", exc_info=True)

    _logger_initialized = True
    _root_logger.info(f"Logger 初始化完成。Execution ID: {EXECUTION_ID}")
    return _root_logger

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    獲取一個 logger 實例。

    如果全域 logger (`taifex_pipeline`) 尚未初始化，此函式會先使用預設參數調用
    `setup_global_logger()` 進行初始化。

    Args:
        name (Optional[str]): Logger 的名稱。
            - 如果提供，則返回 `logging.getLogger(name)`，該 logger 會繼承根 logger 的設定。
              通常建議傳入 `__name__` 以便日誌能準確反映其來源模組。
            - 如果為 `None` (預設)，則返回 `taifex_pipeline` 根 logger。

    Returns:
        logging.Logger: 所需的 Logger 實例。
    """
    if not _logger_initialized:
        # 如果在獲取 logger 時尚未初始化，則使用預設值進行初始化
        # 這確保了即使沒有在應用程式入口明確調用 setup_global_logger，
        # 各模組也能直接使用 get_logger() 獲得一個可用的 logger。
        setup_global_logger()

    if name:
        return logging.getLogger(name)
    return _root_logger

# --- 範例使用 (通常在其他模組中導入 get_logger 並使用) ---
if __name__ == "__main__":
    # 在主腳本或應用程式入口處，可以這樣調用一次 setup_global_logger 來設定期望的日誌級別
    # setup_global_logger(log_level_console=logging.DEBUG, log_level_file=logging.DEBUG)

    # 然後在各個模組中，透過 get_logger 獲取 logger
    # 如果 setup_global_logger 未被明確調用，get_logger() 內部會使用預設值初始化
    logger_main = get_logger(__name__) # 使用當前模組名作為 logger 名稱

    logger_main.debug("這是一條來自 __main__ 的 DEBUG 訊息。")
    logger_main.info("這是一條來自 __main__ 的 INFO 訊息。")
    logger_main.warning("這是一條來自 __main__ 的 WARNING 訊息.")

    # 測試傳遞額外資訊，包括 file_hash
    logger_main.error(
        "這是一條來自 __main__ 的 ERROR 訊息，包含額外資訊。",
        extra={"file_hash": "example_file_hash_123", "custom_field": "some_value"} # type: ignore
    )

    try:
        x = 1 / 0
    except ZeroDivisionError:
        logger_main.critical(
            "這是一條來自 __main__ 的 CRITICAL 訊息，包含異常堆疊追蹤。",
            exc_info=True, # 自動添加異常資訊
            extra={"file_hash": "critical_error_file_hash"} # type: ignore
        )

    # 模擬在其他模組中使用
    # (假設在 hypothetical_module.py 中)
    # from taifex_pipeline.core.logger_setup import get_logger
    # module_logger = get_logger("hypothetical_module") # 或 get_logger(__name__)
    # module_logger.info("這是來自假設模組的訊息。")

    print(f"\n日誌 EXECUTION_ID: {EXECUTION_ID}")
    # 提示使用者檢查日誌檔案，路徑相對於執行 `python logger_setup.py` 的位置
    # 如果從 MyTaifexDataProject/src/taifex_pipeline/core/ 執行，則 logs 在上兩層
    # 如果從 MyTaifexDataProject/ 執行 `python -m src.taifex_pipeline.core.logger_setup`，則 logs 在同層
    # 這裡的 project_root 是相對於此檔案計算的
    project_root_for_msg = Path(__file__).resolve().parents[3]
    log_dir_for_msg = project_root_for_msg / "logs"
    print(f"請檢查位於 '{log_dir_for_msg}' 資料夾下的 .log.json 檔案。")

[end of MyTaifexDataProject/src/taifex_pipeline/core/logger_setup.py]
