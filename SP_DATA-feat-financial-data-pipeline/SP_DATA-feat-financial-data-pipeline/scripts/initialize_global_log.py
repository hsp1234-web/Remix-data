import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import sys
from typing import Optional

LOG_DIR_NAME = "api_test_logs"
LOG_FILE_PATH: Optional[str] = None
_global_logger_initialized_flag = False

_bootstrap_logger = logging.getLogger("BootstrapLogger")
if not _bootstrap_logger.handlers and not logging.getLogger().hasHandlers():
    _ch = logging.StreamHandler(sys.stdout)
    _ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s (bootstrap)'))
    _bootstrap_logger.addHandler(_ch)
    _bootstrap_logger.setLevel(logging.INFO)
    _bootstrap_logger.propagate = False


def get_taipei_time() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=8)

class TaipeiTimeFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.taipei_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        return super().format(record)

def initialize_log_file(log_dir_override: Optional[str] = None, force_reinit: bool = False,
                        project_root_path: Optional[Path] = None) -> Optional[str]:
    global LOG_FILE_PATH, _global_logger_initialized_flag

    current_project_root = project_root_path
    if current_project_root is None:
        try:
            current_project_root = Path(__file__).resolve().parent.parent
        except NameError:
            current_project_root = Path(".").resolve()
            _bootstrap_logger.warning(f"__file__ not defined, using CWD {current_project_root} as project root for log path.")

    current_log_dir_path = Path(log_dir_override) if log_dir_override else current_project_root / LOG_DIR_NAME

    if _global_logger_initialized_flag and not force_reinit and LOG_FILE_PATH:
        if Path(LOG_FILE_PATH).parent == current_log_dir_path.resolve():
            _bootstrap_logger.debug(f"Global logger already initialized. Log file at: {LOG_FILE_PATH}")
            return LOG_FILE_PATH
        else:
            _bootstrap_logger.warning(f"Log dir changed. Forcing re-init. Old: {Path(LOG_FILE_PATH).parent}, New: {current_log_dir_path.resolve()}")
            force_reinit = True

    try:
        current_log_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _bootstrap_logger.error(f"Failed to create log directory {current_log_dir_path}: {e}", exc_info=True)
        return None

    utc_now = datetime.now(timezone.utc)
    timestamp_fn_str = utc_now.strftime("%Y-%m-%dT%H%M%SZ")
    log_fn = f"{timestamp_fn_str}_system_build_log.txt"
    current_log_file_full_path = current_log_dir_path / log_fn

    try:
        file_fmt_str = '%(asctime)s (Taipei: %(taipei_time_str)s) [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
        file_formatter = TaipeiTimeFormatter(file_fmt_str)
        file_handler = logging.FileHandler(current_log_file_full_path, mode='w', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)

        console_fmt_str = '[%(levelname)s] %(name)s: %(message)s'
        console_formatter = logging.Formatter(console_fmt_str)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        root_logger = logging.getLogger()

        if force_reinit and root_logger.hasHandlers():
            _bootstrap_logger.info("Forcing re-initialization of root logger handlers.")
            for handler_to_remove in root_logger.handlers[:]:
                root_logger.removeHandler(handler_to_remove)
                handler_to_remove.close()

        if not root_logger.handlers or force_reinit :
            root_logger.addHandler(file_handler)
            root_logger.addHandler(console_handler)
            root_logger.setLevel(logging.DEBUG)

            _global_logger_initialized_flag = True
            LOG_FILE_PATH = str(current_log_file_full_path)

            logging.getLogger("GlobalLogSetup").info(f"Global log file initialized at {LOG_FILE_PATH}.")
        else:
            _bootstrap_logger.info("Root logger already has handlers and not forcing re-init. Skipping handler setup.")
            if LOG_FILE_PATH is None:
                 LOG_FILE_PATH = str(current_log_file_full_path)
                 _bootstrap_logger.warning(f"LOG_FILE_PATH was None. Reset to: {LOG_FILE_PATH}")

    except Exception as e:
        _bootstrap_logger.error(f"Failed to configure logging to {current_log_file_full_path}: {e}", exc_info=True)
        LOG_FILE_PATH = None
        return None

    return LOG_FILE_PATH


def log_message(message: str, level: str = "INFO", logger_name: Optional[str] = None, exc_info: bool = False, **kwargs):
    if not _global_logger_initialized_flag or LOG_FILE_PATH is None:
        effective_logger = _bootstrap_logger
        if not hasattr(log_message, "_bootstrap_warned_general_use"): # Log general warning only once
            effective_logger.warning(f"Global logger not fully initialized (LOG_FILE_PATH: {LOG_FILE_PATH}). Message: '{message}' logged with bootstrap_logger.")
            setattr(log_message, "_bootstrap_warned_general_use", True)
    else:
        effective_logger = logging.getLogger(logger_name if logger_name else "project_logger.general")

    level_upper = level.upper()
    log_level_int = logging.getLevelName(level_upper)

    log_method = getattr(effective_logger, level_upper.lower(), effective_logger.info)

    # Pass exc_info if it's an error level and exc_info is True
    pass_exc_info = exc_info and (isinstance(log_level_int, int) and log_level_int >= logging.ERROR)

    log_method(message, exc_info=pass_exc_info, extra=kwargs if kwargs else None)


if __name__ == "__main__":
    # When this script is run directly, it should set up its own project root
    main_script_project_root = Path(__file__).resolve().parent.parent
    log_file = initialize_log_file(force_reinit=True, project_root_path=main_script_project_root)

    if log_file:
        log_message("Info message from __main__ of initialize_global_log.", "INFO", logger_name="TestGlobalLog")
        log_message("Warning message from __main__.", "WARNING", logger_name="TestGlobalLog")
        log_message("Debug message (should go to file only).", "DEBUG", logger_name="TestGlobalLog.Debug")
        try:
            raise ValueError("This is a test error for logging with exc_info.")
        except ValueError as e:
            log_message("An error occurred during test.", "ERROR", logger_name="TestGlobalLog.Error", exc_info=True)

        log_message(f"Global log file is confirmed at: {LOG_FILE_PATH}", "INFO", logger_name="TestGlobalLog")
        print(f"Script finished. Log file should be at {LOG_FILE_PATH}")
    else:
        # If log_file is None, it means initialization failed. _bootstrap_logger should have logged the error.
        print("Failed to initialize log file in __main__ of initialize_global_log. Check bootstrap logs if any.")

# **對草案的增強和調整摘要（V3 更新）：**
# *   **`PROJECT_ROOT_DIR` 的確定：** 在 `initialize_log_file` 函數內部計算 `project_root`，並允許通過參數傳入，增加了靈活性。
# *   **`TaipeiTimeFormatter` 中的屬性名：** 確保 Formatter 中使用的屬性名 (`taipei_time_str`) 與格式化字符串中的占位符 `%(taipei_time_str)s` 一致。
# *   **`log_message` 中 `exc_info` 的處理：** 確保 `exc_info=True` 僅在日誌級別為 ERROR 或 CRITICAL 時實際傳遞給底層日誌方法，以避免在 INFO/DEBUG 級別記錄不必要的堆疊追蹤。
# *   **`_bootstrap_logger` 的使用：** 調整了 `log_message` 回退到 `_bootstrap_logger` 時的警告邏輯，使其只在第一次回退時發出一次通用警告。
# *   **`if __name__ == "__main__":` 塊：** 在調用 `initialize_log_file` 時傳遞了計算出的 `project_root_path`。增加了對 `exc_info=True` 的測試。
#
# 這個版本的 `initialize_global_log.py` 在路徑處理、日誌格式化和錯誤情況下的回退邏輯方面更加精細。
