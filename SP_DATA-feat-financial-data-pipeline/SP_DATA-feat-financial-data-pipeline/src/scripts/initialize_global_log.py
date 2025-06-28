import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import sys
from typing import Optional, Any # Added Any for potential use in kwargs

LOG_DIR_NAME = "api_test_logs" # Default log directory name within the project root
LOG_FILE_PATH: Optional[str] = None
_global_logger_initialized_flag = False # Flag to check if initialize_log_file has run successfully

# Bootstrap logger for issues during the logging setup itself or before it's fully set up.
_bootstrap_logger = logging.getLogger("BootstrapLogger")
if not _bootstrap_logger.handlers and not logging.getLogger().hasHandlers(): # Avoid adding handlers multiple times
    _ch_bootstrap = logging.StreamHandler(sys.stdout)
    _ch_bootstrap.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s (bootstrap)'))
    _bootstrap_logger.addHandler(_ch_bootstrap)
    _bootstrap_logger.setLevel(logging.INFO) # Bootstrap logs info and above to console
    _bootstrap_logger.propagate = False # Don't pass bootstrap messages to the root logger if it gets configured later

def get_taipei_time() -> datetime:
    """Returns the current time in Taipei timezone (UTC+8)."""
    return datetime.now(timezone.utc) + timedelta(hours=8)

class TaipeiTimeFormatter(logging.Formatter):
    """Custom formatter to add Taipei time to log records."""
    def format(self, record: logging.LogRecord) -> str:
        record.taipei_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        return super().format(record)

def initialize_log_file(
    log_dir_override: Optional[str] = None,
    force_reinit: bool = False,
    project_root_path: Optional[Path] = None
) -> Optional[str]:
    """
    Initializes a global file logger and a console logger for the application.
    The file logger will save all DEBUG level messages and above.
    The console logger will show INFO level messages and above.
    Logs are stored in a timestamped file within LOG_DIR_NAME (default 'api_test_logs').

    Args:
        log_dir_override: Optional path to a directory where logs should be stored.
                          If None, defaults to 'api_test_logs' under project_root_path.
        force_reinit: If True, removes existing handlers from the root logger and re-adds them.
                      Useful if settings need to change or in test environments.
        project_root_path: Optional Path object to the project's root directory.
                           If None, attempts to infer it (e.g., parent of this script's dir).

    Returns:
        The full path to the initialized log file if successful, otherwise None.
    """
    global LOG_FILE_PATH, _global_logger_initialized_flag

    # Determine the project root path if not provided
    current_project_root: Path
    if project_root_path:
        current_project_root = project_root_path
    else:
        try:
            # Assumes this script is in a 'scripts' subdirectory of the project root
            current_project_root = Path(__file__).resolve().parent.parent
        except NameError: # __file__ might not be defined in some execution contexts (e.g. interactive)
            current_project_root = Path(".").resolve() # Fallback to current working directory
            _bootstrap_logger.warning(f"__file__ not defined, using CWD '{current_project_root}' as project root for log path determination.")

    # Determine the log directory path
    current_log_dir_path: Path
    if log_dir_override:
        current_log_dir_path = Path(log_dir_override)
    else:
        current_log_dir_path = current_project_root / LOG_DIR_NAME

    # Check if logger is already initialized with the same log directory
    if _global_logger_initialized_flag and not force_reinit and LOG_FILE_PATH:
        # Check if the existing log file's parent directory matches the current target log directory
        if Path(LOG_FILE_PATH).parent == current_log_dir_path.resolve():
            _bootstrap_logger.debug(f"Global logger already initialized. Log file: {LOG_FILE_PATH}")
            return LOG_FILE_PATH
        else:
            _bootstrap_logger.warning(
                f"Log directory has changed or re-initialization is forced. "
                f"Old log dir: {Path(LOG_FILE_PATH).parent}, New log dir: {current_log_dir_path.resolve()}. Forcing re-init."
            )
            force_reinit = True # Force re-init if log directory changed

    try:
        current_log_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _bootstrap_logger.error(f"Failed to create log directory '{current_log_dir_path}': {e}", exc_info=True)
        return None

    # Create a timestamped log file name
    utc_now = datetime.now(timezone.utc)
    timestamp_filename_str = utc_now.strftime("%Y-%m-%dT%H%M%SZ") # ISO-like timestamp for filename
    log_filename = f"{timestamp_filename_str}_application_log.txt" # More descriptive name
    current_log_file_full_path = current_log_dir_path / log_filename

    try:
        # File Handler Setup (DEBUG and above)
        file_log_format_str = '%(asctime)s (Taipei: %(taipei_time_str)s) [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
        file_formatter = TaipeiTimeFormatter(file_log_format_str) # Use custom formatter for Taipei time
        file_handler = logging.FileHandler(current_log_file_full_path, mode='w', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)

        # Console Handler Setup (INFO and above)
        console_log_format_str = '[%(levelname)s] %(name)s: %(message)s' # Simpler format for console
        console_formatter = logging.Formatter(console_log_format_str)
        console_handler = logging.StreamHandler(sys.stdout) # Log to standard output
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        # Get the root logger
        root_logger = logging.getLogger()

        # If forcing re-initialization, remove existing handlers from the root logger
        if force_reinit and root_logger.hasHandlers():
            _bootstrap_logger.info("Forcing re-initialization of root logger handlers.")
            for handler_to_remove in root_logger.handlers[:]: # Iterate over a copy
                root_logger.removeHandler(handler_to_remove)
                handler_to_remove.close() # Close handler before removing

        # Add new handlers if no handlers exist or if re-init is forced
        if not root_logger.handlers or force_reinit:
            root_logger.addHandler(file_handler)
            root_logger.addHandler(console_handler)
            root_logger.setLevel(logging.DEBUG) # Root logger captures all messages from DEBUG up

            _global_logger_initialized_flag = True
            LOG_FILE_PATH = str(current_log_file_full_path)

            # Use a specific logger for setup messages to avoid confusion with bootstrap
            logging.getLogger("GlobalLogSetup").info(f"Global logger initialized. Log file: {LOG_FILE_PATH}")
        else:
            # This case should ideally be caught by the check at the beginning of the function
            _bootstrap_logger.info("Root logger already has handlers and not forcing re-init. Current setup maintained.")
            if LOG_FILE_PATH is None: # If somehow flag was true but path was not set
                 LOG_FILE_PATH = str(current_log_file_full_path) # Attempt to set it
                 _bootstrap_logger.warning(f"LOG_FILE_PATH was None but logger seemed initialized. Set to: {LOG_FILE_PATH}")

    except Exception as e:
        _bootstrap_logger.error(f"Failed to configure logging to file '{current_log_file_full_path}': {e}", exc_info=True)
        LOG_FILE_PATH = None # Ensure path is None on failure
        _global_logger_initialized_flag = False # Reset flag on failure
        return None

    return LOG_FILE_PATH

def log_message(
    message: str,
    level: str = "INFO",
    logger_name: Optional[str] = None,
    exc_info: bool = False,
    **kwargs: Any
):
    """
    Logs a message using the globally configured logger or a bootstrap logger if not initialized.

    Args:
        message: The message string to log.
        level: The logging level (e.g., "INFO", "WARNING", "ERROR", "DEBUG").
        logger_name: Optional name for the logger. Defaults to 'project_logger.general'.
        exc_info: If True and level is ERROR/CRITICAL, exception info is added to the log.
        **kwargs: Additional keyword arguments to pass as 'extra' to the logger.
    """
    effective_logger: logging.Logger
    if not _global_logger_initialized_flag or LOG_FILE_PATH is None:
        effective_logger = _bootstrap_logger
        # Log a warning about using bootstrap only once per session for general use
        if not hasattr(log_message, "_bootstrap_warning_issued_for_general_use"):
            effective_logger.warning(
                f"Global logger not fully initialized (Log file path: {LOG_FILE_PATH}). "
                f"Logging message ('{message[:50]}...') with bootstrap logger as fallback."
            )
            setattr(log_message, "_bootstrap_warning_issued_for_general_use", True)
    else:
        effective_logger = logging.getLogger(logger_name if logger_name else "project_logger.general")

    level_upper = level.upper()
    log_level_int = logging.getLevelName(level_upper) # Get integer value of log level

    # Determine the appropriate log method (e.g., logger.info, logger.error)
    log_method = getattr(effective_logger, level_upper.lower(), effective_logger.info) # Default to .info if level invalid

    # Only pass exc_info=True if the log level is ERROR or CRITICAL
    should_pass_exc_info = exc_info and (isinstance(log_level_int, int) and log_level_int >= logging.ERROR)

    try:
        log_method(message, exc_info=should_pass_exc_info, extra=kwargs if kwargs else None)
    except Exception as e:
        # Fallback to bootstrap if the chosen logger fails for some reason
        _bootstrap_logger.error(f"Failed to log message with '{effective_logger.name}'. Original message: '{message}'. Error: {e}", exc_info=True)

# Example usage when this script is run directly
if __name__ == "__main__":
    # When this script is run directly, it should attempt to set up its own project root
    # assuming it's in a 'scripts' folder under the main project directory.
    main_script_project_root_path = Path(__file__).resolve().parent.parent

    log_file_path_main = initialize_log_file(force_reinit=True, project_root_path=main_script_project_root_path)

    if log_file_path_main:
        log_message("Info message from __main__ of initialize_global_log.", "INFO", logger_name="TestInitializeGlobalLog")
        log_message("Warning message from __main__.", "WARNING", logger_name="TestInitializeGlobalLog")
        log_message("Debug message (should go to file, not console by default).", "DEBUG", logger_name="TestInitializeGlobalLog.DebugSub")

        try:
            x = 1 / 0
        except ZeroDivisionError as e:
            # Test logging with exception information
            log_message("A ZeroDivisionError occurred during test.", "ERROR", logger_name="TestInitializeGlobalLog.ErrorSub", exc_info=True)

        log_message(f"Global log file for this direct run is confirmed at: {LOG_FILE_PATH}", "CRITICAL", logger_name="TestInitializeGlobalLog.CriticalSub")
        print(f"Script execution finished. Log file should be at: {LOG_FILE_PATH}")
    else:
        # This means initialize_log_file returned None, indicating a setup failure.
        # _bootstrap_logger should have logged the specific error.
        print("Failed to initialize the log file in __main__ of initialize_global_log. Check console for bootstrap logger errors.")
