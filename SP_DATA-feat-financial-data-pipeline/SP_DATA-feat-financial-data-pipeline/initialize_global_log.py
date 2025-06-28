import logging
from datetime import datetime, timezone, timedelta
import os

LOG_DIR = "api_test_logs"
LOG_FILE_PATH = None # Will be set after filename generation

def get_taipei_time() -> datetime:
    """Gets the current time in Taipei (UTC+8)."""
    return datetime.now(timezone.utc) + timedelta(hours=8)

def initialize_log_file() -> str:
    """
    Initializes the global log file with a Taipei ISO timestamp name.
    Returns the path to the log file.
    """
    global LOG_FILE_PATH
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        print(f"Created directory: {LOG_DIR}")

    taipei_now = get_taipei_time()
    # ISO 8601 format, but make it filename-friendly (replace colons)
    # Also, ensure timezone info is clearly part of it or use UTC for filename and log content for tz.
    # For simplicity, let's use a format like YYYY-MM-DDTHH-MM-SSZ+0800
    # A slightly cleaner way for filenames:
    timestamp_str = taipei_now.strftime("%Y-%m-%dT%H%M%S%z") # This will include +0800
    # Ensure the + is not problematic in some OS, though usually fine.
    # Or, for more universal compatibility, make it all UTC for filename and specify TZ in log.
    # Let's stick to Taipei time in filename for clarity as requested.

    log_filename = f"{timestamp_str}_system_build_log.txt"
    LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

    # Configure basic logging to file
    # Using a basicConfig here for simplicity. For more complex scenarios,
    # one might set up a logger instance and add handlers.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s (Taipei: %(taipei_time)s) [%(levelname)s] - %(message)s',
        filename=LOG_FILE_PATH,
        filemode='w' # Overwrite if exists, for a fresh log per run
    )

    # Add a custom adapter to inject Taipei time easily into log messages
    class TaipeiTimeAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            kwargs['extra'] = kwargs.get('extra', {})
            kwargs['extra']['taipei_time'] = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z%z')
            return msg, kwargs

    # Get the root logger and wrap it with the adapter for future log calls
    # Note: This approach with basicConfig and then adapting root might be tricky.
    # A more robust way is to get a specific logger instance, adapt it, and use that.
    # For now, let's assume direct logging calls will pick up the format.
    # We will define a specific log_message function.

    initial_message = f"Global log file initialized at {LOG_FILE_PATH} (Taipei Time in filename)."
    print(initial_message) # Also print to console
    # Provide 'taipei_time' for this specific log record
    logging.info(initial_message, extra={'taipei_time': taipei_now.strftime('%Y-%m-%d %H:%M:%S %Z%z')})

    log_message("System build process started.", "INFO")
    return LOG_FILE_PATH

def log_message(message: str, level: str = "INFO"):
    """
    Appends a message to the global log file with Taipei time.
    Levels: INFO, WARNING, ERROR, DEBUG
    """
    if LOG_FILE_PATH is None:
        print("ERROR: Log file not initialized. Call initialize_log_file() first.")
        return

    logger_instance = logging.getLogger("SystemBuild") # Use a named logger

    # Ensure this logger has a handler and formatter if basicConfig didn't cover it well enough
    # or if we want a separate configuration for this logger.
    # For this script, basicConfig on root should be fine.

    # Create a record with Taipei time for the custom formatter
    extra_info = {'taipei_time': get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z%z')}

    if level.upper() == "INFO":
        logger_instance.info(message, extra=extra_info)
    elif level.upper() == "WARNING":
        logger_instance.warning(message, extra=extra_info)
    elif level.upper() == "ERROR":
        logger_instance.error(message, extra=extra_info)
    elif level.upper() == "DEBUG":
        logger_instance.debug(message, extra=extra_info)
    else:
        logger_instance.info(f"({level}) {message}", extra=extra_info)

    # Also print to console for real-time feedback
    print(f"{get_taipei_time().strftime('%Y-%m-%d %H:%M:%S')} [{level.upper()}] - {message}")


if __name__ == "__main__":
    log_file = initialize_log_file()
    if log_file:
        log_message("This is a test info message.", "INFO")
        log_message("This is a test warning message.", "WARNING")

        # Add accumulated log messages about API key cleaning
        log_message("Cleaned API Key from fred_api_test.py. Replaced FRED_API_KEY with placeholder.", "INFO")
        log_message("Cleaned API Key from alpha_vantage_api_test.py. Replaced ALPHA_VANTAGE_API_KEY with placeholder.", "INFO")
        log_message("Cleaned API Key from finnhub_api_test.py. Replaced FINNHUB_API_KEY with placeholder.", "INFO")
        log_message("Cleaned API Key from polygon_api_test.py. Replaced POLYGON_API_KEY with placeholder.", "INFO")
        log_message("Cleaned API Key from fmp_api_test.py. Replaced FMP_API_KEY with placeholder.", "INFO")
        log_message("Cleaned API Key from finlab_api_test.py. Replaced FINLAB_API_KEY with placeholder.", "INFO")
        log_message("Cleaned API Token from finmind_real_api_test.py. Replaced FINMIND_API_TOKEN with placeholder.", "INFO")

        log_message(f"Log file is at: {log_file}", "INFO")

        # Example of how other modules/scripts would use log_message
        # (assuming log_message is made accessible, e.g. by importing this script)
        # from initialize_global_log import log_message
        # log_message("Another part of the system reporting.", "DEBUG")
    else:
        print("Failed to initialize log file.")
