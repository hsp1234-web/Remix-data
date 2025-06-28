print("diag_logic.py: [STATUS] Script execution started. Pre-import phase.", flush=True)
import sys
import os
print(f"diag_logic.py: [INFO] Initial sys.path: {sys.path}", flush=True)
print(f"diag_logic.py: [INFO] Current working directory: {os.getcwd()}", flush=True)

# --- 延遲導入 logging 和 Path 直到基本 print 可用 ---
try:
    import logging
    from pathlib import Path
    print("diag_logic.py: [SUCCESS] Imported logging, pathlib.", flush=True)
except ImportError as e_bootstrap:
    print(f"diag_logic.py: [CRITICAL_BOOTSTRAP_FAILURE] Failed to import logging or pathlib: {e_bootstrap}", flush=True)
    # 在此關鍵階段失敗，直接退出可能更清晰，因為後續日誌無法工作
    sys.exit(1)


# --- 路徑設定，確保能導入 src 下的模組 ---
try:
    CURRENT_SCRIPT_PATH = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_SCRIPT_PATH.parent.parent
    SOURCE_ROOT_SRC = PROJECT_ROOT / "src"
    print(f"diag_logic.py: [INFO] PROJECT_ROOT determined as: {PROJECT_ROOT}", flush=True)
    print(f"diag_logic.py: [INFO] SOURCE_ROOT_SRC determined as: {SOURCE_ROOT_SRC}", flush=True)
except NameError:
    PROJECT_ROOT = Path(".").resolve()
    SOURCE_ROOT_SRC = PROJECT_ROOT / "src"
    print(f"diag_logic.py: [WARNING] __file__ not defined. Assuming PROJECT_ROOT: {PROJECT_ROOT}", flush=True)

# 將 src 和 project_root 添加到 sys.path 的操作移到更前面，
# 確保在嘗試導入任何 connectors 子模組或 yaml 等之前，路徑已設定好。
# 這一調整是為了應對 ModuleNotFoundError: No module named 'connectors.finlab_connector'
# 這類問題，確保 Python 解釋器能找到 src 目錄。
# 同時，也確保在 ensure_package_installed 之前，sys.path 是正確的。
if str(SOURCE_ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT_SRC))
    print(f"diag_logic.py: [INFO] Inserted {SOURCE_ROOT_SRC} into sys.path.", flush=True)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    print(f"diag_logic.py: [INFO] Inserted {PROJECT_ROOT} into sys.path.", flush=True)
print(f"diag_logic.py: [INFO] sys.path after path modifications: {sys.path}", flush=True)


# --- 日誌設定 (現在可以安全使用 logging) ---
LOG_DIR_BASE = PROJECT_ROOT / "logs"
LOG_DIR_BASE.mkdir(exist_ok=True)
DIAGNOSIS_LOG_FILE_BASE = LOG_DIR_BASE / "nyfed_diagnosis_detail.log"

if DIAGNOSIS_LOG_FILE_BASE.exists():
    try:
        DIAGNOSIS_LOG_FILE_BASE.unlink()
        print(f"diag_logic.py: [INFO] Removed old detailed log file: {DIAGNOSIS_LOG_FILE_BASE}", flush=True)
    except OSError as e_rm_log:
        print(f"diag_logic.py: [WARNING] Could not remove old detailed log file {DIAGNOSIS_LOG_FILE_BASE}: {e_rm_log}", flush=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DIAGNOSIS_LOG_FILE_BASE, mode='w', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)
logger.info("Logger initialized. Previous print statements were pre-logging.")
print("diag_logic.py: [STATUS] Logger initialized.", flush=True)


# --- 動態依賴注入邏輯 (黑盒子模式：只記錄，不修理) ---
# Optional import for subprocess, only if needed
subprocess_module = None
try:
    import subprocess
    subprocess_module = subprocess
    print("diag_logic.py: [INFO] subprocess module imported successfully for potential (disarmed) pip calls.", flush=True)
except ImportError:
    print("diag_logic.py: [WARNING] subprocess module not found. Dynamic pip install (if re-enabled) would fail.", flush=True)


def ensure_package_installed(package_name: str, import_name: Optional[str] = None):
    module_to_import = import_name if import_name else package_name
    try:
        print(f"diag_logic.py: [ATTEMPT] Trying to import '{module_to_import}' (for package '{package_name}')...", flush=True)
        logger.debug(f"Attempting to import '{module_to_import}' for package '{package_name}'.")
        __import__(module_to_import)
        print(f"diag_logic.py: [SUCCESS] Module '{module_to_import}' is already available.", flush=True)
        logger.info(f"Package '{module_to_import}' (for {package_name}) is already available.")
        return True
    except ImportError:
        print(f"diag_logic.py: [FAILURE] Module '{module_to_import}' not found. Black Box event triggered for package '{package_name}'.", flush=True)
        print(f"diag_logic.py: [CRITICAL_DEPENDENCY] Dependency '{package_name}' is MISSING. Installation is currently DISARMED.", flush=True)
        logger.critical(f"Missing dependency: Package '{package_name}' (tries to import '{module_to_import}') is not installed. Auto-install disabled for diagnosis.")
        return False

REQUIRED_PACKAGES = [
    ("requests", "requests"),
    ("PyYAML", "yaml"),
    ("pandas", "pandas"),
    ("openpyxl", "openpyxl"),
    ("beautifulsoup4", "bs4")
]

print("diag_logic.py: [STATUS] Starting dependency checks (Black Box mode)...", flush=True)
logger.info("Starting dependency check (Black Box mode: will report missing, not install).")
all_deps_met = True
for pkg_name, imp_name in REQUIRED_PACKAGES:
    if not ensure_package_installed(pkg_name, imp_name):
        all_deps_met = False
        # Per instruction, not exiting on first failure to see all missing dependencies.

if not all_deps_met:
    logger.critical("One or more CRITICAL dependencies are missing. Script cannot proceed to NYFedConnector diagnosis.")
    print("diag_logic.py: [ABORT] Critical dependencies missing. Aborting NYFed diagnosis phase.", flush=True)
    sys.exit(1)
logger.info("All required packages for diagnosis logic are reported as available. Proceeding with NYFedConnector import.")
print("diag_logic.py: [STATUS] Dependency checks completed. All reported as available.", flush=True)

# --- 延遲導入 NYFedConnector ---
try:
    print("diag_logic.py: [ATTEMPT] Trying to import 'connectors.nyfed_connector.NYFedConnector'...", flush=True)
    # Make sure src/connectors/__init__.py is correctly isolating NYFedConnector
    from connectors.nyfed_connector import NYFedConnector
    print("diag_logic.py: [SUCCESS] Imported 'connectors.nyfed_connector.NYFedConnector'.", flush=True)

    print("diag_logic.py: [ATTEMPT] Trying to import 'yaml' and 'pandas' again for main logic...", flush=True)
    import yaml
    import pandas as pd
    print("diag_logic.py: [SUCCESS] Imported 'yaml' and 'pandas' for main logic.", flush=True)
except ImportError as e_main_imp:
    logger.critical(f"Failed to import NYFedConnector or other core modules AFTER dependency check: {e_main_imp}.", exc_info=True)
    print(f"diag_logic.py: [CRITICAL_IMPORT_FAILURE] Failed to import modules for main logic: {e_main_imp}. This should not happen if dependency check passed.", flush=True)
    sys.exit(1)
except Exception as e_main_imp_general:
    logger.critical(f"Unknown error during main module imports AFTER dependency check: {e_main_imp_general}", exc_info=True)
    print(f"diag_logic.py: [CRITICAL_UNKNOWN_IMPORT_FAILURE] {e_main_imp_general}", flush=True)
    sys.exit(1)

# --- 診斷邏輯 ---
def run_nyfed_diagnosis():
    print("diag_logic.py: [STATUS] Entered run_nyfed_diagnosis function.", flush=True)
    logger.info("diag_logic.py: [STATUS] Entered run_nyfed_diagnosis function.")
    config_path = PROJECT_ROOT / "config.yaml"
    logger.info(f"diag_logic.py: [INFO] Attempting to load config from: {config_path}")
    print(f"diag_logic.py: [INFO] Attempting to load config from: {config_path}", flush=True)
    if not config_path.exists():
        logger.warning(f"Primary config file {config_path} not found. Attempting to use template.")
        print(f"diag_logic.py: [WARNING] Primary config file {config_path} not found. Attempting template.", flush=True)
        config_path = PROJECT_ROOT / "config.yaml.template"
        if not config_path.exists():
            logger.error(f"Fallback config template {config_path} also not found. Cannot proceed.")
            print(f"diag_logic.py: [FAILURE] Config file and template not found. Aborting diagnosis function.", flush=True)
            return

    full_config = None
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = yaml.safe_load(f)
        if not full_config or not isinstance(full_config, dict):
            logger.error(f"Config file {config_path} is empty or not a valid YAML dictionary.")
            print(f"diag_logic.py: [FAILURE] Config file {config_path} parsing error or empty. Aborting.", flush=True)
            return
        logger.info(f"Config file {config_path} loaded successfully.")
        print(f"diag_logic.py: [SUCCESS] Config file {config_path} loaded.", flush=True)
    except Exception as e_conf_load:
        logger.error(f"Error loading or parsing config file {config_path}: {e_conf_load}", exc_info=True)
        print(f"diag_logic.py: [FAILURE] Exception while loading config {config_path}: {e_conf_load}. Aborting.", flush=True)
        return

    nyfed_api_config = None
    nyfed_settings_in_apis = full_config.get("apis", {}).get("nyfed", {})
    if "download_configs" in nyfed_settings_in_apis and "parser_recipes" in nyfed_settings_in_apis:
        logger.info("Found 'download_configs' and 'parser_recipes' in apis.nyfed.")
        nyfed_api_config = nyfed_settings_in_apis
    elif "download_configs" in full_config and "parser_recipes" in full_config:
        logger.info("Found 'download_configs' and 'parser_recipes' at root of config (using as fallback for NYFed).")
        nyfed_api_config = {
            "requests_per_minute": nyfed_settings_in_apis.get("requests_per_minute", 30),
            "download_configs": full_config.get("download_configs"),
            "parser_recipes": full_config.get("parser_recipes"),
            "requests_config": nyfed_settings_in_apis.get("requests_config", full_config.get("requests_config", {}))
        }
    else:
        logger.error("Crucial 'download_configs' or 'parser_recipes' not found for NYFed in config. Cannot initialize NYFedConnector properly.")
        print("diag_logic.py: [FAILURE] NYFed 'download_configs' or 'parser_recipes' missing in config. Aborting.", flush=True)
        return

    logger.debug(f"NYFed API config to be used: {nyfed_api_config}")
    print(f"diag_logic.py: [INFO] NYFed API config prepared for connector.", flush=True)

    try:
        logger.info("diag_logic.py: [ATTEMPT] Initializing NYFedConnector...")
        print("diag_logic.py: [ATTEMPT] Initializing NYFedConnector...", flush=True)
        connector = NYFedConnector(api_config=nyfed_api_config)
        logger.info("diag_logic.py: [SUCCESS] NYFedConnector initialized.")
        print("diag_logic.py: [SUCCESS] NYFedConnector initialized.", flush=True)
    except Exception as e_conn_init:
        logger.error(f"Failed to initialize NYFedConnector: {e_conn_init}", exc_info=True)
        print(f"diag_logic.py: [FAILURE] NYFedConnector initialization failed: {e_conn_init}. Aborting.", flush=True)
        return

    if not hasattr(connector, 'download_configs') or not connector.download_configs:
        logger.warning("NYFedConnector instance has no 'download_configs' or it's empty. Diagnosis cannot proceed with downloads.")
        print("diag_logic.py: [WARNING] NYFedConnector has no download_configs. Check config structure. Aborting diagnosis function.", flush=True)
        return

    logger.info(f"diag_logic.py: [INFO] NYFedConnector has {len(connector.download_configs)} download_configs items.")
    print(f"diag_logic.py: [INFO] NYFedConnector has {len(connector.download_configs)} download_configs items.", flush=True)

    try:
        logger.info("diag_logic.py: [ATTEMPT] Calling NYFedConnector.get_configured_data()...")
        print("diag_logic.py: [ATTEMPT] Calling NYFedConnector.get_configured_data()...", flush=True)
        result_df = connector.get_configured_data()

        if result_df is None:
             logger.error("diag_logic.py: [FAILURE] NYFedConnector.get_configured_data() returned None. Expected DataFrame.")
             print("diag_logic.py: [CRITICAL_CONNECTOR_FAILURE] NYFedConnector.get_configured_data() returned None.", flush=True)
        elif result_df.empty:
            logger.warning("diag_logic.py: [INFO_EMPTY] NYFedConnector.get_configured_data() returned an EMPTY DataFrame.")
            print("diag_logic.py: [INFO_EMPTY] NYFedConnector.get_configured_data() returned an EMPTY DataFrame. This might be due to no data, download errors, or parsing errors. Check detailed logs.", flush=True)
        else:
            logger.info(f"diag_logic.py: [SUCCESS] NYFedConnector.get_configured_data() returned a DataFrame with {len(result_df)} rows.")
            logger.info(f"diag_logic.py: [DATA_SAMPLE] First 5 rows of NYFed data:\n{result_df.head().to_string()}")
            print(f"diag_logic.py: [SUCCESS] NYFedConnector.get_configured_data() returned {len(result_df)} rows.", flush=True)

    except Exception as e_get_data:
        logger.error(f"diag_logic.py: [FAILURE] Error during NYFedConnector.get_configured_data(): {e_get_data}", exc_info=True)
        print(f"diag_logic.py: [CRITICAL_CONNECTOR_FAILURE] Exception during get_configured_data(): {e_get_data}", flush=True)

    logger.info("diag_logic.py: [STATUS] NYFedConnector diagnosis process finished.")
    print("diag_logic.py: [STATUS] Script execution completed. Check logs for details.", flush=True)

if __name__ == "__main__":
    print("diag_logic.py: [STATUS] __main__ block started.", flush=True)
    # 日誌初始化已移至更前面
    # logger.info("__main__ block started. Running NYFed diagnosis.") # 這條日誌會在 logger 初始化後才出現
    run_nyfed_diagnosis()
    # logger.info("__main__ block finished.") # 這條日誌會在 logger 初始化後才出現
    print("diag_logic.py: [STATUS] __main__ block finished.", flush=True)
