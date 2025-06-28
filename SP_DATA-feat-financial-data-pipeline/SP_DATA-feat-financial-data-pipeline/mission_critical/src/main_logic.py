print("main_logic.py: [STATUS] Script started.", flush=True)
import sys
import os
from datetime import datetime # For current year in config

# --- Path setup for mission_critical structure ---
# When this script (main_logic.py) is run from $MISSION_DIR,
# and nyfed_connector.py is in $MISSION_DIR/src/connectors/,
# we need to add $MISSION_DIR/src to sys.path.
# The 'cd "$MISSION_DIR"' in the bash script handles the CWD.
# So, 'src' should be directly accessible.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # Add $MISSION_DIR to path for 'src'
print(f"main_logic.py: [INFO] Current sys.path: {sys.path}", flush=True)
print(f"main_logic.py: [INFO] Current working directory: {os.getcwd()}", flush=True)


# --- Dynamic Dependency Check ---
# Minimal logging for this critical phase
def _ensure_pkg(pkg_name, imp_name=None):
    module_to_import = imp_name if imp_name else pkg_name
    try:
        print(f"main_logic.py: [DEP_ATTEMPT] Importing '{module_to_import}' (for {pkg_name})...", flush=True)
        __import__(module_to_import)
        print(f"main_logic.py: [DEP_SUCCESS] '{module_to_import}' is available.", flush=True)
        return True
    except ImportError:
        print(f"main_logic.py: [DEP_FAILURE] Critical module '{module_to_import}' (for {pkg_name}) NOT FOUND.", flush=True)
        return False

# These are the absolute minimum for NYFedConnector and this script
# PyYAML is not needed if config is hardcoded as per plan
# Pandas, openpyxl, beautifulsoup4 are needed by NYFedConnector
# requests is needed by NYFedConnector
core_deps = [
    ("requests", "requests"),
    ("pandas", "pandas"),
    ("openpyxl", "openpyxl"),
    ("beautifulsoup4", "bs4"),
    ("yaml", "yaml") # For PyYAML, if loading config from file
]
all_core_deps_ok = True
for pkg, imp in core_deps:
    if not _ensure_pkg(pkg, imp):
        all_core_deps_ok = False

if not all_core_deps_ok:
    print("main_logic.py: [CRITICAL_ABORT] Essential dependencies missing after pip install. Aborting.", flush=True)
    sys.exit(1)
print("main_logic.py: [INFO] All core dependencies seem to be available.", flush=True)

# Now safe to import NYFedConnector and others
try:
    from src.connectors.nyfed_connector import NYFedConnector
    import yaml
    import pandas as pd
    import logging # Import for NYFedConnector's internal logger

    # Configure a basic logger for NYFedConnector if it uses one
    # This ensures its logger.info etc. calls don't fail.
    # It will print to stdout if StreamHandler is added.
    connector_logger = logging.getLogger("src.connectors.nyfed_connector")
    if not connector_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - NYFedConnector(Mission) [%(levelname)s] - %(message)s'))
        connector_logger.addHandler(handler)
        connector_logger.setLevel(logging.INFO) # Or DEBUG for more verbosity from connector

    print("main_logic.py: [SUCCESS] Successfully imported NYFedConnector, yaml, pandas.", flush=True)
except ImportError as e_imp:
    print(f"main_logic.py: [CRITICAL_IMPORT_POST_CHECK_FAIL] Failed to import after dep check: {e_imp}", flush=True)
    sys.exit(1)


# --- Minimal Config for NYFedConnector ---
# This config should match what your NYFedConnector expects.
# Using the AMBS URL as a test case, assuming it's a direct Excel link or a page.
# The actual URL from your config.yaml.template for 'ambs_operations_url' was complex.
# Let's use a simpler, known NYFed page that usually has Excel links for Primary Dealer stats.
# This is more robust for a "does it work at all" test.

# Using the Primary Dealer Statistics page, which should have links to Excel files.
# The actual file name changes weekly/monthly.
# We'll use a general pattern that should match one of the Excel files.
current_year_str = str(datetime.now().year)

# Minimal config for one specific, typically available NYFed data source (Primary Dealer Stats)
# This is an example, you might need to adjust based on the actual NYFed page and file naming.
# The goal is to test the download and parsing for *a* file.
minimal_nyfed_config = {
    "requests_per_minute": 10, # Be very conservative
    "download_configs": [
        {
            "name": "primary_dealer_stats_current",
            # This is a page where Excel files are listed
            "url_template": "https://www.newyorkfed.org/markets/primarydealer_statistics/financial_condition",
            # Pattern to find on the HTML page. Look for "Financial Condition Data" and ".xlsx"
            "file_pattern_on_page": f"Primary Dealer Financial Condition Data – {current_year_str}.xlsx",
            "parser_recipe_name": "primary_dealer_default_recipe",
            "metric_name_override": "NYFED_MISSION/PRIMARY_DEALER_TOTAL_ASSETS"
        }
    ],
    "parser_recipes": {
        "primary_dealer_default_recipe": {
            # These are common values for such reports, but might need adjustment
            # based on the actual current file format from NYFed.
            "header_row": 3,
            "date_column": "As of Date", # Common date column name
            "value_column": "Total assets", # Try to extract "Total assets"
            # "columns_to_sum": ["Net outright par positions U.S. Treasury coupons"], # Example if summing
            "data_unit_multiplier": 1000000, # Assuming millions
            "sheet_name": 0 # Try the first sheet
        }
    },
    "requests_config": {"max_retries": 1, "base_backoff_seconds": 1, "download_timeout": 60}
}
print(f"main_logic.py: [INFO] Using minimal_nyfed_config: {minimal_nyfed_config}", flush=True)


# --- Execute NYFedConnector ---
try:
    print("main_logic.py: [ATTEMPT] Initializing NYFedConnector with minimal config...", flush=True)
    connector = NYFedConnector(api_config=minimal_nyfed_config)
    print("main_logic.py: [SUCCESS] NYFedConnector initialized.", flush=True)

    print("main_logic.py: [ATTEMPT] Calling connector.get_configured_data()...", flush=True)
    data_df = connector.get_configured_data()

    if data_df is not None and not data_df.empty:
        print(f"main_logic.py: [SUCCESS] NYFedConnector.get_configured_data() returned {len(data_df)} rows.", flush=True)
        print("main_logic.py: [DATA_SAMPLE] First 3 rows:", flush=True)
        print(data_df.head(3).to_string(), flush=True)
    elif data_df is not None and data_df.empty:
        print("main_logic.py: [INFO_EMPTY] NYFedConnector.get_configured_data() returned an EMPTY DataFrame. This could be due to no data for the period, download/parsing issues, or the target file not found on the NYFed page. Check connector logs.", flush=True)
    else: # Should not happen if connector adheres to returning empty DF on failure
        print("main_logic.py: [WARNING] NYFedConnector.get_configured_data() returned None. This is unexpected.", flush=True)

except Exception as e:
    print(f"main_logic.py: [CRITICAL_FAILURE] An error occurred during NYFedConnector operation: {e}", flush=True)
    import traceback
    print("main_logic.py: [TRACEBACK]", flush=True)
    traceback.print_exc() # This will print to stderr, which bash script should capture

print("main_logic.py: [STATUS] Script finished.", flush=True)
