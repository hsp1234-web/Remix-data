#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define variables
SCRIPT_DIR=$(dirname "$0")
DATA_PIPELINE_DIR="$SCRIPT_DIR/data_pipeline"
DB_UTILS_SCRIPT="$DATA_PIPELINE_DIR/db_utils.py"
MARKET_DATA_FMP_SCRIPT="$DATA_PIPELINE_DIR/market_data_fmp.py"
DATA_PROCESSOR_SCRIPT="$DATA_PIPELINE_DIR/data_processor.py" # Will be used later
# AGGREGATOR_SCRIPT="$DATA_PIPELINE_DIR/aggregator.py" # Used by data_processor

DB_FILE="data_hub.duckdb" # Defined in db_utils.py, but good to have here for clarity or cleanup

# Default values
DEFAULT_SYMBOL="AAPL" # Default symbol for testing
DEFAULT_START_DATE=$(date -d "35 days ago" +%Y-%m-%d) # Approx last month + buffer
DEFAULT_END_DATE=$(date +%Y-%m-%d)

# Log function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Help function
usage() {
    echo "Usage: $0 --stage <stage_name> [--symbol <symbol>] [--start_date <YYYY-MM-DD>] [--end_date <YYYY-MM-DD>]"
    echo "Stages:"
    echo "  init_db        Initializes the database and core tables (if needed)."
    echo "  download_price Downloads daily price data for the symbol."
    echo "  download_profile Downloads company profile for the symbol."
    echo "  download_financials Downloads financial statements for the symbol."
    echo "  fundamentals   Downloads all fundamental data (profile, financials) and stores it."
    # echo "  process        Processes downloaded data (e.g., calculates indicators, resamples)." # Future stage
    # echo "  store          Stores processed data into the database." # Future stage
    echo "  full_price     Runs download_price for the symbol."
    echo "  full_fundamentals Runs fundamentals stage for the symbol."
    # echo "  full_pipeline  Runs all stages for the symbol (download, process, store)." # Future stage
    echo "Options:"
    echo "  --symbol       Stock symbol (default: $DEFAULT_SYMBOL)."
    echo "  --start_date   Start date for price data (default: $DEFAULT_START_DATE)."
    echo "  --end_date     End date for price data (default: $DEFAULT_END_DATE)."
    echo "  --help         Show this help message."
    exit 1
}

# Parse command-line arguments
STAGE=""
SYMBOL=$DEFAULT_SYMBOL
START_DATE=$DEFAULT_START_DATE
END_DATE=$DEFAULT_END_DATE

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --stage) STAGE="$2"; shift ;;
        --symbol) SYMBOL="$2"; shift ;;
        --start_date) START_DATE="$2"; shift ;;
        --end_date) END_DATE="$2"; shift ;;
        --help) usage ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

if [ -z "$STAGE" ]; then
    log "Error: --stage is a required argument."
    usage
fi

# Ensure Python scripts are executable or called with python
# Default Python command
PYTHON_CMD_BASE="python" # Or "python3" if specifically needed and in PATH

# Check for test API keys and prepend to Python command if they exist
PYTHON_CMD="$PYTHON_CMD_BASE"
ENV_PREFIX=""

if [ -n "$FMP_API_KEY_FOR_TEST" ]; then
    ENV_PREFIX="FMP_API_KEY='$FMP_API_KEY_FOR_TEST' "
    log "Using FMP_API_KEY_FOR_TEST for FMP calls."
fi

if [ -n "$FRED_API_KEY_FOR_TEST" ]; then
    ENV_PREFIX="${ENV_PREFIX}FRED_API_KEY='$FRED_API_KEY_FOR_TEST' "
    log "Using FRED_API_KEY_FOR_TEST for FRED calls (if any)."
fi

if [ -n "$ENV_PREFIX" ]; then
    PYTHON_CMD="env $ENV_PREFIX $PYTHON_CMD_BASE"
    log "Python command will be executed with prefixed environment variables for API keys."
fi


# --- Stage Definitions ---

init_db_stage() {
    log "Stage: init_db - Initializing database..."
    # db_utils.py will create tables if they don't exist upon first use or specific create calls
    # We can add explicit table creation calls here if needed for fmp_profiles and fmp_financial_statements
    $PYTHON_CMD "$DB_UTILS_SCRIPT" # Running db_utils directly will execute its __main__ for testing
    log "Database initialization actions in db_utils.py __main__ completed."
    # Explicitly create fundamental tables
    log "Ensuring fundamental tables exist..."
    # A better way would be a dedicated function in db_utils.py to setup all tables.
    # Let's modify db_utils.py to have an initialize_main_tables function.
    TEMP_INIT_DB_SCRIPT_CONTENT=$(cat <<EOF
from data_pipeline.db_utils import get_db_connection, create_price_data_table, create_weekly_price_data_table, create_fmp_profiles_table, create_fmp_financial_statements_table
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_all_tables(conn_obj, symbol_for_price_tables=None):
    logger.info("Initializing core tables...")
    create_fmp_profiles_table(conn_obj)
    create_fmp_financial_statements_table(conn_obj)
    if symbol_for_price_tables:
        # Price tables are symbol-specific, so only create if a symbol is contextually relevant
        # or create them on-demand when saving data.
        # For a generic init, we might skip symbol specific tables or create for a default/test symbol.
        logger.info(f"Ensuring price tables for symbol: {symbol_for_price_tables}")
        create_price_data_table(conn_obj, symbol_for_price_tables) # Daily
        create_weekly_price_data_table(conn_obj, symbol_for_price_tables) # Weekly
    logger.info("Core table initialization process completed.")

if __name__ == '__main__':
    db_conn = None
    try:
        db_conn = get_db_connection()
        # Pass a default/example symbol if you want to ensure these tables are created during init_db stage
        # Otherwise, these tables will be created when data for a specific symbol is first saved.
        initialize_all_tables(db_conn, "$DEFAULT_SYMBOL")
    finally:
        if db_conn:
            db_conn.close()
EOF
)
    TEMP_INIT_DB_PY="_temp_init_db.py"
    echo "$TEMP_INIT_DB_SCRIPT_CONTENT" > "$DATA_PIPELINE_DIR/$TEMP_INIT_DB_PY"
    $PYTHON_CMD "$DATA_PIPELINE_DIR/$TEMP_INIT_DB_PY"
    rm "$DATA_PIPELINE_DIR/$TEMP_INIT_DB_PY"
    log "Stage init_db completed."
}

# download_price_stage is now mostly for direct download to file, not part of typical pipeline processing
download_price_to_file_stage() {
    log "Stage: download_price_to_file - Downloading daily prices for $SYMBOL to ${SYMBOL}_price_daily.csv..."
    $PYTHON_CMD "$MARKET_DATA_FMP_SCRIPT" "$SYMBOL" --action price --start_date "$START_DATE" --end_date "$END_DATE" --output_file "${SYMBOL}_price_daily.csv"
    log "Daily price data for $SYMBOL downloaded to ${SYMBOL}_price_daily.csv (if successful)."
    log "Stage download_price_to_file completed."
}

download_profile_stage() {
    log "Stage: download_profile - Downloading company profile for $SYMBOL..."
    $PYTHON_CMD "$MARKET_DATA_FMP_SCRIPT" "$SYMBOL" --action profile --output_file "${SYMBOL}_profile.json"
    log "Profile data for $SYMBOL downloaded to ${SYMBOL}_profile.json (if successful)."
    log "Stage download_profile completed."
}

download_financials_stage() {
    log "Stage: download_financials - Downloading financial statements for $SYMBOL..."
    $PYTHON_CMD "$MARKET_DATA_FMP_SCRIPT" "$SYMBOL" --action financials --period quarter --limit 5 --output_file "${SYMBOL}_financials_quarter.json"
    # $PYTHON_CMD "$MARKET_DATA_FMP_SCRIPT" "$SYMBOL" --action financials --period annual --limit 5 --output_file "${SYMBOL}_financials_annual.json"
    log "Financial statements for $SYMBOL downloaded to ${SYMBOL}_financials_quarter.json (if successful)."
    log "Stage download_financials completed."
}

fundamentals_stage() {
    log "Stage: fundamentals - Downloading and storing fundamental data for $SYMBOL..."

    PROFILE_JSON_FILE="${SYMBOL}_profile.json"
    FINANCIALS_QUARTERLY_JSON_FILE="${SYMBOL}_financials_quarter.json"
    # FINANCIALS_ANNUAL_JSON_FILE="${SYMBOL}_financials_annual.json"

    log "Fetching profile data for $SYMBOL..."
    $PYTHON_CMD "$MARKET_DATA_FMP_SCRIPT" "$SYMBOL" --action profile --output_file "$PROFILE_JSON_FILE"

    log "Fetching quarterly financial statements for $SYMBOL..."
    $PYTHON_CMD "$MARKET_DATA_FMP_SCRIPT" "$SYMBOL" --action financials --period quarter --limit 20 --output_file "$FINANCIALS_QUARTERLY_JSON_FILE" # Fetch more for DB

    # log "Fetching annual financial statements for $SYMBOL..."
    # $PYTHON_CMD "$MARKET_DATA_FMP_SCRIPT" "$SYMBOL" --action financials --period annual --limit 5 --output_file "$FINANCIALS_ANNUAL_JSON_FILE"

    log "Storing fundamental data for $SYMBOL into database..."
    # This requires a new script or extending db_utils.py to take file inputs or direct data.
    # Let's create a temporary Python script to handle this or add to db_utils __main__
    # For now, let's assume db_utils.py can be enhanced or we use a dedicated loader script.

    # Create a temporary loader script for this stage
    LOADER_SCRIPT_CONTENT=$(cat <<EOF
import json
import sys
from data_pipeline.db_utils import get_db_connection, save_company_profile, save_financial_statements

logger_setup = __import__('logging').getLogger(__name__) # Basic logger for this temp script

def load_profile(db_con, profile_file):
    try:
        with open(profile_file, 'r') as f:
            profile_data = json.load(f)
        if profile_data:
            save_company_profile(db_con, profile_data) # profile_data is expected to be a dict
            logger_setup.info(f"Profile from {profile_file} processed.")
        else:
            logger_setup.warning(f"No data or empty data in profile file: {profile_file}")
    except FileNotFoundError:
        logger_setup.error(f"Profile file not found: {profile_file}")
    except Exception as e:
        logger_setup.error(f"Error processing profile file {profile_file}: {e}", exc_info=True)

def load_financials(db_con, symbol, financials_file):
    try:
        with open(financials_file, 'r') as f:
            financials_data = json.load(f) # Expected to be a list of statement dicts
        if financials_data:
            save_financial_statements(db_con, symbol, financials_data)
            logger_setup.info(f"Financials from {financials_file} processed for symbol {symbol}.")
        else:
            logger_setup.warning(f"No data or empty data in financials file: {financials_file}")
    except FileNotFoundError:
        logger_setup.error(f"Financials file not found: {financials_file}")
    except Exception as e:
        logger_setup.error(f"Error processing financials file {financials_file}: {e}", exc_info=True)

if __name__ == '__main__':
    symbol_arg = sys.argv[1]
    profile_f_arg = sys.argv[2]
    financials_q_f_arg = sys.argv[3]
    # financials_a_f_arg = sys.argv[4] # If annual is also processed

    db_connection = None
    try:
        db_connection = get_db_connection()
        load_profile(db_connection, profile_f_arg)
        load_financials(db_connection, symbol_arg, financials_q_f_arg)
        # load_financials(db_connection, symbol_arg, financials_a_f_arg)
    finally:
        if db_connection:
            db_connection.close()
EOF
)
    TEMP_LOADER_PY="_temp_fundamental_loader.py"
    echo "$LOADER_SCRIPT_CONTENT" > "$DATA_PIPELINE_DIR/$TEMP_LOADER_PY"

    $PYTHON_CMD "$DATA_PIPELINE_DIR/$TEMP_LOADER_PY" "$SYMBOL" "$PROFILE_JSON_FILE" "$FINANCIALS_QUARTERLY_JSON_FILE"

    # Clean up temporary files
    # rm "$PROFILE_JSON_FILE" # Keep for inspection if needed, or make cleanup optional
    # rm "$FINANCIALS_QUARTERLY_JSON_FILE"
    rm "$DATA_PIPELINE_DIR/$TEMP_LOADER_PY"

    log "Stage fundamentals completed."
}

process_and_store_prices_stage() {
    log "Stage: process_and_store_prices for $SYMBOL..."
    local symbol_upper=$(echo "$SYMBOL" | tr '[:lower:]' '[:upper:]') # Ensure uppercase symbol

    # This script will:
    # 1. Call market_data_fmp.py to get daily_df (as string initially, or direct object if Python caller)
    # 2. Call data_processor.py to process it (get processed_daily_df, weekly_df)
    # 3. Call db_utils.py to save both to their respective tables

    TEMP_PRICE_PROCESSOR_PY="_temp_price_processor.py"
    cat << EOF > "$DATA_PIPELINE_DIR/$TEMP_PRICE_PROCESSOR_PY"
import sys
import pandas as pd
import json
from io import StringIO
from data_pipeline.market_data_fmp import get_daily_price_data, get_fmp_api_key
from data_pipeline.data_processor import process_price_data
from data_pipeline.db_utils import get_db_connection, save_price_data
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main(symbol_arg, start_date_arg, end_date_arg):
    logger.info(f"Starting price processing and storage for {symbol_arg} from {start_date_arg} to {end_date_arg}")

    api_key = get_fmp_api_key()
    if not api_key:
        logger.error("FMP API Key not found. Exiting.")
        return

    # 1. Get daily_df from market_data_fmp
    daily_df_raw = get_daily_price_data(api_key, symbol_arg, start_date_arg, end_date_arg)

    if daily_df_raw is None or daily_df_raw.empty:
        logger.warning(f"No daily price data received from FMP for {symbol_arg}. Skipping further processing.")
        return

    # 2. Process data
    logger.info(f"Processing data for {symbol_arg}...")
    processed_daily_df, weekly_df = process_price_data(daily_df_raw, symbol_arg)

    # 3. Save to DB
    db_conn = None
    try:
        db_conn = get_db_connection()
        if processed_daily_df is not None and not processed_daily_df.empty:
            logger.info(f"Saving processed daily data for {symbol_arg}...")
            save_price_data(db_conn, symbol_arg, processed_daily_df, frequency="1d")
        else:
            logger.warning(f"No processed daily data to save for {symbol_arg}.")

        if weekly_df is not None and not weekly_df.empty:
            logger.info(f"Saving weekly data for {symbol_arg}...")
            save_price_data(db_conn, symbol_arg, weekly_df, frequency="1w")
        else:
            logger.warning(f"No weekly data to save for {symbol_arg}.")

    except Exception as e:
        logger.error(f"Error during database operations for {symbol_arg}: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()
            logger.info(f"DB connection closed for {symbol_arg} price processing.")

if __name__ == '__main__':
    s = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    main(s, sd, ed)
EOF

    $PYTHON_CMD "$DATA_PIPELINE_DIR/$TEMP_PRICE_PROCESSOR_PY" "$symbol_upper" "$START_DATE" "$END_DATE"
    rm "$DATA_PIPELINE_DIR/$TEMP_PRICE_PROCESSOR_PY"
    log "Stage process_and_store_prices completed for $SYMBOL."
}


# --- Main Execution Logic ---
log "Executing pipeline stage: $STAGE for symbol: $SYMBOL"

case $STAGE in
    init_db)
        init_db_stage
        ;;
    download_price_to_file) # Renamed from download_price
        download_price_to_file_stage
        ;;
    download_profile)
        download_profile_stage
        ;;
    download_financials)
        download_financials_stage
        ;;
    fundamentals)
        fundamentals_stage
        ;;
    process_and_store_prices)
        init_db_stage # Ensure tables exist, especially price tables for the given symbol
        process_and_store_prices_stage
        ;;
    full_price_pipeline) # New comprehensive price pipeline stage
        log "Running full_price_pipeline for $SYMBOL..."
        init_db_stage
        process_and_store_prices_stage
        log "Full_price_pipeline completed for $SYMBOL."
        ;;
    full_fundamentals)
        log "Running full_fundamentals pipeline for $SYMBOL..."
        init_db_stage
        fundamentals_stage
        log "Full_fundamentals pipeline completed for $SYMBOL."
        ;;
    full_pipeline)
        log "Running full_pipeline for $SYMBOL..."
        init_db_stage
        process_and_store_prices_stage
        fundamentals_stage
        log "Full_pipeline completed for $SYMBOL."
        ;;
    *)
        log "Error: Unknown stage '$STAGE'"
        usage
        ;;
esac

log "Pipeline execution finished for stage: $STAGE"
exit 0
