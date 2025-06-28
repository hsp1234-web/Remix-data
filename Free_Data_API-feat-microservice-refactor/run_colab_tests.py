import os
import sys
import argparse
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Attempt to import Colab userdata, will fail if not in Colab
try:
    from google.colab import userdata
    IN_COLAB = True
    logger.info("Running in Google Colab environment.")
except ImportError:
    IN_COLAB = False
    logger.info("Not running in Google Colab environment (or google.colab.userdata is not available).")

# Define API Key names as per user's request for Colab Secrets
# These are the names of the secrets expected in Colab.
# The corresponding environment variable names that the scripts expect are derived or explicitly set.
COLAB_SECRET_NAMES = {
    "ALPHA_VANTAGE_API_KEY": "ALPHA_VANTAGE_API_KEY", # Env var will be the same
    "API_KEY_FINMIND": "API_KEY_FINMIND",         # Env var will be the same
    "API_KEY_FINNHUB": "API_KEY_FINNHUB",         # Env var will be the same
    "API_KEY_FMP": "API_KEY_FMP",               # Env var will be the same (market_data_fmp.py expects this)
    "FRED_API_KEY": "FRED_API_KEY",             # Env var will be the same (used by fredapi library or our wrapper)
    "API_KEY_POLYGON": "API_KEY_POLYGON",         # Env var will be the same
    "DEEPSEEK_API_KEY": "DEEPSEEK_API_KEY",       # Env var will be the same
    "GOOGLE_API_KEY": "GOOGLE_API_KEY"          # Env var will be the same
}

def setup_api_keys_from_colab_secrets():
    """
    If running in Colab, attempts to load API keys from Colab Secrets
    and set them as environment variables for other modules to use.
    """
    if not IN_COLAB:
        logger.info("Not in Colab, skipping Colab Secrets setup for API keys. Expecting keys in .env or environment.")
        return

    logger.info("Attempting to load API keys from Colab Secrets...")
    for secret_name, env_var_name in COLAB_SECRET_NAMES.items():
        try:
            key_value = userdata.get(secret_name)
            if key_value:
                os.environ[env_var_name] = key_value
                logger.info(f"Successfully loaded and set environment variable for: {env_var_name} from Colab Secret '{secret_name}'.")
            else:
                logger.warning(f"Colab Secret '{secret_name}' not found or is empty.")
        except Exception as e:
            logger.warning(f"Could not retrieve Colab Secret '{secret_name}': {e}")
    logger.info("Finished attempting to load API keys from Colab Secrets.")


# Import project modules AFTER attempting to set environment variables
# This ensures they pick up the keys if set from Colab Secrets.
try:
    from data_pipeline.db_utils import (
        get_db_connection, create_price_data_table, create_weekly_price_data_table,
        create_fmp_profiles_table, create_fmp_financial_statements_table,
        save_price_data, save_company_profile, save_financial_statements
    )
    from data_pipeline.market_data_fmp import (
        get_fmp_api_key, get_daily_price_data, get_company_profile, get_financial_statements
    )
    from data_pipeline.data_processor import process_price_data
    # Placeholder for verify_db logic, can be imported or integrated
    from verify_db import run_all_verifications
except ImportError as e:
    logger.error(f"Failed to import necessary project modules: {e}. Ensure PYTHONPATH is set correctly or script is run from project root.")
    # Allow script to continue if verify_db is missing, verify_db_command will handle it.
    # sys.exit(1)
    run_all_verifications = None # Define it as None if import fails
    logger.warning("verify_db module or run_all_verifications function not found. DB verification stage will be limited.")


# Default values for arguments
DEFAULT_SYMBOL = "AAPL"
DEFAULT_START_DATE = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d') # Last 1 year
DEFAULT_END_DATE = datetime.now().strftime('%Y-%m-%d')


def init_db_command(args):
    """Initializes all database tables."""
    logger.info("Executing: init_db command")
    db_conn = None
    try:
        db_conn = get_db_connection()
        logger.info(f"Ensuring core tables exist for symbol: {args.symbol} (and generic tables)")
        create_fmp_profiles_table(db_conn)
        create_fmp_financial_statements_table(db_conn)
        create_price_data_table(db_conn, args.symbol.upper()) # Daily price table for the target symbol
        create_weekly_price_data_table(db_conn, args.symbol.upper()) # Weekly price table for the target symbol
        logger.info("Database initialization process completed successfully.")
    except Exception as e:
        logger.error(f"Error during database initialization: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()

def process_and_store_prices_command(args):
    """Fetches, processes, and stores daily and weekly price data."""
    logger.info(f"Executing: process_and_store_prices command for {args.symbol} from {args.start_date} to {args.end_date}")

    fmp_api_key = get_fmp_api_key() # This will now check env var API_KEY_FMP
    if not fmp_api_key:
        logger.error("FMP API Key (API_KEY_FMP) is not configured. Cannot fetch price data.")
        return

    logger.info(f"Fetching raw daily price data for {args.symbol}...")
    daily_df_raw = get_daily_price_data(fmp_api_key, args.symbol, args.start_date, args.end_date)

    if daily_df_raw is None or daily_df_raw.empty:
        logger.warning(f"No daily price data received from FMP for {args.symbol}. Skipping further processing.")
        return

    logger.info(f"Processing price data for {args.symbol}...")
    processed_daily_df, weekly_df = process_price_data(daily_df_raw, args.symbol)

    db_conn = None
    try:
        db_conn = get_db_connection()
        if processed_daily_df is not None and not processed_daily_df.empty:
            logger.info(f"Saving processed daily data for {args.symbol}...")
            save_price_data(db_conn, args.symbol, processed_daily_df, frequency="1d")
        else:
            logger.warning(f"No processed daily data to save for {args.symbol}.")

        if weekly_df is not None and not weekly_df.empty:
            logger.info(f"Saving weekly data for {args.symbol}...")
            save_price_data(db_conn, args.symbol, weekly_df, frequency="1w")
        else:
            logger.warning(f"No weekly data to save for {args.symbol}.")
        logger.info(f"Price data storage for {args.symbol} completed.")
    except Exception as e:
        logger.error(f"Error during database operations for {args.symbol} prices: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()

def process_and_store_fundamentals_command(args):
    """Fetches and stores company profile and financial statements."""
    logger.info(f"Executing: process_and_store_fundamentals command for {args.symbol}")

    fmp_api_key = get_fmp_api_key() # Checks API_KEY_FMP
    if not fmp_api_key:
        logger.error("FMP API Key (API_KEY_FMP) is not configured. Cannot fetch fundamentals.")
        return

    db_conn = None
    try:
        db_conn = get_db_connection()

        # Fetch and save profile
        logger.info(f"Fetching company profile for {args.symbol}...")
        profile_data = get_company_profile(fmp_api_key, args.symbol)
        if profile_data:
            logger.info(f"Saving company profile for {args.symbol}...")
            save_company_profile(db_conn, profile_data)
        else:
            logger.warning(f"No profile data received for {args.symbol}.")

        # Fetch and save financial statements (e.g., quarterly, last 20 periods)
        logger.info(f"Fetching quarterly financial statements for {args.symbol} (limit 20)...")
        financials_data_q = get_financial_statements(fmp_api_key, args.symbol, period="quarter", limit=20)
        if financials_data_q:
            logger.info(f"Saving quarterly financial statements for {args.symbol}...")
            save_financial_statements(db_conn, args.symbol, financials_data_q)
        else:
            logger.warning(f"No quarterly financial statements received for {args.symbol}.")

        # Optionally, fetch and save annual financials
        # logger.info(f"Fetching annual financial statements for {args.symbol} (limit 5)...")
        # financials_data_a = get_financial_statements(fmp_api_key, args.symbol, period="annual", limit=5)
        # if financials_data_a:
        #     logger.info(f"Saving annual financial statements for {args.symbol}...")
        #     save_financial_statements(db_conn, args.symbol, financials_data_a)
        # else:
        #     logger.warning(f"No annual financial statements received for {args.symbol}.")

        logger.info(f"Fundamental data storage for {args.symbol} completed.")
    except Exception as e:
        logger.error(f"Error during fundamentals processing/storage for {args.symbol}: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()

def verify_db_command(args):
    """Runs database verification checks."""
    logger.info(f"Executing: verify_db command for symbol {args.symbol}")
    db_conn_verify = None
    try:
        if run_all_verifications is None:
            logger.error("Verification function 'run_all_verifications' is not available (likely due to import error from verify_db.py).")
            logger.info("Please ensure verify_db.py is in the correct location and has no import errors itself.")
            logger.info("To run full verification, execute 'python verify_db.py' separately.")
            return

        db_conn_verify = get_db_connection() # Get a new read-only connection for verification
        logger.info(f"Running database verifications for symbol: {args.symbol.upper()}...")
        success = run_all_verifications(db_conn_verify, args.symbol.upper())
        if success:
            logger.info("Database verification completed successfully through run_colab_tests.py.")
        else:
            logger.error("Database verification failed through run_colab_tests.py. Check previous logs.")

    except Exception as e:
        logger.error(f"Error during DB verification stage: {e}", exc_info=True)
    finally:
        if db_conn_verify:
            db_conn_verify.close()


def main():
    # Setup API keys from Colab Secrets if in Colab
    setup_api_keys_from_colab_secrets()

    parser = argparse.ArgumentParser(description="Colab Runner for Financial Data Pipeline.")
    parser.add_argument(
        "--stage",
        choices=['init_db', 'process_prices', 'process_fundamentals', 'full_pipeline', 'verify_db'],
        required=True,
        help="Pipeline stage to execute."
    )
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL, help=f"Stock symbol (default: {DEFAULT_SYMBOL}).")
    parser.add_argument("--start_date", type=str, default=DEFAULT_START_DATE, help=f"Start date (YYYY-MM-DD) for price data (default: {DEFAULT_START_DATE}).")
    parser.add_argument("--end_date", type=str, default=DEFAULT_END_DATE, help=f"End date (YYYY-MM-DD) for price data (default: {DEFAULT_END_DATE}).")

    args = parser.parse_args()

    logger.info(f"Running Colab Test Runner with stage: {args.stage}, symbol: {args.symbol}")

    if args.stage == 'init_db':
        init_db_command(args)
    elif args.stage == 'process_prices':
        # Ensure DB is initialized for the target symbol's tables
        # init_db_command(args) # Or assume tables are created on demand by save functions
        process_and_store_prices_command(args)
    elif args.stage == 'process_fundamentals':
        # init_db_command(args) # Or assume tables are created on demand
        process_and_store_fundamentals_command(args)
    elif args.stage == 'full_pipeline':
        logger.info(f"Starting full pipeline for {args.symbol}...")
        init_db_command(args)
        process_and_store_prices_command(args)
        process_and_store_fundamentals_command(args)
        logger.info(f"Full pipeline for {args.symbol} completed.")
        # Optionally run verification at the end
        # logger.info("Running verification after full pipeline...")
        # verify_db_command(args)
    elif args.stage == 'verify_db':
        verify_db_command(args)
    else:
        logger.error(f"Unknown stage: {args.stage}")
        parser.print_help()

if __name__ == "__main__":
    # Example of how to run from command line (e.g., in Colab cell: !python run_colab_tests.py --stage full_pipeline --symbol MSFT)
    # For direct execution in a non-Colab environment for testing the script itself:
    # You would need to have FMP_API_KEY (or API_KEY_FMP) in your .env or environment.
    # And other keys if other data sources were active.

    # To make it runnable directly for simple test without args:
    if len(sys.argv) == 1: # No arguments provided
        logger.info("No arguments provided. Running 'full_pipeline' for default symbol AAPL as an example.")
        # Manually construct args for a default run
        # This is just for making the script somewhat runnable by `python run_colab_tests.py`
        # In Colab, you'd typically use `!python run_colab_tests.py --stage ...`
        sys.argv.extend(['--stage', 'full_pipeline', '--symbol', DEFAULT_SYMBOL])
        # Note: This modification of sys.argv is a bit of a hack for direct execution testing.
        # In production or real use, arguments should be passed normally.

    main()
