import pandas as pd
from datetime import datetime, timezone
import logging
import numpy as np # Import numpy

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import finlab.data as fld
import finlab # For login if it's separate
import numpy as np

FINLAB_API_KEY = "USER_PROVIDED_FINLAB_KEY_REDACTED" # Key was provided by user
# IS_FINLAB_VIP = False # This might be determined by the API key itself or a user setting in a real scenario

# Global variable to store login status
finlab_logged_in = False

def test_finlab_login():
    global finlab_logged_in
    logger.info("--- Testing FinLab Login (Real API) ---")
    try:
        # FinLab's login is typically finlab.login(api_token=...)
        # or might be implicitly handled by data.get if token is set via an env var or config file.
        # For explicit login with the key:
        finlab.login(api_token=FINLAB_API_KEY)
        # Some versions/docs might show fld.login, if data module has login
        # fld.login(api_token=FINLAB_API_KEY)
        logger.info("FinLab login successful (Real API).")
        finlab_logged_in = True
        return True
    except Exception as e:
        logger.error(f"FinLab login failed (Real API): {e}", exc_info=True)
        finlab_logged_in = False
        return False

def test_finlab_data_get(dataset_name: str, sample_tickers: list = None):
    global finlab_logged_in
    logger.info(f"\n--- Testing FinLab data.get('{dataset_name}') (Real API) ---")

    if not finlab_logged_in:
        logger.error("FinLab not logged in. Skipping data retrieval.")
        return {"status": "error", "message": "Not logged in", "data": None}

    try:
        # Assuming fld.get is the correct way to get data
        df = fld.get(dataset_name)
        error_message = None # No error if fld.get succeeds

        # Check for known error conditions if any (e.g. specific return values for errors)
        # For now, assume if it doesn't raise an exception, it's a valid (possibly empty) DataFrame.

    except Exception as e:
        logger.error(f"Error during fld.get('{dataset_name}'): {e}", exc_info=True)
        df = None
        error_message = str(e)

    if error_message:
        logger.error(f"Failed to get '{dataset_name}' (Real API): {error_message}")
        return {"status": "error", "message": error_message, "data": None}

    if df is None: # Should be caught by error_message
        logger.error(f"Getting '{dataset_name}' returned None without an error message.")
        return {"status": "error", "message": "Returned None without error message", "data": None}

    if df.empty:
        logger.warning(f"Dataset '{dataset_name}' is empty (Real API).")
        # For real API, we can't easily predict expected_lag_months without knowing user's VIP status
        # and exact API behavior. We'll just note if data is recent.
        if isinstance(df.index, pd.DatetimeIndex) and not df.empty:
             latest_data_date = df.index.max()
             logger.info(f"Latest data point in empty DataFrame for {dataset_name} is {latest_data_date.date()}")
        return {"status": "success_empty", "message": "DataFrame is empty (Real API).", "data": df}

    logger.info(f"Successfully fetched '{dataset_name}' (Real API). Shape: {df.shape}")
    logger.info(f"Index type: {type(df.index)}, Index name: {df.index.name}")
    logger.info(f"Columns: {df.columns.tolist()[:20]}...") # Show first 20 columns if many
    logger.info(f"First 5 rows of '{dataset_name}' (Real API):\n{df.head().to_string()}")

    # Optional: Check data recency for non-VIP users (rough check)
    # This assumes the API key used is for a free tier.
    # The FinLab note mentioned "free (FREE)用戶獲取的歷史數據會移除最近兩個月的資料"
    # We can perform a rough check here.
    if isinstance(df.index, pd.DatetimeIndex) and not df.empty:
        latest_data_date = df.index.max()
        # Convert to tz-naive if necessary for comparison
        if latest_data_date.tzinfo is not None:
            latest_data_date = latest_data_date.tz_localize(None)

        # Expected cutoff for free tier (approx 2 months ago)
        # Using a more robust way to get "2 months ago"
        today = pd.Timestamp.now(tz=None).normalize() # today, time truncated, tz-naive
        two_months_ago = today - pd.DateOffset(months=2)

        # If latest data is more recent than approx. 2 months ago, it might be VIP or lag is different
        # If latest data is older than 2 months ago, it's consistent with free tier lag
        if latest_data_date > two_months_ago:
            logger.info(f"Data for '{dataset_name}' (latest: {latest_data_date.date()}) is more recent than the expected 2-month lag for free tier. This might be a VIP key or lag rules differ.")
        else:
            logger.info(f"Data for '{dataset_name}' (latest: {latest_data_date.date()}) appears consistent with approx. 2-month lag for free tier (cutoff around {two_months_ago.date()}).")

    # Check if sample tickers are present (if applicable)
    if sample_tickers:
        for ticker in sample_tickers:
            # Finlab often stores tickers without .TW in column names (e.g., '2330' for '2330.TW')
            # It can also store them with .TW depending on the dataset.
            # We'll check for both common conventions.
            plain_ticker = ticker.replace(".TW", "")
            if plain_ticker not in df.columns and ticker not in df.columns:
                logger.warning(f"Sample ticker {ticker} (or {plain_ticker}) not found in columns of '{dataset_name}'. Available (sample): {df.columns.tolist()[:10]}")


    return {"status": "success", "message": None, "data": df}


if __name__ == "__main__":
    logger.info("--- Starting FinLab API Test Script (Real API) ---")

    login_success = test_finlab_login()
    results = {}

    if login_success:
        # Datasets to test, based on the user's FinLab notes
        # Using a smaller set for initial real API test to be mindful of API limits
        datasets_to_test = [
            "price:收盤價",
            "financial_statement:ROE",
            "chip:外資買賣超", # Test a few more key ones
            "monthly_revenue:當月營收",
            "benchmark:發行量加權股價指數", # Test benchmark
            "company_main_business", # Test non-time-series
            "non_existent_dataset:for_error_test"
        ]

        # For real data, we might not know all tickers, so this check is less critical
        # but can be useful for common ones.
        sample_tickers_for_check = ["2330.TW", "0050.TW", "1101"] # Check with and without .TW

        for ds_name in datasets_to_test:
            result = test_finlab_data_get(ds_name, sample_tickers=sample_tickers_for_check)
            results[ds_name] = result
    else:
        logger.error("Cannot proceed with data tests as FinLab login failed (Real API).")

    logger.info("\n\n--- FinLab API Test Script (Real API) Finished ---")
    logger.info("Summary of FinLab (Real API) test results:")
    for ds_name, result in results.items():
        data_shape = result['data'].shape if result['data'] is not None else "N/A"
        logger.info(f"  Dataset: {ds_name}, Status: {result['status']}, Shape: {data_shape}, Error/Msg: {result.get('message', 'None')}")

    # Notes for running this script:
    # 1. Ensure 'finlab' package is installed (pip install finlab).
    # 2. FINLAB_API_KEY at the top must be a valid API key.
    # 3. The script attempts a real login and real data fetches.
    # 4. It includes a rough check for data recency based on typical free tier behavior.
