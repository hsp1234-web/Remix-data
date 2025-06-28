import pandas as pd
from datetime import datetime, timezone
import logging
import sys
import os

# Debugging path issues
import os
import sys
current_script_path = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(current_script_path)) # Assuming script is in project root
src_path = os.path.join(project_root, 'src')

# Print paths for debugging
print(f"Current script directory: {current_script_path}")
print(f"Calculated project_root: {project_root}") # Corrected variable name in log
print(f"Calculated src_path: {src_path}")
if src_path not in sys.path:
    sys.path.insert(0, src_path) # Prepend src_path
print(f"sys.path after modification: {sys.path}")


# It's good practice to handle potential ImportError early
try:
    from connectors.finmind_connector import FinMindConnector
except ImportError as e:
    # Configure logging for this specific error if it hasn't been configured yet
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_init_error = logging.getLogger(__name__) # Use __name__ for the logger
    logger_init_error.error(f"Failed to import FinMindConnector: {e}. Current CWD: {os.getcwd()}. Attempted src_path: {src_path}")
    sys.exit(1) # Exit if connector cannot be imported

# Configure main logging after successful import check (or reconfigure if already set by error)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__) # Use __name__ for the logger

FINMIND_API_TOKEN = "USER_PROVIDED_FINMIND_TOKEN_REDACTED" # Token was provided by user

# Create a temporary config for the connector
test_config = {
    "api_keys": {
        "finmind_api_token": FINMIND_API_TOKEN
    },
    "requests_config": {
        "timeout": 60, # Increased timeout for potentially larger data
        "max_retries": 2, # FinMind SDK might have its own retries, so keep this low
        "base_headers": {
            "User-Agent": "FinMindRealAPITest/1.0"
        }
    }
}

def run_finmind_tests():
    logger.info("--- Starting FinMind Real API Test Script ---")
    all_results = {}

    try:
        # Pass the logger from this script to the connector
        # Create a specific logger for the connector instance for more granular control if needed
        connector_logger = logging.getLogger("FinMindConnectorInstance")
        # Set level for the connector's logger (DEBUG for verbose, INFO for less)
        # Ensure this logger's messages will be displayed by setting its level
        # and ensuring the root logger or a handler is configured to handle this level.
        # If basicConfig is already set for root, child loggers will inherit.
        # Forcing higher verbosity for connector's logger for this test:
        connector_logger.setLevel(logging.DEBUG)

        # If the connector's internal logger (e.g., logging.getLogger(__name__) in the connector file)
        # is not picking up the basicConfig, we might need to add a handler here for connector_logger.
        # Example:
        # if not connector_logger.hasHandlers():
        #     handler = logging.StreamHandler(sys.stdout)
        #     handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        #     connector_logger.addHandler(handler)
        #     connector_logger.propagate = False # Avoid double logging if root also logs

        connector = FinMindConnector(config=test_config, logger=connector_logger)
        logger.info("FinMindConnector initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize FinMindConnector: {e}", exc_info=True)
        all_results["initialization"] = {"status": "error", "message": str(e)}
        # Print summary before exiting due to init failure
        logger.info("\n--- FinMind Real API Test Script Finished (Initialization Failed) ---")
        for test_name, result_item in all_results.items():
             logger.info(f"  Test: {test_name}, Status: {result_item['status']}, Error/Msg: {result_item.get('message', 'None')}")
        return

    # Test 1: Stock Price for a common stock
    try:
        logger.info("\n--- Testing get_stock_price (2330) ---")
        df_price, err_price = connector.get_stock_price(stock_id="2330", start_date="2023-12-01", end_date="2023-12-31")
        if err_price:
            all_results["stock_price_2330"] = {"status": "error", "message": err_price}
        elif df_price is not None and not df_price.empty:
            all_results["stock_price_2330"] = {"status": "success", "data_shape": df_price.shape, "data_head": df_price.head().to_string()}
            logger.info(f"Stock Price (2330) Data Shape: {df_price.shape}")
            logger.info(f"Stock Price (2330) Data Head:\n{df_price.head()}")
        else:
            all_results["stock_price_2330"] = {"status": "success_empty", "message": "Empty DataFrame or None returned."}
            logger.warning("Stock Price (2330): Received empty DataFrame or None.")
    except Exception as e:
        logger.error(f"Exception in get_stock_price(2330) test: {e}", exc_info=True)
        all_results["stock_price_2330"] = {"status": "exception", "message": str(e)}

    # Test 2: Income Statement
    try:
        logger.info("\n--- Testing get_income_statement (2330, from 2022-01-01) ---")
        # FinMind usually returns multiple reports after the start_date
        df_income, err_income = connector.get_income_statement(stock_id="2330", start_date="2022-01-01")
        if err_income:
            all_results["income_statement_2330"] = {"status": "error", "message": err_income}
        elif df_income is not None and not df_income.empty:
            all_results["income_statement_2330"] = {"status": "success", "data_shape": df_income.shape, "data_head": df_income.head().to_string()}
            logger.info(f"Income Statement (2330) Data Shape: {df_income.shape}")
            logger.info(f"Income Statement (2330) Data Head:\n{df_income.head()}")
        else:
            all_results["income_statement_2330"] = {"status": "success_empty", "message": "Empty DataFrame or None returned."}
            logger.warning("Income Statement (2330): Received empty DataFrame or None.")
    except Exception as e:
        logger.error(f"Exception in get_income_statement test: {e}", exc_info=True)
        all_results["income_statement_2330"] = {"status": "exception", "message": str(e)}

    # Test 3: Institutional Trades (Chip Data)
    try:
        logger.info("\n--- Testing get_institutional_trades (2330, 2023-12-01 to 2023-12-07) ---")
        df_chip, err_chip = connector.get_institutional_trades(stock_id="2330", start_date="2023-12-01", end_date="2023-12-07")
        if err_chip:
            all_results["institutional_trades_2330"] = {"status": "error", "message": err_chip}
        elif df_chip is not None and not df_chip.empty:
            all_results["institutional_trades_2330"] = {"status": "success", "data_shape": df_chip.shape, "data_head": df_chip.head().to_string()}
            logger.info(f"Institutional Trades (2330) Data Shape: {df_chip.shape}")
            logger.info(f"Institutional Trades (2330) Data Head:\n{df_chip.head()}")
        else:
            all_results["institutional_trades_2330"] = {"status": "success_empty", "message": "Empty DataFrame or None returned."}
            logger.warning("Institutional Trades (2330): Received empty DataFrame or None.")
    except Exception as e:
        logger.error(f"Exception in get_institutional_trades test: {e}", exc_info=True)
        all_results["institutional_trades_2330"] = {"status": "exception", "message": str(e)}

    # Test 4: Taiwan Weighted Index (TAIEX)
    try:
        logger.info("\n--- Testing get_stock_price (TAIEX) ---")
        df_taiex, err_taiex = connector.get_stock_price(stock_id="TAIEX", start_date="2023-12-01", end_date="2023-12-31")
        if err_taiex:
            all_results["stock_price_TAIEX"] = {"status": "error", "message": err_taiex}
        elif df_taiex is not None and not df_taiex.empty:
            all_results["stock_price_TAIEX"] = {"status": "success", "data_shape": df_taiex.shape, "data_head": df_taiex.head().to_string()}
            logger.info(f"Stock Price (TAIEX) Data Shape: {df_taiex.shape}")
            logger.info(f"Stock Price (TAIEX) Data Head:\n{df_taiex.head()}")
        else:
            all_results["stock_price_TAIEX"] = {"status": "success_empty", "message": "Empty DataFrame or None returned."}
            logger.warning("Stock Price (TAIEX): Received empty DataFrame or None.")
    except Exception as e:
        logger.error(f"Exception in get_stock_price (TAIEX) test: {e}", exc_info=True)
        all_results["stock_price_TAIEX"] = {"status": "exception", "message": str(e)}

    # Test 5: A non-existent stock_id to check error handling from API/SDK
    try:
        logger.info("\n--- Testing get_stock_price (NONEXISTENT_STOCK) ---")
        df_non, err_non = connector.get_stock_price(stock_id="NONEXISTENTSTOCK", start_date="2023-01-01", end_date="2023-01-31")
        if err_non: # This is an expected "error" from the connector's perspective if data not found
            all_results["stock_price_NONEXISTENT"] = {"status": "success_handled_error", "message": err_non} # Or "error" if it indicates a problem
            logger.info(f"Handled error for NONEXISTENT_STOCK as expected: {err_non}")
        elif df_non is not None and not df_non.empty: # Should not happen
            all_results["stock_price_NONEXISTENT"] = {"status": "unexpected_success", "data_shape": df_non.shape}
            logger.warning(f"NONEXISTENT_STOCK actually returned data. Shape: {df_non.shape}")
        else: # Expected: df_non is empty or None, and err_non is None (connector handles it as no data)
            all_results["stock_price_NONEXISTENT"] = {"status": "success_no_data", "message": "No data found as expected."}
            logger.info("NONEXISTENT_STOCK: No data found, as expected.")
    except Exception as e:
        logger.error(f"Exception in get_stock_price (NONEXISTENT_STOCK) test: {e}", exc_info=True)
        all_results["stock_price_NONEXISTENT"] = {"status": "exception", "message": str(e)}


    logger.info("\n\n--- FinMind Real API Test Script Finished ---")
    logger.info("Summary of FinMind Real API test results:")
    for test_name, result in all_results.items():
        if result['status'] == 'success':
            logger.info(f"  Test: {test_name}, Status: {result['status']}, Shape: {result['data_shape']}")
            # logger.debug(f"  Data Head for {test_name}:\n{result['data_head']}") # Too verbose for summary
        else:
            logger.info(f"  Test: {test_name}, Status: {result['status']}, Message: {result.get('message', 'N/A')}")

if __name__ == "__main__":
    # Example of how to run if this script is executed directly
    # This would typically be run by the agent's tool
    run_finmind_tests()
