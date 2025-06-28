import requests
import pandas as pd
from datetime import datetime, timezone
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ALPHA_VANTAGE_API_KEY = "USER_PROVIDED_ALPHA_VANTAGE_KEY_REDACTED" # Provided API Key
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Alpha Vantage Free tier typically has a limit of 5 API requests per minute and 500 requests per day.
# We need to respect this.
REQUEST_DELAY_SECONDS = 15 # 60 seconds / 4 requests per minute (to be safe)

def fetch_alpha_vantage_data(function: str, symbol: str, api_key: str, **kwargs) -> tuple[pd.DataFrame | None, str | None]:
    """
    Fetches data from Alpha Vantage API for a given function and symbol.

    Args:
        function (str): The API function to call (e.g., "TIME_SERIES_DAILY_ADJUSTED", "OVERVIEW").
        symbol (str): The ticker symbol (e.g., "IBM", "AAPL").
        api_key (str): Your Alpha Vantage API key.
        **kwargs: Additional parameters for the API function (e.g., outputsize="compact").

    Returns:
        tuple[pd.DataFrame | None, str | None]: A tuple containing the DataFrame
                                                 if successful, None otherwise, and an
                                                 error message string if an error occurred,
                                                 None otherwise.
    """
    params = {
        "function": function,
        "symbol": symbol,
        "apikey": api_key,
        **kwargs
    }
    logger.info(f"Fetching Alpha Vantage data for function: {function}, symbol: {symbol}")

    try:
        response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        logger.debug(f"Raw Alpha Vantage response for {symbol} ({function}): {data}")

        if "Error Message" in data:
            error_msg = f"API Error for {symbol} ({function}): {data['Error Message']}"
            logger.error(error_msg)
            return None, error_msg
        if "Information" in data and "API call frequency" in data["Information"]:
            # This indicates a rate limit hit for free tier
            error_msg = f"API Rate Limit Hit for {symbol} ({function}): {data['Information']}. Please wait and try again."
            logger.warning(error_msg)
            # It's good practice to wait longer if a rate limit is explicitly hit.
            # However, the main loop already has a delay.
            return None, error_msg
        if not data:
            logger.warning(f"No data returned for {symbol} ({function}). Response was empty JSON.")
            return None, f"No data returned for {symbol} ({function})."

        # Process different types of responses
        if function == "TIME_SERIES_DAILY_ADJUSTED":
            time_series_key = "Time Series (Daily)"
            if time_series_key not in data:
                logger.warning(f"'{time_series_key}' not in response for {symbol}. Keys: {data.keys()}")
                return None, f"'{time_series_key}' not found in response for {symbol}."

            df = pd.DataFrame.from_dict(data[time_series_key], orient='index')
            df.index = pd.to_datetime(df.index)
            df.rename(columns={
                "1. open": "open_price", "2. high": "high_price",
                "3. low": "low_price", "4. close": "close_price",
                "5. adjusted close": "adj_close_price", "6. volume": "volume",
                "7. dividend amount": "dividend_amount", "8. split coefficient": "split_coefficient"
            }, inplace=True)
            # Convert columns to numeric
            numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume', 'dividend_amount', 'split_coefficient']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df.sort_index(inplace=True)
            df['security_id'] = symbol
            df['source_api'] = "alphavantage"
            df['last_updated_timestamp'] = datetime.now(timezone.utc)
            # Ensure 'price_date' is the index name or a column
            df.index.name = 'price_date'
            df.reset_index(inplace=True) # Make price_date a column
            df['price_date'] = pd.to_datetime(df['price_date']).dt.date


        elif function == "OVERVIEW":
            if not data or isinstance(data, list): # Overview returns a single dict, not a list
                logger.warning(f"Unexpected data structure for OVERVIEW {symbol}: {data}")
                return None, f"Unexpected data structure for OVERVIEW {symbol}"
            # Convert single company overview to a DataFrame (1 row)
            # We need to be careful as some values might be 'None' (string) or empty strings
            for key, value in data.items():
                if value == 'None' or value == '-': # Common Alpha Vantage placeholders for N/A
                    data[key] = None

            df = pd.DataFrame([data])
            # Basic standardization
            df['security_id'] = symbol
            df['source_api'] = "alphavantage_overview"
            df['last_updated_timestamp'] = datetime.now(timezone.utc)
            # Further processing would be needed in a real connector to map to a canonical financial_statement or company_profile schema

        elif function == "BALANCE_SHEET":
            reports_key = "annualReports" # or "quarterlyReports"
            if reports_key not in data or not data[reports_key]:
                logger.warning(f"'{reports_key}' not in response or empty for BALANCE_SHEET {symbol}. Keys: {data.keys()}")
                return None, f"'{reports_key}' not found or empty for BALANCE_SHEET {symbol}."
            # Data is a list of reports, usually better to process them all
            # For a simple test, let's take the first one if available
            df = pd.DataFrame(data[reports_key])
            # This will be a wide table, needs transformation to long format for a proper financials table.
            # For now, just return it as is for testing.
            df['security_id'] = symbol
            df['source_api'] = "alphavantage_balance_sheet"
            df['last_updated_timestamp'] = datetime.now(timezone.utc)


        else:
            logger.warning(f"Processing for function '{function}' not fully implemented in this test script. Returning raw dict in a DataFrame.")
            df = pd.DataFrame([data]) # Wrap the dict in a list to make it a row
            df['security_id'] = symbol
            df['source_api'] = f"alphavantage_{function.lower()}"
            df['last_updated_timestamp'] = datetime.now(timezone.utc)


        logger.info(f"Successfully fetched and processed data for {symbol} ({function}). Shape: {df.shape if df is not None else 'N/A'}")
        return df, None

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error for {symbol} ({function}): {e}. Response text: {e.response.text if e.response else 'No response text'}")
        return None, f"HTTP error: {e}"
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {symbol} ({function}): {e}", exc_info=True)
        return None, f"Request error: {e}"
    except Exception as e:
        logger.error(f"Unexpected error processing {symbol} ({function}): {e}", exc_info=True)
        return None, f"Unexpected error: {str(e)}"

if __name__ == "__main__":
    logger.info("--- Starting Alpha Vantage API Test Script ---")

    test_cases = [
        {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": "IBM", "params": {"outputsize": "compact"}},
        {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": "AAPL", "params": {"outputsize": "compact"}}, # Another stock
        {"function": "OVERVIEW", "symbol": "IBM", "params": {}},
        {"function": "OVERVIEW", "symbol": "TSLA", "params": {}}, # Test another company overview
        {"function": "BALANCE_SHEET", "symbol": "IBM", "params": {}}, # Test financial statement
        # Add a deliberate error case (e.g. invalid function or symbol if desired, or let rate limit be the test)
        # For rate limit test, we might need more than 5 quick requests.
        # The loop itself will make 5 requests with delays.
    ]

    all_results = {}

    for i, case in enumerate(test_cases):
        logger.info(f"\nExecuting test case {i+1}/{len(test_cases)}: Function={case['function']}, Symbol={case['symbol']}")
        df, error_message = fetch_alpha_vantage_data(
            function=case["function"],
            symbol=case["symbol"],
            api_key=ALPHA_VANTAGE_API_KEY,
            **case["params"]
        )
        all_results[f"{case['function']}_{case['symbol']}"] = {"status": "error" if error_message else "success", "message": error_message, "data": df}

        if error_message:
            logger.error(f"Failed: {error_message}")
        elif df is not None:
            if df.empty:
                logger.warning(f"Received empty DataFrame for {case['symbol']} ({case['function']}).")
            else:
                logger.info(f"Success. DataFrame shape: {df.shape}. First few rows/cols:")
                if case["function"] == "TIME_SERIES_DAILY_ADJUSTED":
                    logger.info("\n" + df[['price_date', 'security_id', 'open_price', 'close_price', 'adj_close_price', 'volume']].head().to_string())
                elif case["function"] == "OVERVIEW":
                     logger.info("\n" + df[['security_id', 'Symbol', 'Name', 'Exchange', 'Sector', 'Industry', 'MarketCapitalization']].head().to_string())
                elif case["function"] == "BALANCE_SHEET":
                     logger.info(f"\nBalance sheet data columns (sample): {df.columns.tolist()[:5]}")
                     logger.info(f"Balance sheet data shape: {df.shape}")
                     if not df.empty:
                         logger.info(f"Fiscal Date Ending for first report: {df.iloc[0].get('fiscalDateEnding')}")


        # Respect API rate limits
        if i < len(test_cases) - 1: # Don't sleep after the last request
            logger.info(f"Waiting for {REQUEST_DELAY_SECONDS} seconds before next request...")
            time.sleep(REQUEST_DELAY_SECONDS)

    logger.info("\n\n--- Alpha Vantage API Test Script Finished ---")
    logger.info("Summary of Alpha Vantage test results:")
    for case_name, result in all_results.items():
        data_shape = result['data'].shape if result['data'] is not None and isinstance(result['data'], pd.DataFrame) else "N/A"
        logger.info(f"  Test Case: {case_name}, Status: {result['status']}, Shape: {data_shape}, Error/Msg: {result.get('message', 'None')}")

    # Example of how to see one of the dataframes if needed:
    # if all_results["TIME_SERIES_DAILY_ADJUSTED_IBM"]["data"] is not None:
    #     print("\nIBM Daily Data:")
    #     print(all_results["TIME_SERIES_DAILY_ADJUSTED_IBM"]["data"].head())
    # if all_results["OVERVIEW_IBM"]["data"] is not None:
    #     print("\nIBM Overview Data:")
    #     print(all_results["OVERVIEW_IBM"]["data"].iloc[0]) # Overview is a single row
