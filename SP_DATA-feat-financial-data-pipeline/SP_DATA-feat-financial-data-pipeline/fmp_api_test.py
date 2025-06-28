import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FMP_API_KEY = "USER_PROVIDED_FMP_KEY_REDACTED" # Provided API Key
FMP_BASE_URL = "https://financialmodelingprep.com/api"

# FMP free plan has a rate limit of 250 requests per day.
# To be safe during testing a few endpoints, add a small delay.
REQUEST_DELAY_SECONDS = 1 # Small delay, as daily limit is more restrictive than per-minute for this one.

def make_fmp_request(endpoint_path: str, params: dict = None) -> tuple[list | dict | None, str | None]:
    """Helper function to make requests to FMP API."""
    if params is None:
        params = {}
    params['apikey'] = FMP_API_KEY
    url = f"{FMP_BASE_URL}{endpoint_path}"

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        if not response.content:
            logger.warning(f"Empty response from {url} with params {params}")
            return None, "Empty response from API"

        data = response.json()
        logger.debug(f"Raw FMP response from {url}: {data}")

        # FMP error handling: often an empty list/dict or a dict with an "Error Message"
        if isinstance(data, dict) and "Error Message" in data:
            error_msg = f"API Error from {url}: {data['Error Message']}"
            logger.error(error_msg)
            return None, error_msg
        # Sometimes it returns a list with an error message dict inside for some endpoints
        if isinstance(data, list) and data and isinstance(data[0], dict) and "Error Message" in data[0]:
            error_msg = f"API Error from {url}: {data[0]['Error Message']}"
            logger.error(error_msg)
            return None, error_msg

        return data, None

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error for {url}: {e}. Response text: {e.response.text if e.response else 'No response text'}")
        return None, f"HTTP error: {e}. Response: {e.response.text if e.response else 'N/A'}"
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {url}: {e}", exc_info=True)
        return None, f"Request error: {e}"
    except ValueError as e: # JSONDecodeError
        logger.error(f"JSON decode error for {url}. Response text: {response.text[:200] if response else 'No response'}. Error: {e}")
        return None, f"JSON decode error: {e}. Response: {response.text[:200] if response else 'No response'}"
    except Exception as e:
        logger.error(f"Unexpected error for {url}: {e}", exc_info=True)
        return None, f"Unexpected error: {str(e)}"

def test_company_profile(symbol: str):
    logger.info(f"\n--- Testing FMP Company Profile for {symbol} ---")
    endpoint_path = f"/v3/profile/{symbol}"
    data, error_message = make_fmp_request(endpoint_path)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}
    if not data or (isinstance(data, list) and not data): # Profile data is a list with one dict
        logger.warning(f"No profile data returned for {symbol}. Response: {data}")
        return {"status": "success_empty", "message": "No profile data.", "data": data}

    profile_data = data[0] if isinstance(data, list) else data
    df = pd.DataFrame([profile_data])
    logger.info(f"Successfully fetched company profile for {symbol}. Name: {df.iloc[0].get('companyName')}")
    return {"status": "success", "message": None, "data": df}

def test_stock_quote(symbol: str):
    logger.info(f"\n--- Testing FMP Stock Quote for {symbol} ---")
    endpoint_path = f"/v3/quote/{symbol}"
    data, error_message = make_fmp_request(endpoint_path)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}
    if not data or (isinstance(data, list) and not data):
        logger.warning(f"No quote data returned for {symbol}. Response: {data}")
        return {"status": "success_empty", "message": "No quote data.", "data": data}

    quote_data = data[0] if isinstance(data, list) else data
    df = pd.DataFrame([quote_data])
    logger.info(f"Successfully fetched quote for {symbol}. Price: {df.iloc[0].get('price')}")
    return {"status": "success", "message": None, "data": df}

def test_historical_chart_daily(symbol: str, from_date: str, to_date: str):
    logger.info(f"\n--- Testing FMP Historical Chart (Daily) for {symbol} from {from_date} to {to_date} ---")
    endpoint_path = f"/v3/historical-chart/1day/{symbol}" # 1day for daily
    params = {"from": from_date, "to": to_date} # FMP uses 'from' and 'to'

    data, error_message = make_fmp_request(endpoint_path, params=params)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}
    if not data or not isinstance(data, list): # Historical data is a list of dicts
        logger.warning(f"No historical daily data returned for {symbol}. Response: {data}")
        return {"status": "success_empty", "message": "No historical daily data.",
                "data": pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])}

    df = pd.DataFrame(data)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.date
    logger.info(f"Successfully fetched {len(df)} historical daily records for {symbol}.")
    return {"status": "success", "message": None, "data": df}

def test_financial_statements(symbol: str, statement_type: str, period: str="annual", limit: int=1):
    logger.info(f"\n--- Testing FMP {statement_type} ({period}, limit {limit}) for {symbol} ---")
    # statement_type can be 'income-statement', 'balance-sheet-statement', 'cash-flow-statement'
    endpoint_path = f"/v3/{statement_type}/{symbol}"
    params = {"period": period, "limit": limit}

    data, error_message = make_fmp_request(endpoint_path, params=params)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}
    if not data or not isinstance(data, list):
        logger.warning(f"No {statement_type} data returned for {symbol}. Response: {data}")
        return {"status": "success_empty", "message": f"No {statement_type} data.", "data": data}

    df = pd.DataFrame(data)
    logger.info(f"Successfully fetched {len(df)} {period} {statement_type}(s) for {symbol}.")
    return {"status": "success", "message": None, "data": df}

def test_stock_news(symbols: str, limit: int = 5): # Can take multiple symbols comma-separated
    logger.info(f"\n--- Testing FMP Stock News for {symbols} (limit {limit}) ---")
    endpoint_path = f"/v3/stock_news"
    params = {"tickers": symbols, "limit": limit}

    data, error_message = make_fmp_request(endpoint_path, params=params)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}
    if not data or not isinstance(data, list):
        logger.warning(f"No news data returned for {symbols}. Response: {data}")
        return {"status": "success_empty", "message": "No news data.", "data": data}

    df = pd.DataFrame(data)
    logger.info(f"Successfully fetched {len(df)} news articles for {symbols}.")
    if not df.empty:
        logger.info(f"Sample news: Title='{df.iloc[0].get('title')}', Site='{df.iloc[0].get('site')}'")
    return {"status": "success", "message": None, "data": df}


if __name__ == "__main__":
    logger.info("--- Starting FMP API Test Script ---")
    all_results = {}

    symbols_to_test = ["AAPL", "TSLA"]
    to_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    from_date_str = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')

    for symbol in symbols_to_test:
        all_results[f"profile_{symbol}"] = test_company_profile(symbol)
        time.sleep(REQUEST_DELAY_SECONDS)

        all_results[f"quote_{symbol}"] = test_stock_quote(symbol)
        time.sleep(REQUEST_DELAY_SECONDS)

        all_results[f"historical_daily_{symbol}"] = test_historical_chart_daily(symbol, from_date_str, to_date_str)
        time.sleep(REQUEST_DELAY_SECONDS)

        # Test one type of financial statement for brevity
        all_results[f"income_annual_{symbol}"] = test_financial_statements(symbol, 'income-statement', 'annual', limit=1)
        time.sleep(REQUEST_DELAY_SECONDS)
        all_results[f"balance_quarterly_{symbol}"] = test_financial_statements(symbol, 'balance-sheet-statement', 'quarterly', limit=1)
        time.sleep(REQUEST_DELAY_SECONDS)

    # Test news for a single ticker to save API calls
    all_results["news_AAPL"] = test_stock_news("AAPL", limit=3)
    # No delay after the last call

    logger.info("\n\n--- FMP API Test Script Finished ---")
    logger.info("Summary of FMP test results:")
    for case_name, result in all_results.items():
        data_info = "N/A"
        if result.get('data') is not None:
            if isinstance(result['data'], pd.DataFrame):
                data_info = f"DataFrame shape: {result['data'].shape}"
            elif isinstance(result['data'], dict): # Should be converted to DF by test functions
                data_info = f"Dict keys: {list(result['data'].keys()) if result['data'] else 'Empty Dict'}"
            elif isinstance(result['data'], list):
                data_info = f"List (length {len(result['data'])}): {result['data'][:1] if result['data'] else 'Empty List'}" # Show first item if list

        logger.info(f"  Test Case: {case_name}, Status: {result['status']}, Info: {data_info}, Error/Msg: {result.get('message', 'None')}")

        if result['status'] == 'success' and isinstance(result['data'], pd.DataFrame) and not result['data'].empty:
            if case_name.startswith("historical_daily_"):
                logger.debug(f"Sample data for {case_name}:\n{result['data'][['date', 'open', 'close', 'volume']].head().to_string()}")
            elif case_name.startswith("profile_") or case_name.startswith("quote_"):
                 logger.debug(f"Sample data for {case_name}:\n{result['data'].iloc[0].to_dict()}")
            elif case_name.startswith("income_") or case_name.startswith("balance_"):
                 logger.debug(f"Sample Financial Statement data for {case_name} (1st report):\n{result['data'].iloc[0].to_dict() if not result['data'].empty else 'N/A'}")
            elif case_name.startswith("news_") and not result['data'].empty:
                 sample_news_item = result['data'].iloc[0]
                 logger.debug(f"Sample news (1st) for {case_name}: Title: {sample_news_item.get('title', 'N/A')}, Site: {sample_news_item.get('site', 'N/A')}")

    logger.info("FMP tests complete.")
