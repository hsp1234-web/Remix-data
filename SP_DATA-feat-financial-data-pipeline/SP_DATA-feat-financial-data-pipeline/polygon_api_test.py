import requests # Using requests library directly
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

POLYGON_API_KEY = "USER_PROVIDED_POLYGON_KEY_REDACTED" # Provided API Key
POLYGON_BASE_URL = "https://api.polygon.io"

# Polygon.io free plan has a rate limit of 5 API calls per minute.
REQUEST_DELAY_SECONDS = 13 # 60 seconds / 5 calls per minute + a small buffer

def make_polygon_request(endpoint: str, params: dict) -> tuple[dict | list | None, str | None]:
    """Helper function to make requests to Polygon.io API."""
    params['apiKey'] = POLYGON_API_KEY
    url = f"{POLYGON_BASE_URL}{endpoint}"
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)

        # Check for empty response before trying to parse JSON
        if not response.content:
            logger.warning(f"Empty response from {url} with params {params}")
            return None, "Empty response from API"

        data = response.json()
        logger.debug(f"Raw Polygon response from {url}: {data}")

        if isinstance(data, dict) and data.get('status') == 'ERROR':
            error_msg = f"API Error from {url}: {data.get('message', data.get('error', 'Unknown Polygon error'))}"
            logger.error(error_msg)
            return None, error_msg
        if isinstance(data, dict) and data.get('status') == 'DELAYED':
            logger.warning(f"Data from {url} is delayed (free tier limitation).")
            # Proceed with delayed data if that's acceptable for the test

        return data, None

    except requests.exceptions.HTTPError as e:
        # Specific check for 429 Rate Limit Exceeded
        if e.response.status_code == 429:
            logger.warning(f"Rate limit exceeded for {url}. Response: {e.response.text}")
            return None, f"Rate limit exceeded: {e.response.text}"
        logger.error(f"HTTP error for {url}: {e}. Response text: {e.response.text if e.response else 'No response text'}")
        return None, f"HTTP error: {e}. Response: {e.response.text if e.response else 'N/A'}"
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {url}: {e}", exc_info=True)
        return None, f"Request error: {e}"
    except ValueError as e: # JSONDecodeError inherits from ValueError
        logger.error(f"JSON decode error for {url}. Response text: {response.text[:200] if response else 'No response'}. Error: {e}")
        return None, f"JSON decode error: {e}. Response: {response.text[:200] if response else 'No response'}"
    except Exception as e:
        logger.error(f"Unexpected error for {url}: {e}", exc_info=True)
        return None, f"Unexpected error: {str(e)}"


def test_stock_aggregates(symbol: str, from_date: str, to_date: str, timespan='day'):
    logger.info(f"\n--- Testing Stock Aggregates (Bars) for {symbol} from {from_date} to {to_date} ({timespan}) ---")
    endpoint = f"/v2/aggs/ticker/{symbol}/range/1/{timespan}/{from_date}/{to_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000 # Max limit
    }

    data, error_message = make_polygon_request(endpoint, params)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}
    if not data or 'results' not in data or not data['results']:
        logger.warning(f"No aggregate data in 'results' for {symbol}. Response: {data}")
        return {"status": "success_empty", "message": "No aggregate data in 'results'.",
                "data": pd.DataFrame(columns=['timestamp_ms', 'price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'vwap', 'transactions'])}

    # Polygon v2 aggs results are in 'results' list
    # keys are: v (volume), vw (vwap), o (open), c (close), h (high), l (low), t (timestamp_ms), n (transactions)
    df = pd.DataFrame(data['results'])
    if df.empty:
        logger.warning(f"DataFrame is empty after processing aggregates for {symbol}.")
        return {"status": "success_empty", "message": "DataFrame empty after processing.", "data": df}

    df.rename(columns={
        't': 'timestamp_ms', 'o': 'open_price', 'h': 'high_price',
        'l': 'low_price', 'c': 'close_price', 'v': 'volume',
        'vw': 'vwap', 'n': 'transactions'
    }, inplace=True)

    df['price_date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.date
    logger.info(f"Successfully fetched {len(df)} aggregate bars for {symbol}.")
    return {"status": "success", "message": None, "data": df[['price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'vwap', 'transactions', 'timestamp_ms']]}

def test_ticker_details(symbol: str):
    logger.info(f"\n--- Testing Ticker Details for {symbol} ---")
    endpoint = f"/v3/reference/tickers/{symbol}"
    params = {}

    data, error_message = make_polygon_request(endpoint, params)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}

    # v3 ticker details are usually under a 'results' key which is a dict
    details_data = data.get('results') if isinstance(data, dict) else None
    if not details_data or not isinstance(details_data, dict):
        logger.warning(f"No ticker details data or unexpected format for {symbol}. Response: {data}")
        return {"status": "success_empty", "message": "No ticker details or unexpected format.", "data": data}

    logger.info(f"Successfully fetched ticker details for {symbol}.")
    logger.info(f"Details (sample): {{'name': {details_data.get('name')}, 'market': {details_data.get('market')}, 'primary_exchange': {details_data.get('primary_exchange')}}}")
    df = pd.DataFrame([details_data])
    return {"status": "success", "message": None, "data": df}


def test_ticker_news(symbol: str, limit=5):
    logger.info(f"\n--- Testing Ticker News for {symbol} (limit {limit}) ---")
    # Note: Ticker-specific news might be a premium feature or have changed endpoint.
    # Using /v2/reference/news which is more general but can be filtered by ticker.
    endpoint = "/v2/reference/news"
    params = {
        "ticker": symbol, # Filter by ticker
        "limit": limit,
        "order": "desc" # Get latest news
    }

    data, error_message = make_polygon_request(endpoint, params)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}

    news_list = data.get('results') if isinstance(data, dict) else None # news is under 'results'
    if not news_list or not isinstance(news_list, list):
        logger.warning(f"No news data or unexpected format for {symbol}. Response: {data}")
        return {"status": "success_empty", "message": "No news data or unexpected format.", "data": data}

    logger.info(f"Successfully fetched {len(news_list)} news articles for {symbol}.")
    if news_list:
        sample_news = news_list[0]
        logger.info(f"Sample news: Title='{sample_news.get('title')}', Publisher='{sample_news.get('publisher', {}).get('name')}'")

    df = pd.DataFrame(news_list)
    return {"status": "success", "message": None, "data": df}

def test_market_status():
    logger.info(f"\n--- Testing Market Status ---")
    endpoint = "/v1/marketstatus/now" # Market holidays endpoint
    params = {}

    data, error_message = make_polygon_request(endpoint, params)

    if error_message:
        return {"status": "error", "message": error_message, "data": None}
    if not data or not isinstance(data, dict):
        logger.warning(f"No market status data or unexpected format. Response: {data}")
        return {"status": "success_empty", "message": "No market status data or unexpected format.", "data": data}

    logger.info(f"Successfully fetched market status: {data}")
    df = pd.DataFrame([data])
    return {"status": "success", "message": None, "data": df}


if __name__ == "__main__":
    logger.info("--- Starting Polygon.io API Test Script (using Requests) ---")
    all_results = {}

    symbols_to_test = ["AAPL", "TSLA"]
    to_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    # Polygon API expects dates, not datetimes for from/to in aggs
    from_date_str = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')

    for symbol in symbols_to_test:
        all_results[f"aggregates_{symbol}"] = test_stock_aggregates(symbol, from_date_str, to_date_str)
        time.sleep(REQUEST_DELAY_SECONDS)

        all_results[f"details_{symbol}"] = test_ticker_details(symbol)
        time.sleep(REQUEST_DELAY_SECONDS)

        all_results[f"news_{symbol}"] = test_ticker_news(symbol, limit=3)
        time.sleep(REQUEST_DELAY_SECONDS)

    all_results["market_status"] = test_market_status()
    # No delay after the last call

    logger.info("\n\n--- Polygon.io API Test Script (using Requests) Finished ---")
    logger.info("Summary of Polygon.io test results:")
    for case_name, result in all_results.items():
        data_info = "N/A"
        if result.get('data') is not None:
            if isinstance(result['data'], pd.DataFrame):
                data_info = f"DataFrame shape: {result['data'].shape}"
            elif isinstance(result['data'], dict):
                data_info = f"Dict keys: {list(result['data'].keys()) if result['data'] else 'Empty Dict'}"
            elif isinstance(result['data'], list):
                data_info = f"List length: {len(result['data'])}"

        logger.info(f"  Test Case: {case_name}, Status: {result['status']}, Info: {data_info}, Error/Msg: {result.get('message', 'None')}")

        if result['status'] == 'success' and result['data'] is not None and isinstance(result['data'], pd.DataFrame) and not result['data'].empty:
            if case_name.startswith("aggregates_"):
                logger.debug(f"Sample data for {case_name}:\n{result['data'][['price_date', 'open_price', 'close_price', 'volume']].head().to_string()}")
            elif case_name.startswith("details_"):
                 logger.debug(f"Sample data for {case_name}:\n{result['data'].iloc[0].to_dict()}") # Print dict for single row
            elif case_name.startswith("news_") and not result['data'].empty:
                 sample_news_item = result['data'].iloc[0]
                 logger.debug(f"Sample news (1st) for {case_name}: Title: {sample_news_item.get('title', 'N/A')}, Publisher: {sample_news_item.get('publisher', {}).get('name', 'N/A')}")
            elif case_name == "market_status":
                 logger.debug(f"Market Status data: {result['data'].iloc[0].to_dict()}")

    logger.info("Polygon.io tests complete.")
