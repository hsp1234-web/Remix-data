import finnhub
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FINNHUB_API_KEY = "USER_PROVIDED_FINNHUB_KEY_REDACTED" # Provided API Key

# Finnhub free plan has a rate limit, typically around 60 calls/minute.
# We will add a small delay between different types of calls to be respectful.
REQUEST_DELAY_SECONDS = 2 # Small delay

# Initialize Finnhub client
try:
    finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
    logger.info("Finnhub client initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Finnhub client: {e}", exc_info=True)
    finnhub_client = None

def test_company_profile(symbol: str):
    logger.info(f"\n--- Testing Company Profile for {symbol} ---")
    if not finnhub_client:
        logger.error("Finnhub client not initialized.")
        return {"status": "error", "message": "Client not initialized", "data": None}
    try:
        profile = finnhub_client.company_profile2(symbol=symbol) # Using company_profile2 for more data
        if not profile or not isinstance(profile, dict): # company_profile2 returns a dict
            logger.warning(f"No profile data returned or unexpected format for {symbol}. Response: {profile}")
            return {"status": "success_empty", "message": "No profile data or unexpected format.", "data": profile}

        logger.info(f"Successfully fetched company profile for {symbol}.")
        logger.info(f"Profile data (sample): {{'name': {profile.get('name')}, 'exchange': {profile.get('exchange')}, 'finnhubIndustry': {profile.get('finnhubIndustry')}}}")
        # Convert to DataFrame for consistency in results structure, though it's a single profile
        df = pd.DataFrame([profile])
        return {"status": "success", "message": None, "data": df}
    except finnhub.FinnhubAPIException as e:
        logger.error(f"Finnhub API Exception for {symbol} (Profile): {e}")
        return {"status": "error", "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"Unexpected error for {symbol} (Profile): {e}", exc_info=True)
        return {"status": "error", "message": str(e), "data": None}

def test_stock_quote(symbol: str):
    logger.info(f"\n--- Testing Stock Quote for {symbol} ---")
    if not finnhub_client:
        logger.error("Finnhub client not initialized.")
        return {"status": "error", "message": "Client not initialized", "data": None}
    try:
        quote = finnhub_client.quote(symbol)
        if not quote or not isinstance(quote, dict) or quote.get('c') == 0: # c (current price) is 0 if no data
            logger.warning(f"No quote data returned or current price is 0 for {symbol}. Response: {quote}")
            return {"status": "success_empty", "message": "No quote data or current price is 0.", "data": quote}

        logger.info(f"Successfully fetched quote for {symbol}: Current={quote.get('c')}, High={quote.get('h')}, Low={quote.get('l')}, Open={quote.get('o')}")
        df = pd.DataFrame([quote])
        # Add timestamp for when the quote was fetched, as 't' is a Unix timestamp of the quote itself
        df['retrieved_at_utc'] = datetime.now(timezone.utc)
        return {"status": "success", "message": None, "data": df}
    except finnhub.FinnhubAPIException as e:
        logger.error(f"Finnhub API Exception for {symbol} (Quote): {e}")
        return {"status": "error", "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"Unexpected error for {symbol} (Quote): {e}", exc_info=True)
        return {"status": "error", "message": str(e), "data": None}

def test_stock_candles(symbol: str, resolution: str, start_dt: datetime, end_dt: datetime):
    logger.info(f"\n--- Testing Stock Candles for {symbol} (Resolution: {resolution}) from {start_dt.date()} to {end_dt.date()} ---")
    if not finnhub_client:
        logger.error("Finnhub client not initialized.")
        return {"status": "error", "message": "Client not initialized", "data": None}
    try:
        # Convert datetime to UNIX timestamps
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        candles = finnhub_client.stock_candles(symbol, resolution, start_ts, end_ts)
        if not candles or candles.get('s') != 'ok' or not candles.get('c'): # Check status and if close prices exist
            logger.warning(f"No candle data returned or status not 'ok' for {symbol}. Response: {candles}")
            # Return an empty DataFrame with expected columns if no data
            return {"status": "success_empty",
                    "message": "No candle data or status not 'ok'.",
                    "data": pd.DataFrame(columns=['t_unix', 'price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume'])}

        df = pd.DataFrame({
            't_unix': candles['t'],
            'open_price': candles['o'],
            'high_price': candles['h'],
            'low_price': candles['l'],
            'close_price': candles['c'],
            'volume': candles['v']
        })
        df['price_date'] = pd.to_datetime(df['t_unix'], unit='s').dt.date
        logger.info(f"Successfully fetched {len(df)} candles for {symbol}.")
        return {"status": "success", "message": None, "data": df[['price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 't_unix']]}
    except finnhub.FinnhubAPIException as e:
        logger.error(f"Finnhub API Exception for {symbol} (Candles): {e}")
        return {"status": "error", "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"Unexpected error for {symbol} (Candles): {e}", exc_info=True)
        return {"status": "error", "message": str(e), "data": None}

def test_basic_financials(symbol: str):
    logger.info(f"\n--- Testing Basic Financials for {symbol} ---")
    if not finnhub_client:
        logger.error("Finnhub client not initialized.")
        return {"status": "error", "message": "Client not initialized", "data": None}
    try:
        financials = finnhub_client.company_basic_financials(symbol, 'all') # 'all' for all metrics
        if not financials or not financials.get('metric') or not isinstance(financials.get('series'), dict):
            logger.warning(f"No basic financials data or unexpected format for {symbol}. Response: {financials}")
            return {"status": "success_empty", "message": "No basic financials data or unexpected format.", "data": financials}

        logger.info(f"Successfully fetched basic financials for {symbol}.")
        # The data structure is a bit complex: financials['series']['quarterly'] has arrays per metric
        # For a simple test, we'll just show some available metrics.
        # A full connector would parse this into a structured DataFrame.
        logger.info(f"Available metric types: {financials.get('metricType')}")
        logger.info(f"Sample metrics (quarterly): {list(financials.get('series', {}).get('quarterly', {}).keys())[:5]}")
        # For simplicity, returning the raw dict for now.
        return {"status": "success", "message": None, "data": financials}
    except finnhub.FinnhubAPIException as e:
        logger.error(f"Finnhub API Exception for {symbol} (Basic Financials): {e}")
        return {"status": "error", "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"Unexpected error for {symbol} (Basic Financials): {e}", exc_info=True)
        return {"status": "error", "message": str(e), "data": None}

def test_market_news(category='general', min_id=0):
    logger.info(f"\n--- Testing Market News (Category: {category}) ---")
    if not finnhub_client:
        logger.error("Finnhub client not initialized.")
        return {"status": "error", "message": "Client not initialized", "data": None}
    try:
        news = finnhub_client.general_news(category, min_id=min_id)
        if not news or not isinstance(news, list):
            logger.warning(f"No news data returned or unexpected format for category {category}. Response: {news}")
            return {"status": "success_empty", "message": "No news data or unexpected format.", "data": news}

        logger.info(f"Successfully fetched {len(news)} news articles for category '{category}'.")
        if news:
            logger.info(f"Sample news item: Headline='{news[0].get('headline')}', Source='{news[0].get('source')}'")
        df = pd.DataFrame(news)
        return {"status": "success", "message": None, "data": df}
    except finnhub.FinnhubAPIException as e:
        logger.error(f"Finnhub API Exception for Market News (Category: {category}): {e}")
        return {"status": "error", "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"Unexpected error for Market News (Category: {category}): {e}", exc_info=True)
        return {"status": "error", "message": str(e), "data": None}

if __name__ == "__main__":
    if not finnhub_client:
        logger.error("Exiting script as Finnhub client could not be initialized.")
    else:
        logger.info("--- Starting Finnhub API Test Script ---")
        all_results = {}

        # Test cases
        symbols_to_test = ["AAPL", "TSLA"]

        for symbol in symbols_to_test:
            all_results[f"profile_{symbol}"] = test_company_profile(symbol)
            time.sleep(REQUEST_DELAY_SECONDS)

            all_results[f"quote_{symbol}"] = test_stock_quote(symbol)
            time.sleep(REQUEST_DELAY_SECONDS)

            # Stock Candles: Test for 1 month of daily data
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=30)
            all_results[f"candles_{symbol}"] = test_stock_candles(symbol, 'D', start_date, end_date)
            time.sleep(REQUEST_DELAY_SECONDS)

            all_results[f"financials_{symbol}"] = test_basic_financials(symbol)
            time.sleep(REQUEST_DELAY_SECONDS)

        all_results["news_general"] = test_market_news('general')
        time.sleep(REQUEST_DELAY_SECONDS)
        all_results["news_forex"] = test_market_news('forex')
        # time.sleep(REQUEST_DELAY_SECONDS) # No delay after last call

        logger.info("\n\n--- Finnhub API Test Script Finished ---")
        logger.info("Summary of Finnhub test results:")
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

            # Optionally print some data for successful calls
            if result['status'] == 'success' and result['data'] is not None:
                if case_name.startswith("candles_") and not result['data'].empty:
                    logger.debug(f"Sample data for {case_name}:\n{result['data'].head().to_string()}")
                elif case_name.startswith("profile_") and not result['data'].empty:
                    logger.debug(f"Sample data for {case_name}:\n{result['data'].iloc[0]}")
                elif case_name.startswith("quote_") and not result['data'].empty:
                     logger.debug(f"Sample data for {case_name}:\n{result['data'].iloc[0]}")
                elif case_name.startswith("news_") and not result['data'].empty:
                     logger.debug(f"Sample news (1st) for {case_name}: {result['data'].iloc[0]['headline'] if not result['data'].empty else 'N/A'}")

    logger.info("Finnhub tests complete.")
