import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_yfinance_data(ticker_symbol: str, start_date: str, end_date: str) -> tuple[pd.DataFrame | None, str | None]:
    """
    Fetches historical market data for a given ticker symbol from Yahoo Finance.

    Args:
        ticker_symbol (str): The ticker symbol (e.g., "AAPL", "2330.TW").
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.

    Returns:
        tuple[pd.DataFrame | None, str | None]: A tuple containing the DataFrame with
                                                 standardized columns if successful,
                                                 None otherwise, and an error message
                                                 string if an error occurred, None otherwise.
    """
    logger.info(f"Fetching yfinance data for ticker: {ticker_symbol}, from {start_date} to {end_date}")
    try:
        ticker = yf.Ticker(ticker_symbol)
        # interval="1d" is default for daily data
        hist_df = ticker.history(start=start_date, end=end_date, auto_adjust=False) # auto_adjust=False to get 'Adj Close' separately and raw OHLC

        if hist_df.empty:
            logger.warning(f"No data returned by yfinance for ticker: {ticker_symbol} in the given date range.")
            # Return an empty DataFrame with expected columns if no data
            return pd.DataFrame(columns=[
                'price_date', 'security_id', 'open_price', 'high_price',
                'low_price', 'close_price', 'adj_close_price', 'volume', 'source_api',
                'last_updated_timestamp', 'dividends', 'stock_splits'
            ]), None

        logger.debug(f"Raw yfinance data for {ticker_symbol} (first 5 rows):\n{hist_df.head()}")

        # Reset index to make 'Date' a column
        hist_df.reset_index(inplace=True)

        # Standardize column names
        # yfinance columns are typically: Date, Open, High, Low, Close, Adj Close, Volume, Dividends, Stock Splits
        rename_map = {
            "Date": "price_date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Adj Close": "adj_close_price", # Keep adjusted close
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits"
        }
        # Only rename columns that exist in the DataFrame
        existing_rename_map = {k: v for k, v in rename_map.items() if k in hist_df.columns}
        df = hist_df.rename(columns=existing_rename_map)


        # Convert 'price_date' to date object (it's already datetime from yf, just ensure correct type)
        # Ensure it is timezone-naive before converting to just date, or handle timezone appropriately.
        # yfinance usually returns timezone-aware datetimes for Date if the market has a timezone.
        # For consistency, we'll make it timezone-naive date.
        if pd.api.types.is_datetime64_any_dtype(df['price_date']):
            if df['price_date'].dt.tz is not None:
                 df['price_date'] = df['price_date'].dt.tz_convert(None).dt.date
            else:
                 df['price_date'] = df['price_date'].dt.date
        else: # If it's not datetime (e.g. already string from some manipulation)
            df['price_date'] = pd.to_datetime(df['price_date'], errors='coerce').dt.date


        # Add additional standardized columns
        df['security_id'] = ticker_symbol
        df['source_api'] = "yfinance"
        df['last_updated_timestamp'] = datetime.now(timezone.utc)

        # Select and order columns according to a potential canonical model for price data
        # Ensure all expected columns are present
        canonical_price_columns = [
            'price_date', 'security_id', 'open_price', 'high_price',
            'low_price', 'close_price', 'adj_close_price', 'volume', 'source_api',
            'last_updated_timestamp', 'dividends', 'stock_splits' # Include dividends and stock splits
        ]

        # Ensure all canonical columns exist, fill with None if not in original df
        for col in canonical_price_columns:
            if col not in df.columns:
                df[col] = None

        final_df = df[canonical_price_columns]

        # Drop rows where essential data (price_date or close_price) became NaT/NaN
        final_df.dropna(subset=['price_date', 'close_price'], inplace=True)

        if final_df.empty and not hist_df.empty:
             logger.warning(f"DataFrame for {ticker_symbol} became empty after dropping rows with invalid dates or close prices.")

        logger.info(f"Successfully fetched and transformed data for {ticker_symbol}. Shape: {final_df.shape}")
        return final_df, None

    except Exception as e:
        logger.error(f"An error occurred while fetching or processing data for {ticker_symbol}: {e}", exc_info=True)
        return None, f"Error for {ticker_symbol}: {str(e)}"

if __name__ == "__main__":
    logger.info("--- Starting yfinance API Test Script ---")

    tickers_to_test = {
        "AAPL": "Apple Inc.",
        "2330.TW": "TSMC (Taiwan Semiconductor Manufacturing Company)",
        "^GSPC": "S&P 500 Index",
        "^TWII": "TSEC Weighted Index (Taiwan)"
    }
    start_date_test = "2020-01-01"
    end_date_test = "2023-12-31" # yfinance end_date is exclusive, so effectively up to 2023-12-30

    all_ticker_data = {}

    for ticker_symbol, description in tickers_to_test.items():
        logger.info(f"\nTesting Ticker: {ticker_symbol} ({description})")
        df, error_message = fetch_yfinance_data(ticker_symbol, start_date_test, end_date_test)

        if error_message:
            logger.error(f"Failed to fetch data for {ticker_symbol}: {error_message}")
            all_ticker_data[ticker_symbol] = {"status": "error", "message": error_message, "data": None}
        elif df is not None:
            if df.empty:
                logger.warning(f"Received empty DataFrame for {ticker_symbol} (possibly no data in range or ticker delisted/invalid).")
                all_ticker_data[ticker_symbol] = {"status": "success_empty", "message": "No data points returned or ticker invalid/delisted for range.", "data": df}
            else:
                logger.info(f"Successfully fetched data for {ticker_symbol}. First 5 rows:")
                # Log specific columns to avoid overly wide output
                log_df = df[['price_date', 'security_id', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume']].head()
                logger.info("\n" + log_df.to_string())
                all_ticker_data[ticker_symbol] = {"status": "success", "message": None, "data": df}
        else:
            logger.error(f"Failed to fetch data for {ticker_symbol} for an unknown reason (df is None but no error_message).")
            all_ticker_data[ticker_symbol] = {"status": "error", "message": "Unknown error, DataFrame is None.", "data": None}


    logger.info("\n\n--- yfinance API Test Script Finished ---")
    logger.info("Summary of yfinance test results:")
    for ticker_symbol, result in all_ticker_data.items():
        data_shape = result['data'].shape if result['data'] is not None else "N/A"
        logger.info(f"  Ticker: {ticker_symbol}, Status: {result['status']}, Shape: {data_shape}, Error: {result.get('message', 'None')}")

    # Example: Combine if needed
    # combined_yfinance_df = pd.concat([res['data'] for res in all_ticker_data.values() if res['data'] is not None and not res['data'].empty])
    # if not combined_yfinance_df.empty:
    #     logger.info("\n--- Combined yfinance DataFrame (first 10 rows) ---")
    #     logger.info("\n" + combined_yfinance_df[['price_date', 'security_id', 'close_price']].head(10).to_string())
    # else:
    #     logger.info("\nNo yfinance data to combine.")
