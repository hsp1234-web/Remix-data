# data_pipeline/fetcher_service.py
import os
import argparse
import logging
import pandas as pd
# import duckdb # No longer directly used in this file
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from .db_utils import save_dataframe  # Import the utility function

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_fmp_api_key():
    load_dotenv()
    api_key = os.getenv("API_KEY_FMP")
    if not api_key:
        raise ValueError("API_KEY_FMP not found in environment variables.")
    return api_key


def fetch_single_symbol(symbol, api_key):
    """Fetches daily price data for a single symbol."""
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={api_key}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json().get("historical", [])
        if not data:
            logging.warning(f"No historical data for {symbol}.")
            return None
        df = pd.DataFrame(data)
        df['symbol'] = symbol
        return df
    except requests.RequestException as e:
        logging.error(f"Failed to fetch {symbol}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Fetcher Service - Fetches raw data from FMP API.")
    parser.add_argument("--symbols", required=True,
                        help="Comma-separated list of stock symbols.")
    parser.add_argument("--output-db", required=True,
                        help="Path to the output DuckDB raw database.")
    args = parser.parse_args()

    api_key = get_fmp_api_key()
    symbols_list = [s.strip().upper() for s in args.symbols.split(',')]
    all_data = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_single_symbol, symbol, api_key)
                   for symbol in symbols_list]
        for future in futures:
            result_df = future.result()
            if result_df is not None:
                all_data.append(result_df)

    if not all_data:
        logging.info("No data fetched for any symbol. Exiting.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    logging.info(
        f"Successfully fetched a total of {len(full_df)} rows for {len(symbols_list)} symbols.")

    # Define table name and primary keys for raw_prices
    table_name = "raw_prices"
    primary_keys = ["date", "symbol"]

    # Select and ensure correct order of columns for saving
    # This also handles potential extra columns in full_df that are not in the DB schema
    columns_to_save = ['date', 'symbol', 'open',
                       'high', 'low', 'close', 'adjClose', 'volume']
    df_to_save = full_df[columns_to_save]

    # Save to DuckDB using the utility function
    try:
        save_dataframe(df_to_save, table_name, primary_keys, args.output_db)
        logging.info(
            f"Raw data saved successfully using db_utils to table '{table_name}' in '{args.output_db}'.")
    except Exception as e:
        logging.error(f"Failed to save data using db_utils: {e}")
        # Depending on desired behavior, you might want to exit or raise the exception
        # For now, just log and continue, or you could sys.exit(1)


if __name__ == "__main__":
    main()
