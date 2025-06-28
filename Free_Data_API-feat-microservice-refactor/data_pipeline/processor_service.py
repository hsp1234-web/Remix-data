# data_pipeline/processor_service.py
import argparse
import logging
import pandas as pd
import duckdb  # Still needed for read access con_in
from .db_utils import save_dataframe  # Import the utility function

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def process_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Processes raw price data to calculate indicators and resample weekly."""
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    # Calculate daily indicators
    df['ma20'] = df.groupby('symbol')['close'].transform(
        lambda x: x.rolling(window=20).mean())

    # Resample to weekly
    weekly_df = df.groupby('symbol').resample('W-FRI').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    return df.reset_index(), weekly_df.reset_index()


def main():
    parser = argparse.ArgumentParser(
        description="Processor Service - Cleans and transforms raw data.")
    parser.add_argument("--input-db", required=True,
                        help="Path to the input DuckDB raw database.")
    parser.add_argument("--output-db", required=True,
                        help="Path to the output DuckDB features database.")
    args = parser.parse_args()

    con_in = duckdb.connect(args.input_db, read_only=True)
    raw_df = con_in.execute("SELECT * FROM raw_prices").fetchdf()
    con_in.close()

    if raw_df.empty:
        logging.info("Raw data is empty. Nothing to process.")
        return

    logging.info(
        f"Read {len(raw_df)} rows from raw database. Starting processing...")
    processed_daily_df, processed_weekly_df = process_data(raw_df)

    # Define primary keys for the feature tables
    daily_features_pk = ['date', 'symbol']
    # Assuming 'date' in weekly_df is the resampled week-ending date
    weekly_features_pk = ['date', 'symbol']

    try:
        if not processed_daily_df.empty:
            save_dataframe(processed_daily_df, "daily_features",
                           daily_features_pk, args.output_db)
            logging.info(
                f"Saved {len(processed_daily_df)} rows to 'daily_features' table using db_utils.")
        else:
            logging.info("No daily features data to save.")

        if not processed_weekly_df.empty:
            save_dataframe(processed_weekly_df, "weekly_features",
                           weekly_features_pk, args.output_db)
            logging.info(
                f"Saved {len(processed_weekly_df)} rows to 'weekly_features' table using db_utils.")
        else:
            logging.info("No weekly features data to save.")

    except Exception as e:
        logging.error(f"Failed to save processed data using db_utils: {e}")
        # Depending on desired behavior, you might want to exit or raise the exception

    logging.info(
        f"Data processing complete. Features saved to '{args.output_db}'.")


if __name__ == "__main__":
    main()
