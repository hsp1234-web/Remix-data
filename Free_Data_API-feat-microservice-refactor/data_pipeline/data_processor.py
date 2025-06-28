import pandas as pd
import logging
from .aggregator import resample_ohlcv  # Use relative import

logger = logging.getLogger(__name__)


def process_price_data(daily_df: pd.DataFrame, symbol: str) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    Processes daily price data:
    1. Standardizes columns (e.g., lowercase, renames).
    2. Calculates MA20.
    3. Resamples to weekly data.

    Args:
        daily_df (pd.DataFrame): Input DataFrame with daily prices.
                                 Expected to have 'date' and OHLCV columns.
        symbol (str): The stock symbol, used for adding a 'symbol' column.

    Returns:
        tuple[pd.DataFrame | None, pd.DataFrame | None]:
            - Processed daily DataFrame (with 'symbol' and 'ma20').
            - Resampled weekly DataFrame (with 'symbol').
            Returns (None, None) if processing fails.
    """
    if daily_df is None or daily_df.empty:
        logger.warning(
            f"Input daily DataFrame for {symbol} is None or empty. Skipping processing.")
        return None, None

    logger.info(f"Processing daily price data for {symbol}...")
    processed_daily_df = daily_df.copy()

    # --- 1. Standardize columns and ensure 'date' is index ---
    # Assuming columns from FMP are like: 'date', 'open', 'high', 'low', 'close', 'adjClose', 'volume', ...
    # We need to ensure they are suitable for processing and storage.

    # Rename columns to lowercase for consistency if needed (FMP already provides lowercase for historical)
    # processed_daily_df.columns = [col.lower() for col in processed_daily_df.columns]

    if 'date' not in processed_daily_df.columns:
        logger.error(f"'date' column missing in daily data for {symbol}.")
        return None, None

    try:
        processed_daily_df['date'] = pd.to_datetime(processed_daily_df['date'])
        # Setting date as index for MA calculation and resampling, will reset later for storage.
        processed_daily_df = processed_daily_df.set_index('date')
    except Exception as e:
        logger.error(
            f"Failed to convert 'date' column to datetime or set as index for {symbol}: {e}", exc_info=True)
        return None, None

    # Ensure essential OHLCV columns exist
    ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
    missing_essential_cols = [
        col for col in ohlcv_cols if col not in processed_daily_df.columns]
    if missing_essential_cols:
        logger.error(
            f"Missing essential OHLCV columns for {symbol}: {missing_essential_cols}")
        return None, None

    # --- 2. Calculate MA20 ---
    if 'close' in processed_daily_df.columns:
        try:
            processed_daily_df['ma20'] = processed_daily_df['close'].rolling(
                window=20).mean()
            logger.info(f"Calculated MA20 for {symbol}.")
        except Exception as e:
            logger.warning(
                f"Could not calculate MA20 for {symbol}: {e}. Skipping MA20.", exc_info=True)
            # Add column with None if calculation fails
            processed_daily_df['ma20'] = None
    else:
        logger.warning(
            f"'close' column not found for {symbol}, cannot calculate MA20.")
        processed_daily_df['ma20'] = None

    # Add symbol column before returning (and before resampling if resampler doesn't add it)
    processed_daily_df['symbol'] = symbol.upper()

    # Reset index to have 'date' as a column for storage
    processed_daily_df.reset_index(inplace=True)

    # --- 3. Resample to weekly data ---
    logger.info(f"Resampling daily data to weekly for {symbol}...")
    # The aggregator expects 'date' as index.
    # We already set it for MA20 calculation. If not, set it here.
    # df_for_resample = processed_daily_df.set_index('date', drop=False) # Keep date as column too

    # Pass the DataFrame with DatetimeIndex to resample_ohlcv
    # The `resample_ohlcv` expects standard column names, which FMP usually provides.
    # If not, an ohlc_col_map would be needed.
    # Our current daily_df from FMP should have lowercase 'open', 'high', 'low', 'close', 'volume'.

    # Re-set index for resample_ohlcv as it expects DatetimeIndex
    temp_daily_for_resample = processed_daily_df.set_index('date')

    # Default rule W-FRI (Week ending Friday)
    weekly_df = resample_ohlcv(temp_daily_for_resample, rule='W-FRI')

    if weekly_df is not None and not weekly_df.empty:
        # Ensure symbol column is in weekly data
        weekly_df['symbol'] = symbol.upper()
        weekly_df.reset_index(inplace=True)  # To get 'date' as a column
        logger.info(
            f"Successfully resampled to weekly data for {symbol}. Shape: {weekly_df.shape}")
    elif weekly_df is not None and weekly_df.empty:
        logger.warning(
            f"Weekly resampling for {symbol} resulted in an empty DataFrame.")
        # weekly_df will be an empty DF, which is fine.
    else:
        logger.warning(
            f"Weekly resampling failed for {symbol}. Proceeding without weekly data.")
        # weekly_df will be None

    logger.info(
        f"Finished processing price data for {symbol}. Daily shape: {processed_daily_df.shape}")
    return processed_daily_df, weekly_df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logger.info(
        "Running data_processor.py directly for testing process_price_data.")

    # Create a sample daily DataFrame (mimicking FMP output)
    sample_dates = pd.to_datetime([
        '2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
        '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10',
        '2023-01-11', '2023-01-12', '2023-01-13', '2023-01-14', '2023-01-15',
        '2023-01-16', '2023-01-17', '2023-01-18', '2023-01-19', '2023-01-20',
        '2023-01-21', '2023-01-22', '2023-01-23', '2023-01-24', '2023-01-25'
    ])
    data = {
        'date': sample_dates,
        'open':  [i + 10 for i in range(len(sample_dates))],
        'high':  [i + 10.5 for i in range(len(sample_dates))],
        'low':   [i + 9.5 for i in range(len(sample_dates))],
        'close': [i + 10.2 for i in range(len(sample_dates))],
        'adjClose': [i + 10.2 for i in range(len(sample_dates))],
        'volume': [100 + i*10 for i in range(len(sample_dates))],
        'unadjustedVolume': [100 + i*10 for i in range(len(sample_dates))],
        'change': [0.2 + (0.1 * (i % 2)) for i in range(len(sample_dates))],
        'changePercent': [0.02 + (0.001 * (i % 2)) for i in range(len(sample_dates))],
        'vwap': [i + 10.1 for i in range(len(sample_dates))],
        'label': [d.strftime('%B %d, %Y') for d in sample_dates],
        'changeOverTime': [0.01 * i for i in range(len(sample_dates))]
    }
    sample_daily_df = pd.DataFrame(data)
    test_symbol = "TESTPROC"

    logger.info(f"\n--- Input Daily Data for {test_symbol} ---")
    print(sample_daily_df.head())

    processed_daily, processed_weekly = process_price_data(
        sample_daily_df.copy(), test_symbol)

    if processed_daily is not None:
        logger.info(
            f"\n--- Processed Daily Data for {test_symbol} (with MA20 and symbol) ---")
        print(processed_daily.head())
        print(processed_daily.tail())
        if 'ma20' in processed_daily.columns:
            logger.info(f"MA20 NaNs: {processed_daily['ma20'].isna().sum()}")
        if 'symbol' in processed_daily.columns:
            logger.info(
                f"Symbol column unique values: {processed_daily['symbol'].unique()}")

    if processed_weekly is not None:
        logger.info(
            f"\n--- Resampled Weekly Data for {test_symbol} (W-FRI) ---")
        print(processed_weekly)
        if 'symbol' in processed_weekly.columns:
            logger.info(
                f"Symbol column unique values in weekly: {processed_weekly['symbol'].unique()}")

    logger.info("\n--- Testing with empty input ---")
    empty_daily, empty_weekly = process_price_data(pd.DataFrame(), "EMPTYTEST")
    assert empty_daily is None
    assert empty_weekly is None
    logger.info("Empty input test completed (should return None, None).")

    logger.info("\n--- Testing with missing 'date' column ---")
    missing_date_df = sample_daily_df.drop(columns=['date'])
    md_daily, md_weekly = process_price_data(
        missing_date_df, "MISSINGDATETEST")
    assert md_daily is None
    assert md_weekly is None
    logger.info(
        "Missing 'date' column test completed (should return None, None).")

    logger.info("\n--- Testing with missing 'close' column (for MA20) ---")
    missing_close_df = sample_daily_df.drop(columns=['close'])
    mc_daily, mc_weekly = process_price_data(
        missing_close_df.copy(), "MISSINGCLOSETEST")
    if mc_daily is not None:
        assert 'ma20' in mc_daily.columns
        assert mc_daily['ma20'].isna().all()
        logger.info(
            "Missing 'close' column test: MA20 column exists and is all NaN (Correct).")
    else:
        logger.error(
            "Missing 'close' column test: processed_daily was None, unexpected.")

    logger.info("Data processor tests finished.")
