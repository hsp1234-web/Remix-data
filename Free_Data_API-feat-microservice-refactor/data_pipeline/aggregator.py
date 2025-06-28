import pandas as pd
import logging

logger = logging.getLogger(__name__)


def resample_ohlcv(df: pd.DataFrame, rule: str, ohlc_col_map: dict = None) -> pd.DataFrame | None:
    """
    Resamples OHLCV DataFrame to a new time frequency.

    Args:
        df (pd.DataFrame): Input DataFrame with a DatetimeIndex and OHLCV columns.
                           Expected columns: 'open', 'high', 'low', 'close', 'volume'.
                           If column names are different, use ohlc_col_map.
        rule (str): The offset string or object representing target conversion (e.g., 'W-FRI', 'M', 'Q').
        ohlc_col_map (dict, optional): A dictionary to map custom column names to standard
                                       OHLCV names. Example: {'Open': 'open', 'High': 'high', ...}.
                                       Defaults to None, assuming standard names.

    Returns:
        pd.DataFrame | None: Resampled DataFrame with standard OHLCV columns, or None if an error occurs.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.error("Input DataFrame must have a DatetimeIndex.")
        # Attempt to set index if 'date' column exists and is datetime-like
        if 'date' in df.columns:
            try:
                df_copy = df.copy()  # Work on a copy
                df_copy['date'] = pd.to_datetime(df_copy['date'])
                df_copy = df_copy.set_index('date')
                logger.info(
                    "Automatically set 'date' column as DatetimeIndex.")
                df = df_copy
            except Exception as e:
                logger.error(
                    f"Failed to automatically set 'date' column as index: {e}")
                return None
        else:
            logger.error("No 'date' column found to set as DatetimeIndex.")
            return None

    if df.empty:
        logger.warning("Input DataFrame is empty. Returning empty DataFrame.")
        return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])

    # Standard column names
    standard_cols = {
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume'
    }

    # Rename columns if a map is provided
    df_to_resample = df.copy()  # Work on a copy
    if ohlc_col_map:
        df_to_resample.rename(columns=ohlc_col_map, inplace=True)

    # Check if all standard columns are present
    missing_cols = [col for col in standard_cols.keys(
    ) if col not in df_to_resample.columns]
    if missing_cols:
        logger.error(
            f"Missing required OHLCV columns after mapping: {missing_cols}")
        return None

    aggregation_rules = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }

    try:
        logger.info(f"Resampling data to rule: {rule}")
        resampled_df = df_to_resample.resample(rule).agg(aggregation_rules)

        # Drop rows where all OHLCV values are NaN (typically happens for periods with no trades)
        # but keep rows if volume is 0 but OHLC exists (e.g. from 'first'/'last' propagation)
        # A common case is when a period has no trades, 'volume' will be sum (0), and OHLC will be NaN.
        # If 'volume' is NaN (e.g. if input df had NaN volume), it means no data at all.
        resampled_df.dropna(
            subset=['open', 'high', 'low', 'close', 'volume'], how='all', inplace=True)

        # Handle potential NaNs in OHLC if volume is 0 for a resampled period
        # For periods where volume sum is 0 (no trades in the period but it's a valid period),
        # 'first', 'max', 'min', 'last' might yield NaN.
        # If volume is 0, OHLC should ideally be the previous close, but resample doesn't do that out of box.
        # For simplicity, if volume is 0 and OHLC are NaN, we can fill OHLC with the 'close' of that period
        # if it's due to 'last' propagation, or leave as NaN if no data propagated.
        # A common approach is to fill NaNs in OHLC with the previous close if volume is zero.
        # However, resample().agg already handles this to some extent.
        # 'first' and 'last' will propagate values if there's at least one trade.
        # If a period has zero trades, all will be NaN for OHLC and 0 for volume.
        # These are often dropped or handled carefully.
        # For now, we keep the dropna(how='all') which removes periods with no data at all.
        # If a period has 0 volume, but OHLC were propagated (e.g. from a single tick), they'd be kept.

        logger.info(
            f"Resampling successful. Original rows: {len(df_to_resample)}, Resampled rows: {len(resampled_df)}")
        return resampled_df

    except Exception as e:
        logger.error(f"Error during resampling: {e}", exc_info=True)
        return None


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logger.info("Running aggregator.py directly for testing resample_ohlcv.")

    # Create a sample daily DataFrame
    data = {
        'date': pd.to_datetime([
            '2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
            '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10',
            '2023-01-15', '2023-01-16',  # Week 3
            '2023-02-01', '2023-02-02'  # Month 2
        ]),
        'Open':  [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
        'High':  [10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5, 20.5, 21.5, 22.5, 23.5],
        'Low':   [9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5, 20.5, 21.5, 22.5],
        'Close': [10.2, 11.2, 12.2, 13.2, 14.2, 15.2, 16.2, 17.2, 18.2, 19.2, 20.2, 21.2, 22.2, 23.2],
        'Volume': [100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230]
    }
    daily_df = pd.DataFrame(data)
    # daily_df = daily_df.set_index('date') # resample_ohlcv will handle this

    ohlc_map = {'Open': 'open', 'High': 'high',
                'Low': 'low', 'Close': 'close', 'Volume': 'volume'}

    logger.info("\n--- Testing Weekly Resampling (End of Week Friday) ---")
    weekly_df_fri = resample_ohlcv(
        daily_df.copy(), 'W-FRI', ohlc_col_map=ohlc_map)
    if weekly_df_fri is not None:
        print(weekly_df_fri)

    logger.info("\n--- Testing Weekly Resampling (End of Week Sunday) ---")
    weekly_df_sun = resample_ohlcv(
        daily_df.copy(), 'W-SUN', ohlc_col_map=ohlc_map)
    if weekly_df_sun is not None:
        print(weekly_df_sun)

    logger.info("\n--- Testing Monthly Resampling ---")
    # Pandas default is 'ME' (Month End)
    monthly_df = resample_ohlcv(daily_df.copy(), 'M', ohlc_col_map=ohlc_map)
    if monthly_df is not None:
        print(monthly_df)

    logger.info("\n--- Testing Quarterly Resampling ---")
    # Pandas default is 'QE' (Quarter End)
    quarterly_df = resample_ohlcv(daily_df.copy(), 'Q', ohlc_col_map=ohlc_map)
    if quarterly_df is not None:
        print(quarterly_df)

    logger.info("\n--- Testing with standard column names (no map) ---")
    standard_daily_df = daily_df.rename(columns=ohlc_map).set_index('date')
    weekly_standard_df = resample_ohlcv(standard_daily_df.copy(), 'W-FRI')
    if weekly_standard_df is not None:
        print(weekly_standard_df)

    logger.info("\n--- Testing with empty DataFrame ---")
    empty_df = pd.DataFrame(
        columns=['date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    # empty_df = empty_df.set_index('date')
    resampled_empty = resample_ohlcv(empty_df, 'W-FRI', ohlc_col_map=ohlc_map)
    if resampled_empty is not None:
        print(resampled_empty)

    logger.info(
        "\n--- Testing with DataFrame missing DatetimeIndex and 'date' column ---")
    no_date_df = pd.DataFrame({'Open': [1, 2], 'High': [1, 2], 'Low': [
                              1, 2], 'Close': [1, 2], 'Volume': [1, 2]})
    resampled_no_date = resample_ohlcv(
        no_date_df, 'W-FRI', ohlc_col_map=ohlc_map)
    if resampled_no_date is None:
        logger.info(
            "Correctly returned None for DataFrame missing date information.")

    logger.info("\n--- Testing with DataFrame missing an OHLCV column ---")
    missing_col_df = daily_df.copy().set_index('date').rename(
        columns=ohlc_map).drop(columns=['volume'])
    resampled_missing_col = resample_ohlcv(missing_col_df, 'W-FRI')
    if resampled_missing_col is None:
        logger.info(
            "Correctly returned None for DataFrame missing a required column (volume).")

    logger.info("Aggregator tests finished.")
