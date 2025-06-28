import pandas as pd
from typing import Dict, Any, Optional # Ensure Optional is imported
import numpy as np
import logging
import sys # Not strictly necessary for this script's direct functionality but good practice if other parts use it

# Basic logger setup for the module, will be overridden if a logger_instance is passed to the class
# For atomic script, this logger will likely write to wherever the global logger (from main.py/initialize_global_log) is configured.
logger = logging.getLogger(f"project_logger.{__name__}")
if not logger.handlers and not logging.getLogger().hasHandlers(): # Check root logger too
    logger.addHandler(logging.NullHandler())
    logger.debug(f"Logger for {__name__} (IndicatorEngine module) configured with NullHandler for atomic script.")

class IndicatorEngine:
    """
    封裝計算衍生指標，特別是「債券壓力指標」的邏輯。
    """
    def __init__(self, data_frames: Dict[str, pd.DataFrame], params: Optional[Dict[str, Any]] = None, logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            # Fallback to a module-specific logger if no instance is provided
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                 self.logger.addHandler(logging.NullHandler())
                 self.logger.debug(f"Instance logger for {self.__class__.__name__} using NullHandler for atomic script.")

        self.raw_macro_df = data_frames.get('macro', pd.DataFrame()) # Default to empty DF
        self.raw_move_df = data_frames.get('move', pd.DataFrame())   # Default to empty DF
        self.params = params if params is not None else {}
        self.df_prepared: Optional[pd.DataFrame] = None

        if self.raw_macro_df.empty:
            self.logger.warning("IndicatorEngine initialized: 'macro' data is missing or empty.")
        if self.raw_move_df.empty:
            self.logger.warning("IndicatorEngine initialized: 'move' data (for ^MOVE) is missing or empty.")

    def _prepare_data(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Preparing data for stress index calculation...")

        if self.raw_macro_df.empty:
            self.logger.warning("IndicatorEngine: Macro data (raw_macro_df) is empty. Proceeding without macro indicators for pivot.")
            # Create an empty DataFrame with a DatetimeIndex if MOVE data might exist, to allow merging
            # However, if MOVE is also empty, this won't help much.
            # Consider the case where only MOVE data is present.
            if self.raw_move_df.empty:
                self.logger.error("IndicatorEngine: Both macro and MOVE data are empty. Cannot prepare data.")
                return None
            # If only MOVE data is present, macro_wide_df will be effectively empty or non-existent
            # and combined_df logic should handle it.
            macro_wide_df = pd.DataFrame()
        else:
            try:
                current_macro_df = self.raw_macro_df.copy()
                if 'metric_date' not in current_macro_df.columns:
                    self.logger.error("IndicatorEngine: 'metric_date' column missing in macro data.")
                    return None
                current_macro_df['metric_date'] = pd.to_datetime(current_macro_df['metric_date'], errors='coerce')
                current_macro_df.dropna(subset=['metric_date'], inplace=True)

                if current_macro_df.empty:
                    self.logger.error("IndicatorEngine: Macro data has no valid 'metric_date' entries after coercion.")
                    return None

                # Pivot macro data
                if not all(col in current_macro_df.columns for col in ['metric_name', 'metric_value']):
                    self.logger.error("IndicatorEngine: 'metric_name' or 'metric_value' missing for pivot.")
                    return None
                macro_wide_df = current_macro_df.pivot_table(
                    index='metric_date', columns='metric_name', values='metric_value'
                )
                macro_wide_df.index.name = 'date' # Standardize index name
                self.logger.debug(f"IndicatorEngine: Pivoted macro data shape: {macro_wide_df.shape}")
            except Exception as e:
                self.logger.error(f"IndicatorEngine: Failed to pivot macro_df: {e}", exc_info=True)
                return None

        # Prepare MOVE data
        move_wide_df = pd.DataFrame() # Initialize as empty
        if not self.raw_move_df.empty:
            if all(col in self.raw_move_df.columns for col in ['price_date', 'close_price', 'security_id']):
                move_df_filtered = self.raw_move_df[self.raw_move_df['security_id'] == '^MOVE'].copy()
                if not move_df_filtered.empty:
                    move_df_filtered['price_date'] = pd.to_datetime(move_df_filtered['price_date'], errors='coerce')
                    move_df_filtered.dropna(subset=['price_date'], inplace=True)
                    if not move_df_filtered.empty:
                        # Set index to price_date and rename close_price to ^MOVE
                        move_wide_df = move_df_filtered.set_index('price_date')[['close_price']].rename(columns={'close_price': '^MOVE'})
                        move_wide_df.index.name = 'date' # Standardize index name
                        self.logger.debug(f"IndicatorEngine: Prepared ^MOVE index data. Non-NaN count: {move_wide_df['^MOVE'].notna().sum()}")
                    else:
                        self.logger.warning("IndicatorEngine: ^MOVE data had no valid 'price_date' entries after coercion.")
                else:
                    self.logger.warning("IndicatorEngine: ^MOVE security_id not found in provided yfinance data (raw_move_df).")
            else:
                self.logger.warning("IndicatorEngine: ^MOVE DataFrame (raw_move_df) missing required columns (price_date, close_price, security_id).")
        else:
            self.logger.warning("IndicatorEngine: ^MOVE data (raw_move_df) is missing or empty. ^MOVE index will be NaN if not in macro_wide_df.")

        # Combine macro and MOVE data
        if macro_wide_df.empty and move_wide_df.empty:
            self.logger.error("IndicatorEngine: Both pivoted macro and MOVE data are empty. Cannot combine.")
            return None
        elif macro_wide_df.empty:
            combined_df = move_wide_df
            self.logger.warning("IndicatorEngine: Pivoted macro data was empty, using only MOVE data for combined_df.")
        elif move_wide_df.empty:
            combined_df = macro_wide_df
            if '^MOVE' not in combined_df.columns: # Ensure ^MOVE column exists if it's expected later
                combined_df['^MOVE'] = np.nan
            self.logger.warning("IndicatorEngine: MOVE data was empty, using only macro data for combined_df.")
        else:
            # Outer join to keep all dates, then decide on fill strategy
            combined_df = pd.merge(macro_wide_df, move_wide_df, left_index=True, right_index=True, how='outer')
            self.logger.debug(f"IndicatorEngine: Combined macro and MOVE data. Shape: {combined_df.shape}")

        if '^MOVE' not in combined_df.columns: # Ensure ^MOVE column exists after merge if it wasn't there
                combined_df['^MOVE'] = np.nan

        combined_df.sort_index(inplace=True)
        # Forward fill, then backward fill to handle NaNs robustly
        # Limit ffill/bfill to avoid excessive propagation if data is very sparse, e.g. 7 days
        combined_df = combined_df.ffill(limit=7).bfill(limit=7)
        combined_df.dropna(how='all', inplace=True) # Drop rows where all values are NaN after filling

        if combined_df.empty:
            self.logger.error("IndicatorEngine: Prepared data is empty after merge and fill operations.")
            return None

        self.logger.info(f"IndicatorEngine: Data preparation complete. Final shape: {combined_df.shape}")
        return combined_df

    def calculate_dealer_stress_index(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Calculating Dealer Stress Index...")
        # Always call _prepare_data to get the latest state based on inputs
        current_prepared_data = self._prepare_data()

        if current_prepared_data is None or current_prepared_data.empty:
            self.logger.error("IndicatorEngine: Prepared data is None or empty. Cannot calculate stress index.")
            self.df_prepared = current_prepared_data # Store the (empty) prepared data state
            return None

        # Store the successfully prepared data (potentially including ^MOVE from yfinance)
        # This df_prepared will be used for briefing if calculation is successful.
        self.df_prepared = current_prepared_data.copy()
        df = self.df_prepared.copy() # Work on a copy for calculations

        # Parameters for the index
        window = self.params.get('rolling_window_days', 252)
        weights_config = self.params.get('stress_index_weights', {})
        min_periods_ratio = self.params.get('min_periods_ratio_for_rolling', 0.5) # Ratio of window for min_periods

        # Define components and their expected column names in the prepared DataFrame
        component_map = {
            'sofr_deviation': 'FRED/SOFR_Dev',
            'spread_10y2y': 'spread_10y2y',
            'primary_dealer_position': 'NYFED/PRIMARY_DEALER_NET_POSITION', # This comes from NYFed data
            'move_index': '^MOVE',             # This comes from yfinance data
            'vix_index': 'FRED/VIXCLS',        # This comes from FRED data
            'pos_res_ratio': 'pos_res_ratio'   # Derived from FRED/WRESBAL and NYFED positions
        }
        self.logger.debug(f"IndicatorEngine: Stress Index Params: Window={window}, Weights={weights_config}, MinPeriodsRatio={min_periods_ratio}")

        # Calculate derived components first
        # 1. 10Y-2Y Spread
        if 'FRED/DGS10' in df.columns and 'FRED/DGS2' in df.columns:
            df['spread_10y2y'] = df['FRED/DGS10'] - df['FRED/DGS2']
        else:
            df['spread_10y2y'] = np.nan
            self.logger.warning("IndicatorEngine: FRED/DGS10 or FRED/DGS2 missing. 'spread_10y2y' will be NaN.")

        # 2. SOFR Deviation from its 20-day MA
        if 'FRED/SOFR' in df.columns and df['FRED/SOFR'].notna().sum() >= 20: # Need enough data for MA
             df['FRED/SOFR_MA20'] = df['FRED/SOFR'].rolling(window=20, min_periods=15).mean()
             df['FRED/SOFR_Dev'] = df['FRED/SOFR'] - df['FRED/SOFR_MA20']
        else:
            df['FRED/SOFR_Dev'] = np.nan
            self.logger.warning("IndicatorEngine: FRED/SOFR has insufficient data for 20-day MA or is missing. 'FRED/SOFR_Dev' will be NaN.")

        # 3. Primary Dealer Positions to Reserves Ratio
        if 'NYFED/PRIMARY_DEALER_NET_POSITION' in df.columns and 'FRED/WRESBAL' in df.columns:
            # Ensure WRESBAL (reserves) is not zero to avoid division by zero; replace 0 with NaN
            res_safe = df['FRED/WRESBAL'].replace(0, np.nan)
            df['pos_res_ratio'] = df['NYFED/PRIMARY_DEALER_NET_POSITION'] / res_safe
            df['pos_res_ratio'].replace([np.inf, -np.inf], np.nan, inplace=True) # Handle infinities if res_safe was NaN then became 0 through ops
        else:
            df['pos_res_ratio'] = np.nan
            self.logger.warning("IndicatorEngine: NYFED/PRIMARY_DEALER_NET_POSITION or FRED/WRESBAL missing. 'pos_res_ratio' will be NaN.")

        # Update self.df_prepared to include these newly derived columns before percentile ranking
        # This ensures that the briefing can access these intermediate calculations.
        self.df_prepared = df.copy()

        # Calculate rolling percentiles for each component
        percentiles_df = pd.DataFrame(index=df.index)
        active_component_weights = {} # Store weights of components that are actually used

        min_rolling_periods = max(2, int(window * min_periods_ratio)) # Ensure at least 2 periods

        for key, col_name in component_map.items():
            if weights_config.get(key, 0) == 0: # Skip if weight is zero
                self.logger.debug(f"IndicatorEngine: Skipping rank for {key} ({col_name}) due to zero weight.")
                percentiles_df[f"{key}_pct_rank"] = np.nan # Keep column for completeness if needed
                continue

            if col_name in df.columns and df[col_name].notna().any():
                series_to_rank = df[col_name]
                if series_to_rank.notna().sum() >= min_rolling_periods:
                    # Calculate rolling rank (percentile)
                    # rank(pct=True) gives percentile from 0 to 1. iloc[-1] takes the last value in the window.
                    rolling_percentile = series_to_rank.rolling(window=window, min_periods=min_rolling_periods).apply(
                        lambda x_window: pd.Series(x_window).rank(pct=True).iloc[-1] if pd.Series(x_window).notna().any() else np.nan,
                        raw=False # raw=False needed for DataFrames/Series with datetime index
                    )
                    # For 'spread_10y2y', lower is more stress (inverted yield curve), so invert percentile
                    percentiles_df[f"{key}_pct_rank"] = (1.0 - rolling_percentile) if key == 'spread_10y2y' else rolling_percentile
                    active_component_weights[key] = weights_config[key]
                    self.logger.debug(f"IndicatorEngine: Calculated rolling percentile for {key} ({col_name}).")
                else:
                    self.logger.warning(f"IndicatorEngine: Insufficient data for {col_name} (key: {key}) for rolling rank. Window: {window}, MinPeriods: {min_rolling_periods}, Available: {series_to_rank.notna().sum()}. Skipping rank.")
                    percentiles_df[f"{key}_pct_rank"] = np.nan
            else:
                self.logger.warning(f"IndicatorEngine: Component {key} ({col_name}) not found in prepared data or is all NaN. Skipping rank.")
                percentiles_df[f"{key}_pct_rank"] = np.nan

        if not active_component_weights:
            self.logger.error("IndicatorEngine: No active components with valid data and non-zero weights for stress index calculation.")
            return None # Or return df_prepared to show intermediate steps? For now, None if index fails.

        # Normalize active weights (so they sum to 1)
        total_active_weight = sum(active_component_weights.values())
        if total_active_weight == 0: # Should be caught by above, but safeguard
            self.logger.error("IndicatorEngine: Sum of active component weights is zero. Cannot normalize.")
            return None

        normalized_weights = {k: w / total_active_weight for k, w in active_component_weights.items()}
        self.logger.info(f"IndicatorEngine: Normalized Stress Index Weights (for active components): {normalized_weights}")

        # Calculate the weighted stress index
        # Initialize series for sum of weighted percentiles and sum of effective weights
        final_stress_index_series = pd.Series(0.0, index=df.index)
        sum_of_effective_weights = pd.Series(0.0, index=df.index)

        for component_key, weight in normalized_weights.items():
            percentile_col_name = f"{component_key}_pct_rank"
            if percentile_col_name in percentiles_df.columns and percentiles_df[percentile_col_name].notna().any():
                # Fill NaNs in percentile ranks with 0.5 (neutral) before weighting
                # This assumes that if a component's rank is missing, it contributes neutrally.
                component_contribution = percentiles_df[percentile_col_name].fillna(0.5) * weight
                final_stress_index_series = final_stress_index_series.add(component_contribution, fill_value=0)
                # Track sum of weights for rows where percentile rank was available (not NaN before fillna(0.5))
                sum_of_effective_weights = sum_of_effective_weights.add(percentiles_df[percentile_col_name].notna() * weight, fill_value=0)
            else:
                self.logger.warning(f"IndicatorEngine: Percentile rank column {percentile_col_name} for component {component_key} is missing or all NaN. This component will not contribute to the index.")

        # Adjust index for cases where some components were missing for certain dates
        # by dividing by the sum of effective weights for those dates.
        # Avoid division by zero if sum_of_effective_weights is 0 for some rows.
        adjusted_stress_index = final_stress_index_series.divide(sum_of_effective_weights.replace(0, np.nan))

        # Scale to 0-100 and clip
        final_stress_index_scaled = (adjusted_stress_index * 100).clip(0, 100)

        # Create result DataFrame
        result_df = pd.DataFrame({'DealerStressIndex': final_stress_index_scaled}, index=df.index)
        result_df = result_df.join(percentiles_df) # Join with individual percentile ranks for transparency

        # Drop rows where the final DealerStressIndex is NaN (e.g., if all components were NaN for that date)
        final_result_df = result_df.dropna(subset=['DealerStressIndex'])

        if final_result_df.empty:
            self.logger.warning("IndicatorEngine: Dealer Stress Index is all NaN after calculation and processing.")
            return None # Or an empty DataFrame with the columns?

        self.logger.info(f"IndicatorEngine: Dealer Stress Index calculated successfully. Final shape: {final_result_df.shape}")
        return final_result_df


# Test block for direct execution
if __name__ == '__main__':
    # Setup basic logging for test execution
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_eng_main = logging.getLogger("IndicatorEngineTestRun_Atomic")
    if not test_logger_eng_main.handlers:
        ch_eng_main = logging.StreamHandler(sys.stdout)
        ch_eng_main.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_eng_main.addHandler(ch_eng_main)
        test_logger_eng_main.propagate = False

    # Sample data for testing
    dates_sample = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
                                   '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10'])

    macro_data_test = {
        'metric_date': list(dates_sample) * 6, # Repeat dates for each metric
        'metric_name': (['FRED/DGS10'] * len(dates_sample) + ['FRED/DGS2'] * len(dates_sample) +
                        ['FRED/SOFR'] * len(dates_sample) + ['FRED/VIXCLS'] * len(dates_sample) +
                        ['NYFED/PRIMARY_DEALER_NET_POSITION'] * len(dates_sample) + ['FRED/WRESBAL'] * len(dates_sample)),
        'metric_value': (
            list(np.linspace(3.0, 3.5, len(dates_sample))) + # DGS10
            list(np.linspace(2.0, 2.5, len(dates_sample))) + # DGS2
            list(np.linspace(1.0, 1.2, len(dates_sample))) + # SOFR
            list(np.linspace(15, 25, len(dates_sample))) +   # VIXCLS
            list(np.linspace(1000e6, 1200e6, len(dates_sample))) + # Primary Dealer Positions (example: 1B to 1.2B)
            list(np.linspace(2.5e12, 2.7e12, len(dates_sample)))   # Reserves (example: 2.5T to 2.7T)
        )
    }
    sample_macro_df = pd.DataFrame(macro_data_test)

    move_data_test = {
        'price_date': dates_sample,
        'security_id': ['^MOVE'] * len(dates_sample),
        'close_price': np.linspace(80, 95, len(dates_sample)) # ^MOVE values
    }
    sample_move_df = pd.DataFrame(move_data_test)

    engine_params_config = {
        'rolling_window_days': 5, # Shorter window for test data
        'min_periods_ratio_for_rolling': 0.6, # Need 3 out of 5 days
        'stress_index_weights': {
            'sofr_deviation': 0.20,
            'spread_10y2y': 0.20,
            'primary_dealer_position': 0.15,
            'move_index': 0.25,
            'vix_index': 0.15,
            'pos_res_ratio': 0.05
        }
    }

    test_logger_eng_main.info("\n--- Test IndicatorEngine Full Calculation ---")
    engine_instance = IndicatorEngine(
        data_frames={'macro': sample_macro_df, 'move': sample_move_df},
        params=engine_params_config,
        logger_instance=test_logger_eng_main
    )

    stress_index_output = engine_instance.calculate_dealer_stress_index()

    if stress_index_output is not None and not stress_index_output.empty:
        test_logger_eng_main.info(f"Stress Index Output Shape: {stress_index_output.shape}")
        test_logger_eng_main.info(f"Stress Index Output Head:\n{stress_index_output.head().to_string()}")
        assert 'DealerStressIndex' in stress_index_output.columns, "Test Failed: DealerStressIndex column missing"

        # Check df_prepared for intermediate calculations
        if engine_instance.df_prepared is not None and not engine_instance.df_prepared.empty:
            test_logger_eng_main.info(f"Engine's df_prepared head (should include derived components like spread_10y2y, SOFR_Dev, pos_res_ratio):\n{engine_instance.df_prepared.head().to_string()}")
            assert 'spread_10y2y' in engine_instance.df_prepared.columns, "Test Failed: spread_10y2y missing in df_prepared"
            assert 'FRED/SOFR_Dev' in engine_instance.df_prepared.columns, "Test Failed: FRED/SOFR_Dev missing in df_prepared"
            assert 'pos_res_ratio' in engine_instance.df_prepared.columns, "Test Failed: pos_res_ratio missing in df_prepared"
            assert '^MOVE' in engine_instance.df_prepared.columns, "Test Failed: ^MOVE missing in df_prepared"
        else:
            test_logger_eng_main.error("Test Failed: engine_instance.df_prepared is None or empty after calculation.")
    elif stress_index_output is not None and stress_index_output.empty:
         test_logger_eng_main.warning("Stress Index calculation resulted in an empty DataFrame.")
    else: # stress_index_output is None
        test_logger_eng_main.error("Stress Index calculation failed and returned None.")

    test_logger_eng_main.info("\n--- Test with missing MOVE data ---")
    engine_no_move = IndicatorEngine(
        data_frames={'macro': sample_macro_df, 'move': pd.DataFrame()}, # Empty move DataFrame
        params=engine_params_config,
        logger_instance=test_logger_eng_main
    )
    stress_no_move_output = engine_no_move.calculate_dealer_stress_index()
    if stress_no_move_output is not None:
        test_logger_eng_main.info(f"Stress Index (no MOVE) Shape: {stress_no_move_output.shape}")
        if 'move_index_pct_rank' in stress_no_move_output.columns:
             assert stress_no_move_output['move_index_pct_rank'].isna().all(), "MOVE percentile should be all NaN if MOVE data missing"
        test_logger_eng_main.info(f"Stress Index (no MOVE) Head:\n{stress_no_move_output.head().to_string()}")
        if engine_no_move.df_prepared is not None:
             assert ('^MOVE' not in engine_no_move.df_prepared or engine_no_move.df_prepared['^MOVE'].isna().all()), "df_prepared should reflect missing MOVE"

    test_logger_eng_main.info("\n--- Test with completely empty input ---")
    engine_empty_all = IndicatorEngine(
        data_frames={'macro': pd.DataFrame(), 'move': pd.DataFrame()},
        params=engine_params_config,
        logger_instance=test_logger_eng_main
    )
    stress_empty_all_output = engine_empty_all.calculate_dealer_stress_index()
    assert stress_empty_all_output is None, "Expected None for completely empty input"
    test_logger_eng_main.info(f"Stress Index (empty input): {'None as expected' if stress_empty_all_output is None else 'FAIL: Unexpectedly got data'}")

    test_logger_eng_main.info("--- IndicatorEngine Test Finished ---")
