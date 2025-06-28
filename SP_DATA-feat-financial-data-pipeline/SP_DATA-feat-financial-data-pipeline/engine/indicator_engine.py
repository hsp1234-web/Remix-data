import pandas as pd
from typing import Dict, Any, Optional
import numpy as np
import logging
import sys

logger = logging.getLogger(f"project_logger.{__name__}")
if not logger.handlers and not logging.getLogger().hasHandlers():
    logger.addHandler(logging.NullHandler())
    logger.debug(f"Logger for {__name__} configured with NullHandler.")


class IndicatorEngine:
    """
    封裝計算衍生指標，特別是「債券壓力指標」的邏輯。
    """
    def __init__(self, data_frames: Dict[str, pd.DataFrame], params: Optional[Dict[str, Any]] = None, logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                 self.logger.addHandler(logging.NullHandler())
                 self.logger.debug(f"Instance logger for {self.__class__.__name__} using NullHandler.")

        self.raw_macro_df = data_frames.get('macro')
        self.raw_move_df = data_frames.get('move')
        self.params = params if params else {}
        self.df_prepared: Optional[pd.DataFrame] = None

        if self.raw_macro_df is None or self.raw_macro_df.empty:
            self.logger.warning("IndicatorEngine: 'macro' data is missing or empty.")
        if self.raw_move_df is None or self.raw_move_df.empty:
            self.logger.warning("IndicatorEngine: 'move' data for ^MOVE is missing or empty.")

    def _prepare_data(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Preparing data...")
        if self.raw_macro_df is None or self.raw_macro_df.empty:
            self.logger.error("IndicatorEngine: Macro data unavailable for preparation.")
            return None

        try:
            current_macro_df = self.raw_macro_df.copy()
            current_macro_df['metric_date'] = pd.to_datetime(current_macro_df['metric_date'], errors='coerce')
            current_macro_df.dropna(subset=['metric_date'], inplace=True)
            if current_macro_df.empty:
                self.logger.error("IndicatorEngine: Macro data has no valid metric_date entries after coercion.")
                return None

            macro_wide_df = current_macro_df.pivot_table(
                index='metric_date', columns='metric_name', values='metric_value'
            )
            macro_wide_df.index.name = 'date'
            self.logger.debug(f"IndicatorEngine: Pivoted macro data shape: {macro_wide_df.shape}")
        except Exception as e:
            self.logger.error(f"IndicatorEngine: Failed to pivot macro_df: {e}", exc_info=True)
            return None

        move_wide_df = pd.DataFrame(index=macro_wide_df.index)
        move_wide_df['^MOVE'] = np.nan

        if self.raw_move_df is not None and not self.raw_move_df.empty:
            if all(col in self.raw_move_df.columns for col in ['price_date', 'close_price', 'security_id']):
                move_df_filtered = self.raw_move_df[self.raw_move_df['security_id'] == '^MOVE'].copy()
                if not move_df_filtered.empty:
                    move_df_filtered['price_date'] = pd.to_datetime(move_df_filtered['price_date'], errors='coerce')
                    move_df_filtered.dropna(subset=['price_date'], inplace=True)
                    if not move_df_filtered.empty:
                        move_df_temp = move_df_filtered.set_index('price_date')[['close_price']].rename(columns={'close_price': '^MOVE'})
                        if not move_wide_df.empty:
                            move_wide_df.update(move_df_temp['^MOVE'])
                        else:
                            move_wide_df = move_df_temp
                        self.logger.debug(f"IndicatorEngine: Prepared MOVE index. Non-NaN count: {move_wide_df['^MOVE'].notna().sum()}")
                    else: self.logger.warning("IndicatorEngine: ^MOVE data had no valid price_date entries.")
                else: self.logger.warning("IndicatorEngine: ^MOVE security_id not found in provided yfinance data.")
            else: self.logger.warning("IndicatorEngine: MOVE DataFrame from yfinance missing required columns (price_date, close_price, security_id).")
        else: self.logger.warning("IndicatorEngine: ^MOVE data (raw_move_df) is missing or empty. MOVE index will be NaN in combined data.")

        if macro_wide_df.empty and (move_wide_df.empty or '^MOVE' not in move_wide_df.columns or move_wide_df['^MOVE'].isna().all()):
            self.logger.error("IndicatorEngine: Both macro and MOVE data are effectively empty before merge.")
            return None
        elif macro_wide_df.empty:
            combined_df = move_wide_df
            if '^MOVE' not in combined_df.columns: combined_df['^MOVE'] = np.nan
            self.logger.warning("IndicatorEngine: Macro data was empty, using only MOVE data for combined_df.")
        elif move_wide_df.empty or '^MOVE' not in move_wide_df.columns or move_wide_df['^MOVE'].isna().all():
            combined_df = macro_wide_df
            if '^MOVE' not in combined_df.columns: combined_df['^MOVE'] = np.nan
            self.logger.warning("IndicatorEngine: MOVE data was effectively empty, using only macro data for combined_df.")
        else:
            combined_df = pd.merge(macro_wide_df, move_wide_df[['^MOVE']], left_index=True, right_index=True, how='left')

        combined_df.sort_index(inplace=True)
        combined_df = combined_df.ffill().bfill()
        combined_df.dropna(how='all', inplace=True)

        if combined_df.empty:
            self.logger.error("IndicatorEngine: Prepared data is empty after merge and fill.")
            return None

        self.logger.info(f"IndicatorEngine: Data preparation complete. Shape: {combined_df.shape}")
        return combined_df


    def calculate_dealer_stress_index(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Calculating Dealer Stress Index...")
        df_for_calc = self._prepare_data()

        if df_for_calc is None or df_for_calc.empty:
            self.logger.error("IndicatorEngine: Prepared data is None or empty. Cannot calculate stress index.")
            self.df_prepared = df_for_calc
            return None

        df = df_for_calc.copy()

        window = self.params.get('rolling_window_days', 252)
        weights_config = self.params.get('stress_index_weights', {})

        component_map = {
            'sofr_deviation': 'FRED/SOFR_Dev', 'spread_10y2y': 'spread_10y2y',
            'primary_dealer_position': 'NYFED/PRIMARY_DEALER_NET_POSITION',
            'move_index': '^MOVE', 'vix_index': 'FRED/VIXCLS', 'pos_res_ratio': 'pos_res_ratio'
        }
        self.logger.debug(f"IndicatorEngine: Stress Index Params: Window={window}, Weights={weights_config}")

        if 'FRED/DGS10' in df.columns and 'FRED/DGS2' in df.columns:
            df['spread_10y2y'] = df['FRED/DGS10'] - df['FRED/DGS2']
        else: df['spread_10y2y'] = np.nan; self.logger.warning("IndicatorEngine: DGS10 or DGS2 missing for spread.")

        if 'FRED/SOFR' in df.columns and df['FRED/SOFR'].notna().sum() >= 20:
             df['FRED/SOFR_MA20'] = df['FRED/SOFR'].rolling(window=20, min_periods=15).mean()
             df['FRED/SOFR_Dev'] = df['FRED/SOFR'] - df['FRED/SOFR_MA20']
        else: df['FRED/SOFR_Dev'] = np.nan; self.logger.warning("IndicatorEngine: SOFR data insufficient for MA/Dev.")

        if 'NYFED/PRIMARY_DEALER_NET_POSITION' in df.columns and 'FRED/WRESBAL' in df.columns:
            res_safe = df['FRED/WRESBAL'].replace(0, np.nan)
            df['pos_res_ratio'] = df['NYFED/PRIMARY_DEALER_NET_POSITION'] / res_safe
            df['pos_res_ratio'].replace([np.inf, -np.inf], np.nan, inplace=True)
        else: df['pos_res_ratio'] = np.nan; self.logger.warning("IndicatorEngine: Dealer Pos or Reserves missing for Ratio.")

        self.df_prepared = df.copy()

        percentiles_df = pd.DataFrame(index=df.index)
        active_weights = {}

        for key, col_name in component_map.items():
            if col_name in df.columns and df[col_name].notna().any() and weights_config.get(key, 0) > 0:
                series = df[col_name]
                min_roll_periods = max(2, int(window * 0.5))
                if series.notna().sum() >= min_roll_periods:
                    rank = series.rolling(window=window, min_periods=min_roll_periods).apply(
                        lambda x: pd.Series(x).rank(pct=True).iloc[-1] if pd.Series(x).notna().any() else np.nan, raw=False
                    )
                    percentiles_df[f"{key}_pct"] = 1.0 - rank if key == 'spread_10y2y' else rank
                    active_weights[key] = weights_config[key]
                    self.logger.debug(f"IndicatorEngine: Ranked {key} ({col_name}).")
                else:
                    self.logger.warning(f"IndicatorEngine: Insufficient data for {col_name} (key: {key}) for rolling rank. Window: {window}, Available: {series.notna().sum()}. Skipping rank.")
                    percentiles_df[f"{key}_pct"] = np.nan
            else:
                self.logger.debug(f"IndicatorEngine: Skipping rank for {key} ({col_name}): not in data, all NaN, or zero weight.")
                percentiles_df[f"{key}_pct"] = np.nan

        if not active_weights: self.logger.error("IndicatorEngine: No active components for stress index."); return None

        total_w = sum(active_weights.values())
        if total_w == 0: self.logger.error("IndicatorEngine: Sum of active weights is zero."); return None

        norm_weights = {k: v / total_w for k, v in active_weights.items()}
        self.logger.info(f"IndicatorEngine: Normalized Stress Index Weights: {norm_weights}")

        stress_sum = pd.Series(0.0, index=df.index)
        effective_weights_sum = pd.Series(0.0, index=df.index)

        for component_key, weight in norm_weights.items():
            pct_col = f"{component_key}_pct"
            if pct_col in percentiles_df.columns and percentiles_df[pct_col].notna().any():
                component_values = percentiles_df[pct_col].fillna(0.5)
                stress_sum = stress_sum.add(component_values * weight, fill_value=0)
                effective_weights_sum = effective_weights_sum.add(percentiles_df[pct_col].notna() * weight, fill_value=0)
            else:
                self.logger.warning(f"IndicatorEngine: Percentile rank col {pct_col} missing or all NaN. Not included in index.")

        stress_idx_0_1 = stress_sum.divide(effective_weights_sum.replace(0, np.nan))
        final_stress_idx = (stress_idx_0_1 * 100).clip(0, 100)

        result_df = pd.DataFrame({'DealerStressIndex': final_stress_idx}, index=df.index)
        result_df = result_df.join(percentiles_df)
        final_result = result_df.dropna(subset=['DealerStressIndex'])

        if final_result.empty: self.logger.warning("IndicatorEngine: Dealer Stress Index is all NaN."); return None

        self.logger.info(f"IndicatorEngine: Dealer Stress Index calculated. Shape: {final_result.shape}")
        return final_result

if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_eng = logging.getLogger("IndicatorEngineTestRunV3")
    if not test_logger_eng.handlers:
        ch_eng = logging.StreamHandler(sys.stdout); ch_eng.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s')); test_logger_eng.addHandler(ch_eng); test_logger_eng.propagate = False

    dates_test = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-01-06'])
    macro_test_data = {
        'metric_date': list(dates_test)*6,
        'metric_name': (['FRED/DGS10']*6 + ['FRED/DGS2']*6 + ['FRED/SOFR']*6 +
                        ['FRED/VIXCLS']*6 + ['NYFED/PRIMARY_DEALER_NET_POSITION']*6 + ['FRED/WRESBAL']*6),
        'metric_value': (list(np.array([3.0,3.1,3.05,3.15,3.2,3.22])) + list(np.array([2.0,2.1,2.05,2.15,2.2,2.22])) +
                         list(np.array([1.0,1.1,1.05,1.15,1.2,1.22])) + list(np.array([15,16,14,17,18,15])) +
                         list(np.array([1000,1100,1050,1200,1150,1250])*1e3) + list(np.array([2.5,2.5,2.6,2.6,2.7,2.7])*1e6))}
    sample_macro = pd.DataFrame(macro_test_data)

    move_test_data = {'price_date': dates_test, 'security_id': ['^MOVE']*6, 'close_price': [80,85,82,88,90,86]}
    sample_move = pd.DataFrame(move_test_data)

    engine_params_test = {
        'rolling_window_days': 3,
        'stress_index_weights': {
            'sofr_deviation': 0.20, 'spread_10y2y': 0.20, 'primary_dealer_position': 0.15,
            'move_index': 0.25, 'vix_index': 0.15, 'pos_res_ratio': 0.05 }}

    test_logger_eng.info("\n--- Test IndicatorEngine Full Calc ---")
    eng_test = IndicatorEngine(data_frames={'macro':sample_macro, 'move':sample_move}, params=engine_params_test, logger_instance=test_logger_eng)

    stress_output_test = eng_test.calculate_dealer_stress_index()
    if stress_output_test is not None:
        test_logger_eng.info(f"Stress Index Output Shape: {stress_output_test.shape}")
        test_logger_eng.info(f"Stress Index Output Head:\n{stress_output_test.head().to_string()}")
        assert not stress_output_test.empty, "Test Failed: Stress output DF should not be empty"
        assert 'DealerStressIndex' in stress_output_test.columns, "Test Failed: DealerStressIndex column missing"
        assert eng_test.df_prepared is not None and not eng_test.df_prepared.empty, "Test Failed: df_prepared not set or empty"
        test_logger_eng.info(f"Engine's prepared_df head (should include derived like spread_10y2y):\n{eng_test.df_prepared.head().to_string()}")
        assert 'spread_10y2y' in eng_test.df_prepared.columns, "Test Failed: spread_10y2y missing in df_prepared"


    test_logger_eng.info("\n--- Test with missing MOVE ---")
    eng_no_mv_test = IndicatorEngine({'macro':sample_macro, 'move':pd.DataFrame()}, engine_params_test, test_logger_eng)
    stress_no_mv_test = eng_no_mv_test.calculate_dealer_stress_index()
    if stress_no_mv_test is not None:
        test_logger_eng.info(f"Stress Index (no MOVE) Shape: {stress_no_mv_test.shape}")
        if 'move_index_pct' in stress_no_mv_test.columns:
             assert stress_no_mv_test['move_index_pct'].isna().all(), "MOVE percentile should be all NaN if MOVE data missing"
        test_logger_eng.info(f"Stress Index (no MOVE) Head:\n{stress_no_mv_test.head().to_string()}")


    test_logger_eng.info("\n--- Test with insufficient data for rolling ---")
    dates_short = pd.to_datetime(['2023-01-01'])
    short_macro_test = sample_macro[sample_macro['metric_date'].isin(dates_short)].copy()
    short_move_test = sample_move[sample_move['price_date'].isin(dates_short)].copy()

    eng_short_test = IndicatorEngine({'macro':short_macro_test, 'move':short_move_test}, params=engine_params_test, logger_instance=test_logger_eng)
    stress_short_test = eng_short_test.calculate_dealer_stress_index()
    assert stress_short_test is None or stress_short_test.empty, "Expected None or empty for insufficient rolling data"
    test_logger_eng.info(f"Stress Index (short data): {'None or Empty as expected' if stress_short_test is None or stress_short_test.empty else 'FAIL: Unexpectedly got data'}")

# **對草案的增強和調整摘要（V3 更新）：**
# *   **`_prepare_data` 方法的健壯性：**
#     *   在數據透視 `macro_df` 之前，確保 `metric_date` 列已轉換為 datetime 物件並移除了無效日期。
#     *   在處理 `raw_move_df` 時，增加了對其是否為空以及是否包含必要欄位（`price_date`, `close_price`, `security_id`）的檢查。
#     *   `move_wide_df` 初始化時使用 `macro_wide_df.index`，然後用 `update` 方法填充 `^MOVE` 數據，這樣可以更好地處理 `^MOVE` 數據與宏觀數據日期不完全對齊的情況。
#     *   在合併 `macro_wide_df` 和 `move_wide_df` 之前，增加了對兩者是否都為空的判斷，並相應處理。
# *   **`calculate_dealer_stress_index` 方法的健壯性：**
#     *   在調用 `_prepare_data` 後，將結果（可能是處理過的寬表，也可能是 `None`）賦值給 `self.df_prepared`。這樣 `main.py` 在後續生成市場簡報時，總能訪問到 `IndicatorEngine` 內部最後準備好的數據狀態。
#     *   使用 `df_calc = self.df_prepared.copy()` 開始計算，確保 `self.df_prepared` 在計算衍生指標前是乾淨的基礎數據。在所有衍生指標（利差、SOFR偏差、持倉/準備金比率）計算完畢後，再次將這個包含所有輸入和基礎衍生指標的 `df_calc` 更新回 `self.df_prepared`。
#     *   對每個衍生指標的計算都增加了對其所需原始欄位是否存在的檢查。
#     *   在計算滾動百分位排名時，對 `series.notna().sum()` 的檢查使用了 `max(2, int(window * 0.5))` 作為 `min_periods`，確保至少有兩個點才能計算排名，並且至少是窗口期的一半。
#     *   `lambda x: pd.Series(x).rank(pct=True).iloc[-1] if pd.Series(x).notna().any() else np.nan` 確保了如果窗口內全是 NaN，則排名也是 NaN。
#     *   **加權與指數合成：**
#         *   權重從 `self.params` 中讀取。
#         *   只對那些成功計算出百分位排名的「活躍成分」進行加權和正規化。
#         *   在加權求和時，將百分位排名中的 NaN 用 0.5（中性值）填充。
#         *   計算有效權重之和 `effective_weights_sum`，並在計算最終指數時用其作為分母。
#     *   最終結果 `final_result_df` 會移除 `DealerStressIndex` 本身為 NaN 的行。
# *   **`if __name__ == '__main__':` 測試塊：**
#     *   更新了模擬數據以包含 `FRED/WRESBAL`。
#     *   確保了傳遞給 `IndicatorEngine` 的 `params` 字典鍵名與引擎內部期望的一致。
#     *   增加了對 `eng_test.df_prepared` 是否被正確填充並包含衍生指標（如 `spread_10y2y`）的斷言。
#     *   修正了「insufficient data」測試中，數據篩選後應使用 `.copy()`。
#
# 這個版本的 `IndicatorEngine` 在數據準備和計算過程中對各種可能的數據缺失或不足的情況做了更周全的處理。
