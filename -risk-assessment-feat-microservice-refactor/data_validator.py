# data_validator.py

import yaml
import os
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

class DataValidator:
    """
    根據設定檔中定義的規則，對傳入的 Pandas DataFrame 中的數據指標進行驗證。
    """
    def __init__(self, rules_config_path: str = "config/dq_rules.yaml"):
        """
        初始化 DataValidator。

        Args:
            rules_config_path (str): DQ 規則設定檔 (YAML) 的路徑。
        """
        self.rules_config_path = rules_config_path
        self.rules: Dict[str, List[Dict[str, Any]]] = self._load_rules()
        logger.info(f"DataValidator 初始化完成，已從 {rules_config_path} 載入 {len(self.rules)} 個指標的規則。")

    def _load_rules(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        從 YAML 文件載入 DQ 規則。
        """
        if not os.path.exists(self.rules_config_path):
            err_msg = f"DQ 規則設定檔未找到: {self.rules_config_path}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)
        try:
            with open(self.rules_config_path, 'r', encoding='utf-8') as f:
                rules_data = yaml.safe_load(f)
            if not isinstance(rules_data, dict):
                raise ValueError("DQ 規則設定檔頂層應為字典結構。")
            # 可以在此處添加更詳細的規則格式驗證
            return rules_data
        except yaml.YAMLError as e:
            err_msg = f"解析 DQ 規則設定檔 {self.rules_config_path} 失敗: {e}"
            logger.error(err_msg, exc_info=True)
            raise
        except ValueError as e:
            logger.error(str(e), exc_info=True)
            raise

    def _apply_range_check(self, series: pd.Series, params: Dict[str, Any]) -> Tuple[pd.Series, pd.Series]:
        """應用範圍檢查規則。"""
        min_val = params.get('min')
        max_val = params.get('max')
        status = pd.Series(['PASS'] * len(series), index=series.index, name=f"{series.name}_dq_status_range")
        notes = pd.Series([''] * len(series), index=series.index, name=f"{series.name}_dq_notes_range")

        if min_val is not None:
            outside_min = series < min_val
            status[outside_min] = 'FAIL_RANGE_MIN'
            notes[outside_min] = notes[outside_min] + f"Value < {min_val}; "
        if max_val is not None:
            outside_max = series > max_val
            status[outside_max & (status != 'FAIL_RANGE_MIN')] = 'FAIL_RANGE_MAX' # 避免覆蓋FAIL_MIN
            status[outside_max & (status == 'FAIL_RANGE_MIN')] = 'FAIL_RANGE_BOTH' # 如果同時小於min又大於max (雖然不太可能，除非min>max)
            notes[outside_max] = notes[outside_max] + f"Value > {max_val}; "
        return status, notes

    def _apply_spike_check(self, series: pd.Series, params: Dict[str, Any]) -> Tuple[pd.Series, pd.Series]:
        """應用尖峰檢查規則。"""
        window = params.get('window', 60)
        threshold_std = params.get('threshold_std', 5.0)
        status = pd.Series(['PASS'] * len(series), index=series.index, name=f"{series.name}_dq_status_spike")
        notes = pd.Series([''] * len(series), index=series.index, name=f"{series.name}_dq_notes_spike")

        if len(series.dropna()) < window / 2: # 數據太少無法可靠計算滾動統計
            logger.warning(f"指標 {series.name} 的數據點不足 ({len(series.dropna())})，無法執行窗口為 {window} 的尖峰檢查。")
            status[:] = 'SKIP_SPIKE_INSUFFICIENT_DATA'
            notes[:] = f"Skipped spike check due to insufficient data (need > {window/2}); "
            return status, notes

        diff = series.diff().abs()
        rolling_mean = diff.rolling(window=window, min_periods=max(1, int(window * 0.5))).mean()
        rolling_std = diff.rolling(window=window, min_periods=max(1, int(window * 0.5))).std()

        # 處理滾動標準差可能為0的情況 (例如，在一段時間內數據不變)
        # 如果 std 為 0，任何微小的 diff 都會被認為是尖峰，這不合理。
        # 在這種情況下，只有當 diff 本身也非0時才認為是尖峰，或者定義一個最小的絕對變化閾值。
        # 簡單處理：如果 rolling_std 接近0，則不進行該點的尖峰判斷，除非 diff 顯著。
        # 這裡我們允許 rolling_std 為0，如果 diff 也為0，則不會判為 spike。
        # 如果 rolling_std 為0但 diff 非0，則會判為 spike (無限大的標準差倍數)。

        # 避免除以零或極小的標準差導致的過度敏感
        # 設置一個最小標準差，例如序列本身標準差的一小部分，或者一個固定的epsilon
        min_std_threshold = max(series.std() * 0.01, 1e-6) if series.std() > 0 else 1e-6
        effective_std = np.maximum(rolling_std, min_std_threshold)

        is_spike = diff > (rolling_mean + threshold_std * effective_std)

        status[is_spike] = 'FAIL_SPIKE'
        notes[is_spike] = notes[is_spike] + f"Value change > mean_diff + {threshold_std}*std_diff (win={window}); "
        return status, notes

    def _apply_not_null_check(self, series: pd.Series, params: Dict[str, Any]) -> Tuple[pd.Series, pd.Series]:
        """應用非空檢查規則。"""
        enabled = params.get('enabled', False)
        lookback_days = params.get('lookback_days', 1) # 預設只檢查最新值
        status = pd.Series(['PASS'] * len(series), index=series.index, name=f"{series.name}_dq_status_notnull")
        notes = pd.Series([''] * len(series), index=series.index, name=f"{series.name}_dq_notes_notnull")

        if not enabled:
            return status, notes

        # 只檢查最近 lookback_days 的數據點 (如果數據是日度的)
        # 假設 series 的 index 是 DatetimeIndex
        if isinstance(series.index, pd.DatetimeIndex) and not series.empty:
            cutoff_date = series.index.max() - pd.Timedelta(days=lookback_days -1)
            recent_series = series[series.index >= cutoff_date]
            is_null_recent = recent_series.isnull()

            if is_null_recent.any(): # 如果近期有任何一個null
                # 為了簡化，我們只標記最新的那個點，如果它是null
                if pd.isnull(series.iloc[-1]):
                    status.iloc[-1] = 'FAIL_NOT_NULL_RECENT'
                    notes.iloc[-1] = f"Recent value (within last {lookback_days} days) is null; "
        elif series.isnull().iloc[-1]: # 如果不是DatetimeIndex，或為空，只檢查最後一個點
             status.iloc[-1] = 'FAIL_NOT_NULL_RECENT'
             notes.iloc[-1] = f"Last value is null; "

        return status, notes

    def _apply_stale_check(self, series: pd.Series, params: Dict[str, Any]) -> Tuple[pd.Series, pd.Series]:
        """應用數據過期檢查規則。"""
        max_days_stale = params.get('max_days_stale', 3)
        status = pd.Series(['PASS'] * len(series), index=series.index, name=f"{series.name}_dq_status_stale")
        notes = pd.Series([''] * len(series), index=series.index, name=f"{series.name}_dq_notes_stale")

        if not isinstance(series.index, pd.DatetimeIndex) or series.dropna().empty:
            logger.warning(f"指標 {series.name} 的索引不是 DatetimeIndex 或無有效數據，無法執行過期檢查。")
            status[:] = 'SKIP_STALE_CHECK_INVALID_INDEX_OR_DATA'
            notes[:] = "Skipped stale check due to invalid index or no data; "
            return status, notes

        last_valid_date = series.dropna().index.max()
        today = pd.Timestamp('today').normalize() # 使用normalize確保比較的是日期部分

        if (today - last_valid_date).days > max_days_stale:
            # 標記整個序列為過期，或者只標記最新可能的日期點
            # 這裡選擇標記最新的一個點（如果序列延伸到今天）
            if not series.empty:
                status.iloc[-1] = 'FAIL_STALE_DATA'
                notes.iloc[-1] = f"Data last updated on {last_valid_date.date()}, older than {max_days_stale} days; "
        return status, notes


    def validate(self, data_df: pd.DataFrame, indicator_col_name: str) -> pd.DataFrame:
        """
        對 DataFrame 中指定的指標列應用所有已定義的 DQ 規則。

        Args:
            data_df (pd.DataFrame): 包含待驗證數據的 DataFrame。索引應為 DatetimeIndex。
            indicator_col_name (str): data_df 中需要驗證的指標列的名稱。

        Returns:
            pd.DataFrame: 原始 DataFrame，並附加了DQ狀態和註釋列。
                          例如：{indicator_name}_dq_status, {indicator_name}_dq_notes
        """
        if indicator_col_name not in data_df.columns:
            logger.warning(f"指標列 '{indicator_col_name}' 不在提供的 DataFrame 中。跳過驗證。")
            return data_df.copy()

        if indicator_col_name not in self.rules:
            logger.info(f"指標 '{indicator_col_name}' 沒有定義 DQ 規則。跳過驗證。")
            return data_df.copy()

        logger.info(f"開始驗證指標: {indicator_col_name}")
        series_to_validate = data_df[indicator_col_name].copy()

        # 確保數據是數值型，以便進行範圍和尖峰檢查
        if not pd.api.types.is_numeric_dtype(series_to_validate):
            logger.warning(f"指標 {indicator_col_name} 的數據類型不是數值型 ({series_to_validate.dtype})，嘗試轉換。DQ 結果可能不準確。")
            series_to_validate = pd.to_numeric(series_to_validate, errors='coerce')


        all_statuses = []
        all_notes = []

        for rule in self.rules[indicator_col_name]:
            rule_type = rule.get('rule_type')
            params = rule.get('parameters', {})
            severity = rule.get('severity', 'WARNING').upper() # 預設嚴重性為警告

            rule_status_col = f"{indicator_col_name}_dq_status_{rule_type.lower()}"
            rule_notes_col = f"{indicator_col_name}_dq_notes_{rule_type.lower()}"

            current_status = pd.Series(['PASS'] * len(series_to_validate), index=series_to_validate.index)
            current_notes = pd.Series([''] * len(series_to_validate), index=series_to_validate.index)

            if rule_type == "range_check":
                current_status, current_notes = self._apply_range_check(series_to_validate, params)
            elif rule_type == "spike_check":
                current_status, current_notes = self._apply_spike_check(series_to_validate, params)
            elif rule_type == "not_null_check":
                current_status, current_notes = self._apply_not_null_check(series_to_validate, params)
            elif rule_type == "stale_check":
                current_status, current_notes = self._apply_stale_check(series_to_validate, params)
            else:
                logger.warning(f"指標 '{indicator_col_name}' 的規則類型 '{rule_type}' 未知。跳過此規則。")
                continue

            # 根據嚴重性調整狀態標記
            current_status[current_status.str.startswith('FAIL_')] = severity + "_" + current_status[current_status.str.startswith('FAIL_')]

            all_statuses.append(current_status.rename(rule_status_col))
            all_notes.append(current_notes.rename(rule_notes_col))

        output_df = data_df.copy()
        if not all_statuses: # 如果沒有應用任何規則
             return output_df

        # 合併所有規則的狀態和註釋
        # 創建一個總的 dq_status 和 dq_notes 列
        # 總體狀態：如果任何 ERROR 級別檢查失敗，則為 ERROR；否則如果任何 WARNING 失敗，則為 WARNING；否則為 PASS
        # 總體註釋：合併所有非空的註釋

        combined_status_df = pd.concat(all_statuses, axis=1)
        combined_notes_df = pd.concat(all_notes, axis=1)

        # 確定最終的 dq_status
        final_status = pd.Series(['PASS'] * len(output_df), index=output_df.index)
        # 優先級：ERROR > WARNING > SKIP > PASS
        if not combined_status_df.empty:
            has_error = combined_status_df.apply(lambda row: any(s.startswith('ERROR_') for s in row if isinstance(s, str)), axis=1)
            has_warning = combined_status_df.apply(lambda row: any(s.startswith('WARNING_') for s in row if isinstance(s, str)), axis=1)
            has_skip = combined_status_df.apply(lambda row: any(s.startswith('SKIP_') for s in row if isinstance(s, str)), axis=1)

            final_status[has_error] = 'ERROR'
            final_status[has_warning & ~has_error] = 'WARNING'
            final_status[has_skip & ~has_warning & ~has_error] = 'SKIPPED_CHECKS'

        output_df[f"{indicator_col_name}_dq_status"] = final_status

        # 合併所有註釋
        def combine_notes(row):
            return "; ".join(note for note in row if pd.notna(note) and note.strip()).strip()

        if not combined_notes_df.empty:
            output_df[f"{indicator_col_name}_dq_notes"] = combined_notes_df.apply(combine_notes, axis=1)
        else:
            output_df[f"{indicator_col_name}_dq_notes"] = ""

        # 可以選擇是否保留每個單獨規則的狀態和註釋列，或者只保留總體列
        # for s_col in all_statuses:
        #     output_df[s_col.name] = s_col
        # for n_col in all_notes:
        #     output_df[n_col.name] = n_col

        logger.info(f"指標 {indicator_col_name} 驗證完成。")
        return output_df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # 創建一個模擬的 DataFrame 進行測試
    dates = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
                            '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10'])
    data = {
        'FRAOIS': [0.5, 0.51, -2.5, 0.53, 6.0, 0.55, 0.56, 3.0, 0.58, None], # 包含超出範圍, 尖峰, 空值
        'TEDRATE': [0.2, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29], # 正常數據
        'VIX_YF': [10.0, 12.0, 15.0, 80.0, 160.0, 18.0, 20.0, 22.0, 25.0, 28.0], # 包含尖峰和超出範圍
        'NO_RULES_INDICATOR': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'STALE_DATA_TEST': [1.0] * 9 + [None] # 假設今天遠大於 2023-01-09
    }
    test_df = pd.DataFrame(data, index=dates)

    # 模擬今天日期以便 stale_check 生效
    # 注意：在真實環境中，pd.Timestamp('today') 會獲取當前日期
    # 為了測試的確定性，可以固定一個 "today"
    # 例如，如果今天是 2023-01-20，那麼 STALE_DATA_TEST 在 01-09 更新就會被標記為過期

    # 創建一個空的 dq_rules.yaml 以避免 FileNotFoundError (如果尚未創建)
    if not os.path.exists("config/dq_rules.yaml"):
        logger.warning("測試警告: config/dq_rules.yaml 未找到，將創建一個空的臨時文件。某些驗證可能不會執行。")
        os.makedirs("config", exist_ok=True)
        with open("config/dq_rules.yaml", "w") as f:
            f.write("# Empty DQ rules for testing\n")
            f.write("EMPTY_INDICATOR:\n") # 添加一個空指標避免解析錯誤
            f.write("  - rule_type: \"range_check\"\n")
            f.write("    parameters: {min: 0, max: 1}\n")


    validator = DataValidator()

    logger.info("\n--- 驗證 FRAOIS ---")
    validated_df_fraois = validator.validate(test_df.copy(), 'FRAOIS')
    print(validated_df_fraois[['FRAOIS', 'FRAOIS_dq_status', 'FRAOIS_dq_notes']])

    logger.info("\n--- 驗證 TEDRATE (應通過大部分檢查) ---")
    validated_df_tedrate = validator.validate(test_df.copy(), 'TEDRATE')
    print(validated_df_tedrate[['TEDRATE', 'TEDRATE_dq_status', 'TEDRATE_dq_notes']])

    logger.info("\n--- 驗證 VIX_YF ---")
    validated_df_vix = validator.validate(test_df.copy(), 'VIX_YF')
    print(validated_df_vix[['VIX_YF', 'VIX_YF_dq_status', 'VIX_YF_dq_notes']])

    logger.info("\n--- 驗證 NO_RULES_INDICATOR (應跳過) ---")
    validated_df_norules = validator.validate(test_df.copy(), 'NO_RULES_INDICATOR')
    if f'NO_RULES_INDICATOR_dq_status' not in validated_df_norules.columns:
        print("NO_RULES_INDICATOR 正確跳過驗證 (沒有生成DQ列)。")
    else:
        print("錯誤：NO_RULES_INDICATOR 不應生成DQ列。")
        print(validated_df_norules[['NO_RULES_INDICATOR', 'NO_RULES_INDICATOR_dq_status', 'NO_RULES_INDICATOR_dq_notes']])

    logger.info("\n--- 驗證 STALE_DATA_TEST (需要 config/dq_rules.yaml 中有 STALE_DATA_TEST 的 stale_check 規則) ---")
    # 確保 dq_rules.yaml 中有 STALE_DATA_TEST 的 stale_check 規則才能正確測試
    # 例如:
    # STALE_DATA_TEST:
    #   - rule_type: "stale_check"
    #     parameters: {max_days_stale: 3}
    #     severity: "ERROR"
    if "STALE_DATA_TEST" in validator.rules:
        validated_df_stale = validator.validate(test_df.copy(), 'STALE_DATA_TEST')
        print(validated_df_stale[['STALE_DATA_TEST', 'STALE_DATA_TEST_dq_status', 'STALE_DATA_TEST_dq_notes']])
    else:
        logger.warning("STALE_DATA_TEST 的規則未在 dq_rules.yaml 中定義，跳過此特定測試。")

    # 清理臨時的空配置文件 (如果創建了)
    # if os.path.exists("config/dq_rules.yaml") and validator.rules.get("EMPTY_INDICATOR"):
    #     # 簡單判斷是否是我們創建的空文件
    #     if len(validator.rules) <=2 : # 假設只有 EMPTY_INDICATOR 和可能的其他少量測試指標
    #         try:
    #             os.remove("config/dq_rules.yaml")
    #             logger.info("已移除臨時創建的空 dq_rules.yaml。")
    #             if not os.listdir("config"): # 如果 config 資料夾也空了
    #                 os.rmdir("config")
    #                 logger.info("已移除臨時創建的空 config 資料夾。")
    #         except OSError as e_rm:
    #             logger.warning(f"移除臨時配置文件時出錯: {e_rm}")
    #     else:
    #          logger.info("dq_rules.yaml 似乎不是空的臨時文件，將其保留。")

    logger.info("\nDataValidator 測試演示完成。")
