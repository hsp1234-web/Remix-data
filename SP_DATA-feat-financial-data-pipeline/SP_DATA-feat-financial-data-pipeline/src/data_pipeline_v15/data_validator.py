import pandas as pd
import logging # Or use the logger passed from Orchestrator

class Validator:
    def __init__(self, rules_config: dict, logger: logging.Logger):
        self.rules_config = rules_config # e.g., {"table_name_or_schema_name": {"column_name": {"non_null": True, "min_value": 0}}}
        self.logger = logger

    def validate(self, df: pd.DataFrame, source_file: str, table_or_schema_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        if not isinstance(df, pd.DataFrame) or df.empty:
            # Ensure columns for invalid_df are consistent even if input df is empty
            # This requires knowing the original columns if df is empty but was supposed to have columns.
            # For simplicity, if df is empty, assume it has no columns for quarantine_df creation here.
            # This part might need refinement if an empty df with predefined columns can occur.
            q_cols = list(df.columns) if isinstance(df, pd.DataFrame) else []
            return df, pd.DataFrame(columns=q_cols + ["quarantine_reason", "source_file"])

        # Get rules for the specific table/schema
        table_rules = self.rules_config.get(table_or_schema_name, {})
        if not table_rules:
            self.logger.info(f"No validation rules found for '{table_or_schema_name}'. Skipping validation.")
            q_cols = list(df.columns) # Get columns from input df
            return df, pd.DataFrame(columns=q_cols + ["quarantine_reason", "source_file"])

        original_columns = list(df.columns)
        # Ensure 'source' column exists if it was added by FileParser, even if not in rules_config
        # This is important because the quarantine table expects it.
        # However, the Validator should primarily work on columns defined in rules or existing in df.
        # The 'source' column from FileParser will be part of original_columns if present.

        invalid_rows_mask = pd.Series([False] * len(df), index=df.index)
        # Initialize quarantine_reasons as a Series of lists or accumulative strings
        quarantine_reasons_series = pd.Series([[] for _ in range(len(df))], index=df.index)


        for col_name, rules in table_rules.items():
            if col_name not in df.columns:
                self.logger.warning(f"Rule defined for column '{col_name}' in '{table_or_schema_name}', but column not in DataFrame. Skipping rule.")
                continue

            current_col_invalid_mask = pd.Series([False] * len(df), index=df.index)
            reason_prefix = f"Column '{col_name}': "

            if rules.get("non_null"):
                is_null = df[col_name].isnull()
                current_col_invalid_mask |= is_null
                for idx in df.index[is_null]:
                    quarantine_reasons_series.loc[idx].append(reason_prefix + "is null")

            if "min_value" in rules:
                # Attempt to convert to numeric, coercing errors to NaN
                numeric_series = pd.to_numeric(df[col_name], errors='coerce')

                is_lt_min = numeric_series < rules["min_value"]
                current_col_invalid_mask |= is_lt_min
                for idx in df.index[is_lt_min]:
                    quarantine_reasons_series.loc[idx].append(reason_prefix + f"is less than {rules['min_value']}")

                conversion_failed_mask = numeric_series.isnull() & df[col_name].notnull()
                current_col_invalid_mask |= conversion_failed_mask
                for idx in df.index[conversion_failed_mask]:
                    quarantine_reasons_series.loc[idx].append(reason_prefix + "failed numeric conversion for range check")

            # Add more rule types here (e.g., max_value, pattern_match, allowed_values)

            invalid_rows_mask |= current_col_invalid_mask

        valid_df = df[~invalid_rows_mask].copy()
        # Prepare invalid_df. It should contain all original columns plus new ones.
        final_invalid_df_columns = original_columns + ["quarantine_reason", "source_file"]

        if invalid_rows_mask.any():
            invalid_df_temp = df[invalid_rows_mask].copy()
            # Join lists of reasons into a single string per row
            invalid_df_temp["quarantine_reason"] = quarantine_reasons_series[invalid_rows_mask].apply(lambda reasons: "; ".join(reasons) if reasons else "")
            invalid_df_temp["source_file"] = source_file

            # Reindex to ensure all original columns are present, plus the new ones
            invalid_df = invalid_df_temp.reindex(columns=final_invalid_df_columns, fill_value=pd.NA)
        else:
            # No invalid rows, create an empty DataFrame with the correct columns
            invalid_df = pd.DataFrame(columns=final_invalid_df_columns)


        self.logger.info(f"Validation for '{source_file}' (schema: {table_or_schema_name}): {len(valid_df)} valid rows, {len(invalid_df)} invalid rows.")
        return valid_df, invalid_df
