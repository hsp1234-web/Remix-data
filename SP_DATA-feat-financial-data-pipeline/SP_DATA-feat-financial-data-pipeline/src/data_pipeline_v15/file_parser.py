# -*- coding: utf-8 -*-

import os
import zipfile
import hashlib
from io import BytesIO
import io # Added
import pandas as pd
from .core import constants # 修改：導入常數


class FileParser:
    def __init__(self, manifest_manager, logger, schemas_config): # Added schemas_config
        self.manifest_manager = manifest_manager
        self.logger = logger
        self.schemas_config = schemas_config # Store schemas_config

    def parse_file(self, file_path: str, staging_dir: str) -> dict: # Removed schemas_config
        """處理單一來源檔案（CSV 或 ZIP），將其解析並轉換為 Parquet 格式。
        (docstring 已省略)
        """
        original_filename = os.path.basename(file_path)

        if zipfile.is_zipfile(file_path):
            try:
                with zipfile.ZipFile(file_path, "r") as z:
                    csv_files = [
                        f
                        for f in z.namelist()
                        if f.lower().endswith(".csv") and not f.startswith("__MACOSX")
                    ]
                    if not csv_files:
                        return {
                            constants.KEY_STATUS: constants.STATUS_ERROR,
                            constants.KEY_FILE: original_filename,
                            constants.KEY_REASON: "ZIP 檔中未找到任何 CSV 檔案",
                        }
                    all_results = []
                    for csv_name in csv_files:
                        display_name_in_zip = f"{original_filename}/{csv_name}"
                        try:
                            with z.open(csv_name) as csv_file_in_zip:
                                csv_content = BytesIO(csv_file_in_zip.read())
                                result = self._parse_content( # Renamed
                                    csv_content,
                                    staging_dir,
                                    # schemas_config removed, will use self.schemas_config
                                    display_name_in_zip,
                                )
                                all_results.append(result)
                        except Exception as e_zip_read:
                            all_results.append(
                                {
                                    constants.KEY_STATUS: constants.STATUS_ERROR,
                                    constants.KEY_FILE: display_name_in_zip,
                                    constants.KEY_REASON: f"讀取 ZIP 中 CSV 時發生錯誤: {e_zip_read}",
                                }
                            )
                    return {
                        constants.KEY_STATUS: constants.STATUS_GROUP_RESULT,
                        constants.KEY_RESULTS: all_results,
                        constants.KEY_FILE: original_filename,
                    }
            except zipfile.BadZipFile:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: original_filename,
                    constants.KEY_REASON: "損壞的 ZIP 檔案",
                }
            except Exception as e:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: original_filename,
                    constants.KEY_REASON: f"處理 ZIP 時發生未知錯誤: {e}",
                }
        else: # Not a ZIP file, assume CSV or other parsable content
            # schemas_config removed, will use self.schemas_config
            return self._parse_content(file_path, staging_dir, original_filename) # Renamed

    def _parse_content( # Renamed
        self, file_input, staging_dir: str, display_name: str # Renamed file_or_buffer to file_input and removed schemas_config
    ) -> dict:
        """解析單個檔案內容（來自路徑或記憶體緩衝區），進行內容嗅探、綱要匹配、欄位正規化，並儲存為 Parquet 格式。
        (docstring 已省略)
        """
        try:
            encodings_to_try = ["utf-8", "big5", "ms950", "cp950"]
            head_content_bytes = b""
            matched_schema_name = None
            detected_encoding = None

            if isinstance(file_input, str): # File path
                try:
                    with open(file_input, "rb") as f:
                        head_content_bytes = f.read(4096) # Read first 4KB
                except Exception as e:
                    return {
                        constants.KEY_STATUS: constants.STATUS_ERROR,
                        constants.KEY_FILE: display_name,
                        constants.KEY_REASON: f"讀取檔案失敗: {e}",
                    }
            elif isinstance(file_input, BytesIO):
                current_pos = file_input.tell()
                head_content_bytes = file_input.read(4096)
                file_input.seek(current_pos) # Reset pointer to original position for pd.read_csv
            else:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: "不支援的輸入類型 file_input",
                }

            if not head_content_bytes:
                 return {
                    constants.KEY_STATUS: constants.STATUS_SKIPPED,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: "檔案為空或無法讀取頭部內容。",
                }

            # Content-based schema detection
            # Use self.schemas_config instead of local variable
            attempted_schemas = [] # Initialize list to store attempted schemas
            for schema_name, schema_def in self.schemas_config.items():
                attempted_schemas.append(schema_name) # Record schema name
                keywords = schema_def.get("keywords", [])
                if not keywords:
                    continue
                for enc in encodings_to_try:
                    try:
                        head_content_str = head_content_bytes.decode(enc)
                        if any(k.lower() in head_content_str.lower() for k in keywords):
                            matched_schema_name = schema_name
                            detected_encoding = enc
                            self.logger.info(f"檔案 '{display_name}' 透過關鍵字在內容中匹配到 schema: '{matched_schema_name}' 使用編碼 '{detected_encoding}'")
                            break
                    except UnicodeDecodeError:
                        continue # Try next encoding
                if matched_schema_name:
                    break

            # Use self.schemas_config here as well
            if not matched_schema_name and "default_daily" in self.schemas_config: # Fallback to default if specified
                self.logger.warning(f"檔案 '{display_name}' 未透過內容關鍵字匹配到特定 schema，嘗試使用 'default_daily'。")
                matched_schema_name = "default_daily"
                # detected_encoding remains None, pd.read_csv will try its list

            if not matched_schema_name:
                log_msg = f"檔案 '{display_name}' 無法匹配任何 schema。已嘗試匹配: {', '.join(attempted_schemas) if attempted_schemas else '無可用 schema'}，且無 default_daily 後備或 default_daily 嘗試失敗。"
                self.logger.warning(log_msg)
                return {
                    constants.KEY_STATUS: constants.STATUS_SKIPPED,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: f"無法根據內容關鍵字識別檔案類型。已嘗試匹配 schema: {', '.join(attempted_schemas) if attempted_schemas else '無可用 schema'}。",
                }

            # Proceed with parsing using pandas
            df = None
            error_messages = []
            print(f"DBG: Initial detected_encoding: {detected_encoding} for {display_name}")

            # Use detected_encoding first if available, otherwise try all
            pandas_encodings_to_try = [detected_encoding] + encodings_to_try if detected_encoding else encodings_to_try

            for enc in pandas_encodings_to_try:
                if enc is None: continue # Skip if detected_encoding was None and is first in list
                try:
                    input_for_pandas = None
                    if isinstance(file_input, BytesIO):
                        file_input.seek(0) # Reset for each read attempt
                        csv_bytes = file_input.read()
                        try:
                            # Use the current encoding `enc` from the loop for decoding
                            csv_string = csv_bytes.decode(enc)
                            if csv_string.startswith('\ufeff'): # Check for BOM
                                self.logger.debug(f"DBG: Found BOM for {display_name} with encoding {enc}, removing it.")
                                csv_string = csv_string[1:]
                            print(f"DBG_CSV_STRING for {display_name} with {enc}:\n'''{csv_string[:500]}'''") # Print first 500 chars
                            input_for_pandas = io.StringIO(csv_string)
                            file_input.seek(0) # Reset again if other parts of the code re-read file_input
                        except UnicodeDecodeError as ude_detail:
                            self.logger.debug(f"DBG: Failed to decode BytesIO with {enc} for {display_name}: {ude_detail}")
                            error_messages.append(f"使用 {enc} 編碼解碼 BytesIO 失敗: {ude_detail}")
                            continue # Try next encoding
                    else: # It's a file path (string)
                        input_for_pandas = file_input

                    df = pd.read_csv(input_for_pandas, low_memory=False, encoding=enc if isinstance(input_for_pandas, str) else None, on_bad_lines="skip")

                    if not df.empty:
                        self.logger.info(f"成功使用編碼 '{enc}' 讀取檔案 '{display_name}'。")
                        print(f"DBG: Columns after pd.read_csv with {enc}: {df.columns.tolist()} for {display_name}")
                        break
                    else:
                        # If df is empty, it could be due to on_bad_lines='skip' removing all lines.
                        # Or the file was genuinely empty of data.
                        error_messages.append(f"使用 {enc} 編碼讀取後檔案為空或所有行均格式錯誤")
                except UnicodeDecodeError: # This might still be hit if input_for_pandas is a path and encoding is wrong
                    error_messages.append(f"使用 {enc} 編碼解碼失敗")
                except pd.errors.EmptyDataError:
                    error_messages.append(f"使用 {enc} 編碼時檔案為空 (EmptyDataError)")
                except Exception as read_e:
                    error_messages.append(f"使用 {enc} 編碼讀取時發生非預期錯誤: {read_e}")

            if df is None or df.empty:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: f"無法使用支援的編碼成功讀取檔案內容，或檔案為空. Errors: {'; '.join(error_messages)}",
                }

            # Use self.schemas_config here as well
            schema = self.schemas_config.get(matched_schema_name)
            if not schema or not schema.get("columns_map"):
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: f"Schema '{matched_schema_name}' 未定義或其 column_map 為空", # This error message is fine
                }
            df.rename(columns=lambda c: str(c).strip().lower(), inplace=True)
            print(f"DBG: Columns after strip().lower(): {df.columns.tolist()} for {display_name}")
            column_map = {
                alias.lower(): target_col_name
                for target_col_name, details in schema["columns_map"].items()
                for alias in details.get("aliases", [])
            }
            print(f"DBG: Generated column_map: {column_map} for schema {matched_schema_name} for {display_name}")
            df.rename(columns=column_map, inplace=True)
            print(f"DBG: Columns after alias rename: {df.columns.tolist()} for {display_name}")
            target_columns = list(schema["columns_map"].keys())
            print(f"DBG: Target columns for schema {matched_schema_name}: {target_columns} for {display_name}")

            # --- Custom data cleaning for specific columns ---
            canonical_volume_column_name = "volume"
            if canonical_volume_column_name in df.columns:
                self.logger.info(f"檔案 '{display_name}' 偵測到欄位 '{canonical_volume_column_name}'，嘗試進行千分位逗號移除與數值轉換。")
                try:
                    # 確保該欄位轉換為字串型態，以便進行字串操作
                    df[canonical_volume_column_name] = df[canonical_volume_column_name].astype(str)

                    # 新增：移除前後空白並處理潛在的無效字串，例如 '--' 或完全空白的字串，將它們轉換為 pd.NA
                    df[canonical_volume_column_name] = df[canonical_volume_column_name].str.strip()
                    df[canonical_volume_column_name] = df[canonical_volume_column_name].replace(['--', '', 'None', 'nan', 'NaN'], pd.NA, regex=False)

                    # 移除千分位逗號
                    df[canonical_volume_column_name] = df[canonical_volume_column_name].str.replace(",", "", regex=False)

                    # 轉換為數值型態，無效解析則設為 NaN
                    df[canonical_volume_column_name] = pd.to_numeric(df[canonical_volume_column_name], errors='coerce')

                    self.logger.info(f"檔案 '{display_name}' 的 '{canonical_volume_column_name}' 欄位已成功轉換為數值型態。")
                except Exception as e_clean:
                    self.logger.warning(f"檔案 '{display_name}' 在清理 '{canonical_volume_column_name}' 欄位時發生錯誤: {e_clean}。該欄位可能未正確轉換。")
            else:
                self.logger.debug(f"檔案 '{display_name}' (schema: {matched_schema_name}) 中未找到預期的成交量欄位 '{canonical_volume_column_name}' 供清理，跳過此步驟。")
            # --- End of custom data cleaning ---

            # Check if any of the target columns are present after renaming
            # This is important to ensure the schema matching was meaningful
            if not any(col in df.columns for col in target_columns):
                # Log current columns for debugging
                self.logger.warning(f"檔案 '{display_name}' 匹配到 schema '{matched_schema_name}'，但重命名後找不到任何目標欄位。可用欄位: {df.columns.tolist()}")
                # Try to find at least one common column based on original names (before rename) to be more robust
                # This part can be enhanced, but for now, we stick to target_columns
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: f"欄位重命名後，檔案 '{display_name}' 內容與 schema '{matched_schema_name}' 的目標欄位不符。檢查 schema 關鍵字或檔案內容。",
                }

            df = df.reindex(columns=target_columns) # Reindex to ensure consistent column order and add missing columns as NaN

            # --- Add source column ---
            source_value = "unknown_source" # Default value
            if matched_schema_name == "weekly_report":
                source_value = "taifex_weekly_csv"
            elif matched_schema_name == "default_daily":
                source_value = "taifex_daily_csv"
            # Add more elif blocks here for other schemas if needed

            df["source"] = source_value
            self.logger.info(f"為檔案 '{display_name}' (schema: {matched_schema_name}) 添加 source 欄位，值為: '{source_value}'")
            # Ensure 'source' is part of target_columns if it wasn't already implicitly
            # (it should be if defined in columns_map and used by df.reindex)
            # If 'source' was not in schema["columns_map"].keys(), df.reindex would not have added it.
            # However, we added "source" to columns_map in schemas.json, so target_columns includes "source".
            # df.reindex would have created it (likely with NaNs if not previously existing).
            # The assignment df["source"] = source_value correctly fills it.

            # --- Required columns check ---
            required_columns = schema.get("required_columns", [])
            print(f"DBG: Required columns for schema {matched_schema_name}: {required_columns} for {display_name}")
            if required_columns:
                empty_or_null_required_cols = []
                for req_col in required_columns:
                    if req_col not in df.columns:
                        # This implies 'req_col' from 'required_columns' was NOT in 'target_columns' (schema definition error)
                        # or it's a column that should have been mapped but wasn't.
                        empty_or_null_required_cols.append(f"{req_col} (definition/mapping issue, not in DataFrame columns)")
                        print(f"DBG: Required col {req_col} not in df.columns for {display_name}. Current df columns: {df.columns.tolist()}")
                        continue

                    if df[req_col].isnull().all():
                        empty_or_null_required_cols.append(req_col)
                        print(f"DBG: Required col {req_col} is all null in df for {display_name}")


                if empty_or_null_required_cols:
                    self.logger.warning(
                        f"檔案 '{display_name}' 匹配到 schema '{matched_schema_name}', "
                        f"但以下必要欄位缺失或完全為空: {', '.join(empty_or_null_required_cols)}. "
                        f"DataFrame 欄位: {df.columns.tolist()}"
                    )
                    return {
                        constants.KEY_STATUS: constants.STATUS_ERROR,
                        constants.KEY_FILE: display_name,
                        constants.KEY_REASON: (
                            f"內容與 schema '{matched_schema_name}' 的必要欄位不符. "
                            f"缺失或為空的必要欄位: {', '.join(empty_or_null_required_cols)}."
                        ),
                    }
            # --- End of required columns check ---

            output_hash = hashlib.sha256(display_name.encode("utf-8")).hexdigest()
            output_dir = os.path.join(staging_dir, matched_schema_name)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{output_hash}.parquet")
            # df.to_parquet(output_path, engine="pyarrow", index=False) # Parquet writing will be handled by Orchestrator after validation
            db_table_name = schema.get("db_table_name", matched_schema_name) # Use db_table_name if available

            # The decision is to return the DataFrame itself for validation.
            # The orchestrator will then handle writing the valid_df to Parquet.
            # The 'output_path' here might become irrelevant or represent a temporary path if FileParser writes one.
            # For now, let's assume FileParser's main output for success is the DataFrame.
            # The KEY_PATH returned for now will be None or a conceptual path, as the final path is determined after validation.

            return {
                constants.KEY_STATUS: constants.STATUS_SUCCESS,
                constants.KEY_FILE: display_name, # original filename or display name (e.g. zip/file.csv)
                constants.KEY_TABLE: db_table_name,
                constants.KEY_COUNT: len(df),
                constants.KEY_DATAFRAME: df, # Return the processed DataFrame
                constants.KEY_MATCHED_SCHEMA_NAME: matched_schema_name, # Return the schema name used for parsing/validation rules
                constants.KEY_PATH: None, # No final Parquet path from parser itself anymore, Orchestrator handles it
            }
        except Exception as e:
            self.logger.error(f"處理檔案 '{display_name}' 時發生未知錯誤: {e}", exc_info=True)
            return {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: display_name,
                constants.KEY_REASON: f"解析檔案時發生未知錯誤: {e}",
                constants.KEY_DATAFRAME: None, # Ensure dataframe key exists even on error
                constants.KEY_MATCHED_SCHEMA_NAME: matched_schema_name if 'matched_schema_name' in locals() else "unknown_schema_due_to_error"
            }
