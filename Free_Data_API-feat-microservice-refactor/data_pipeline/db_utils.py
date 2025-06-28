# data_pipeline/db_utils.py
import duckdb
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def save_dataframe(df: pd.DataFrame, table_name: str, primary_keys: list, db_path: str):
    """
    將一個 Pandas DataFrame 透過 UPSERT 邏輯存入指定的 DuckDB 表格。

    此函數會自動處理資料庫連線、表格創建、以及基於主鍵的更新或插入操作。

    Args:
        df (pd.DataFrame): 要儲存的數據。
        table_name (str): 目標資料庫中的表格名稱。
        primary_keys (list): 一個包含主鍵欄位名稱的列表，用於 UPSERT 判斷。
        db_path (str): DuckDB 資料庫檔案的路徑。
    """
    if df is None or df.empty:
        logging.warning(f"傳入的 DataFrame 為空，無需存入表格 '{table_name}'。")
        return

    if not primary_keys:
        raise ValueError("主鍵列表 `primary_keys` 不可為空。")

    if not all(pk in df.columns for pk in primary_keys):
        raise ValueError(
            f"主鍵 {primary_keys} 未全部存在於 DataFrame 的欄位中: {df.columns}")

    con = None
    try:
        con = duckdb.connect(database=db_path)

        # 註冊 DataFrame 以便在 SQL 中引用
        # 使用唯一的臨時視圖名稱以避免潛在的衝突，儘管 DuckDB 通常能處理好
        # temp_view_creation_name was here, removed as unused.
        temp_view_upsert_name = f"temp_df_for_upsert_{table_name}"

        # Define pk_string early so it's available for CREATE TABLE and UPSERT logic
        pk_string = ", ".join(
            [f'"{pk}"' for pk in primary_keys]) if primary_keys else ""

        # con.register(temp_view_creation_name, df) # No longer needed if not using AS SELECT
        # 自動從 DataFrame 推斷並創建表格 (如果不存在) 並定義主鍵

        # 檢查表格是否已存在，如果存在則不執行 CREATE TABLE
        # tables_query = con.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone() # For older DuckDB or compatibility
        # Using duckdb_tables() for newer versions:
        tables_query = con.execute(
            f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table_name}'").fetchone()

        if tables_query and tables_query[0] == 0:
            # 表格不存在，創建它並設定主鍵
            cols_with_types = []
            for col_name, dtype in df.dtypes.items():
                # Basic type mapping, can be expanded
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "DOUBLE"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    # Check if it's date or datetime
                    # Check if all valid dates in the column are midnight to consider it a DATE type
                    is_date_col = False
                    dropped_na_series = df[col_name].dropna()
                    if not dropped_na_series.empty:
                        # Ensure it's not an empty series after dropna
                        all_midnight = all(
                            d.time() == pd.Timestamp(0).time() for d in dropped_na_series
                        )
                        # hasattr checks if tz attribute exists, then check if it's None
                        is_not_timezone_aware = not (
                            hasattr(df[col_name].dtype, 'tz') and
                            df[col_name].dtype.tz is not None
                        )
                        if all_midnight and is_not_timezone_aware:
                            sql_type = "DATE"
                            is_date_col = True
                    if not is_date_col:
                        sql_type = "TIMESTAMP"
                elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
                    sql_type = "VARCHAR"
                else:
                    sql_type = "VARCHAR"  # Default
                cols_with_types.append(f'"{col_name}" {sql_type}')

            pk_constraint_str = f", PRIMARY KEY ({pk_string})" if pk_string else ""  # Check pk_string directly
            columns_definition = ", ".join(cols_with_types)

            create_table_sql = (
                f"CREATE TABLE \"{table_name}\" ("
                f"{columns_definition}"
                f"{pk_constraint_str})"
            )
            con.execute(create_table_sql)
            logging.info(
                f"表格 '{table_name}' 已創建，主鍵為: {primary_keys if primary_keys else '無'}")
        # else: table already exists, assume schema is compatible or UPSERT will handle it.

        # con.unregister(temp_view_creation_name) # No longer used

        # 使用 DuckDB 的 ON CONFLICT (UPSERT) 功能
        con.register(temp_view_upsert_name, df)

        # 為主鍵加上引號，以防包含特殊字元或保留字
        pk_string = ", ".join([f'"{pk}"' for pk in primary_keys])

        update_columns = [col for col in df.columns if col not in primary_keys]

        if not update_columns:
            # 如果所有欄位都是主鍵，或者沒有其他欄位可更新，則只嘗試插入並在衝突時不做任何事
            upsert_sql = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_view_upsert_name}
            ON CONFLICT ({pk_string}) DO NOTHING;
            """
        else:
            update_string = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in update_columns])  # 為欄位名加上引號
            upsert_sql = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_view_upsert_name}
            ON CONFLICT ({pk_string}) DO UPDATE SET
                {update_string};
            """

        con.execute(upsert_sql)
        logging.info(
            f"成功將 {len(df)} 筆記錄 UPSERT 至表格 '{table_name}' "
            f"({db_path})。")

    except Exception as e:
        logging.error(
            f"儲存 DataFrame 至表格 '{table_name}' 時發生錯誤: {e}", exc_info=True)
        raise
    finally:
        if con:
            # 確保臨時視圖在 finally 區塊中被反註冊
            # temp_view_creation_name相關的try-except塊已移除，因為該變數已不再使用。

            try:
                con.unregister(temp_view_upsert_name)
            except Exception as e_unregister:
                logging.debug(
                    f"反註冊視圖 {temp_view_upsert_name} 時發生非致命錯誤 (可能未註冊或已反註冊)。錯誤: {e_unregister}")

            con.close()
