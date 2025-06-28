

"""
資料庫載入模組

此模組負責將處理完成的 DataFrame 數據，高效地載入至 DuckDB 資料庫中。
"""

import logging
import json # Added for loading schemas
import os # Added for os.path.exists
from typing import TYPE_CHECKING

import duckdb
# pandas is no longer needed in this file directly for load_data
# import pandas as pd

if TYPE_CHECKING:
    from logging import Logger


class DatabaseLoader:
    """
    負責與 DuckDB 互動，並載入處理後的數據。
    """

    def __init__(self, database_file: str, logger: "Logger"):
        """
        初始化 DatabaseLoader。

        Args:
            database_file (str): DuckDB 資料庫檔案的完整路徑。
            logger (Logger): 用於記錄日誌的 Logger 物件。
        """
        self.database_file = database_file
        self.logger = logger
        self.connection = None
        self.allowed_table_names = set()
        self.table_primary_keys = {} # New: Store primary key info
        self._load_schema_configurations() # Renamed and enhanced method
        self._connect()

    def _load_schema_configurations(self):
        """
        Loads allowed table names and their primary key definitions from the schemas.json configuration file.
        """
        schemas_file_path = "config/schemas.json" # Consider making this configurable if needed
        try:
            self.logger.info(f"載入資料表名稱和主鍵設定，來源: '{schemas_file_path}'...")
            with open(schemas_file_path, "r", encoding="utf-8") as f:
                schemas_data = json.load(f)

            for schema_key, schema_config in schemas_data.items():
                if not isinstance(schema_config, dict):
                    self.logger.warning(f"Schema '{schema_key}' 在 '{schemas_file_path}' 中的項目不是有效的字典。")
                    continue

                table_name = schema_config.get("db_table_name")
                if not table_name:
                    self.logger.warning(f"Schema '{schema_key}' 在 '{schemas_file_path}' 中缺少 'db_table_name'。")
                    continue

                self.allowed_table_names.add(table_name)

                primary_key_columns = schema_config.get("primary_key")
                if primary_key_columns and isinstance(primary_key_columns, list) and len(primary_key_columns) > 0:
                    self.table_primary_keys[table_name] = primary_key_columns
                    self.logger.info(f"資料表 '{table_name}' 找到主鍵定義: {primary_key_columns}")
                else:
                    self.logger.info(f"資料表 '{table_name}' 未找到有效的 'primary_key' 定義。")

            if self.allowed_table_names:
                self.logger.info(
                    f"成功載入 {len(self.allowed_table_names)} 個允許的資料表名稱: {self.allowed_table_names}"
                )
            else:
                self.logger.warning(
                    f"未從 '{schemas_file_path}' 載入任何允許的資料表名稱。"
                    "請確保檔案存在、格式正確，並包含有效的 'db_table_name' 項目。"
                )

        except FileNotFoundError:
            self.logger.error(
                f"Schemas 檔案 '{schemas_file_path}' 未找到。將不允許任何資料表名稱，且無法定義主鍵。"
            )
            self.allowed_table_names = set()
            self.table_primary_keys = {}
        except json.JSONDecodeError as e:
            self.logger.error(
                f"解析 JSON 檔案 '{schemas_file_path}' 時發生錯誤: {e}。將不允許任何資料表名稱，且無法定義主鍵。"
            )
            self.allowed_table_names = set()
            self.table_primary_keys = {}
        except Exception as e:
            self.logger.error(
                f"從 '{schemas_file_path}' 載入資料表名稱和主鍵時發生未預期錯誤: {e}",
                exc_info=True
            )
            self.allowed_table_names = set()
            self.table_primary_keys = {}

    def _connect(self):
        """
        建立與 DuckDB 資料庫的連線。
        """
        try:
            self.logger.info(f"正在連線至 DuckDB 資料庫：{self.database_file}")
            self.connection = duckdb.connect(database=self.database_file, read_only=False)
            self.logger.info("✅ DuckDB 資料庫連線成功。")
        except Exception as e:
            self.logger.critical(f"❌ 無法連線至 DuckDB 資料庫！錯誤：{e}", exc_info=True)
            raise

    def load_parquet(self, table_name: str, parquet_file_path: str):
        """
        將 Parquet 檔案的數據載入到 DuckDB 的指定資料表中。

        Args:
            table_name (str): 目標資料表的名稱。
            parquet_file_path (str): Parquet 檔案的路徑。
        """
        if self.connection is None:
            self.logger.error("資料庫連線不存在，無法載入 Parquet 資料。")
            return

        if table_name not in self.allowed_table_names:
            self.logger.error(
                f"Table name '{table_name}' is not in the allowed list of table names. "
                f"Allowed names: {self.allowed_table_names}. Aborting load operation for '{parquet_file_path}'."
            )
            return

        if not os.path.exists(parquet_file_path):
            self.logger.error(
                f"Parquet file '{parquet_file_path}' does not exist. Aborting load operation for table '{table_name}'."
            )
            return

        self.logger.info(f"準備將 Parquet 檔案 '{parquet_file_path}' 載入至資料表 '{table_name}'...")

        try:
            # Check if table exists, and if not, create it with potential PRIMARY KEY
            # Check if table exists
            check_table_exists_sql = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
            table_exists = self.connection.execute(check_table_exists_sql).fetchone()

            if not table_exists:
                pk_columns = self.table_primary_keys.get(table_name)
                if pk_columns:
                    self.logger.info(f"資料表 '{table_name}' 不存在，將使用定義的主鍵 {pk_columns} 建立。")
                    # Describe Parquet to get column names and types
                    cursor = self.connection.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_file_path}')")
                    columns_description = cursor.fetchall()

                    column_definitions = []
                    for col_name_desc, col_type_desc, _, _, _, _ in columns_description:
                        # Ensure column names are quoted if they contain special characters or are keywords
                        column_definitions.append(f'"{col_name_desc}" {col_type_desc}')

                    create_table_statement = f'CREATE TABLE "{table_name}" ({", ".join(column_definitions)}'

                    # Ensure primary key column names are also quoted
                    quoted_pk_columns = [f'"{col}"' for col in pk_columns]
                    create_table_statement += f', PRIMARY KEY ({", ".join(quoted_pk_columns)})'
                    create_table_statement += ');'

                    self.connection.execute(create_table_statement)
                    self.logger.info(f"資料表 '{table_name}' 已使用定義的欄位和主鍵建立。")
                else:
                    self.logger.warning(
                        f"資料表 '{table_name}' 在 schemas.json 中沒有找到主鍵定義。"
                        "將使用 Parquet 結構建立表格，ON CONFLICT 可能無法按預期工作或會基於所有欄位。"
                    )
                    # Fallback to old method if no primary key defined
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS "{table_name}" AS
                    SELECT * FROM read_parquet('{parquet_file_path}') LIMIT 0;
                    """
                    self.connection.execute(create_table_sql)
                    self.logger.info(f"資料表 '{table_name}' 結構已基於 Parquet 檔案確認/建立 (無顯式主鍵)。")
            else:
                 self.logger.info(f"資料表 '{table_name}' 已存在。將嘗試插入資料。")


            # Get current total row count in the table BEFORE insert
            count_before_query = f'SELECT COUNT(*) FROM "{table_name}"'
            count_before = self.connection.execute(count_before_query).fetchone()[0]

            # Insert data
            pk_columns = self.table_primary_keys.get(table_name)

            if pk_columns:
                # If primary keys are defined, use ON CONFLICT DO NOTHING
                # Ensure primary key column names are quoted for the ON CONFLICT clause
                pk_conflict_str = ", ".join([f'"{col}"' for col in pk_columns])
                insert_sql = f"""
                INSERT INTO "{table_name}"
                SELECT * FROM read_parquet('{parquet_file_path}')
                ON CONFLICT ({pk_conflict_str}) DO NOTHING;
                """
                self.logger.info(f"資料已使用 ON CONFLICT ({pk_conflict_str}) DO NOTHING 策略嘗試載入至資料表 '{table_name}'。")
            else:
                # If no primary keys, use a simple INSERT
                insert_sql = f"""
                INSERT INTO "{table_name}"
                SELECT * FROM read_parquet('{parquet_file_path}');
                """
                self.logger.info(f"資料已使用普通 INSERT 策略嘗試載入至資料表 '{table_name}' (無主鍵衝突處理)。")

            self.connection.execute(insert_sql)

            # Get current total row count in the table AFTER insert
            count_after = self.connection.execute(count_before_query).fetchone()[0]
            rows_actually_inserted = count_after - count_before

            # To know how many rows were *attempted* to be inserted from the Parquet file:
            rows_in_source_parquet_query = f"SELECT COUNT(*) FROM read_parquet('{parquet_file_path}')"
            rows_in_source_parquet = self.connection.execute(rows_in_source_parquet_query).fetchone()[0]

            self.logger.info(
                f"✅ 資料表 '{table_name}': Parquet 源文件含 {rows_in_source_parquet} 行, 本次實際插入 {rows_actually_inserted} 行。"
                f" 資料表 '{table_name}' 現在總共有 {count_after} 筆記錄。"
            )
            return {"rows_in_source": rows_in_source_parquet, "rows_inserted": rows_actually_inserted}

        except Exception as e:
            self.logger.error(
                f"❌ 載入 Parquet 檔案 '{parquet_file_path}' 至資料表 '{table_name}' 時發生錯誤: {e}",
                exc_info=True
            )
            # Return zero counts or raise to indicate failure to caller
            return {"rows_in_source": 0, "rows_inserted": 0} # Or based on attempted_insert_count if available before error

    def close_connection(self):
        """
        安全地關閉與資料庫的連線。
        """
        if self.connection:
            try:
                self.logger.info("正在執行 CHECKPOINT 並關閉 DuckDB 資料庫連線...")
                self.connection.execute("CHECKPOINT;")
                self.connection.close()
                self.connection = None # Ensure connection attribute is set to None after closing
                self.logger.info("資料庫連線已成功 CHECKPOINT 並關閉。")
            except Exception as e:
                self.logger.error(f"關閉資料庫連線時發生錯誤 (可能在 CHECKPOINT 期間): {e}", exc_info=True)
                # Attempt to close again if checkpoint failed but connection is still there
                if self.connection:
                    try:
                        self.connection.close()
                    except Exception as e_close:
                        self.logger.error(f"嘗試再次關閉連接失敗: {e_close}", exc_info=True)
                self.connection = None # Still set to None

