import duckdb # Changed from sqlite3
import pandas as pd
import logging
from typing import List

class DatabaseWriter:
    def __init__(self, db_path: str, logger: logging.Logger):
        """
        初始化 DatabaseWriter。

        Args:
            db_path (str): DuckDB 資料庫文件的路徑。
            logger (logging.Logger): 外部傳入的日誌記錄器實例。
        """
        self.db_path = db_path
        self.logger = logger
        self.conn = None
        try:
            # DuckDB connect returns a connection object.
            # The database file is created if it doesn't exist.
            self.conn = duckdb.connect(database=self.db_path, read_only=False)
            self.logger.info(f"DatabaseWriter: 已成功連接到 DuckDB 資料庫: {self.db_path}")
        except duckdb.Error as e: # Changed from sqlite3.Error
            self.logger.error(f"DatabaseWriter: 連接到 DuckDB 資料庫 {self.db_path} 失敗: {e}", exc_info=True)
            raise

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, primary_keys: List[str]):
        """
        將 DataFrame 的數據 "Upsert" 到指定的資料庫表中。
        如果表不存在，會嘗試創建它（依賴於 df.to_sql 的行為，但最好是預先創建表）。

        Args:
            df (pd.DataFrame): 需要寫入的 Pandas DataFrame。
            table_name (str): 目標資料庫表的名稱。
            primary_keys (List[str]): 組成主鍵的欄位名稱列表。
        """
        if not self.conn:
            self.logger.error("DatabaseWriter: 資料庫連接未建立，無法執行 upsert 操作。")
            return

        if df.empty:
            self.logger.info(f"DataFrame 為空，跳過對 {table_name} 的寫入。")
            return

        if not primary_keys:
            self.logger.error(f"Upsert 操作需要定義主鍵 (primary_keys)，但未提供給表 {table_name}。")
            # 或者可以選擇退回到簡單的 append 操作，但這可能會導致重複
            # raise ValueError(f"Primary keys must be provided for upsert operation on table {table_name}")
            return

        temp_table = f"temp_{table_name}_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S%f')}" # 更唯一的臨時表名

        try:
            # 將數據寫入臨時表
            df.to_sql(temp_table, self.conn, if_exists='replace', index=False)
            self.logger.debug(f"數據已寫入臨時表 {temp_table}。")

            # 準備 SQL 語法
            cols = [f'"{c}"' for c in df.columns]
            # DuckDB (like PostgreSQL) uses EXCLUDED.column for conflict updates
            update_cols_set = [f'"{col.strip()}" = EXCLUDED."{col.strip()}"' for col in df.columns if col.strip() not in primary_keys]

            if not update_cols_set:
                self.logger.warning(f"表 {table_name} 的所有列都在主鍵中，沒有可更新的列。將執行 INSERT ... ON CONFLICT DO NOTHING。")
                upsert_sql = f"""
                INSERT INTO "{table_name}" ({', '.join(cols)})
                SELECT {', '.join(cols)} FROM "{temp_table}"
                ON CONFLICT ({', '.join(f'"{pk}"' for pk in primary_keys)}) DO NOTHING;
                """
            else:
                upsert_sql = f"""
                INSERT INTO "{table_name}" ({', '.join(cols)})
                SELECT {', '.join(cols)} FROM "{temp_table}"
                ON CONFLICT ({', '.join(f'"{pk}"' for pk in primary_keys)}) DO UPDATE SET
                {', '.join(update_cols_set)};
                """

            self.logger.debug(f"準備執行的 Upsert SQL for table {table_name}: {upsert_sql}")
            # DuckDB connection can execute SQL directly, no separate cursor needed for simple exec
            self.conn.execute(upsert_sql)
            # DuckDB typically runs in autocommit mode by default unless a transaction is explicitly started.
            # If not in a transaction, commit is not needed. If in a transaction, it would be.
            # For simplicity here, assuming default autocommit or explicit commit if a transaction was started by to_sql.
            # self.conn.commit() # Usually not needed unless inside BEGIN/COMMIT transaction block.

            # Let's ensure changes are written, especially if multiple operations or if not in autocommit
            # However, for a single execute, often not required.
            # If using `to_sql` and then `execute`, it's better to manage transactions explicitly if needed.
            # For now, assuming `execute` commits if not in an explicit transaction.
            # DuckDB's Python API auto-commits by default after each statement if not in a transaction.
            self.logger.info(f"成功將 {len(df)} 筆數據寫入/更新至 {table_name}。")

        except duckdb.Error as e: # Changed from sqlite3.Error
            self.logger.error(f"寫入 {table_name} 時發生 DuckDB 錯誤: {e}", exc_info=True)
            # DuckDB connections don't have a separate rollback() method like sqlite3.
            # If an error occurs, the transaction (if any was started) is usually rolled back automatically.
            # If running in autocommit mode, there's nothing to roll back for the failed statement.
            # self.conn.rollback() # Not a standard method on duckdb connection object
        except Exception as e:
            self.logger.error(f"寫入 {table_name} 時發生非 DuckDB 錯誤: {e}", exc_info=True)
            # No specific rollback for general exceptions either unless manually managing transactions.
        finally:
            try:
                if self.conn:
                    # No separate cursor needed for DROP TABLE with DuckDB's connection object
                    self.conn.execute(f"DROP TABLE IF EXISTS \"{temp_table}\"")
                    self.logger.debug(f"臨時表 {temp_table} 已刪除。")
            except duckdb.Error as e: # Changed from sqlite3.Error
                self.logger.error(f"刪除臨時表 {temp_table} 時失敗: {e}", exc_info=True)


    def close(self):
        """關閉資料庫連接。"""
        if self.conn:
            try:
                self.conn.close()
                self.logger.info(f"資料庫連接 {self.db_path} 已成功關閉。")
                self.conn = None
            except duckdb.Error as e: # Changed from sqlite3.Error
                self.logger.error(f"關閉資料庫連接 {self.db_path} 時失敗: {e}", exc_info=True)

    def __enter__(self):
        # For context manager support
        # Connection is already established in __init__ if successful
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # For context manager support
        self.close()
