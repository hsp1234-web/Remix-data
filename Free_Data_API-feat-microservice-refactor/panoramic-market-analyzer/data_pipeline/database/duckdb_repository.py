# database/duckdb_repository.py
import duckdb
import pandas as pd
import logging
from typing import Optional
from ..interfaces.database_interface import DatabaseInterface

class DuckDBRepository(DatabaseInterface):
    """使用 DuckDB 作為後端存儲的數據庫實現。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        # 不在 __init__ 中自動連接，讓 Commander 控制連接時機
        # self.connect()

    def connect(self):
        """建立數據庫連接。"""
        if self.connection is not None: # 檢查是否已存在連接實例
            # 可以選擇性地添加一個ping或簡單查詢來驗證連接是否仍然活躍
            # 但為了簡潔，這裡假設如果 self.connection 不是 None，它就是有效的
            # 或者依賴於後續操作失敗時 DuckDB 自動拋出的異常
            try:
                # 嘗試一個無害的操作來檢查連接是否真的有效
                self.connection.execute("SELECT 42")
                self.logger.info(f"Already connected and connection is active to DuckDB database: {self.db_path}")
                return
            except Exception as e: # duckdb.ConnectionException 或類似的
                self.logger.warning(f"Connection object existed but was not active for {self.db_path}: {e}. Reconnecting.")
                self.connection = None # 標記為無效，以便重新連接

        try:
            self.connection = duckdb.connect(database=self.db_path, read_only=False)
            self.logger.info(f"Successfully connected to DuckDB database: {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to connect to DuckDB at {self.db_path}: {e}", exc_info=True)
            raise # 重新拋出異常，讓上層處理

    def disconnect(self):
        """斷開數據庫連接。"""
        if self.connection is not None:
            try:
                self.connection.close()
                self.logger.info(f"DuckDB database connection to {self.db_path} closed.")
            except Exception as e:
                self.logger.error(f"Error closing DuckDB connection to {self.db_path}: {e}", exc_info=True)
            finally:
                self.connection = None # 確保在 close 後將引用設為 None
        else:
            self.logger.info(f"No active DuckDB connection to {self.db_path} to close.")


    def _create_ohlcv_table_if_not_exists(self, table_name: str):
        """
        創建指定的 OHLCV 表格 (如果它不存在)。
        包含 symbol 欄位。
        """
        if self.connection is None:
            self.logger.error("Cannot create table, database connection is None (likely disconnected or failed to connect).")
            raise ConnectionError("DuckDB connection is not active.")

        # 使用更標準的 SQL 類型，並確保 'date' 和 'symbol' 是複合主鍵
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            date TIMESTAMP,
            symbol VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (date, symbol)
        );
        """
        try:
            self.connection.execute(create_sql)
            self.logger.debug(f"Table '{table_name}' checked/created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating table '{table_name}': {e}", exc_info=True)
            raise

    def upsert_ohlcv(self, data: pd.DataFrame, table_name: str):
        """
        將 OHLCV DataFrame 數據保存到指定的表中 (更新或插入)。
        數據中必須包含 'symbol' 欄位。
        """
        if self.connection is None:
            self.logger.error(f"Cannot upsert data to '{table_name}', database connection is None.")
            raise ConnectionError("DuckDB connection is not active.")

        if data is None or data.empty:
            self.logger.warning(f"Received empty or None DataFrame, no data to upsert into '{table_name}'.")
            return

        required_cols = {'date', 'symbol', 'open', 'high', 'low', 'close', 'adj_close', 'volume'}
        if not required_cols.issubset(data.columns):
            missing_cols = required_cols - set(data.columns)
            self.logger.error(f"DataFrame is missing required columns for upsert: {missing_cols}. Cannot proceed with table '{table_name}'.")
            return

        self._create_ohlcv_table_if_not_exists(table_name)

        # 準備數據以匹配表格結構
        data_to_insert = data.copy()

        # 確保 'date' 列是 Pandas datetime 類型，DuckDB 會自動轉換
        try:
            data_to_insert['date'] = pd.to_datetime(data_to_insert['date'])
        except Exception as e:
            self.logger.error(f"Error converting 'date' column to datetime for upsert into '{table_name}': {e}", exc_info=True)
            return # 如果日期轉換失敗，則不繼續

        # DuckDB 的 Python API 可以直接註冊 DataFrame 並執行 SQL
        # 使用 DuckDB 的 UPSERT (ON CONFLICT ... DO UPDATE) 功能
        # 注意：表名和列名在 SQL 中最好用雙引號括起來，以避免關鍵字衝突或大小寫問題
        # 但 DuckDB 通常對大小寫不敏感，除非特別配置

        # 創建一個暫時的視圖名稱，以避免與已存在的表衝突
        temp_view_name = f"temp_upsert_view_{table_name}"

        try:
            # 將 pandas DataFrame 註冊為 DuckDB 中的一個臨時視圖
            self.connection.register(temp_view_name, data_to_insert)

            upsert_sql = f"""
            INSERT INTO "{table_name}" (date, symbol, open, high, low, close, adj_close, volume)
            SELECT date, symbol, open, high, low, close, adj_close, volume FROM "{temp_view_name}"
            ON CONFLICT (date, symbol) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adj_close = EXCLUDED.adj_close,
                volume = EXCLUDED.volume;
            """
            self.connection.execute(upsert_sql)
            self.logger.info(f"Successfully upserted {len(data_to_insert)} rows into '{table_name}'.")
        except Exception as e:
            self.logger.error(f"Error upserting data into '{table_name}': {e}", exc_info=True)
            # 可以考慮是否要 raise e，讓上層知道操作失敗
        finally:
            # 務必取消註冊臨時視圖
            self.connection.unregister(temp_view_name)


    def get_ohlcv(self, symbol: str, table_name: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """從指定表中獲取特定股票在特定時間範圍的 OHLCV 數據。"""
        if self.connection is None:
            self.logger.error(f"Cannot get data from '{table_name}', database connection is None.")
            raise ConnectionError("DuckDB connection is not active.")

        query = f"""
        SELECT * FROM "{table_name}"
        WHERE symbol = ? AND date >= ? AND date <= ?
        ORDER BY date ASC;
        """
        try:
            # 使用參數化查詢以防止 SQL 注入
            result_df = self.connection.execute(query, [symbol, start_date, end_date]).fetchdf()
            if result_df.empty:
                self.logger.info(f"No data found for symbol '{symbol}' in table '{table_name}' between {start_date} and {end_date}.")
            else:
                self.logger.info(f"Fetched {len(result_df)} rows for symbol '{symbol}' from table '{table_name}'.")
            return result_df
        except Exception as e:
            self.logger.error(f"Error fetching OHLCV data for '{symbol}' from '{table_name}': {e}", exc_info=True)
            return None

    def table_exists(self, table_name: str) -> bool:
        """檢查指定的表格是否存在於數據庫中。"""
        if self.connection is None:
            self.logger.error("Cannot check table existence, database connection is None.")
            raise ConnectionError("DuckDB connection is not active.")
        try:
            # DuckDB information_schema.tables
            query = f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';"
            result = self.connection.execute(query).fetchone()
            return result is not None
        except Exception as e:
            self.logger.error(f"Error checking if table '{table_name}' exists: {e}", exc_info=True)
            return False

    def get_distinct_symbols(self, table_name: str) -> list[str]:
        """從指定表中獲取所有不重複的股票代碼。"""
        if self.connection is None:
            self.logger.error(f"Cannot get distinct symbols from '{table_name}', database connection is None.")
            raise ConnectionError("DuckDB connection is not active.")

        if not self.table_exists(table_name):
            self.logger.warning(f"Table '{table_name}' does not exist. Cannot fetch distinct symbols.")
            return []

        query = f'SELECT DISTINCT symbol FROM "{table_name}" ORDER BY symbol;'
        try:
            symbols = self.connection.execute(query).fetchall()
            return [s[0] for s in symbols if s[0] is not None]
        except Exception as e:
            self.logger.error(f"Error fetching distinct symbols from '{table_name}': {e}", exc_info=True)
            return []

# 示例用法 (用於本地測試，不應包含在最終提交的類中)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db_path_test = 'test_market_data.duckdb' # 使用測試數據庫文件
    repo = DuckDBRepository(db_path=db_path_test)

    try:
        repo.connect() # 手動連接

        # 準備一些測試數據
        sample_data_1 = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-01']),
            'symbol': ['AAPL', 'AAPL', 'MSFT'],
            'open': [150.0, 152.0, 250.0],
            'high': [153.0, 154.0, 252.0],
            'low': [149.0, 151.0, 249.0],
            'close': [152.5, 153.0, 251.5],
            'adj_close': [152.0, 152.5, 251.0],
            'volume': [1000000, 1200000, 800000]
        })

        table_name_test = "ohlcv_daily_test"

        print(f"\nUpserting data for AAPL and MSFT into '{table_name_test}'...")
        repo.upsert_ohlcv(sample_data_1, table_name_test)

        print(f"\nFetching AAPL data from '{table_name_test}'...")
        aapl_data = repo.get_ohlcv('AAPL', table_name_test, '2023-01-01', '2023-01-31')
        if aapl_data is not None:
            print(aapl_data)

        print(f"\nFetching MSFT data from '{table_name_test}'...")
        msft_data = repo.get_ohlcv('MSFT', table_name_test, '2023-01-01', '2023-01-01')
        if msft_data is not None:
            print(msft_data)

        # 測試更新數據
        sample_data_update = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01']), # Same date and symbol as an existing record
            'symbol': ['AAPL'],
            'open': [160.0], # New open price
            'high': [163.0],
            'low': [159.0],
            'close': [162.5],
            'adj_close': [162.0],
            'volume': [1100000]
        })
        print(f"\nUpserting (updating) AAPL data for 2023-01-01 in '{table_name_test}'...")
        repo.upsert_ohlcv(sample_data_update, table_name_test)

        print(f"\nFetching updated AAPL data from '{table_name_test}'...")
        aapl_updated_data = repo.get_ohlcv('AAPL', table_name_test, '2023-01-01', '2023-01-01')
        if aapl_updated_data is not None:
            print(aapl_updated_data) # Should show the updated open price

        print(f"\nFetching distinct symbols from '{table_name_test}'...")
        symbols = repo.get_distinct_symbols(table_name_test)
        print(f"Distinct symbols: {symbols}")

        print(f"\nChecking if table '{table_name_test}' exists: {repo.table_exists(table_name_test)}")
        print(f"Checking if table 'non_existent_table' exists: {repo.table_exists('non_existent_table')}")

    except Exception as e_main:
        print(f"An error occurred in the main test block: {e_main}")
    finally:
        if repo:
            repo.disconnect() # 手動斷開
        # 清理測試數據庫文件
        import os
        if os.path.exists(db_path_test):
            try:
                os.remove(db_path_test)
                os.remove(f"{db_path_test}.wal") # DuckDB 的 WAL 文件
                print(f"\nCleaned up test database file: {db_path_test} and its .wal file.")
            except OSError as e:
                print(f"Error removing test database file {db_path_test} or its .wal file: {e}")
