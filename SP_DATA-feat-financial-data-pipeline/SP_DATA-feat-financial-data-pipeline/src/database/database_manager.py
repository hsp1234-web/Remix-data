import duckdb
import pandas as pd
from typing import Dict, Any, Optional, List
import logging
from pathlib import Path # Ensure Path is imported
import os # Required for os.urandom

class DatabaseManager:
    """
    管理與 DuckDB 資料庫的連接和操作。
    """
    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, project_root_dir: Optional[str] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        self.db_config = config.get('database', {})
        db_path_str = self.db_config.get('path', 'data/default_financial_data.duckdb') # Default path

        # Ensure the database path is absolute or relative to a known project root
        if project_root_dir:
            self.db_file = Path(project_root_dir) / db_path_str
        else:
            # Fallback if project_root_dir is not provided (e.g. direct testing)
            # This might need adjustment based on where run_prototype.sh executes from.
            # For the atomic script, main.py should pass its PROJECT_ROOT.
            self.db_file = Path(db_path_str)
            self.logger.warning(f"project_root_dir not provided to DatabaseManager. Database path resolved to: {self.db_file.resolve()}")

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.logger.info(f"DatabaseManager initialized. DB file target: {self.db_file.resolve()}")

    def connect(self):
        """建立與 DuckDB 資料庫的連接。"""
        if self.conn is not None: # Check if connection object exists
            try:
                # Try a simple query to see if connection is active
                self.conn.execute("SELECT 1")
                self.logger.info("Database connection already active and valid.")
                return
            except Exception as e: # duckdb.duckdb.ConnectionException or similar if closed/broken
                self.logger.warning(f"Existing connection object found but it's not usable ({e}). Will try to reconnect.")
                self.conn = None # Reset to force reconnection

        try:
            # Ensure the parent directory for the database file exists
            self.db_file.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(database=str(self.db_file), read_only=False)
            self.logger.info(f"Successfully connected to DuckDB database: {self.db_file.resolve()}")
            self._create_tables_if_not_exist() # Create tables upon connection
        except Exception as e:
            self.logger.critical(f"Failed to connect to DuckDB database at {self.db_file.resolve()}: {e}", exc_info=True)
            self.conn = None # Ensure conn is None if connection fails
            raise # Re-raise the exception to signal failure to the caller

    def disconnect(self):
        """關閉資料庫連接。"""
        if self.conn is not None:
            try:
                self.conn.close()
                self.logger.info(f"Disconnected from DuckDB database: {self.db_file.resolve()}")
            except Exception as e:
                self.logger.error(f"Error while closing DuckDB connection: {e}", exc_info=True)
        else:
            self.logger.info("Database connection already None or not established.")
        self.conn = None


    def _create_tables_if_not_exist(self):
        """如果表不存在，則創建它們。"""
        if self.conn is None:
            self.logger.error("Cannot create tables: Database connection is None.")
            return

        try:
            self.logger.info("Dropping and recreating tables to ensure fresh schema...")
            self.conn.execute("DROP TABLE IF EXISTS fact_macro_economic_data;")
            self.conn.execute("DROP TABLE IF EXISTS fact_stock_price;")
            self.logger.info("Old tables (if any) dropped.")

            # Schema for fact_macro_economic_data
            # metric_date: DATE, metric_name: VARCHAR, metric_value: DOUBLE,
            # source_api: VARCHAR, data_snapshot_timestamp: TIMESTAMP
            self.conn.execute("""
                CREATE TABLE fact_macro_economic_data (
                    metric_date DATE,
                    metric_name VARCHAR,
                    metric_value DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (metric_date, metric_name, source_api) -- Assuming this combination is unique
                );
            """)
            self.logger.info("Table 'fact_macro_economic_data' checked/created.")

            # Schema for fact_stock_price
            # price_date: DATE, security_id: VARCHAR, open_price: DOUBLE, high_price: DOUBLE,
            # low_price: DOUBLE, close_price: DOUBLE, adj_close_price: DOUBLE, volume: BIGINT,
            # dividends: DOUBLE, stock_splits: DOUBLE, source_api: VARCHAR, data_snapshot_timestamp: TIMESTAMP
            self.conn.execute("""
                CREATE TABLE fact_stock_price (
                    price_date DATE,
                    security_id VARCHAR,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    adj_close_price DOUBLE,
                    volume BIGINT,
                    dividends DOUBLE,
                    stock_splits DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (price_date, security_id, source_api) -- Assuming this combination is unique
                );
            """)
            self.logger.info("Table 'fact_stock_price' checked/created.")
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}", exc_info=True)
            # Depending on severity, might want to raise this

    def bulk_insert_or_replace(self, table_name: str, df: pd.DataFrame, unique_cols: List[str]):
        """
        將 DataFrame 中的數據批量插入或替換到指定的表中。
        使用 DuckDB 的 INSERT ... ON CONFLICT DO UPDATE (upsert) 功能。
        """
        if self.conn is None:
            self.logger.error(f"Cannot insert into {table_name}: Database connection is None.")
            return False
        if df.empty:
            self.logger.info(f"DataFrame for table {table_name} is empty. Nothing to insert.")
            return True # Not an error, just nothing to do

        self.logger.debug(f"Attempting to bulk insert/replace into {table_name}, {len(df)} rows. Unique cols: {unique_cols}")

        try:
            # Ensure DataFrame columns match table schema and have correct types if necessary.
            # DuckDB is quite good at type inference from Pandas, but explicit casting might be needed for complex cases.
            # For date/timestamp, ensure they are in a compatible format.
            # Example: df['metric_date'] = pd.to_datetime(df['metric_date']).dt.date
            # This should ideally be handled by the connectors before this stage.

            # Register DataFrame as a temporary table
            temp_table_name = f"temp_{table_name}_{os.urandom(4).hex()}" # Unique temp table name
            self.conn.register(temp_table_name, df)

            # Build the ON CONFLICT part of the SQL query
            if not unique_cols:
                raise ValueError("unique_cols must be provided for upsert operation.")

            conflict_target = ", ".join(unique_cols)

            # Build the SET part for DO UPDATE
            # Exclude unique_cols from update as they are used for conflict resolution
            update_cols = [col for col in df.columns if col not in unique_cols]
            if not update_cols: # If all columns are part of unique_cols, it's effectively an INSERT OR IGNORE
                 set_clause = "NOTHING" # Placeholder for DO NOTHING, adjust if needed
                 # For DuckDB, if all columns are unique keys, an insert on conflict would just do nothing.
                 # A more explicit "DO NOTHING" might be:
                 # INSERT INTO target_table SELECT * FROM source_table ON CONFLICT (unique_cols) DO NOTHING;
                 # However, we'll try to update other columns if they exist.
                 # If no columns to update, then an insert that conflicts will do nothing.
                 # To be safe, if update_cols is empty, we can simply do an INSERT OR IGNORE.
                 # For now, let's assume there's always at least one column to update or this case is handled by table design.
                 self.logger.warning(f"No columns to update for table {table_name} as all columns are in unique_cols. Conflicting rows will be ignored.")
                 # Simple insert, relying on PK to prevent duplicates if that's the desired behavior without explicit update
                 # This part needs careful consideration based on exact "replace" semantics desired.
                 # A common approach for "replace" with DuckDB is to delete and insert.
                 # This part needs careful consideration based on exact "replace" semantics desired.
                 # A common approach for "replace" with DuckDB is to delete and insert.
                 # Let's use the upsert functionality.

                # The logic is: if no columns to update (all are unique keys), then DO NOTHING on conflict.
                # Otherwise, DO UPDATE the non-key columns.
                 sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO NOTHING;"
                 self.logger.debug(f"Executing SQL (INSERT OR IGNORE style as no update_cols): {sql}")
            else: # There are columns to update
                set_statements = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
                sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO UPDATE SET {set_statements};"
                self.logger.debug(f"Executing SQL (UPSERT style): {sql}")

            self.conn.execute(sql) # This should be at the same indentation level as the if/else that defines sql
            self.conn.unregister(temp_table_name) # Clean up temporary table
            self.logger.info(f"Successfully inserted/replaced {len(df)} rows into {table_name}.")
            return True
        except Exception as e:
            self.logger.error(f"Error during bulk insert/replace into {table_name}: {e}", exc_info=True)
            # Attempt to unregister temp table even on error
            if 'temp_table_name' in locals() and self.conn.table(temp_table_name) is not None: # Check if temp table exists
                try:
                    self.conn.unregister(temp_table_name)
                except Exception as e_unreg:
                    self.logger.error(f"Failed to unregister temp table {temp_table_name} on error: {e_unreg}")
            return False

    def fetch_all_for_engine(self, table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_column: str = 'metric_date') -> Optional[pd.DataFrame]:
        """
        從指定的表中獲取所有數據，可選地按日期範圍過濾。
        """
        if self.conn is None:
            self.logger.error(f"Cannot fetch from {table_name}: Database connection is None.")
            return None

        self.logger.debug(f"Fetching all data for engine from {table_name}, date_col: {date_column}, start: {start_date}, end: {end_date}")

        query = f"SELECT * FROM {table_name}"
        params = []
        conditions = []

        if start_date:
            conditions.append(f"{date_column} >= ?")
            params.append(start_date)
        if end_date:
            conditions.append(f"{date_column} <= ?")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY {date_column}"

        try:
            result_df = self.conn.execute(query, params).fetchdf()
            self.logger.info(f"Successfully fetched {len(result_df)} rows from {table_name}.")
            return result_df
        except Exception as e:
            self.logger.error(f"Error fetching data from {table_name}: {e}", exc_info=True)
            return None

    def execute_query(self, query: str, params: Optional[list] = None) -> Optional[pd.DataFrame]:
        """執行一個自定義的 SQL 查詢並返回結果為 DataFrame。"""
        if self.conn is None:
            self.logger.error("Cannot execute query: Database connection is None.")
            return None
        try:
            self.logger.debug(f"Executing custom query: {query} with params: {params}")
            return self.conn.execute(query, params).fetchdf()
        except Exception as e:
            self.logger.error(f"Error executing custom query '{query}': {e}", exc_info=True)
            return None

    def close(self): # Alias for disconnect for convenience
        self.disconnect()

if __name__ == '__main__':
    # This __main__ block is for basic, standalone testing of DatabaseManager.
    # It will create a DuckDB file in the current directory or a 'data' subdirectory.

    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_db = logging.getLogger("DatabaseManagerTestRun_Atomic")
    if not test_logger_db.handlers:
        ch_db = logging.StreamHandler(sys.stdout)
        ch_db.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_db.addHandler(ch_db)
        test_logger_db.propagate = False

    # Test configuration for the database
    # Assume this script is run from the project root where 'data/' would be created.
    # For atomic script, PROJECT_ROOT will be passed from main.py
    test_db_config = {
        "database": {
            "path": "data/test_financial_data_atomic.duckdb"
        }
    }
    # Determine project root for test (assuming this test is run from project root)
    test_project_root = str(Path(".").resolve())

    test_logger_db.info("--- Starting DatabaseManager Test ---")
    # Clean up old test DB if it exists
    old_db_file = Path(test_project_root) / test_db_config["database"]["path"]
    if old_db_file.exists():
        test_logger_db.info(f"Deleting old test database: {old_db_file}")
        old_db_file.unlink()

    db_man = DatabaseManager(config=test_db_config, logger_instance=test_logger_db, project_root_dir=test_project_root)

    try:
        db_man.connect()
        assert db_man.conn is not None, "Connection failed"
        test_logger_db.info("DB Connection successful.")

        # Test table creation (should happen in connect)
        test_logger_db.info("Checking if tables were created...")
        tables_df = db_man.execute_query("SHOW TABLES;")
        assert tables_df is not None, "SHOW TABLES query failed"
        test_logger_db.info(f"Tables in DB:\n{tables_df}")
        table_names = tables_df['name'].tolist()
        assert 'fact_macro_economic_data' in table_names, "fact_macro_economic_data not created"
        assert 'fact_stock_price' in table_names, "fact_stock_price not created"
        test_logger_db.info("Table creation check passed.")

        # Test bulk_insert_or_replace for macro data
        macro_sample_data = {
            'metric_date': [datetime(2023,1,1).date(), datetime(2023,1,2).date(), datetime(2023,1,1).date()],
            'metric_name': ['FRED/DGS10', 'FRED/DGS10', 'FRED/UNRATE'],
            'metric_value': [2.5, 2.6, 3.5],
            'source_api': ['FRED', 'FRED', 'FRED'],
            'data_snapshot_timestamp': [datetime.now(timezone.utc)] * 3
        }
        macro_df_test = pd.DataFrame(macro_sample_data)
        test_logger_db.info(f"\nInserting macro data (1st time):\n{macro_df_test}")
        success_macro_insert1 = db_man.bulk_insert_or_replace('fact_macro_economic_data', macro_df_test, unique_cols=['metric_date', 'metric_name', 'source_api'])
        assert success_macro_insert1, "First macro insert failed"

        fetched_macro1 = db_man.fetch_all_for_engine('fact_macro_economic_data')
        assert fetched_macro1 is not None and len(fetched_macro1) == 3, f"Expected 3 rows after 1st macro insert, got {len(fetched_macro1) if fetched_macro1 is not None else 'None'}"
        test_logger_db.info(f"Macro data after 1st insert ({len(fetched_macro1)} rows):\n{fetched_macro1}")

        # Test upsert: update one row, insert a new one
        macro_update_data = {
            'metric_date': [datetime(2023,1,1).date(), datetime(2023,1,3).date()], # Update DGS10 on 2023-01-01, new DGS10 on 2023-01-03
            'metric_name': ['FRED/DGS10', 'FRED/DGS10'],
            'metric_value': [2.55, 2.7], # Updated value, new value
            'source_api': ['FRED', 'FRED'],
            'data_snapshot_timestamp': [datetime.now(timezone.utc)] * 2
        }
        macro_df_update = pd.DataFrame(macro_update_data)
        test_logger_db.info(f"\nUpserting macro data (update 1, insert 1):\n{macro_df_update}")
        success_macro_upsert = db_man.bulk_insert_or_replace('fact_macro_economic_data', macro_df_update, unique_cols=['metric_date', 'metric_name', 'source_api'])
        assert success_macro_upsert, "Macro upsert failed"

        fetched_macro2 = db_man.fetch_all_for_engine('fact_macro_economic_data')
        assert fetched_macro2 is not None and len(fetched_macro2) == 4, f"Expected 4 rows after macro upsert, got {len(fetched_macro2) if fetched_macro2 is not None else 'None'}"
        test_logger_db.info(f"Macro data after upsert ({len(fetched_macro2)} rows):\n{fetched_macro2}")
        # Check updated value
        updated_val = fetched_macro2[(fetched_macro2['metric_date'] == datetime(2023,1,1).date()) & (fetched_macro2['metric_name'] == 'FRED/DGS10')]['metric_value'].iloc[0]
        assert updated_val == 2.55, f"Expected updated DGS10 value to be 2.55, got {updated_val}"
        test_logger_db.info("Macro data upsert successful.")

        # Test fetch_all_for_engine with date range
        fetched_macro_ranged = db_man.fetch_all_for_engine('fact_macro_economic_data', start_date='2023-01-02', end_date='2023-01-03')
        assert fetched_macro_ranged is not None and len(fetched_macro_ranged) == 2, f"Expected 2 rows in date range, got {len(fetched_macro_ranged) if fetched_macro_ranged is not None else 'None'}"
        test_logger_db.info(f"Macro data for 2023-01-02 to 2023-01-03 ({len(fetched_macro_ranged)} rows):\n{fetched_macro_ranged}")

        test_logger_db.info("DatabaseManager tests passed successfully.")

    except Exception as e_test:
        test_logger_db.error(f"DatabaseManager test failed: {e_test}", exc_info=True)
    finally:
        db_man.disconnect()
        test_logger_db.info("--- DatabaseManager Test Finished ---")
        # Optional: delete the test database file after test
        # if old_db_file.exists():
        #     test_logger_db.info(f"Deleting test database after run: {old_db_file}")
        #     old_db_file.unlink(missing_ok=True)
