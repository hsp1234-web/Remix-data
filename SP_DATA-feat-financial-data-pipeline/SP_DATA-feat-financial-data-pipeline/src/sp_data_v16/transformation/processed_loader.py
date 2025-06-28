import pandas as pd
import duckdb
import os

class ProcessedDBLoader:
    """
    A class to load processed pandas DataFrames into a DuckDB database.
    """

    def __init__(self, db_path: str):
        """
        Initializes the ProcessedDBLoader with the path to the DuckDB database.

        Args:
            db_path: The file path for the DuckDB database.
                     Example: 'data/processed/processed_data.db'
        """
        self.db_path = db_path
        self.con = None

        try:
            # Ensure the parent directory for the database file exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir: # Check if db_dir is not an empty string (e.g. if db_path is just a filename)
                os.makedirs(db_dir, exist_ok=True)

            # Establish connection to DuckDB
            self.con = duckdb.connect(database=self.db_path, read_only=False)
            print(f"Successfully connected to DuckDB at '{self.db_path}'")
        except duckdb.Error as e:
            print(f"Error connecting to DuckDB at '{self.db_path}': {e}")
            # Potentially re-raise or handle more gracefully depending on application needs
            raise
        except OSError as e:
            print(f"Error creating directory for DuckDB database '{self.db_path}': {e}")
            raise

    def load_dataframe(self, dataframe: pd.DataFrame, table_name: str, schema_definition: dict):
        """
        將 pandas DataFrame 載入到 DuckDB 資料庫的指定資料表中，使用「寫入或更新 (Upsert)」邏輯。

        Args:
            dataframe: 要載入的 pandas DataFrame。
            table_name: 資料庫中目標資料表的名稱。
            schema_definition: 包含唯一鍵 (`unique_key`) 等資訊的結構定義。
        """
        if not self.con:
            print("錯誤：未建立資料庫連線。無法載入 DataFrame。")
            return

        if dataframe.empty:
            print(f"資訊：傳入的 DataFrame 為空，無需載入至資料表 '{table_name}'。")
            return

        temp_view_name = None # 初始化 temp_view_name
        try:
            # 從 schema_definition 獲取唯一鍵
            unique_key = schema_definition.get('unique_key')
            if not unique_key:
                # 如果沒有唯一鍵，退回使用 append 模式 (或拋出錯誤，視乎需求)
                print(f"警告：資料表 '{table_name}' 的結構定義中未指定 'unique_key'。將使用 append 模式載入。")
                dataframe.to_sql(name=table_name, con=self.con, if_exists='append', index=False)
                print(f"已成功將 DataFrame 附加到資料表 '{table_name}'。")
                return

            # 註冊 DataFrame 為暫存視圖，以便在 SQL 查詢中使用
            temp_view_name = f"temp_view_{table_name}"
            self.con.register(temp_view_name, dataframe)

            # 動態建立 ON CONFLICT 子句
            # 例如 unique_key = ['col1', 'col2'] -> ON CONFLICT (col1, col2)
            conflict_target = ", ".join(unique_key)

            # 動態建立 DO UPDATE SET 子句
            # 排除唯一鍵欄位，更新其他所有欄位
            # df_columns = [col for col in dataframe.columns if col not in unique_key] # 這樣會排除 unique_key 被更新，通常這是期望的
            df_columns = dataframe.columns # 更新所有欄位，包括 unique_key (雖然它們不會變)
            if not df_columns.tolist(): # 檢查是否有可更新的欄位
                print(f"錯誤：DataFrame 中沒有欄位可用於更新資料表 '{table_name}'。")
                self.con.unregister(temp_view_name)
                return

            set_clauses = []
            for col in df_columns:
                # 使用 excluded.{col} 來引用插入衝突列中的值
                set_clauses.append(f'"{col}" = excluded."{col}"')
            update_set_statement = ", ".join(set_clauses)

            # 組合完整的 Upsert SQL 語句
            # 注意：欄位名稱和資料表名稱可能需要加上引號，以處理特殊字元或保留字
            # DuckDB 通常對大小寫不敏感，但為求明確，可以使用引號

            # 首先，確保資料表存在
            # 從 DataFrame 推斷欄位類型以建立資料表，DuckDB 通常能很好地處理這個
            # 我們也可以從 schema_definition 獲取更精確的 DB 類型，但這會更複雜
            # 為了簡化，我們先讓 DuckDB 從 DataFrame 推斷
            # 更理想的做法是使用 schema_definition 中的 db_type 來定義欄位

            # 檢查資料表是否已存在
            table_exists_query = self.con.execute(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table_name}';").fetchone()

            if not table_exists_query:
                # 動態產生 CREATE TABLE 語句，包含 UNIQUE 約束
                # 這裡我們需要知道欄位的類型。為了簡化，我們讓 DuckDB 從 DataFrame 推斷類型，然後再添加約束。
                # 一個更穩健的方法是從 schema_definition 獲取欄位類型。
                # 暫時先用 DataFrame 的欄位直接建立，然後嘗試 ALTER TABLE (如果 DuckDB 支援這樣添加約束)
                # 或者，在 CREATE TABLE 時就定義好。

                # 從 DataFrame 獲取欄位名稱和類型 (DuckDB SQL 類型)
                # DuckDB 的 from_df 會自動推斷類型
                self.con.execute(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM {temp_view_name} WHERE 1=0;") # 先用 DataFrame 結構建立空表

                # 如果有 unique_key，則添加 UNIQUE 約束
                # 注意：DuckDB 對於 ALTER TABLE ADD CONSTRAINT 的支援可能有限或語法特定
                # 一個更安全的做法是在 CREATE TABLE 時就包含約束，但這需要明確的欄位類型定義
                # 這裡我們假設 unique_key 中的欄位都存在於 DataFrame 中
                if unique_key:
                    unique_key_str = ", ".join([f'"{col}"' for col in unique_key])
                    # 嘗試為已建立的空表添加 UNIQUE 約束
                    # DuckDB 可能不支援直接 ALTER TABLE ADD CONSTRAINT UNIQUE(...) 這樣的方式
                    # 另一種方法是建立帶有約束的新表，然後如果舊表有資料則轉移 (但這裡表是新建的)
                    # 或者，在 CREATE TABLE 語句中直接定義欄位和約束，這需要知道類型
                    # 例如: CREATE TABLE my_table (id INTEGER PRIMARY KEY, name VARCHAR, value DOUBLE UNIQUE)
                    # 由於我們是從 DataFrame 動態建立，這比較棘手。
                    # DuckDB 的 `CREATE TABLE ... AS SELECT ...` 不直接支援添加約束。
                    # 我們需要先建立帶約束的表，然後 INSERT。

                    # 退而求其次，如果直接 ALTER 不方便，我們可以在 INSERT ... ON CONFLICT 時依賴它。
                    # 但錯誤訊息指出需要約束。
                    # 因此，我們必須在 CREATE TABLE 時定義它。
                    # 這需要我們從 schema_definition 中獲取欄位的 DB 類型。

                    # 讓我們嘗試一種更明確的 CREATE TABLE 語句
                    # 這需要 `schema_definition` 包含每個欄位的 `db_type`

                    columns_with_types = []
                    if 'columns' in schema_definition and isinstance(schema_definition['columns'], dict):
                        for col_name, col_def in schema_definition['columns'].items():
                            if col_name in dataframe.columns: # 只處理 DataFrame 中實際存在的欄位
                                db_type = col_def.get('db_type', 'VARCHAR') # 預設為 VARCHAR
                                columns_with_types.append(f'"{col_name}" {db_type}')

                    if not columns_with_types: # 如果 schema_definition 中沒有欄位類型資訊，退回之前的方法
                         self.con.execute(f"CREATE TABLE IF NOT EXISTS \"{table_name}\" AS SELECT * FROM {temp_view_name} WHERE 1=0;")
                    else:
                        # 修改 CREATE TABLE 語句，將 PRIMARY KEY 直接附加到欄位定義
                        column_definitions_for_create = []
                        for col_name, col_def in schema_definition['columns'].items():
                            if col_name in dataframe.columns:
                                db_type = col_def.get('db_type', 'VARCHAR')
                                col_def_str = f'"{col_name}" {db_type}'
                                # 如果此欄位是 unique_key (假設 unique_key 只有一個欄位，或我們只取第一個作為 PK)
                                # 為了 ON CONFLICT，我們需要一個 PRIMARY KEY 或 UNIQUE 約束
                                # 假設 unique_key (現在被視為 primary key) 列表的第一個元素是主鍵
                                if unique_key and col_name == unique_key[0]: # 假設 unique_key 只有一個欄位或第一個是主鍵
                                    col_def_str += " PRIMARY KEY"
                                elif unique_key and col_name in unique_key: # 其他 unique_key 欄位可以設為 NOT NULL (如果需要)
                                     # 如果 schema 中有 nullable: False，可以加上 NOT NULL
                                     if col_def.get("nullable") is False:
                                         col_def_str += " NOT NULL"
                                column_definitions_for_create.append(col_def_str)

                        create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(column_definitions_for_create)});"
                        # 如果 unique_key 包含多個欄位，則上述 PRIMARY KEY(unique_key[0]) 的方法不完整
                        # 需要一個表級的 PRIMARY KEY (col1, col2, ...) 約束
                        # 因此，恢復之前的表級約束方式，但要確保語法正確

                        # 重新使用之前的表級約束方式，因為它更能處理複合主鍵
                        create_table_sql_parts = []
                        for col_name, col_def in schema_definition['columns'].items():
                            if col_name in dataframe.columns:
                                db_type = col_def.get('db_type', 'VARCHAR')
                                nullable = col_def.get('nullable', True)
                                part = f'"{col_name}" {db_type}'
                                if not nullable:
                                    part += " NOT NULL"
                                create_table_sql_parts.append(part)

                        if unique_key: # unique_key 被用作 PRIMARY KEY
                             # primary_key_constraint_str = ", ".join([f'"{col}"' for col in unique_key]) # 在 ALTER TABLE 中使用
                             # create_table_sql_parts.append(f"PRIMARY KEY ({primary_key_constraint_str})") # 不在 CREATE TABLE 時直接加
                             pass

                        create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(create_table_sql_parts)});"
                        print(f"準備執行 CREATE TABLE SQL (無內建約束): {create_table_sql}")
                        self.con.execute(create_table_sql)

                        # 如果有 unique_key，則在建表後嘗試用 ALTER TABLE 添加 PRIMARY KEY 約束
                        if unique_key:
                            try:
                                pk_columns_str = ", ".join([f'"{col}"' for col in unique_key])
                                alter_sql = f"ALTER TABLE \"{table_name}\" ADD PRIMARY KEY ({pk_columns_str});"
                                print(f"準備執行 ALTER TABLE ADD PRIMARY KEY SQL: {alter_sql}")
                                self.con.execute(alter_sql)
                                print(f"已為資料表 '{table_name}' 添加 PRIMARY KEY 約束於欄位: {pk_columns_str}")
                            except duckdb.Error as alter_err:
                                print(f"警告：為資料表 '{table_name}' 添加 PRIMARY KEY 約束失敗: {alter_err}。 Upsert 可能會失敗。")
                                # 即使 ALTER 失敗，也繼續嘗試 Upsert，看看會發生什麼

                        # 在 CREATE/ALTER TABLE 之後，執行一個簡單的查詢以確保表格結構被資料庫完全識別
                        self.con.execute(f"SELECT * FROM \"{table_name}\" WHERE 1=0;")
                print(f"已確保資料表 '{table_name}' 存在，並嘗試設定 PRIMARY KEY 約束。")
            else:
                print(f"資料表 '{table_name}' 已存在。")


            upsert_sql = f"""
            INSERT INTO "{table_name}" SELECT * FROM {temp_view_name}
            ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set_statement};
            """

            print(f"準備執行 Upsert SQL 至資料表 '{table_name}':\n{upsert_sql}")
            self.con.execute(upsert_sql)

            # 清理暫存視圖
            self.con.unregister(temp_view_name)

            print(f"已成功將 DataFrame 寫入或更新至資料表 '{table_name}'。")

        except duckdb.Error as e:
            print(f"DuckDB 錯誤：載入 DataFrame 至資料表 '{table_name}' 時發生錯誤: {e}")
            # 嘗試取消註冊視圖，以防萬一
            if temp_view_name and self.con:
                try:
                    self.con.unregister(temp_view_name)
                except duckdb.Error: # 可能視圖未成功註冊
                    pass
            raise # 重新拋出異常，讓呼叫者知道載入失敗
        except AttributeError as e:
            print(f"屬性錯誤：無法操作資料庫連線 (是否已初始化？): {e}")
            raise # 重新拋出
        except Exception as e:
            print(f"載入 DataFrame 至資料表 '{table_name}' 時發生未預期的錯誤: {e}")
            if temp_view_name and self.con: # 同上，嘗試清理
                try:
                    self.con.unregister(temp_view_name)
                except duckdb.Error:
                    pass
            raise # 重新拋出
    def close(self):
        """
        Closes the connection to the DuckDB database.
        """
        if self.con:
            try:
                self.con.close()
                print(f"Successfully closed connection to DuckDB at '{self.db_path}'.")
            except duckdb.Error as e:
                print(f"Error closing DuckDB connection: {e}")
        else:
            print("No active DuckDB connection to close.")

# Example Usage (optional, for testing)
if __name__ == '__main__':
    # Define a path for the test database
    test_db_file = 'test_processed_data.db'

    # Clean up any existing test database file from previous runs
    if os.path.exists(test_db_file):
        os.remove(test_db_file)
    if os.path.exists(test_db_file + ".wal"): # DuckDB Write-Ahead Log file
        os.remove(test_db_file + ".wal")

    # Create a loader instance
    try:
        loader = ProcessedDBLoader(db_path=test_db_file)
    except Exception as e:
        print(f"Failed to initialize loader: {e}")
        exit()

    # Create a sample DataFrame
    sample_data = {
        'id': [1, 2, 3, 4],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor'],
        'price': [1200.00, 25.00, 75.00, 300.00],
        'quantity': [10, 50, 30, 15]
    }
    df_products = pd.DataFrame(sample_data)

    sample_data_sales = {
        'sale_id': ['S1001', 'S1002', 'S1003'],
        'product_id': [1, 2, 1],
        'sale_date': pd.to_datetime(['2023-01-15', '2023-01-17', '2023-01-18']),
        'amount': [1200.00, 25.00, 1200.00]
    }
    df_sales = pd.DataFrame(sample_data_sales)

    # Load the DataFrames into different tables
    if loader.con: # Proceed only if connection was successful
        loader.load_dataframe(df_products, 'products')
        loader.load_dataframe(df_sales, 'sales')

        # Example of trying to load an empty DataFrame
        print("\nAttempting to load an empty DataFrame...")
        empty_df = pd.DataFrame()
        loader.load_dataframe(empty_df, 'empty_table') # Should still create the table via to_sql

        # Verify data by reading it back (optional)
        print("\nVerifying data from 'products' table:")
        try:
            result_products = loader.con.execute("SELECT * FROM products").fetchdf()
            print(result_products)
        except duckdb.Error as e:
            print(f"Error reading from 'products' table: {e}")

        print("\nVerifying data from 'sales' table:")
        try:
            result_sales = loader.con.execute("SELECT * FROM sales").fetchdf()
            print(result_sales)
        except duckdb.Error as e:
            print(f"Error reading from 'sales' table: {e}")

        print("\nVerifying 'empty_table' structure:")
        try:
            result_empty = loader.con.execute("DESCRIBE empty_table").fetchdf() # DuckDB command to get schema
            print(result_empty)
        except duckdb.Error as e:
            print(f"Error describing 'empty_table': {e}")


    # Close the connection
    loader.close()

    # Test case: loader initialization fails (e.g. permission denied if db_path was in a restricted area)
    print("\n--- Test Case: Loader initialization failure (simulated by using an invalid path) ---")
    # This is a bit hard to simulate reliably without changing file permissions,
    # but we can check if the error handling in __init__ for OSError works if directory creation fails.
    # For now, the existing __init__ handles duckdb connection errors.
    # If os.makedirs were to fail (e.g. due to permissions), it should be caught.

    # Clean up the test database file after example run
    if os.path.exists(test_db_file):
        os.remove(test_db_file)
    if os.path.exists(test_db_file + ".wal"):
        os.remove(test_db_file + ".wal")

    print("\nExample run finished.")
