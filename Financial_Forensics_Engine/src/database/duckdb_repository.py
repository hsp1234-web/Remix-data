import duckdb
import os
import pandas as pd

# 為了方便類型提示
from typing import Optional, List, Dict, Any, Union

class DuckDBRepository:
    """
    一個用於與 DuckDB 資料庫進行互動的倉儲類。
    它處理資料庫連接、Schema 初始化、執行查詢以及數據的讀取和寫入。
    """
    def __init__(self, db_path: str, schemas_config: Optional[Dict[str, Any]] = None, logger=None):
        """
        初始化 DuckDBRepository。

        Args:
            db_path (str): DuckDB 資料庫檔案的路徑。
                           可以是絕對路徑，或相對於 data_workspace 的路徑。
                           如果檔案不存在，將會被創建。
            schemas_config (Optional[Dict[str, Any]]):
                從 database_schemas.json 加載的配置內容。
                用於初始化 curated_mart 的 schema。如果為 None，則不執行 schema 初始化。
            logger: 日誌記錄器實例。
        """
        self.db_path = db_path
        self.schemas_config = schemas_config
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.logger = logger if logger else self._get_default_logger()

        self._ensure_db_directory_exists()

    def _get_default_logger(self):
        import logging
        logger = logging.getLogger(__name__)
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _ensure_db_directory_exists(self):
        """確保資料庫檔案所在的目錄存在。"""
        dir_name = os.path.dirname(self.db_path)
        if dir_name and not os.path.exists(dir_name):
            try:
                os.makedirs(dir_name)
                self.logger.info(f"資料庫目錄 {dir_name} 已創建。")
            except OSError as e:
                self.logger.error(f"創建資料庫目錄 {dir_name} 失敗：{e}")
                raise

    def connect(self):
        """建立到 DuckDB 資料庫的連接。"""
        if self.conn is None or self.conn.closed:
            try:
                self.conn = duckdb.connect(database=self.db_path, read_only=False)
                self.logger.info(f"成功連接到資料庫：{self.db_path}")
            except Exception as e:
                self.logger.error(f"連接資料庫 {self.db_path} 失敗：{e}")
                self.conn = None # 確保 conn 狀態正確
                raise
        return self.conn

    def disconnect(self):
        """關閉資料庫連接。"""
        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
                self.logger.info(f"資料庫連接已關閉：{self.db_path}")
            except Exception as e:
                self.logger.error(f"關閉資料庫連接 {self.db_path} 失敗：{e}")
        self.conn = None


    def initialize_schema(self, overwrite_existing: bool = False):
        """
        根據提供的 schemas_config 初始化資料庫的 schema。
        僅當 self.schemas_config 被提供時執行。

        Args:
            overwrite_existing (bool): 如果為 True，則在創建表之前先刪除已存在的同名表。
                                       預設為 False，即如果表已存在則跳過創建 (CREATE TABLE IF NOT EXISTS)。
        """
        if not self.schemas_config:
            self.logger.info("未提供 schemas_config，跳過 schema 初始化。")
            return

        if not self.conn or self.conn.closed:
            self.connect()

        if not self.conn:
            self.logger.error("無法初始化 schema，資料庫未連接。")
            return

        self.logger.info(f"開始初始化資料庫 schema：{self.db_path}")
        for table_name, table_definition in self.schemas_config.items():
            if overwrite_existing:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name};")
                    self.logger.info(f"已刪除已存在的表：{table_name} (overwrite_existing=True)")
                except Exception as e:
                    self.logger.error(f"刪除表 {table_name} 失敗: {e}")
                    continue # 繼續嘗試創建下一張表

            columns_sql = []
            if "columns" in table_definition:
                for col_def in table_definition["columns"]:
                    col_sql = f"{col_def['name']} {col_def['type']}"
                    if not col_def.get('nullable', True): # 預設可為空
                        col_sql += " NOT NULL"
                    columns_sql.append(col_sql)

            if not columns_sql:
                self.logger.warning(f"表 {table_name} 沒有定義欄位，跳過創建。")
                continue

            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)});"

            try:
                self.conn.execute(create_table_sql)
                self.logger.info(f"表 {table_name} 已創建或已存在。SQL: {create_table_sql.strip()}")

                # 創建主鍵 (如果定義了)
                # DuckDB 的 CREATE TABLE IF NOT EXISTS 不支援直接在裡面定義主鍵，所以分開處理
                # 但實際上，如果表已存在且已有主鍵，再次添加會報錯。
                # DuckDB 的主鍵更多是邏輯約束，不直接創建物理索引。
                # if "primary_key" in table_definition and table_definition["primary_key"]:
                #     pk_columns = ", ".join(table_definition["primary_key"])
                #     # 檢查表是否為空，或者是否有主鍵，避免對已存在的表重複添加主鍵
                #     # 這裡簡化處理，假設新創建的表可以添加主鍵
                #     try:
                #         # DuckDB 不支援 ALTER TABLE ADD PRIMARY KEY，主鍵需在 CREATE TABLE 時定義
                #         # 如果需要在 IF NOT EXISTS 後添加，可能需要更複雜的邏輯
                #         # 此處的 schema 主要用於指導，實際主鍵約束在建表時已隱含

                #         # 更新：DuckDB 在 CREATE TABLE 語句中支援 PRIMARY KEY 子句
                        # 所以上面的 create_table_sql 可以修改為包含 PRIMARY KEY
                        # 但為了與 IF NOT EXISTS 兼容，還是保持原樣，並在此處不執行 ALTER
                        # self.logger.info(f"表 {table_name} 的主鍵 ({pk_columns}) 應在創建時定義。")
                        # pass

                    # except Exception as e:
                    #    self.logger.warning(f"為表 {table_name} 添加主鍵 ({pk_columns}) 失敗: {e}")


                # 創建索引
                if "indexes" in table_definition:
                    for index_def in table_definition["indexes"]:
                        index_name = index_def["name"]
                        index_columns = ", ".join(index_def["columns"])
                        create_index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({index_columns});"
                        try:
                            self.conn.execute(create_index_sql)
                            self.logger.info(f"索引 {index_name} ON {table_name} ({index_columns}) 已創建或已存在。")
                        except Exception as e:
                            self.logger.error(f"為表 {table_name} 創建索引 {index_name} 失敗: {e}")

            except Exception as e:
                self.logger.error(f"創建表 {table_name} 失敗: {e}. SQL: {create_table_sql.strip()}")
        self.logger.info(f"資料庫 schema 初始化完成：{self.db_path}")

    def execute_query(self, query: str, params: Optional[Union[List, Dict[str, Any]]] = None) -> Optional[duckdb.DuckDBPyRelation]:
        """
        執行一個 SQL 查詢 (例如 INSERT, UPDATE, DELETE, CREATE)。

        Args:
            query (str): 要執行的 SQL 查詢語句。
            params (Optional[Union[List, Dict[str, Any]]]): 查詢的參數。

        Returns:
            Optional[duckdb.DuckDBPyRelation]: DuckDB 的關係對象，如果查詢成功。
                                                如果連接不存在或查詢失敗，則返回 None。
        """
        if not self.conn or self.conn.closed:
            self.logger.warning("資料庫未連接，嘗試重新連接。")
            self.connect()
            if not self.conn or self.conn.closed: # 再次檢查
                 self.logger.error("執行查詢失敗，資料庫未連接。")
                 return None
        try:
            self.logger.debug(f"執行查詢: {query}, 參數: {params}")
            if params:
                return self.conn.execute(query, params)
            else:
                return self.conn.execute(query)
        except Exception as e:
            self.logger.error(f"執行查詢失敗: {e}\n查詢: {query}\n參數: {params}")
            return None

    def fetch_data(self, query: str, params: Optional[Union[List, Dict[str, Any]]] = None) -> Optional[pd.DataFrame]:
        """
        執行一個 SELECT 查詢並將結果作為 Pandas DataFrame 返回。

        Args:
            query (str): 要執行的 SELECT SQL 查詢語句。
            params (Optional[Union[List, Dict[str, Any]]]): 查詢的參數。

        Returns:
            Optional[pd.DataFrame]: 包含查詢結果的 DataFrame，如果查詢成功。
                                    如果查詢失敗或沒有結果，返回 None。
        """
        relation = self.execute_query(query, params)
        if relation:
            try:
                return relation.fetchdf()
            except Exception as e:
                self.logger.error(f"從查詢結果轉換 DataFrame 失敗: {e}\n查詢: {query}\n參數: {params}")
                return None
        return None

    def table_exists(self, table_name: str) -> bool:
        """檢查指定的表是否存在於資料庫中。"""
        if not self.conn or self.conn.closed:
            self.connect()

        query = f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        # DuckDB 使用 information_schema.tables
        query_duckdb = f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}';"
        try:
            result = self.conn.execute(query_duckdb).fetchone()
            return result[0] > 0 if result else False
        except Exception as e:
            self.logger.error(f"檢查表 {table_name} 是否存在時出錯: {e}")
            return False

    def insert_df(self, table_name: str, df: pd.DataFrame, overwrite: bool = False, create_table_if_not_exists: bool = True):
        """
        將 Pandas DataFrame 的數據插入到指定的表中。

        Args:
            table_name (str): 目標表的名稱。
            df (pd.DataFrame): 要插入的數據。
            overwrite (bool): 如果為 True，則先刪除表再創建並插入。預設為 False (追加)。
                              注意：如果表結構與 DataFrame 不匹配，追加可能會失敗。
            create_table_if_not_exists (bool): 如果為 True 且表不存在，則嘗試根據 DataFrame 的 schema 創建表。
                                               此創建不使用 database_schemas.json。
        """
        if df.empty:
            self.logger.info(f"DataFrame 為空，不向表 {table_name} 插入任何數據。")
            return

        if not self.conn or self.conn.closed:
            self.connect()

        try:
            if overwrite:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.logger.info(f"表 {table_name} 已被刪除 (overwrite=True)。")
                # DuckDB 的 register 和 insert 會自動創建表，如果它不存在
                self.conn.register(f'{table_name}_temp_df_view', df)
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {f'{table_name}_temp_df_view'}")
                self.conn.unregister(f'{table_name}_temp_df_view') # 清理視圖
                self.logger.info(f"數據已覆寫到表 {table_name}，共 {len(df)} 行。")

            elif create_table_if_not_exists and not self.table_exists(table_name):
                # 如果表不存在且允許創建，則基於 DataFrame schema 創建
                self.conn.register(f'{table_name}_temp_df_view', df)
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {f'{table_name}_temp_df_view'}")
                self.conn.unregister(f'{table_name}_temp_df_view')
                self.logger.info(f"表 {table_name} 不存在，已根據 DataFrame 創建並插入 {len(df)} 行數據。")
            else:
                # 表存在，追加數據
                # DuckDB可以直接插入DataFrame
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df", params={'df': df})
                self.logger.info(f"{len(df)} 行數據已追加到表 {table_name}。")

        except Exception as e:
            self.logger.error(f"向表 {table_name} 插入 DataFrame 失敗: {e}")
            # 可以考慮更詳細的錯誤，例如 schema 不匹配
            if self.table_exists(table_name):
                try:
                    existing_schema_df = self.fetch_data(f"DESCRIBE {table_name};")
                    self.logger.info(f"表 {table_name} 的現有 Schema:\n{existing_schema_df}")
                    self.logger.info(f"嘗試插入的 DataFrame Schema:\n{df.dtypes}")
                except Exception as desc_e:
                    self.logger.error(f"獲取表 {table_name} 的 schema 失敗: {desc_e}")
            raise

    def get_table_schema(self, table_name: str) -> Optional[pd.DataFrame]:
        """獲取指定表的 schema 信息。"""
        if not self.table_exists(table_name):
            self.logger.warning(f"表 {table_name} 不存在，無法獲取 schema。")
            return None
        return self.fetch_data(f"DESCRIBE {table_name};")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


if __name__ == '__main__':
    # 獲取此腳本 (duckdb_repository.py) 所在的目錄 (src/database)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # 從 src/database 推斷出專案根目錄 (Financial_Forensics_Engine)
    project_root_dir = os.path.dirname(os.path.dirname(current_script_dir))

    # 測試用的資料庫路徑和 schema 配置路徑
    test_db_path = os.path.join(project_root_dir, "data_workspace", "test_db.duckdb")
    test_schema_config_path = os.path.join(project_root_dir, "config", "database_schemas.json")

    print(f"測試資料庫路徑: {test_db_path}")
    print(f"測試 Schema 配置路徑: {test_schema_config_path}")

    # 清理舊的測試資料庫檔案 (如果存在)
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        print(f"已刪除舊的測試資料庫: {test_db_path}")

    # 加載 schema 配置 (模擬 config_loader)
    try:
        with open(test_schema_config_path, 'r', encoding='utf-8') as f:
            schemas = json.load(f)
        print("測試 Schema 配置已加載。")
    except Exception as e:
        print(f"加載測試 Schema 配置失敗: {e}。將不進行 schema 初始化測試。")
        schemas = None

    # 1. 測試連接和 Schema 初始化
    print("\n--- 測試 1: 連接和 Schema 初始化 ---")
    repo = None # 確保 repo 在 try 外部定義
    try:
        with DuckDBRepository(test_db_path, schemas_config=schemas) as repo:
            if schemas:
                repo.initialize_schema()
                print("Schema 初始化完成。")

                # 驗證表是否創建
                if repo.table_exists("fact_daily_market_summary"):
                    print("表 'fact_daily_market_summary' 已成功創建。")
                    schema_df = repo.get_table_schema("fact_daily_market_summary")
                    print("Schema of 'fact_daily_market_summary':\n", schema_df)
                else:
                    print("錯誤: 表 'fact_daily_market_summary' 未能創建。")

            # 2. 測試插入和讀取數據
            print("\n--- 測試 2: 插入和讀取 DataFrame ---")
            sample_data = {
                'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
                'symbol': ['AAPL', 'MSFT'],
                'name': ['Apple Inc.', 'Microsoft Corp.'],
                'open_price': [170.0, 280.0],
                'high_price': [172.0, 282.0],
                'low_price': [169.0, 279.0],
                'close_price': [171.0, 281.0],
                'volume': [1000000, 800000],
                'turnover': [171000000.0, 224800000.0],
                'change': [1.0, 1.0],
                'change_percent': [0.0058, 0.0035],
                'source': ['Test', 'Test']
            }
            sample_df = pd.DataFrame(sample_data)

            # 確保欄位順序和類型與 fact_daily_market_summary 匹配 (如果表已按 schema 創建)
            # DuckDB 在從 DataFrame 創建表時會推斷類型，但在插入已存在的表時類型需要匹配
            if repo.table_exists("fact_daily_market_summary") and schemas:
                 # DuckDB 對日期類型比較嚴格，確保是 date 而非 datetime
                sample_df['date'] = sample_df['date'].dt.date


            repo.insert_df("fact_daily_market_summary", sample_df, overwrite=False, create_table_if_not_exists=True)
            print(f"已向 'fact_daily_market_summary' 插入 {len(sample_df)} 行數據。")

            retrieved_df = repo.fetch_data("SELECT * FROM fact_daily_market_summary;")
            print("從 'fact_daily_market_summary' 讀取的數據:\n", retrieved_df)
            if retrieved_df is not None and len(retrieved_df) == len(sample_df):
                print("數據插入和讀取成功。")
            else:
                print("錯誤: 數據插入或讀取失敗。")

            # 測試覆寫
            sample_df_overwrite = sample_df.head(1).copy()
            sample_df_overwrite['symbol'] = 'GOOG'
            repo.insert_df("fact_daily_market_summary", sample_df_overwrite, overwrite=True)
            print(f"已覆寫 'fact_daily_market_summary'，新數據 {len(sample_df_overwrite)} 行。")
            retrieved_overwrite_df = repo.fetch_data("SELECT * FROM fact_daily_market_summary;")
            print("覆寫後從 'fact_daily_market_summary' 讀取的數據:\n", retrieved_overwrite_df)
            if retrieved_overwrite_df is not None and len(retrieved_overwrite_df) == len(sample_df_overwrite):
                 print("數據覆寫成功。")
            else:
                print("錯誤: 數據覆寫失敗。")


            # 3. 測試執行任意查詢
            print("\n--- 測試 3: 執行任意查詢 ---")
            repo.execute_query("CREATE TABLE IF NOT EXISTS test_table_manual (id INTEGER, name VARCHAR);")
            if repo.table_exists("test_table_manual"):
                print("手動創建表 'test_table_manual' 成功。")
                repo.execute_query("INSERT INTO test_table_manual VALUES (1, 'Alice'), (2, 'Bob');")
                manual_data = repo.fetch_data("SELECT * FROM test_table_manual ORDER BY id;")
                print("從 'test_table_manual' 讀取的數據:\n", manual_data)
            else:
                print("錯誤: 手動創建表 'test_table_manual' 失敗。")

            # 測試不存在的表
            print("\n--- 測試 4: 處理不存在的表 ---")
            print(f"表 'non_existent_table' 是否存在: {repo.table_exists('non_existent_table')}")
            non_existent_df = repo.fetch_data("SELECT * FROM non_existent_table")
            if non_existent_df is None:
                print("正確處理: 查詢不存在的表返回 None。")

    except Exception as e:
        print(f"DuckDBRepository 測試過程中發生錯誤: {e}")
    finally:
        # 再次確保連接已關閉
        if repo and repo.conn and not repo.conn.closed:
            repo.disconnect()
        # 清理測試資料庫檔案
        # if os.path.exists(test_db_path):
        #     os.remove(test_db_path)
        #     print(f"\n已刪除測試資料庫: {test_db_path}")
        print(f"\n測試完畢。如果需要，請手動刪除測試資料庫: {test_db_path}")

    print("\nDuckDBRepository 測試完畢。")
