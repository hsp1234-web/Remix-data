import sqlite3
import os
import sys # For sys.path modification in __main__
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger

logger = get_logger(__name__)

# 預設數據庫文件名
DEFAULT_DB_FILENAME = "project_data.sqlite" # 改為 .sqlite
# 預設數據庫文件存放目錄 (相對於項目根目錄的 data/ 子目錄)
DEFAULT_DB_DIRECTORY = "data"

class DatabaseManager:
    """
    管理 SQLite 數據庫的連接和基本操作。
    """
    def __init__(self, db_path=None):
        """
        初始化 DatabaseManager。

        Args:
            db_path (str, optional): SQLite 數據庫文件的完整路徑。
                                     如果為 None，將使用預設路徑
                                     (在項目根目錄下的 data/project_data.sqlite)。
        """
        if db_path:
            self.db_path = db_path
        else:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            self.db_path = os.path.join(project_root, DEFAULT_DB_DIRECTORY, DEFAULT_DB_FILENAME)

        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
                logger.info(f"數據庫目錄不存在，已創建: {db_dir}")
            except OSError as e:
                logger.error(f"創建數據庫目錄 {db_dir} 失敗: {e}", exc_info=True)

        self.conn = None
        logger.info(f"DatabaseManager (SQLite) 初始化，數據庫路徑設置為: {self.db_path}")

    def get_connection(self): # 移除了 read_only 參數，SQLite 的 connect 會創建文件
        """
        獲取一個到 SQLite 數據庫的連接。如果連接不存在，則創建它。
        """
        if self.conn:
            # SQLite 連接對象沒有 isclosed() 方法，我們在 close_connection() 中設為 None
            logger.debug("返回現有的 SQLite 連接。")
            return self.conn

        try:
            logger.info(f"嘗試連接到 SQLite 數據庫: {self.db_path}")
            # isolation_level=None 表示自動提交模式 (autocommit)
            # 或者在 execute_modification 後手動调用 conn.commit()
            # 為了與之前 DuckDB 的行為（execute 後自動提交 DDL，修改語句後commit）保持一致
            # 我們可以在 execute_modification 和 execute_script 後調用 commit。
            self.conn = sqlite3.connect(self.db_path, timeout=10) # 增加 timeout
            # 為 TEXT 類型設置 text_factory 以正確處理 UTF-8 (通常默認即可)
            # self.conn.text_factory = str
            logger.info("成功連接到 SQLite 數據庫。")
            return self.conn
        except sqlite3.Error as e:
            logger.error(f"連接到 SQLite 數據庫 {self.db_path} 失敗: {e}", exc_info=True)
            raise

    def close_connection(self):
        """
        關閉數據庫連接。
        """
        if self.conn:
            try:
                self.conn.close()
                logger.info(f"SQLite 數據庫連接已關閉: {self.db_path}")
            except sqlite3.Error as e:
                logger.error(f"關閉 SQLite 連接時發生錯誤: {e}", exc_info=True)
            finally: # 確保 self.conn 被設為 None
                self.conn = None
        else:
            logger.debug("SQLite 連接已關閉或未初始化，無需操作。")


    def execute_query(self, query: str, params=None):
        """
        執行一個 SQL 查詢 (通常是 SELECT)。
        """
        conn = self.get_connection()
        cursor = None
        try:
            logger.debug(f"執行查詢 (SQLite): {query[:100]}... (參數: {params if params else '無'})")
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            logger.info(f"查詢成功執行，返回 {len(results)} 行。")
            return results
        except sqlite3.Error as e:
            logger.error(f"執行查詢失敗 (SQLite): {query[:100]}...錯誤: {e}", exc_info=True)
            return None
        finally:
            if cursor:
                cursor.close()

    def execute_script(self, script_path: str):
        """
        從 SQL 文件執行一個 SQL 腳本。
        """
        conn = self.get_connection()
        cursor = None
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                sql_script = f.read()

            logger.info(f"開始執行 SQL 腳本 (SQLite): {script_path}")
            cursor = conn.cursor()
            cursor.executescript(sql_script) # sqlite3 可以執行多個語句的腳本
            conn.commit()
            logger.info(f"SQL 腳本 {script_path} 成功執行 (SQLite)。")
            return True
        except FileNotFoundError:
            logger.error(f"SQL 腳本文件未找到: {script_path}")
            return False
        except sqlite3.Error as e:
            logger.error(f"執行 SQL 腳本 {script_path} 失敗 (SQLite): {e}", exc_info=True)
            try:
                if conn: # 只有在連接存在時才嘗試回滾
                    conn.rollback()
            except sqlite3.Error as rb_err:
                logger.error(f"執行 SQL 腳本失敗後回滾時出錯 (SQLite): {rb_err}")
            return False
        finally:
            if cursor:
                cursor.close()


    def execute_modification(self, query: str, params=None):
        """
        執行一個數據修改語句 (INSERT, UPDATE, DELETE, DDL 等)。
        """
        conn = self.get_connection()
        cursor = None
        try:
            logger.debug(f"執行修改語句 (SQLite): {query[:100]}... (參數: {params if params else '無'})")
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            logger.info("修改語句成功執行並已提交 (SQLite)。")
            return True
        except sqlite3.Error as e:
            logger.error(f"執行修改語句失敗 (SQLite): {query[:100]}...錯誤: {e}", exc_info=True)
            try:
                if conn:
                    conn.rollback()
            except sqlite3.Error as rb_err:
                logger.error(f"執行修改語句失敗後回滾時出錯 (SQLite): {rb_err}")
            return False
        finally:
            if cursor:
                cursor.close()

    def table_exists(self, table_name: str) -> bool:
        """檢查指定的表是否存在於數據庫中。"""
        # SQLite 中檢查表存在的標準方式
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
        conn = self.get_connection()
        cursor = None
        try:
            logger.debug(f"檢查表是否存在 (SQLite): {table_name}")
            cursor = conn.cursor()
            cursor.execute(query, [table_name])
            result = cursor.fetchone()
            exists = result is not None
            logger.debug(f"表 '{table_name}' 是否存在 (SQLite): {exists}")
            return exists
        except sqlite3.Error as e:
            logger.error(f"檢查表 '{table_name}' 是否存在時出錯 (SQLite): {e}", exc_info=True)
            return False
        finally:
            if cursor:
                cursor.close()

if __name__ == "__main__":
    import logging
    # --- 為了直接運行此文件進行測試 ---
    current_dir_db = os.path.dirname(os.path.abspath(__file__))
    project_src_dir_db = os.path.abspath(os.path.join(current_dir_db, '..'))
    project_root_db = os.path.abspath(os.path.join(project_src_dir_db, '..'))

    if project_root_db not in sys.path:
        sys.path.insert(0, project_root_db)

    from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME
    setup_logger(logger_name=PROJECT_LOGGER_NAME, level=logging.DEBUG)

    logger.info("--- DatabaseManager (SQLite __main__) 測試開始 ---")

    test_db_path = ":memory:"
    db_manager = DatabaseManager(db_path=test_db_path) # 使用內存數據庫

    try:
        logger.info("\n測試1: 連接和關閉")
        conn = db_manager.get_connection()
        assert conn is not None, "連接不應為 None"
        # sqlite3 連接對象沒有 isclosed() 方法，我們通過 self.conn 是否為 None 來判斷
        db_manager.close_connection()
        assert db_manager.conn is None, "連接應已關閉 (self.conn 設為 None)"
        logger.info("連接和關閉測試通過。")

        logger.info("\n測試2: 執行 DDL (創建表) 和檢查表是否存在")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_main_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            value REAL
        );
        """
        success_ddl = db_manager.execute_modification(create_table_sql)
        assert success_ddl, "創建表應成功"
        assert db_manager.table_exists("test_main_table"), "表 'test_main_table' 應存在"
        assert not db_manager.table_exists("non_existent_table"), "表 'non_existent_table' 不應存在"
        logger.info("創建表並檢查是否存在測試通過。")

        logger.info("\n測試3: 插入數據並查詢")
        insert_sql = "INSERT INTO test_main_table (name, value) VALUES (?, ?);" # ID 是 AUTOINCREMENT
        db_manager.execute_modification(insert_sql, ["alpha", 10.5])
        db_manager.execute_modification(insert_sql, ["beta", 20.3])

        select_all_sql = "SELECT id, name, value FROM test_main_table ORDER BY id;"
        results = db_manager.execute_query(select_all_sql)
        assert results is not None, "查詢結果不應為 None"
        assert len(results) == 2, f"應返回2行，實際返回 {len(results)}"
        # AUTOINCREMENT 的 ID 從 1 開始
        assert results[0] == (1, "alpha", 10.5), f"第一行數據不匹配: {results[0]}"
        assert results[1] == (2, "beta", 20.3), f"第二行數據不匹配: {results[1]}"
        logger.info(f"插入和查詢數據測試通過。查詢結果: {results}")

        logger.info("\n測試4: 從文件執行 SQL 腳本")
        schema_dir = os.path.join(project_root_db, "config")
        if not os.path.exists(schema_dir): os.makedirs(schema_dir)
        test_script_path = os.path.join(schema_dir, "temp_test_schema_sqlite.sql")
        with open(test_script_path, "w", encoding="utf-8") as f:
            f.write("CREATE TABLE IF NOT EXISTS script_table (col1 TEXT, col2 INTEGER);\n")
            f.write("INSERT INTO script_table VALUES ('gamma', 300), ('delta', 400);")

        success_script = db_manager.execute_script(test_script_path)
        assert success_script, "執行腳本應成功"
        assert db_manager.table_exists("script_table"), "由腳本創建的表 'script_table' 應存在"

        script_results = db_manager.execute_query("SELECT COUNT(*) FROM script_table;")
        assert script_results and script_results[0][0] == 2, "腳本表應包含2行數據"
        logger.info("從文件執行 SQL 腳本測試通過。")
        os.remove(test_script_path)

    except AssertionError as ae:
        logger.error(f"DatabaseManager (SQLite __main__) 測試斷言失敗: {ae}", exc_info=True)
    except Exception as e:
        logger.error(f"DatabaseManager (SQLite __main__) 測試期間發生意外錯誤: {e}", exc_info=True)
    finally:
        db_manager.close_connection()

    logger.info("--- DatabaseManager (SQLite __main__) 測試結束 ---")


def initialize_database_from_schema(db_filepath: str, schema_filepath: str):
    """
    一個輔助函數，用於創建 DatabaseManager 實例並執行 schema SQL 文件。
    設計為可以從外部 Python 腳本調用。

    Args:
        db_filepath (str): 要初始化/連接的數據庫文件路徑。
        schema_filepath (str): 包含 DDL 的 SQL schema 文件路徑。

    Returns:
        bool: True 如果成功，False 如果失敗。
    """
    # logger 實例應該在模塊級別獲取，或者作為參數傳遞
    # 為了讓這個獨立函數也能記錄，我們在這裡獲取/確保 logger 已設置
    # (假設 setup_logger 已在某處被調用)
    op_logger = get_logger(__name__ + ".initialize_database_from_schema")

    op_logger.info(f"開始初始化數據庫 '{db_filepath}' 使用 schema '{schema_filepath}' (SQLite)")
    db_manager_instance = DatabaseManager(db_path=db_filepath)
    success = False
    try:
        if not os.path.exists(schema_filepath):
            op_logger.error(f"Schema 文件未找到: {schema_filepath}")
            return False

        success = db_manager_instance.execute_script(schema_filepath)
        if success:
            op_logger.info(f"數據庫 '{db_filepath}' 已成功使用 schema '{schema_filepath}' 初始化/更新 (SQLite)。")
        else:
            op_logger.error(f"數據庫 '{db_filepath}' 初始化/更新失敗 (SQLite)。")
    except Exception as e:
        op_logger.error(f"初始化數據庫 '{db_filepath}' 期間發生嚴重錯誤 (SQLite): {e}", exc_info=True)
        success = False
    finally:
        db_manager_instance.close_connection()
    return success
