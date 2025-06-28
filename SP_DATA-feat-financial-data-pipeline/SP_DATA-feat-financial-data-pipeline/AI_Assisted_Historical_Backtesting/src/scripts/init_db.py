import argparse
import os
import sys

# 為了能夠導入 project_root/src/database/db_manager.py 和 project_root/src/utils/logger.py
# 需要將項目根目錄的父目錄，或者項目根目錄本身（如果 src 是 PYTHONPATH 的一部分）加入 sys.path
# 假設此腳本 (init_db.py) 位於 project_root/src/scripts/init_db.py
# project_root = AI_Assisted_Historical_Backtesting
# project_root_parent = AI_Assisted_Historical_Backtesting 的父目錄

current_script_dir = os.path.dirname(os.path.abspath(__file__)) # .../src/scripts
src_dir = os.path.dirname(current_script_dir) # .../src
project_root_dir = os.path.dirname(src_dir) # AI_Assisted_Historical_Backtesting
project_root_parent_dir = os.path.dirname(project_root_dir) # Parent of AI_Assisted_Historical_Backtesting

# 將項目根目錄的父目錄添加到 sys.path
# 這樣 from AI_Assisted_Historical_Backtesting.src.database.db_manager import ... 就能工作
if project_root_parent_dir not in sys.path:
    sys.path.insert(0, project_root_parent_dir)

# print(f"init_db.py: sys.path after modification: {sys.path}") # 用於調試

try:
    # 現在應該可以從頂層包名開始導入了
    from AI_Assisted_Historical_Backtesting.src.database.db_manager import initialize_database_from_schema, DEFAULT_DB_DIRECTORY, DEFAULT_DB_FILENAME
    from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME, get_logger
except ImportError as e:
    print(f"ERROR: Failed to import necessary modules in init_db.py. Exception: {e}")
    print(f"Current sys.path: {sys.path}")
    print(f"Ensure '{project_root_parent_dir}' is in sys.path for top-level imports like 'AI_Assisted_Historical_Backtesting.src...'.")
    sys.exit(1)

# 在導入後設置日誌記錄器
setup_logger(logger_name=PROJECT_LOGGER_NAME, level="INFO") # 使用 INFO 級別，DEBUG 太囉嗦
logger = get_logger(__name__) # init_db.py 自己的 logger

def main():
    parser = argparse.ArgumentParser(description="Initialize the DuckDB database with the defined schema.")
    parser.add_argument(
        "--dbpath",
        type=str,
        help=f"Optional. Full path to the DuckDB database file. Defaults to 'project_root/{DEFAULT_DB_DIRECTORY}/{DEFAULT_DB_FILENAME}'."
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=os.path.join(project_root_dir, "config", "schema.sql"), # 預設 schema 文件路徑
        help="Optional. Full path to the SQL schema file. Defaults to 'project_root/config/schema.sql'."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-initialization even if the database file already exists. Use with caution."
    )

    args = parser.parse_args()

    db_filepath = args.dbpath
    if not db_filepath:
        # 如果未提供 dbpath，則使用預設路徑
        # 確保 data 目錄存在（DatabaseManager 的構造函數會做這件事，但這裡也可以預先檢查）
        data_dir_default = os.path.join(project_root_dir, DEFAULT_DB_DIRECTORY)
        if not os.path.exists(data_dir_default):
            try:
                os.makedirs(data_dir_default)
                logger.info(f"預設數據目錄不存在，已創建: {data_dir_default}")
            except OSError as e:
                logger.error(f"創建預設數據目錄 {data_dir_default} 失敗: {e}")
                # 如果無法創建，DatabaseManager 可能會在錯誤位置創建數據庫或失敗
        db_filepath = os.path.join(data_dir_default, DEFAULT_DB_FILENAME)
        logger.info(f"--dbpath 未提供，使用預設路徑: {db_filepath}")

    schema_filepath = args.schema

    logger.info(f"目標數據庫文件: {db_filepath}")
    logger.info(f"Schema SQL 文件: {schema_filepath}")

    if not os.path.exists(schema_filepath):
        logger.error(f"指定的 Schema SQL 文件未找到: {schema_filepath}")
        sys.exit(1)

    if os.path.exists(db_filepath) and not args.force:
        logger.warning(f"數據庫文件 '{db_filepath}' 已存在。")
        logger.warning("Schema 初始化腳本通常用於創建表結構，在已存在的數據庫上運行可能無效果或導致重複創建錯誤（如果表已存在且 schema.sql 未使用 IF NOT EXISTS）。")
        logger.warning("如果希望重新初始化（可能導致數據丟失，除非 schema 設計為非破壞性），請使用 --force 參數。")
        # 可以選擇在這裡退出，或者讓 initialize_database_from_schema 處理 (它會執行 CREATE TABLE IF NOT EXISTS)
        # 為了安全，如果用戶沒有用 --force，我們提示一下然後繼續（因為 schema.sql 用了 IF NOT EXISTS）
        # print("To re-apply schema idempotent DDL or re-initialize, use --force.")
        # sys.exit(0)
        logger.info("由於 schema.sql 使用 'IF NOT EXISTS'，將繼續嘗試應用 schema。")


    if args.force and os.path.exists(db_filepath):
        logger.warning(f"檢測到 --force 參數，將嘗試刪除已存在的數據庫文件: {db_filepath}")
        try:
            os.remove(db_filepath)
            logger.info(f"已存在的數據庫文件 '{db_filepath}' 已被刪除。")
        except OSError as e:
            logger.error(f"刪除已存在的數據庫文件 '{db_filepath}' 失敗: {e}")
            logger.error("請手動刪除該文件或檢查權限，然後重試。")
            sys.exit(1)

    success = initialize_database_from_schema(db_filepath, schema_filepath)

    if success:
        logger.info("數據庫初始化成功完成。")
        sys.exit(0)
    else:
        logger.error("數據庫初始化過程中發生錯誤。")
        sys.exit(1)

if __name__ == "__main__":
    main()
