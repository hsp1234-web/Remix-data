import os
import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import logging

# 為了能夠導入位於 src 目錄下的模듈，需要調整 sys.path
# 專案根目錄是 tests 目錄的上一層，也是當前腳本的父目錄的父目錄
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 將專案根目錄添加到 sys.path，這樣就可以使用 from src.taifex_pipeline... 導入
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 現在可以導入被測模組，從 src 開始
from src.taifex_pipeline.database.db_manager import DBManager
from src.taifex_pipeline.database.constants import (
    FileStatus, TABLE_FILE_MANIFEST, TABLE_RAW_FILES,
    COLUMN_FILE_HASH, COLUMN_STATUS, COLUMN_ORIGINAL_PATH,
    COLUMN_INGESTION_TIMESTAMP, COLUMN_ERROR_MESSAGE, COLUMN_NOTES,
    COLUMN_TRANSFORMATION_START_TIMESTAMP, COLUMN_TRANSFORMATION_END_TIMESTAMP
)

# 設定基礎日誌以便觀察測試過程中的 DBManager 日誌
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# --- 輔助函式 ---
def get_sample_dataframe() -> pd.DataFrame:
    """提供一個範例 DataFrame 用於測試。"""
    data = {
        'col_a': [1, 2, 3],
        'col_b': ['apple', 'banana', 'cherry'],
        'col_c': [0.1, 0.2, 0.3]
    }
    return pd.DataFrame(data)

# --- 測試函式 ---

def test_initialization_and_setup():
    logger.info("開始測試: test_initialization_and_setup")
    manager = None
    try:
        manager = DBManager(db_path=":memory:")
        assert manager.conn is not None, "資料庫連線應已建立"
        manager.setup_tables()

        cursor = manager.conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_FILE_MANIFEST}';")
        assert cursor.fetchone() is not None, f"{TABLE_FILE_MANIFEST} 表格應已建立"

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_RAW_FILES}';")
        assert cursor.fetchone() is not None, f"{TABLE_RAW_FILES} 表格應已建立"
        logger.info("✅ PASSED: test_initialization_and_setup")
    except Exception as e:
        logger.error(f"❌ FAILED: test_initialization_and_setup - {e}", exc_info=True)
        raise
    finally:
        if manager:
            manager.close()

def test_add_check_manifest_record():
    logger.info("開始測試: test_add_check_manifest_record")
    manager = DBManager(db_path=":memory:")
    manager.setup_tables()
    try:
        file_hash = "tc_hash001"
        original_path = "/test/file_tc001.txt"

        assert not manager.check_hash_exists(file_hash), "新 hash 不應存在"
        manager.add_manifest_record(file_hash, original_path, file_size_bytes=100)
        assert manager.check_hash_exists(file_hash), "添加後 hash 應存在"

        record_df = manager.conn.execute(f"SELECT * FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?", (file_hash,)).fetchdf()
        assert not record_df.empty
        assert record_df.iloc[0][COLUMN_ORIGINAL_PATH] == original_path
        assert record_df.iloc[0][COLUMN_STATUS] == FileStatus.RAW_INGESTED.value
        assert pd.notna(record_df.iloc[0][COLUMN_INGESTION_TIMESTAMP])
        logger.info("✅ PASSED: test_add_check_manifest_record")
    except Exception as e:
        logger.error(f"❌ FAILED: test_add_check_manifest_record - {e}", exc_info=True)
        raise
    finally:
        manager.close()

def test_store_get_raw_file():
    logger.info("開始測試: test_store_get_raw_file")
    manager = DBManager(db_path=":memory:")
    manager.setup_tables()
    try:
        file_hash = "tc_rawhash001"
        content = b"binary content for test \x01\x02"
        manager.store_raw_file(file_hash, content)

        retrieved_content = manager.get_raw_file_content(file_hash)
        assert retrieved_content == content, "獲取的內容與儲存的內容不符"

        assert manager.get_raw_file_content("non_existent_hash") is None, "查詢不存在的 hash 應返回 None"
        logger.info("✅ PASSED: test_store_get_raw_file")
    except Exception as e:
        logger.error(f"❌ FAILED: test_store_get_raw_file - {e}", exc_info=True)
        raise
    finally:
        manager.close()

def test_get_manifest_records_by_status():
    logger.info("開始測試: test_get_manifest_records_by_status")
    manager = DBManager(db_path=":memory:")
    manager.setup_tables()
    try:
        h1 = "tc_status_h1"
        h2 = "tc_status_h2"
        h3 = "tc_status_h3"
        manager.add_manifest_record(h1, "f1.txt")
        manager.add_manifest_record(h2, "f2.txt")
        manager.conn.execute(f"UPDATE {TABLE_FILE_MANIFEST} SET status=? WHERE {COLUMN_FILE_HASH}=?", (FileStatus.TRANSFORMED_SUCCESS.value, h1))

        raw_records = manager.get_manifest_records_by_status(FileStatus.RAW_INGESTED.value)
        assert len(raw_records) == 1, f"應只有一條 RAW_INGESTED 記錄, 實際: {len(raw_records)}"
        assert raw_records[0][COLUMN_FILE_HASH] == h2

        success_records = manager.get_manifest_records_by_status(FileStatus.TRANSFORMED_SUCCESS.value)
        assert len(success_records) == 1
        assert success_records[0][COLUMN_FILE_HASH] == h1

        empty_records = manager.get_manifest_records_by_status("NON_EXISTENT")
        assert len(empty_records) == 0
        logger.info("✅ PASSED: test_get_manifest_records_by_status")
    except Exception as e:
        logger.error(f"❌ FAILED: test_get_manifest_records_by_status - {e}", exc_info=True)
        raise
    finally:
        manager.close()

def test_load_dataframe():
    logger.info("開始測試: test_load_dataframe")
    manager = DBManager(db_path=":memory:")
    manager.setup_tables() # 雖然此測試不直接操作 manifest/raw_files，但 manager 需要初始化
    sample_df = get_sample_dataframe()
    table_name = "test_target_table"
    try:
        # 測試 append 到新表 (實際會創建)
        manager.load_dataframe_to_table(sample_df, table_name, if_exists='append')
        res_df1 = manager.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        pd.testing.assert_frame_equal(res_df1, sample_df, check_dtype=False)

        # 測試 append 到現有表
        manager.load_dataframe_to_table(sample_df, table_name, if_exists='append')
        res_df2 = manager.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        assert len(res_df2) == 2 * len(sample_df)

        # 測試 replace
        manager.load_dataframe_to_table(sample_df, table_name, if_exists='replace')
        res_df3 = manager.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        assert len(res_df3) == len(sample_df)
        pd.testing.assert_frame_equal(res_df3, sample_df, check_dtype=False)

        # 測試載入空 DataFrame
        empty_df = pd.DataFrame()
        manager.load_dataframe_to_table(empty_df, "empty_table_test", if_exists='append')
        query_res = manager.conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='empty_table_test';").fetchone()
        assert query_res[0] == 0, "載入空 DataFrame (append) 不應創建表"

        logger.info("✅ PASSED: test_load_dataframe")
    except Exception as e:
        logger.error(f"❌ FAILED: test_load_dataframe - {e}", exc_info=True)
        raise
    finally:
        manager.close()

def test_update_manifest_status():
    logger.info("開始測試: test_update_manifest_status")
    manager = DBManager(db_path=":memory:")
    manager.setup_tables()
    file_hash = "tc_update_h1"
    try:
        manager.add_manifest_record(file_hash, "update_me.txt", notes="Original Notes.")

        start_ts_str = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        end_ts_str = datetime.now(timezone.utc).isoformat()

        update_params = {
            "file_hash": file_hash,
            "status": FileStatus.TRANSFORMED_SUCCESS.value,
            "processed_rows": 123,
            "transformation_start_timestamp": start_ts_str,
            "transformation_end_timestamp": end_ts_str,
            "target_table": "final_table",
            "recipe_id": "recipe_xyz",
            "error_message": None
        }
        manager.update_manifest_transformation_status(**update_params)

        res_df = manager.conn.execute(f"SELECT * FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?",(file_hash,)).fetchdf()
        assert not res_df.empty
        record = res_df.iloc[0]

        assert record[COLUMN_STATUS] == FileStatus.TRANSFORMED_SUCCESS.value
        assert record[COLUMN_ERROR_MESSAGE] is None
        assert pd.Timestamp(record[COLUMN_TRANSFORMATION_START_TIMESTAMP]).isoformat().startswith(start_ts_str[:19]) # 比較到秒
        assert pd.Timestamp(record[COLUMN_TRANSFORMATION_END_TIMESTAMP]).isoformat().startswith(end_ts_str[:19])
        assert "Processed rows: 123" in record[COLUMN_NOTES]
        assert "Recipe: recipe_xyz" in record[COLUMN_NOTES]
        assert "Target: final_table" in record[COLUMN_NOTES]
        assert "Original Notes." in record[COLUMN_NOTES] # 確保舊筆記被保留

        # 測試更新為失敗狀態
        manager.update_manifest_transformation_status(file_hash, FileStatus.TRANSFORMATION_FAILED.value, error_message="It failed.")
        res_df_failed = manager.conn.execute(f"SELECT {COLUMN_STATUS}, {COLUMN_ERROR_MESSAGE} FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?",(file_hash,)).fetchdf()
        assert res_df_failed.iloc[0][COLUMN_STATUS] == FileStatus.TRANSFORMATION_FAILED.value
        assert res_df_failed.iloc[0][COLUMN_ERROR_MESSAGE] == "It failed."

        logger.info("✅ PASSED: test_update_manifest_status")
    except Exception as e:
        logger.error(f"❌ FAILED: test_update_manifest_status - {e}", exc_info=True)
        raise
    finally:
        manager.close()

# --- 主執行區塊 ---
if __name__ == "__main__":
    logger.info("="*80)
    logger.info("🚀 開始執行 DBManager 獨立測試腳本...")
    logger.info("="*80 + "\n")

    tests_to_run = [
        test_initialization_and_setup,
        test_add_check_manifest_record,
        test_store_get_raw_file,
        test_get_manifest_records_by_status,
        test_load_dataframe,
        test_update_manifest_status
    ]

    overall_passed = True
    for test_func in tests_to_run:
        try:
            test_func()
        except Exception:
            overall_passed = False
            # 錯誤已在 test_func 內部記錄

    logger.info("\n" + "="*80)
    if overall_passed:
        logger.info("🎉🎉🎉 所有 DBManager 測試均通過！ 🎉🎉🎉")
    else:
        logger.error("🔥🔥🔥 部分 DBManager 測試失敗，請檢查上面的日誌！ 🔥🔥🔥")
    logger.info("="*80)

    # 如果希望在 CI 環境中明確指示失敗，可以取消註解下一行
    # if not overall_passed:
    #     sys.exit(1)
```
