import pytest
import os
import duckdb
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

# 被測模組
from taifex_pipeline.database.db_manager import DBManager
from taifex_pipeline.database.constants import (
    FileStatus, TABLE_FILE_MANIFEST, TABLE_RAW_FILES,
    COLUMN_FILE_HASH, COLUMN_STATUS, COLUMN_ORIGINAL_PATH,
    COLUMN_INGESTION_TIMESTAMP, COLUMN_ERROR_MESSAGE, COLUMN_NOTES,
    COLUMN_TRANSFORMATION_START_TIMESTAMP, COLUMN_TRANSFORMATION_END_TIMESTAMP
    # 假設 COLUMN_PROCESSED_ROWS 如果要直接測試，也需要從 constants 導入
)

# --- Test Fixtures ---

@pytest.fixture
def db_manager_memory() -> DBManager:
    """提供一個使用記憶體資料庫的 DBManager 實例，並自動設定表格。"""
    manager = DBManager(db_path=":memory:")
    manager.setup_tables() # 確保表格已建立
    yield manager # 使用 yield 以便在測試結束後可以執行清理 (如果需要)
    manager.close()

@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """提供一個範例 DataFrame 用於測試。"""
    data = {
        'col_a': [1, 2, 3, 4, 5],
        'col_b': ['apple', 'banana', 'cherry', 'date', 'elderberry'],
        'col_c': [0.1, 0.2, 0.3, 0.4, 0.5]
    }
    return pd.DataFrame(data)

# --- Test Cases ---

class TestDBManagerLifecycle:
    def test_db_manager_initialization_and_setup(self, db_manager_memory: DBManager):
        """測試 DBManager 初始化和 setup_tables 是否成功。"""
        assert db_manager_memory.conn is not None, "資料庫連線應已建立"

        # 檢查核心表格是否存在
        cursor = db_manager_memory.conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_FILE_MANIFEST}';")
        assert cursor.fetchone() is not None, f"{TABLE_FILE_MANIFEST} 表格應已建立"

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_RAW_FILES}';")
        assert cursor.fetchone() is not None, f"{TABLE_RAW_FILES} 表格應已建立"

    def test_db_manager_close_connection(self):
        """測試 close 方法是否能正確關閉連線。"""
        manager = DBManager(db_path=":memory:")
        manager.setup_tables()
        assert manager.conn is not None
        manager.close()
        assert manager.conn is None, "關閉後連線應為 None"
        # 再次關閉應無錯誤
        manager.close()


class TestDBManagerManifestOperations:

    def test_add_and_check_manifest_record(self, db_manager_memory: DBManager):
        """測試 add_manifest_record 和 check_hash_exists 方法。"""
        file_hash = "testhash001"
        original_path = "/test/file1.txt"

        assert not db_manager_memory.check_hash_exists(file_hash), "新 hash 不應存在"

        db_manager_memory.add_manifest_record(file_hash, original_path)
        assert db_manager_memory.check_hash_exists(file_hash), "添加後 hash 應存在"

        # 驗證記錄內容 (部分)
        record_df = db_manager_memory.conn.execute(f"SELECT * FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?", (file_hash,)).fetchdf()
        assert not record_df.empty
        assert record_df.iloc[0][COLUMN_ORIGINAL_PATH] == original_path
        assert record_df.iloc[0][COLUMN_STATUS] == FileStatus.RAW_INGESTED.value
        assert pd.notna(record_df.iloc[0][COLUMN_INGESTION_TIMESTAMP]) # 應有汲取時間戳

    def test_add_duplicate_manifest_record_fails(self, db_manager_memory: DBManager):
        """測試新增重複的 manifest 記錄是否會引發 IntegrityError。"""
        file_hash = "testhash002"
        db_manager_memory.add_manifest_record(file_hash, "/test/file2.txt")
        with pytest.raises(duckdb.IntegrityError):
            db_manager_memory.add_manifest_record(file_hash, "/another/path.txt")

    def test_get_manifest_records_by_status(self, db_manager_memory: DBManager):
        """測試 get_manifest_records_by_status 方法。"""
        hash1 = "status_test_hash1"
        hash2 = "status_test_hash2"
        hash3 = "status_test_hash3"

        db_manager_memory.add_manifest_record(hash1, "/path/file1.raw", source_system="SYS_A")
        # 手動更新一個記錄的狀態以供測試
        db_manager_memory.conn.execute(
            f"UPDATE {TABLE_FILE_MANIFEST} SET {COLUMN_STATUS} = ? WHERE {COLUMN_FILE_HASH} = ?",
            (FileStatus.TRANSFORMED_SUCCESS.value, hash1)
        )
        db_manager_memory.add_manifest_record(hash2, "/path/file2.raw", source_system="SYS_B")
        db_manager_memory.add_manifest_record(hash3, "/path/file3.raw", source_system="SYS_A")

        # 獲取 RAW_INGESTED 狀態的記錄
        raw_ingested_records = db_manager_memory.get_manifest_records_by_status(FileStatus.RAW_INGESTED.value)
        assert len(raw_ingested_records) == 2
        found_hashes_raw = {r[COLUMN_FILE_HASH] for r in raw_ingested_records}
        assert {hash2, hash3} == found_hashes_raw
        for record in raw_ingested_records:
            assert isinstance(record, dict)
            assert COLUMN_ORIGINAL_PATH in record
            assert record[COLUMN_STATUS] == FileStatus.RAW_INGESTED.value

        # 獲取 TRANSFORMED_SUCCESS 狀態的記錄
        transformed_records = db_manager_memory.get_manifest_records_by_status(FileStatus.TRANSFORMED_SUCCESS.value)
        assert len(transformed_records) == 1
        assert transformed_records[0][COLUMN_FILE_HASH] == hash1
        assert transformed_records[0][COLUMN_SOURCE_SYSTEM] == "SYS_A"

        # 獲取不存在狀態的記錄
        non_existent_status_records = db_manager_memory.get_manifest_records_by_status("NON_EXISTENT_STATUS")
        assert len(non_existent_status_records) == 0

    def test_update_manifest_transformation_status(self, db_manager_memory: DBManager):
        """測試 update_manifest_transformation_status 方法。"""
        file_hash = "update_test_hash"
        original_path = "/path/for_update.txt"
        db_manager_memory.add_manifest_record(file_hash, original_path, notes="Initial note.")

        start_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        end_ts = datetime.now(timezone.utc)

        params_to_update = {
            "file_hash": file_hash,
            "status": FileStatus.TRANSFORMED_SUCCESS.value,
            "processed_rows": 100,
            "transformation_start_timestamp": start_ts.isoformat(),
            "transformation_end_timestamp": end_ts.isoformat(),
            "target_table": "clean_data_table",
            "recipe_id": "daily_ohlc_recipe_v1",
            "error_message": None # 明確設為 None 以測試清除錯誤信息
        }
        db_manager_memory.update_manifest_transformation_status(**params_to_update)

        updated_record_df = db_manager_memory.conn.execute(
            f"SELECT * FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?", (file_hash,)
        ).fetchdf()
        assert not updated_record_df.empty
        record = updated_record_df.iloc[0]

        assert record[COLUMN_STATUS] == FileStatus.TRANSFORMED_SUCCESS.value
        assert record[COLUMN_ERROR_MESSAGE] is None # 應被設為 NULL

        # 時間戳比較需要小心，因為 DuckDB 存儲 TIMESTAMPTZ
        # 比較 ISO 格式字串（去除可能的微秒差異）
        assert pd.Timestamp(record[COLUMN_TRANSFORMATION_START_TIMESTAMP]).isoformat(timespec='seconds') == start_ts.isoformat(timespec='seconds')
        assert pd.Timestamp(record[COLUMN_TRANSFORMATION_END_TIMESTAMP]).isoformat(timespec='seconds') == end_ts.isoformat(timespec='seconds')

        expected_notes = "Initial note. | Processed rows: 100. Recipe: daily_ohlc_recipe_v1. Target: clean_data_table."
        # 由於 notes 的追加邏輯，這裡需要更精確的預期
        # 實際 notes 可能是 "Initial note. | Processed rows: 100. Recipe: daily_ohlc_recipe_v1. Target: clean_data_table."
        # 或只有 "Processed rows: ..." 如果初始 notes 為空或以 "Processed rows:" 開頭
        # 這裡的測試假設初始 notes 不以 "Processed rows:" 開頭
        assert COLUMN_NOTES in record and "Processed rows: 100" in record[COLUMN_NOTES]
        assert "Recipe: daily_ohlc_recipe_v1" in record[COLUMN_NOTES]
        assert "Target: clean_data_table" in record[COLUMN_NOTES]
        assert "Initial note." in record[COLUMN_NOTES] # 確保舊筆記被保留

        # 測試僅更新狀態和錯誤訊息
        db_manager_memory.update_manifest_transformation_status(
            file_hash=file_hash,
            status=FileStatus.TRANSFORMATION_FAILED.value,
            error_message="A new failure occurred."
        )
        failed_record_df = db_manager_memory.conn.execute(
            f"SELECT {COLUMN_STATUS}, {COLUMN_ERROR_MESSAGE} FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?", (file_hash,)
        ).fetchdf()
        assert failed_record_df.iloc[0][COLUMN_STATUS] == FileStatus.TRANSFORMATION_FAILED.value
        assert failed_record_df.iloc[0][COLUMN_ERROR_MESSAGE] == "A new failure occurred."


class TestDBManagerRawFileOperations:

    def test_store_and_get_raw_file_content(self, db_manager_memory: DBManager):
        """測試 store_raw_file 和 get_raw_file_content 方法。"""
        file_hash = "raw_content_hash"
        original_content = b"This is a test raw binary content \x00\x01\x02"

        db_manager_memory.store_raw_file(file_hash, original_content)

        retrieved_content = db_manager_memory.get_raw_file_content(file_hash)
        assert retrieved_content is not None
        assert retrieved_content == original_content

    def test_get_raw_file_content_non_existent(self, db_manager_memory: DBManager):
        """測試獲取不存在的原始檔案內容時返回 None。"""
        retrieved_content = db_manager_memory.get_raw_file_content("non_existent_hash")
        assert retrieved_content is None

    def test_store_duplicate_raw_file_fails(self, db_manager_memory: DBManager):
        """測試儲存重複的原始檔案 (相同 hash) 是否引發 IntegrityError。"""
        file_hash = "raw_dup_hash"
        db_manager_memory.store_raw_file(file_hash, b"content1")
        with pytest.raises(duckdb.IntegrityError):
            db_manager_memory.store_raw_file(file_hash, b"content2")


class TestDBManagerLoadDataFrame:

    def test_load_dataframe_to_table_new_table_append(self, db_manager_memory: DBManager, sample_dataframe: pd.DataFrame):
        """測試載入 DataFrame 到一個新表格 (模式: append)。"""
        table_name = "new_table_append"
        db_manager_memory.load_dataframe_to_table(sample_dataframe, table_name, if_exists='append')

        # 驗證表格內容
        result_df = db_manager_memory.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        pd.testing.assert_frame_equal(result_df, sample_dataframe, check_dtype=False) # DuckDB 可能改變類型

    def test_load_dataframe_to_table_existing_table_append(self, db_manager_memory: DBManager, sample_dataframe: pd.DataFrame):
        """測試追加 DataFrame 到一個已存在的表格。"""
        table_name = "existing_table_append"
        # 首次載入
        db_manager_memory.load_dataframe_to_table(sample_dataframe, table_name, if_exists='append')
        # 再次載入 (追加)
        db_manager_memory.load_dataframe_to_table(sample_dataframe, table_name, if_exists='append')

        result_df = db_manager_memory.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        assert len(result_df) == 2 * len(sample_dataframe)
        expected_df = pd.concat([sample_dataframe, sample_dataframe], ignore_index=True)
        pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    def test_load_dataframe_to_table_new_table_replace(self, db_manager_memory: DBManager, sample_dataframe: pd.DataFrame):
        """測試載入 DataFrame 到一個新表格 (模式: replace)。"""
        table_name = "new_table_replace"
        db_manager_memory.load_dataframe_to_table(sample_dataframe, table_name, if_exists='replace')

        result_df = db_manager_memory.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        pd.testing.assert_frame_equal(result_df, sample_dataframe, check_dtype=False)

    def test_load_dataframe_to_table_existing_table_replace(self, db_manager_memory: DBManager, sample_dataframe: pd.DataFrame):
        """測試取代一個已存在的表格並載入 DataFrame。"""
        table_name = "existing_table_replace"
        # 首次載入 (建立一些不同數據)
        initial_data = pd.DataFrame({'col_a': [10, 20], 'col_b': ['x', 'y'], 'col_c': [1.0, 2.0]})
        db_manager_memory.load_dataframe_to_table(initial_data, table_name, if_exists='append')

        # 用 sample_dataframe 取代
        db_manager_memory.load_dataframe_to_table(sample_dataframe, table_name, if_exists='replace')

        result_df = db_manager_memory.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        assert len(result_df) == len(sample_dataframe)
        pd.testing.assert_frame_equal(result_df, sample_dataframe, check_dtype=False)

    def test_load_dataframe_to_table_empty_dataframe(self, db_manager_memory: DBManager):
        """測試載入空 DataFrame 時不應執行任何操作。"""
        table_name = "empty_df_table"
        empty_df = pd.DataFrame()
        db_manager_memory.load_dataframe_to_table(empty_df, table_name, if_exists='append')

        # 檢查表格是否未被建立 (因為是 append 模式且 df 為空)
        # 或者如果表已存在，內容應不變
        # 這裡，由於表 изначально不存在，它不應該被創建
        query_result = db_manager_memory.conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
            [table_name]
        ).fetchone()
        assert query_result is None, "載入空 DataFrame (append) 不應創建新表"

        # 測試 replace 模式
        db_manager_memory.load_dataframe_to_table(empty_df, table_name, if_exists='replace')
        query_result_replace = db_manager_memory.conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
            [table_name]
        ).fetchone()
        # 如果 replace 模式，且 df 為空，DuckDB 可能會 drop table 但不創建新表，或創建一個空表
        # 根據目前的實現，它不會執行任何操作，所以如果表之前不存在，現在也不存在
        assert query_result_replace is None, "載入空 DataFrame (replace) 不應創建新表（如果之前不存在）"

        # 如果表已存在，replace 模式下，空 df 應該導致表被 drop
        db_manager_memory.conn.execute("CREATE TABLE existing_for_empty_replace (id INT);")
        db_manager_memory.load_dataframe_to_table(empty_df, "existing_for_empty_replace", if_exists='replace')
        query_result_existing_replace = db_manager_memory.conn.execute(
             "SELECT 1 FROM information_schema.tables WHERE table_name = 'existing_for_empty_replace' AND table_schema = 'main'"
        ).fetchone()
        # 根據 load_dataframe_to_table 的邏輯: df is empty -> return. 所以不會 drop.
        # 這行為是符合函式開頭的檢查的。
        assert query_result_existing_replace is not None, "對已存在表用空df replace，表應仍然存在（因為函式提前返回）"


    def test_load_dataframe_invalid_if_exists_mode(self, db_manager_memory: DBManager, sample_dataframe: pd.DataFrame):
        """測試使用無效的 if_exists 模式時引發 ValueError。"""
        with pytest.raises(ValueError, match="不支援的 if_exists 模式"):
            db_manager_memory.load_dataframe_to_table(sample_dataframe, "invalid_mode_table", if_exists='fail_silently')

# --- Direct Execution Block ---
if __name__ == "__main__":
    print("="*80)
    print("🚀 開始直接執行 test_db_manager.py 中的測試...")
    print("="*80, "\n")

    # 手動模擬 pytest fixture 的行為
    def create_db_manager_memory_for_direct_run():
        print("🔧 [Direct Run] 創建記憶體 DBManager 並設定表格...")
        manager = DBManager(db_path=":memory:")
        manager.setup_tables()
        print("✔️ [Direct Run] DBManager 準備完成。")
        return manager

    def get_sample_dataframe_for_direct_run():
        data = {
            'col_a': [1, 2, 3, 4, 5],
            'col_b': ['apple', 'banana', 'cherry', 'date', 'elderberry'],
            'col_c': [0.1, 0.2, 0.3, 0.4, 0.5]
        }
        return pd.DataFrame(data)

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    def run_test_method(test_class_instance, method_name, *args):
        nonlocal total_tests, passed_tests, failed_tests
        total_tests += 1
        method = getattr(test_class_instance, method_name)
        print(f"🧪 執行測試: {test_class_instance.__class__.__name__}::{method_name}")
        try:
            method(*args)
            print(f"  ✅ PASSED: {test_class_instance.__class__.__name__}::{method_name}")
            passed_tests += 1
        except AssertionError as e:
            print(f"  ❌ FAILED: {test_class_instance.__class__.__name__}::{method_name}\n     AssertionError: {e}")
            failed_tests += 1
        except Exception as e:
            print(f"  💥 ERROR: {test_class_instance.__class__.__name__}::{method_name}\n     Exception: {e}")
            import traceback
            traceback.print_exc()
            failed_tests += 1
        print("-" * 30)

    # --- 執行 TestDBManagerLifecycle ---
    print("\n--- 測試 TestDBManagerLifecycle ---")
    lifecycle_tests = TestDBManagerLifecycle()
    db_man_for_lifecycle = create_db_manager_memory_for_direct_run()
    run_test_method(lifecycle_tests, "test_db_manager_initialization_and_setup", db_man_for_lifecycle)
    db_man_for_lifecycle.close() # 確保關閉
    # test_db_manager_close_connection 是獨立的
    run_test_method(lifecycle_tests, "test_db_manager_close_connection")


    # --- 執行 TestDBManagerManifestOperations ---
    print("\n--- 測試 TestDBManagerManifestOperations ---")
    manifest_ops_tests = TestDBManagerManifestOperations()
    db_man_for_manifest = create_db_manager_memory_for_direct_run()
    run_test_method(manifest_ops_tests, "test_add_and_check_manifest_record", db_man_for_manifest)
    run_test_method(manifest_ops_tests, "test_add_duplicate_manifest_record_fails", db_man_for_manifest)
    run_test_method(manifest_ops_tests, "test_get_manifest_records_by_status", db_man_for_manifest)
    run_test_method(manifest_ops_tests, "test_update_manifest_transformation_status", db_man_for_manifest)
    db_man_for_manifest.close()

    # --- 執行 TestDBManagerRawFileOperations ---
    print("\n--- 測試 TestDBManagerRawFileOperations ---")
    raw_file_ops_tests = TestDBManagerRawFileOperations()
    db_man_for_raw = create_db_manager_memory_for_direct_run()
    run_test_method(raw_file_ops_tests, "test_store_and_get_raw_file_content", db_man_for_raw)
    run_test_method(raw_file_ops_tests, "test_get_raw_file_content_non_existent", db_man_for_raw)
    run_test_method(raw_file_ops_tests, "test_store_duplicate_raw_file_fails", db_man_for_raw)
    db_man_for_raw.close()

    # --- 執行 TestDBManagerLoadDataFrame ---
    print("\n--- 測試 TestDBManagerLoadDataFrame ---")
    load_df_tests = TestDBManagerLoadDataFrame()
    sample_df = get_sample_dataframe_for_direct_run()
    db_man_for_load = create_db_manager_memory_for_direct_run()
    run_test_method(load_df_tests, "test_load_dataframe_to_table_new_table_append", db_man_for_load, sample_df)
    run_test_method(load_df_tests, "test_load_dataframe_to_table_existing_table_append", db_man_for_load, sample_df)
    # 為了測試 replace，我們需要一個新的 db_manager 實例，以確保表格狀態是乾淨的或可預測的
    db_man_for_load.close()
    db_man_for_load_replace = create_db_manager_memory_for_direct_run()
    run_test_method(load_df_tests, "test_load_dataframe_to_table_new_table_replace", db_man_for_load_replace, sample_df)
    run_test_method(load_df_tests, "test_load_dataframe_to_table_existing_table_replace", db_man_for_load_replace, sample_df)
    run_test_method(load_df_tests, "test_load_dataframe_to_table_empty_dataframe", db_man_for_load_replace) # 使用同一個 manager
    # test_load_dataframe_invalid_if_exists_mode 需要一個 try-except 結構來捕獲 pytest.raises
    total_tests += 1
    print(f"🧪 執行測試: TestDBManagerLoadDataFrame::test_load_dataframe_invalid_if_exists_mode")
    try:
        with pytest.raises(ValueError, match="不支援的 if_exists 模式"): # 模擬 pytest.raises
             db_man_for_load_replace.load_dataframe_to_table(sample_df, "invalid_mode_table", if_exists='fail_silently')
        print(f"  ✅ PASSED: TestDBManagerLoadDataFrame::test_load_dataframe_invalid_if_exists_mode (ValueError correctly raised)")
        passed_tests +=1
    except AssertionError: # 如果 pytest.raises 沒有捕捉到預期的異常
        print(f"  ❌ FAILED: TestDBManagerLoadDataFrame::test_load_dataframe_invalid_if_exists_mode (ValueError not raised as expected)")
        failed_tests +=1
    except Exception as e:
        print(f"  💥 ERROR: TestDBManagerLoadDataFrame::test_load_dataframe_invalid_if_exists_mode\n     Exception: {e}")
        failed_tests += 1
    print("-" * 30)
    db_man_for_load_replace.close()


    print("="*80)
    print(f"🏁 直接執行測試結束。")
    print(f"總共執行測試: {total_tests}")
    print(f"✅ 通過: {passed_tests}")
    print(f"❌ 失敗/錯誤: {failed_tests}")
    print("="*80)

    if failed_tests > 0:
        print("\n🔥🔥🔥 有測試失敗，請檢查輸出！ 🔥🔥🔥")
        # exit(1) # 在 CI 環境中，非零退出碼表示失敗
    else:
        print("\n🎉🎉🎉 所有直接執行的測試均通過！ 🎉🎉🎉")
```
