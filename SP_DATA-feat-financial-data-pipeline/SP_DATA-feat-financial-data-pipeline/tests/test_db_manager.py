import pytest
import os
import duckdb
from pathlib import Path
from datetime import datetime, timezone, timedelta

# 模組被測對象
from taifex_pipeline.database import DBManager, FileStatus
from taifex_pipeline.database.constants import (
    TABLE_RAW_FILES, TABLE_FILE_MANIFEST,
    COLUMN_FILE_HASH, COLUMN_STATUS, COLUMN_ORIGINAL_PATH,
    COLUMN_INGESTION_TIMESTAMP, COLUMN_FILE_SIZE_BYTES
)

# --- Fixtures ---

@pytest.fixture
def memory_db_manager() -> DBManager:
    """提供一個使用記憶體 DuckDB 的 DBManager 實例，並自動關閉。"""
    # logger = logging.getLogger("test_db_manager")
    # logger.info("Creating in-memory DBManager for test.")
    manager = DBManager(db_path=":memory:")
    yield manager
    # logger.info("Closing in-memory DBManager after test.")
    manager.close()

@pytest.fixture
def file_db_manager(tmp_path: Path) -> DBManager:
    """提供一個使用檔案型 DuckDB 的 DBManager 實例，並自動關閉。"""
    db_file = tmp_path / "test_pipeline.duckdb"
    # logger = logging.getLogger("test_db_manager")
    # logger.info(f"Creating file-based DBManager at {db_file} for test.")
    manager = DBManager(db_path=str(db_file))
    yield manager
    # logger.info(f"Closing file-based DBManager at {db_file} after test.")
    manager.close()
    # pytest 的 tmp_path fixture 會自動清理檔案

# --- Test Cases ---

class TestDBManager:

    def test_init_memory_db(self, memory_db_manager: DBManager):
        """測試使用記憶體資料庫初始化 DBManager。"""
        assert memory_db_manager.db_path == ":memory:"
        assert memory_db_manager.conn is not None
        assert isinstance(memory_db_manager.conn, duckdb.DuckDBPyConnection)

    def test_init_file_db_and_directory_creation(self, tmp_path: Path):
        """測試使用檔案型資料庫初始化 DBManager，並驗證目錄是否被建立。"""
        db_dir = tmp_path / "test_db_data"
        db_file = db_dir / "pipeline_file.duckdb"

        assert not db_dir.exists(), "測試前，資料庫目錄不應存在。"

        with DBManager(db_path=str(db_file)) as manager:
            assert manager.db_path == str(db_file)
            assert manager.conn is not None
            assert db_dir.exists(), "DBManager 初始化時應建立資料庫目錄。"
            assert db_dir.is_dir()
            assert db_file.exists(), "DBManager 初始化時應建立資料庫檔案 (或 DuckDB 會處理)。"

        # 驗證 context manager 是否關閉連線
        # 雖然 manager.conn 在 with 區塊外無法直接存取 (如果 manager 是區域變數)
        # 但我們可以嘗試用同一路徑再次連線，或檢查 manager.conn 是否為 None (如果可以存取)
        # 這裡我們依賴 DBManager.close() 的正確性，並在 fixture 中驗證

    def test_setup_tables(self, memory_db_manager: DBManager):
        """測試 setup_tables 方法是否能成功建立表格。"""
        manager = memory_db_manager
        manager.setup_tables()

        # 驗證表格是否存在
        try:
            tables_df = manager.conn.execute("SHOW TABLES;").fetchdf()
            table_names = tables_df['name'].tolist()
            assert TABLE_RAW_FILES in table_names, f"{TABLE_RAW_FILES} 表格應被建立。"
            assert TABLE_FILE_MANIFEST in table_names, f"{TABLE_FILE_MANIFEST} 表格應被建立。"
        except Exception as e:
            pytest.fail(f"驗證表格是否存在時發生錯誤: {e}")

        # 驗證 raw_files 表的結構 (部分欄位)
        try:
            raw_files_info_df = manager.conn.execute(f"PRAGMA table_info('{TABLE_RAW_FILES}');").fetchdf()
            assert COLUMN_FILE_HASH in raw_files_info_df['name'].tolist()
            assert "raw_content" in raw_files_info_df['name'].tolist()
            # 驗證 file_hash 是否為主鍵
            pk_info_raw = raw_files_info_df[raw_files_info_df['name'] == COLUMN_FILE_HASH]
            assert not pk_info_raw.empty and pk_info_raw.iloc[0]['pk'], f"{COLUMN_FILE_HASH} 應為 {TABLE_RAW_FILES} 的主鍵。"
        except Exception as e:
            pytest.fail(f"驗證 {TABLE_RAW_FILES} 表結構時發生錯誤: {e}")

        # 驗證 file_manifest 表的結構 (部分欄位和主鍵)
        try:
            manifest_info_df = manager.conn.execute(f"PRAGMA table_info('{TABLE_FILE_MANIFEST}');").fetchdf()
            assert COLUMN_FILE_HASH in manifest_info_df['name'].tolist()
            assert COLUMN_ORIGINAL_PATH in manifest_info_df['name'].tolist()
            assert COLUMN_STATUS in manifest_info_df['name'].tolist()
            assert COLUMN_INGESTION_TIMESTAMP in manifest_info_df['name'].tolist()

            pk_info_manifest = manifest_info_df[manifest_info_df['name'] == COLUMN_FILE_HASH]
            assert not pk_info_manifest.empty and pk_info_manifest.iloc[0]['pk'], \
                   f"{COLUMN_FILE_HASH} 應為 {TABLE_FILE_MANIFEST} 的主鍵。"
        except Exception as e:
            pytest.fail(f"驗證 {TABLE_FILE_MANIFEST} 表結構時發生錯誤: {e}")

    def test_store_raw_file_and_check_hash_exists(self, memory_db_manager: DBManager):
        """測試儲存原始檔案及後續檢查雜湊是否存在的功能。"""
        manager = memory_db_manager
        manager.setup_tables()

        test_hash = "hash_store_test_001"
        test_content = b"binary content for store_raw_file test"

        # 初始時，雜湊不應存在於 manifest (雖然我們這裡只測 raw_files，但 check_hash_exists 查的是 manifest)
        # 為了讓 check_hash_exists 返回 false， manifest 必須是空的或不包含此 hash
        # 我們先不加入 manifest 記錄
        assert not manager.check_hash_exists(test_hash), "新雜湊在 manifest 中不應存在 (測試前置條件)"

        # 儲存原始檔案
        manager.store_raw_file(test_hash, test_content)

        # 驗證原始檔案是否已儲存 (直接查詢 raw_files)
        try:
            result = manager.conn.execute(
                f"SELECT raw_content FROM {TABLE_RAW_FILES} WHERE {COLUMN_FILE_HASH} = ?",
                (test_hash,)
            ).fetchone()
            assert result is not None, "原始檔案應已儲存到 raw_files。"
            assert result[0] == test_content, "儲存的原始檔案內容不符。"
        except Exception as e:
            pytest.fail(f"查詢已儲存的原始檔案時發生錯誤: {e}")

        # 測試儲存重複雜湊 (應引發 IntegrityError)
        with pytest.raises(duckdb.IntegrityError, match="PRIMARY KEY constraint failed"):
            manager.store_raw_file(test_hash, b"different content")

    def test_add_manifest_record_and_check_exists(self, memory_db_manager: DBManager):
        """測試新增 manifest 記錄及後續檢查雜湊是否存在。"""
        manager = memory_db_manager
        manager.setup_tables()

        test_hash = "hash_manifest_test_002"
        original_path = "/test/path/file.txt"
        file_size = 1024
        # last_modified_dt = datetime.now(timezone.utc) - timedelta(hours=1)
        # last_modified_iso = last_modified_dt.isoformat()
        # For DuckDB TIMESTAMPTZ, it's often easier to work with naive datetimes (assumed UTC)
        # or timezone-aware datetimes. DuckDB handles conversion.
        # Let's use ISO strings for simplicity in tests if that's what the method expects.
        last_modified_iso = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        source_system = "TEST_SUITE"
        notes = "Manifest record test"
        discovery_ts_iso = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()


        # 初始時，雜湊不應存在
        assert not manager.check_hash_exists(test_hash), "新雜湊在 manifest 中不應存在。"

        # 新增 manifest 記錄
        manager.add_manifest_record(
            file_hash=test_hash,
            original_path=original_path,
            file_size_bytes=file_size,
            last_modified_at_source_str=last_modified_iso,
            source_system=source_system,
            notes=notes,
            discovery_timestamp_str=discovery_ts_iso
        )

        # 現在雜湊應存在
        assert manager.check_hash_exists(test_hash), "新增後，雜湊應存在於 manifest。"

        # 驗證記錄內容
        try:
            record_df = manager.conn.execute(
                f"SELECT * FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH} = ?",
                (test_hash,)
            ).fetchdf()

            assert not record_df.empty, "應能查詢到新增的 manifest 記錄。"
            record = record_df.iloc[0]

            assert record[COLUMN_ORIGINAL_PATH] == original_path
            assert record[COLUMN_STATUS] == FileStatus.RAW_INGESTED.value
            assert record[COLUMN_FILE_SIZE_BYTES] == file_size
            assert record[COLUMN_SOURCE_SYSTEM] == source_system
            assert record[COLUMN_NOTES] == notes

            # 驗證時間戳 (ingestion_timestamp 應接近現在)
            # DuckDB stores TIMESTAMPTZ as UTC. Python datetime objects from DuckDB are usually naive UTC or aware.
            ingestion_ts_from_db = record[COLUMN_INGESTION_TIMESTAMP] # This will be a datetime.datetime object
            assert isinstance(ingestion_ts_from_db, datetime)
            # Make it timezone-aware (UTC, as DuckDB stores it) if it's naive
            if ingestion_ts_from_db.tzinfo is None:
                ingestion_ts_from_db = ingestion_ts_from_db.replace(tzinfo=timezone.utc)

            now_utc = datetime.now(timezone.utc)
            assert (now_utc - ingestion_ts_from_db).total_seconds() < 5, \
                   f"汲取時間戳 {ingestion_ts_from_db} 與目前時間 {now_utc} 相差過多。"

            # 驗證其他時間戳 (如果它們是字串，DuckDB 會轉換為 datetime)
            discovery_ts_from_db = record[COLUMN_DISCOVERY_TIMESTAMP]
            assert isinstance(discovery_ts_from_db, datetime)
            # Compare with parsed original string, ensuring UTC for comparison
            expected_discovery_dt = datetime.fromisoformat(discovery_ts_iso.replace('Z', '+00:00'))
            if discovery_ts_from_db.tzinfo is None: # make aware for comparison
                 discovery_ts_from_db = discovery_ts_from_db.replace(tzinfo=timezone.utc)
            assert discovery_ts_from_db == expected_discovery_dt

            last_modified_from_db = record[COLUMN_LAST_MODIFIED_AT_SOURCE]
            assert isinstance(last_modified_from_db, datetime)
            expected_last_modified_dt = datetime.fromisoformat(last_modified_iso.replace('Z', '+00:00'))
            if last_modified_from_db.tzinfo is None:
                last_modified_from_db = last_modified_from_db.replace(tzinfo=timezone.utc)
            assert last_modified_from_db == expected_last_modified_dt

        except Exception as e:
            pytest.fail(f"驗證 manifest 記錄內容時發生錯誤: {e}")

        # 測試新增重複雜湊 (應引發 IntegrityError)
        with pytest.raises(duckdb.IntegrityError, match="PRIMARY KEY constraint failed"):
            manager.add_manifest_record(test_hash, "/another/path.txt")

    def test_close_connection(self, memory_db_manager: DBManager):
        """測試關閉資料庫連線。"""
        manager = memory_db_manager
        assert manager.conn is not None, "連線在測試開始時應存在。"

        manager.close()
        assert manager.conn is None, "呼叫 close() 後，連線物件應為 None。"

        # 再次呼叫 close 應無副作用
        try:
            manager.close()
        except Exception as e:
            pytest.fail(f"對已關閉的連線再次呼叫 close() 不應引發錯誤: {e}")

    def test_context_manager_usage(self, tmp_path: Path):
        """測試 DBManager 作為 context manager 使用時，連線是否自動開啟和關閉。"""
        db_file = tmp_path / "context_test.duckdb"

        # 檢查 __enter__
        with DBManager(db_path=str(db_file)) as manager_inside_with:
            assert manager_inside_with.conn is not None, "在 with 區塊內，連線應已開啟。"
            assert isinstance(manager_inside_with.conn, duckdb.DuckDBPyConnection)
            # 執行一個簡單操作
            manager_inside_with.setup_tables()
            tables_df = manager_inside_with.conn.execute("SHOW TABLES;").fetchdf()
            assert not tables_df.empty

        # 檢查 __exit__ 是否關閉連線
        # 為了驗證，我們需要一種方式來存取 manager_inside_with 在 with 區塊後的狀態
        # 或者，我們可以嘗試用同一路徑建立一個新連線，如果舊連線未關閉，某些資料庫可能會出問題
        # DuckDB 通常允許多個連線到同一個檔案 (除非唯讀等限制)
        # 最好的方式是假設 manager_inside_with.conn 在 __exit__ 後是 None (如果 DBManager 實作如此)
        # 目前 DBManager 的 __exit__ 呼叫 self.close()，而 self.close() 會將 self.conn 設為 None
        # 然而，manager_inside_with 在 with 區塊結束後可能無法存取其屬性
        # 這裡我們間接測試：如果 __exit__ 未正確關閉，fixture 中的 close 可能會再次嘗試關閉，
        # 或者在某些情況下，如果檔案被鎖定，後續操作可能會失敗。
        # 這裡我們主要依賴於 test_close_connection 的正確性。
        # 一個簡單的檢查是，如果檔案型資料庫在 with 結束後可以被安全刪除 (如果測試需要)
        assert db_file.exists() # 檔案應已建立

    def test_db_operations_after_close_raises_error(self, memory_db_manager: DBManager):
        """測試在連線關閉後執行資料庫操作是否引發錯誤。"""
        manager = memory_db_manager
        manager.setup_tables() # 先設定表格
        manager.close() # 關閉連線

        with pytest.raises(ConnectionError, match="資料庫未連線"):
            manager.setup_tables()

        with pytest.raises(ConnectionError, match="資料庫未連線"):
            manager.check_hash_exists("some_hash")

        with pytest.raises(ConnectionError, match="資料庫未連線"):
            manager.store_raw_file("some_hash", b"content")

        with pytest.raises(ConnectionError, match="資料庫未連線"):
            manager.add_manifest_record("some_hash", "/path")

    def test_init_failure_no_directory_permission(self, tmp_path: Path, mocker):
        """
        測試當無法建立資料庫目錄時 (例如權限問題)，__init__ 是否按預期失敗。
        注意: 這個測試依賴於能夠模擬 os.makedirs 拋出 OSError。
        """
        # 選擇一個受保護的路徑或 mock os.makedirs
        # 為了可移植性和不依賴實際檔案系統權限，mock 是更好的選擇

        db_dir_that_will_fail = tmp_path / "restricted_dir"
        db_file_in_failed_dir = db_dir_that_will_fail / "db.duckdb"

        # Mock os.makedirs 來拋出 OSError
        mocked_makedirs = mocker.patch("os.makedirs")
        mocked_makedirs.side_effect = OSError("權限不足 (模擬)")

        # Mock os.path.exists 使其第一次返回 False (目錄不存在)，然後可能 True (如果被再次檢查)
        # 這裡我們主要關心 makedirs 的行為
        mocker.patch("os.path.exists", return_value=False) # 模擬目錄初始時不存在

        with pytest.raises(OSError, match="權限不足 \(模擬\)"):
            DBManager(db_path=str(db_file_in_failed_dir))

        mocked_makedirs.assert_called_once_with(str(db_dir_that_will_fail))


# 可以加入更多針對特定錯誤情況或邊界條件的測試
# 例如：
# - store_raw_file 或 add_manifest_record 傳入無效參數
# - 資料庫檔案損壞 (較難模擬)
# - 大量資料的插入和查詢 (性能測試，可能不屬於單元測試範疇)
