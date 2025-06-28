import pytest
import duckdb
import os
import datetime
from src.sp_data_v16.ingestion.manifest import ManifestManager

@pytest.fixture
def in_memory_manager():
    """Provides a ManifestManager instance using an in-memory DuckDB."""
    manager = ManifestManager(db_path=':memory:')
    yield manager
    manager.close()

@pytest.fixture
def temp_db_manager(tmp_path):
    """Provides a ManifestManager instance using a temporary DB file."""
    db_file = tmp_path / "test_manifest.db"
    manager = ManifestManager(db_path=str(db_file))
    yield manager
    manager.close()

def test_initialize_schema(in_memory_manager: ManifestManager):
    """Tests that the file_manifest table is created upon initialization."""
    # Check if the table exists by trying to query it
    try:
        in_memory_manager.con.execute("SELECT * FROM file_manifest LIMIT 1;")
    except duckdb.CatalogException:
        pytest.fail("Table 'file_manifest' was not created or not found.")

    # More specific check for table columns (optional, but good for thoroughness)
    table_info = in_memory_manager.con.execute("PRAGMA table_info('file_manifest');").fetchall()
    # 修正：將 ingestion_timestamp 更正為 registration_timestamp
    # 修正：將 file_path 修正回 source_path 以匹配 manifest.py 的最新更改
    expected_columns = {
        'file_hash': 'VARCHAR',
        'source_path': 'VARCHAR',
        'status': 'VARCHAR',
        'registration_timestamp': 'TIMESTAMP'
    }
    actual_columns = {row[1]: row[2] for row in table_info} # name: type

    # 檢查 manifest.py 中的欄位是否都存在於預期中
    # CREATE TABLE IF NOT EXISTS file_manifest (
    # file_hash VARCHAR PRIMARY KEY,
    # source_path VARCHAR, # 已在 manifest.py 中修正
    # registration_timestamp TIMESTAMP DEFAULT current_timestamp,
    # status VARCHAR DEFAULT 'registered'
    # );
    # 所以 expected_columns 應該是: file_hash, source_path, registration_timestamp, status

    assert len(actual_columns) == len(expected_columns), \
        f"欄位數量不符。預期: {len(expected_columns)}, 實際: {len(actual_columns)}. " \
        f"預期欄位: {list(expected_columns.keys())}, 實際欄位: {list(actual_columns.keys())}"
    for col_name, col_type in expected_columns.items():
        assert col_name in actual_columns
        # DuckDB's PRAGMA table_info might return types like 'TIMESTAMP WITH TIME ZONE'
        # So we check if the expected type is a substring of the actual type.
        assert col_type.upper() in actual_columns[col_name].upper()

    # Check primary key
    pk_column_index = -1
    for i, row_detail in enumerate(table_info):
        if row_detail[1] == 'file_hash': # column name
            pk_column_index = i
            break
    assert pk_column_index != -1, "file_hash column not found for PK check"
    assert table_info[pk_column_index][5] == 1, "file_hash is not the primary key" # pk flag is at index 5


def test_register_file_and_hash_exists(temp_db_manager: ManifestManager):
    """Tests registering a new file and then checking if its hash exists."""
    file_hash = "test_hash_123"
    source_path = "/path/to/some/file.txt"

    assert not temp_db_manager.hash_exists(file_hash), "Hash should not exist before registration."

    temp_db_manager.register_file(file_hash, source_path)
    assert temp_db_manager.hash_exists(file_hash), "Hash should exist after registration."

    # Verify content
    result = temp_db_manager.con.execute("SELECT source_path, status FROM file_manifest WHERE file_hash = ?", [file_hash]).fetchone()
    assert result is not None, "Record not found after registration."
    # 修正：source_path 應為 file_path
    assert result[0] == source_path # manifest.py 的 register_file 參數是 source_path，但存入 DB 的欄位是 file_path
    assert result[1] == 'registered'
    # Timestamp check, 修正：ingestion_timestamp 應為 registration_timestamp
    ts_result = temp_db_manager.con.execute("SELECT registration_timestamp FROM file_manifest WHERE file_hash = ?", [file_hash]).fetchone()
    assert isinstance(ts_result[0], datetime.datetime), "Timestamp was not recorded correctly."


def test_hash_exists_for_unregistered_hash(in_memory_manager: ManifestManager):
    """Tests that hash_exists returns False for a hash that hasn't been registered."""
    assert not in_memory_manager.hash_exists("unregistered_hash_456")

def test_register_duplicate_hash_raises_exception(temp_db_manager: ManifestManager):
    """
    Tests that attempting to register the same file_hash twice raises a ConstraintException.
    This verifies the primary key constraint on file_hash for de-duplication.
    """
    file_hash = "duplicate_hash_789"
    source_path_1 = "/path/to/fileA.txt"
    source_path_2 = "/path/to/fileB.txt" # Different source, same hash

    temp_db_manager.register_file(file_hash, source_path_1) # First registration should succeed

    with pytest.raises(duckdb.ConstraintException) as excinfo:
        temp_db_manager.register_file(file_hash, source_path_2) # Second registration should fail

    error_message_lower = str(excinfo.value).lower()
    assert "duplicate key" in error_message_lower and \
           ("primary key" in error_message_lower or "unique constraint" in error_message_lower)


def test_update_file_status(temp_db_manager: ManifestManager, mocker):
    """測試 update_status 方法是否正確呼叫資料庫執行和提交。"""
    file_hash = "status_update_hash"
    new_status = "processed"
    # 為了驗證 .execute 和 .commit，我們需要 mock manifest manager 內部使用的 connection 物件
    mock_con = mocker.patch.object(temp_db_manager, 'con', autospec=True)

    # 呼叫 update_status (注意：ManifestManager 中的方法名是 update_status)
    temp_db_manager.update_status(file_hash, new_status)

    # 驗證 .execute 是否以正確的參數被呼叫
    mock_con.execute.assert_called_once_with(
        "UPDATE file_manifest SET status = ? WHERE file_hash = ?",
        (new_status, file_hash)
    )
    # 驗證 .commit 是否被呼叫
    mock_con.commit.assert_called_once()

    # 測試更新不存在的雜湊值 (也應該呼叫 execute 和 commit)
    mock_con.reset_mock() # 重置 mock 物件的呼叫記錄
    non_existent_hash = "non_existent_hash_for_status"
    temp_db_manager.update_status(non_existent_hash, "error")
    mock_con.execute.assert_called_once_with(
        "UPDATE file_manifest SET status = ? WHERE file_hash = ?",
        ("error", non_existent_hash)
    )
    mock_con.commit.assert_called_once()


# Example of an additional test for get_file_status
def test_get_file_status(temp_db_manager: ManifestManager):
    file_hash = "get_status_hash"
    source_path = "/path/to/get_status_file.txt"
    temp_db_manager.register_file(file_hash, source_path)

    status = temp_db_manager.get_file_status(file_hash)
    assert status == "registered"

    # 修正：呼叫正確的方法名稱 update_status 而非 update_file_status
    temp_db_manager.update_status(file_hash, "error")
    status = temp_db_manager.get_file_status(file_hash)
    assert status == "error"

    status_non_existent = temp_db_manager.get_file_status("non_existent_for_get_status")
    assert status_non_existent is None

# --- Database Exception Hardening Tests ---

@pytest.fixture
def manifest_manager_with_mock_con(mocker):
    """
    Fixture to provide a ManifestManager instance where duckdb.connect
    is patched to return a MagicMock for the connection object.
    The _initialize_schema is also manually called on this mock connection.
    """
    mock_con = mocker.MagicMock(spec=duckdb.DuckDBPyConnection)
    mocker.patch('duckdb.connect', return_value=mock_con)

    # Instantiate the manager. __init__ will call duckdb.connect (which is patched)
    # and then _initialize_schema.
    # We need to ensure _initialize_schema's execute and commit calls don't fail immediately
    # unless that's the specific test. For a generic mock_con, let them pass.
    mock_con.execute.return_value = None # Default for execute
    mock_con.commit.return_value = None # Default for commit

    manager = ManifestManager(db_path="dummy_path_for_mock_con.db")

    # Reset mocks for execute/commit if they were called during init,
    # so individual tests can assert their specific calls.
    mock_con.execute.reset_mock()
    mock_con.commit.reset_mock()
    return manager, mock_con

def test_init_duckdb_connect_error(mocker):
    """測試在 ManifestManager 初始化時，如果 duckdb.connect 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    mocker.patch('duckdb.connect', side_effect=duckdb.Error("Mocked DB Connection Error"))
    with pytest.raises(duckdb.Error, match="Mocked DB Connection Error"):
        ManifestManager(db_path="dummy_path/fail_connect.db")

def test_initialize_schema_db_error(mocker):
    """測試在 _initialize_schema 中，如果 self.con.execute 拋出 duckdb.Error，該錯誤會被正確處理或拋出。"""
    # 先讓 duckdb.connect 成功
    mock_con_instance = mocker.MagicMock()
    mocker.patch('duckdb.connect', return_value=mock_con_instance)

    # 模擬 self.con.execute 在 _initialize_schema (即 __init__ 過程中) 拋出錯誤
    mock_con_instance.execute.side_effect = duckdb.Error("Mocked Schema Init Error")

    with pytest.raises(duckdb.Error, match="Mocked Schema Init Error"):
        ManifestManager(db_path=":memory:")

    # 驗證 execute 至少被呼叫一次（嘗試執行 CREATE TABLE）
    mock_con_instance.execute.assert_called_once() # This refers to the execute during _initialize_schema

def test_hash_exists_db_error(manifest_manager_with_mock_con, mocker):
    """測試在 hash_exists 中，如果 self.con.execute 拋出 duckdb.Error，則錯誤會被正確拋出。"""
    manager, mock_con = manifest_manager_with_mock_con
    mock_con.execute.side_effect = duckdb.Error("Mocked DB Error on execute for hash_exists")
    with pytest.raises(duckdb.Error, match="Mocked DB Error on execute for hash_exists"):
        manager.hash_exists("any_hash")
    mock_con.execute.assert_called_once()

def test_get_file_status_db_error(manifest_manager_with_mock_con, mocker):
    """測試在 get_file_status 中，如果 self.con.execute 拋出 duckdb.Error，則錯誤會被正確拋出。"""
    manager, mock_con = manifest_manager_with_mock_con
    mock_con.execute.side_effect = duckdb.Error("Mocked DB Error on execute for get_file_status")
    with pytest.raises(duckdb.Error, match="Mocked DB Error on execute for get_file_status"):
        manager.get_file_status("any_hash")
    mock_con.execute.assert_called_once()

def test_register_file_db_execute_error(manifest_manager_with_mock_con, mocker):
    """測試在 register_file 中，如果第一次 self.con.execute (INSERT) 拋出 duckdb.Error，則錯誤會被正確拋出。"""
    manager, mock_con = manifest_manager_with_mock_con
    mock_con.execute.side_effect = duckdb.Error("Mocked DB Error on execute for register_file")
    with pytest.raises(duckdb.Error, match="Mocked DB Error on execute for register_file"):
        manager.register_file("test_hash", "/path/to/file")
    mock_con.execute.assert_called_once()
    mock_con.commit.assert_not_called() # 驗證 commit 不會被呼叫

def test_register_file_db_commit_error(manifest_manager_with_mock_con, mocker):
    """測試在 register_file 中，如果 self.con.commit 拋出 duckdb.Error，則錯誤會被正確拋出。"""
    manager, mock_con = manifest_manager_with_mock_con
    # execute 成功，commit 失敗
    mock_con.execute.return_value = None # 確保 execute 不拋錯
    mock_con.commit.side_effect = duckdb.Error("Mocked DB Error on commit for register_file")

    with pytest.raises(duckdb.Error, match="Mocked DB Error on commit for register_file"):
        manager.register_file("test_hash", "/path/to/file")

    mock_con.execute.assert_called_once()
    mock_con.commit.assert_called_once()

def test_update_status_db_execute_error(manifest_manager_with_mock_con, mocker):
    """測試在 update_status 中，如果 self.con.execute 拋出 duckdb.Error，則該錯誤會被印出且不向外拋出（根據目前實作）。"""
    manager, mock_con = manifest_manager_with_mock_con
    mock_con.execute.side_effect = duckdb.Error("Mocked DB Error on execute for update")
    mocker.patch('builtins.print') # Mock print to check its call

    manager.update_status("any_hash", "new_status")

    builtins.print.assert_called_once_with(f"Error updating status for any_hash to new_status: Mocked DB Error on execute for update")
    mock_con.execute.assert_called_once()
    mock_con.commit.assert_not_called()

def test_update_status_db_commit_error(manifest_manager_with_mock_con, mocker):
    """測試在 update_status 中，如果 self.con.commit 拋出 duckdb.Error，則該錯誤會被印出且不向外拋出（根據目前實作）。"""
    manager, mock_con = manifest_manager_with_mock_con
    mock_con.execute.return_value = None # execute 成功
    mock_con.commit.side_effect = duckdb.Error("Mocked DB Error on commit for update")
    mocker.patch('builtins.print')

    manager.update_status("any_hash", "new_status")

    mock_con.execute.assert_called_once()
    mock_con.commit.assert_called_once()
    builtins.print.assert_called_once_with(f"Error updating status for any_hash to new_status: Mocked DB Error on commit for update")

def test_close_db_error(manifest_manager_with_mock_con, mocker):
    """測試在 close 中，如果 self.con.close 拋出 duckdb.Error，錯誤應該被正確拋出。"""
    manager, mock_con = manifest_manager_with_mock_con
    mock_con.close.side_effect = duckdb.Error("Mocked DB Error on close")

    with pytest.raises(duckdb.Error, match="Mocked DB Error on close"):
        manager.close()

    mock_con.close.assert_called_once()

# 需要 import builtins 來 mock print
import builtins
