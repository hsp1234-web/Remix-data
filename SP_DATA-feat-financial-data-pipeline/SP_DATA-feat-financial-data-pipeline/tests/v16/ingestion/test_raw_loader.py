import pytest
from unittest.mock import MagicMock, mock_open
import pathlib
from src.sp_data_v16.ingestion.raw_loader import RawLakeLoader

@pytest.fixture
def mock_duckdb_connection(mocker):
    """Fixture 來模擬 duckdb 連線。"""
    mock_con = MagicMock()
    mocker.patch('duckdb.connect', return_value=mock_con)
    return mock_con

@pytest.fixture
def raw_loader_instance(mocker, mock_duckdb_connection):
    """Fixture 來建立 RawLakeLoader 的實例，並注入模擬的 duckdb 連線。"""
    # 模擬 pathlib.Path，使其在測試中可控
    mocker.patch('pathlib.Path.read_bytes', return_value=b"test data")
    loader = RawLakeLoader(db_path="dummy_path/test_raw_lake.db")
    return loader

def test_raw_lake_loader_init_success(raw_loader_instance, mock_duckdb_connection):
    """測試 RawLakeLoader 的 __init__ 方法是否成功初始化並建立資料表。"""
    # 驗證 duckdb.connect 是否以正確的路徑被呼叫
    # duckdb.connect 的呼叫在 fixture 中已經 patch，這裡檢查 fixture 是否正確設定
    assert raw_loader_instance.db_path == pathlib.Path("dummy_path/test_raw_lake.db")
    # 驗證 _initialize_schema 是否被呼叫（隱含地透過 execute 被呼叫）
    mock_duckdb_connection.execute.assert_any_call(
        """
            CREATE TABLE IF NOT EXISTS raw_files (
                file_hash VARCHAR PRIMARY KEY,
                raw_content BLOB
            )
            """
    )

def test_save_file_success(raw_loader_instance, mock_duckdb_connection, mocker):
    """測試 save_file 方法是否成功儲存檔案內容到資料庫。"""
    mock_file_path = MagicMock(spec=pathlib.Path)
    mock_file_path.read_bytes.return_value = b"binary file content"
    file_hash = "test_file_hash_123"

    raw_loader_instance.save_file(mock_file_path, file_hash)

    # 驗證檔案的 read_bytes 方法被呼叫
    mock_file_path.read_bytes.assert_called_once()
    # 驗證資料庫 execute 和 commit 方法被呼叫
    mock_duckdb_connection.execute.assert_called_with(
        "INSERT INTO raw_files VALUES (?, ?)", (file_hash, b"binary file content")
    )
    mock_duckdb_connection.commit.assert_called_once()

def test_close_success(raw_loader_instance, mock_duckdb_connection):
    """測試 close 方法是否成功關閉資料庫連線。"""
    raw_loader_instance.close()
    # 驗證資料庫連線的 close 方法被呼叫
    mock_duckdb_connection.close.assert_called_once()

def test_init_db_connection_error(mocker):
    """測試資料庫連線失敗時，__init__ 是否會引發例外。"""
    # 這裡的 mocker.patch('duckdb.connect', side_effect=Exception("DB Connection Error"))
    # 會被 raw_loader.py 中的 except Exception as e: print(...) 捕捉並重新 raise
    # 為了精確測試 duckdb.Error，我們明確指定 side_effect 為 duckdb.Error
    mocker.patch('duckdb.connect', side_effect=duckdb.Error("Mocked DB Connection Error"))
    with pytest.raises(duckdb.Error, match="Mocked DB Connection Error"):
        RawLakeLoader(db_path="dummy_path/test_raw_lake.db")

# --- IO/Database Exception Hardening Tests ---

@pytest.fixture
def raw_loader_with_mock_con(mocker):
    """
    Fixture to provide a RawLakeLoader instance where duckdb.connect
    is patched to return a MagicMock for the connection object.
    """
    mock_con = mocker.MagicMock(spec=duckdb.DuckDBPyConnection)
    mocker.patch('duckdb.connect', return_value=mock_con)

    # Mock pathlib.Path methods used by the loader for general cases
    # Individual tests can override these if needed
    mocker.patch('pathlib.Path.read_bytes', return_value=b"default mock data")

    # Instantiate the loader. __init__ will call _initialize_schema.
    # Let _initialize_schema's execute pass by default for this fixture.
    mock_con.execute.return_value = None

    loader = RawLakeLoader(db_path="dummy_path/mock_con_raw_lake.db")

    # Reset mocks for execute/commit if they were called during init
    mock_con.execute.reset_mock()
    mock_con.commit.reset_mock()
    return loader, mock_con

def test_init_initialize_schema_db_error(mocker):
    """測試在 RawLakeLoader 初始化時，如果 _initialize_schema 中的 execute 失敗，錯誤會被傳播。"""
    mock_con_instance = mocker.MagicMock(spec=duckdb.DuckDBPyConnection)
    mocker.patch('duckdb.connect', return_value=mock_con_instance)
    mock_con_instance.execute.side_effect = duckdb.Error("Schema init DB error")

    with pytest.raises(duckdb.Error, match="Schema init DB error"):
        RawLakeLoader(db_path="dummy_db_path.db")
    mock_con_instance.execute.assert_called_once() # Verifies _initialize_schema was attempted

def test_save_file_read_bytes_io_error(raw_loader_with_mock_con, mocker):
    """測試在 save_file 中，如果 file_path.read_bytes() 拋出 IOError，則該錯誤會被正確拋出。"""
    loader, mock_con = raw_loader_with_mock_con

    mock_file_path = MagicMock(spec=pathlib.Path)
    mock_file_path.read_bytes.side_effect = IOError("Mocked Read Bytes IOError")
    file_hash = "test_hash_io_error"

    with pytest.raises(IOError, match="Mocked Read Bytes IOError"):
        loader.save_file(mock_file_path, file_hash)

    mock_file_path.read_bytes.assert_called_once()
    mock_con.execute.assert_not_called() # execute 不應被呼叫
    mock_con.commit.assert_not_called() # commit 不應被呼叫

def test_save_file_db_execute_error(raw_loader_with_mock_con, mocker):
    """測試在 save_file 中，如果 self.con.execute 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    loader, mock_con = raw_loader_with_mock_con

    mock_file_path = MagicMock(spec=pathlib.Path)
    mock_file_path.read_bytes.return_value = b"some data" # read_bytes 成功
    file_hash = "test_hash_db_execute_error"

    mock_con.execute.side_effect = duckdb.Error("Mocked DB Execute Error for save_file")

    with pytest.raises(duckdb.Error, match="Mocked DB Execute Error for save_file"):
        loader.save_file(mock_file_path, file_hash)

    mock_file_path.read_bytes.assert_called_once()
    mock_con.execute.assert_called_once()
    mock_con.commit.assert_not_called() # commit 不應被呼叫

def test_save_file_db_commit_error(raw_loader_with_mock_con, mocker):
    """測試在 save_file 中，如果 self.con.commit 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    loader, mock_con = raw_loader_with_mock_con

    mock_file_path = MagicMock(spec=pathlib.Path)
    mock_file_path.read_bytes.return_value = b"some data" # read_bytes 成功
    file_hash = "test_hash_db_commit_error"

    mock_con.execute.return_value = None # execute 成功
    mock_con.commit.side_effect = duckdb.Error("Mocked DB Commit Error for save_file")

    with pytest.raises(duckdb.Error, match="Mocked DB Commit Error for save_file"):
        loader.save_file(mock_file_path, file_hash)

    mock_file_path.read_bytes.assert_called_once()
    mock_con.execute.assert_called_once()
    mock_con.commit.assert_called_once()

def test_close_db_error(raw_loader_with_mock_con, mocker):
    """測試在 close 中，如果 self.con.close 拋出 duckdb.Error，錯誤會被正確拋出。"""
    loader, mock_con = raw_loader_with_mock_con
    mock_con.close.side_effect = duckdb.Error("Mocked DB Error on close")

    with pytest.raises(duckdb.Error, match="Mocked DB Error on close"):
        loader.close()

    mock_con.close.assert_called_once()

# 需要 duckdb 來指定 Error 類型
import duckdb
