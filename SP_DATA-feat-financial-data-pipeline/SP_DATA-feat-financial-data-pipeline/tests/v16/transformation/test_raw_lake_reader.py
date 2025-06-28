import pytest
import pathlib
import duckdb
from src.sp_data_v16.transformation.raw_lake_reader import RawLakeReader

@pytest.fixture(scope="function")
def setup_dummy_raw_lake_db(tmp_path: pathlib.Path):
    """
    Sets up a temporary DuckDB database for RawLakeReader tests.
    Creates a 'raw_files' table and populates it with sample data.
    Yields the path to the dummy database file.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "test_raw_lake.db"

    conn = None
    try:
        conn = duckdb.connect(database=str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_files (
                file_hash VARCHAR PRIMARY KEY,
                raw_content BLOB
            );
        """)
        conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)",
                       ('hash_exists', b'actual content'))
        conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)",
                       ('hash_empty_content', b''))
        conn.commit() # Ensure data is written
    finally:
        if conn:
            conn.close()

    yield db_path
    # tmp_path fixture handles cleanup of the directory and file

def test_get_raw_content_exists(setup_dummy_raw_lake_db):
    """
    Tests retrieving existing raw content.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        content = reader.get_raw_content('hash_exists')
        assert content == b'actual content'
    finally:
        if reader:
            reader.close()

def test_close_handles_duckdb_error(setup_dummy_raw_lake_db, monkeypatch):
    """
    測試在關閉資料庫連線時發生 duckdb.Error，程式是否能正確處理而不崩潰。
    """
    db_path = setup_dummy_raw_lake_db # Fixture for path, though connect is mocked
    reader = None

    class MockConnectionFailsClose:
        def close(self):
            raise duckdb.Error("Simulated DB Error During Close")
        # Add other methods that might be called by duckdb.connect or RawLakeReader init/usage if any
        def execute(self, *args, **kwargs): pass
        def cursor(self, *args, **kwargs): return self # if cursor is used

    def mock_duckdb_connect_for_close_error(database, read_only):
        # This mock connect returns a connection that will fail on close
        return MockConnectionFailsClose()

    monkeypatch.setattr(duckdb, "connect", mock_duckdb_connect_for_close_error)

    try:
        reader = RawLakeReader(db_path=str(db_path)) # duckdb.connect is mocked here
        reader.close()  # This should call MockConnectionFailsClose.close() and trigger the error
                        # The test asserts that RawLakeReader's close method handles this error gracefully.
    except Exception as e:
        # If any other exception escapes, the test fails.
        pytest.fail(f"RawLakeReader.close() raised an unhandled exception: {e}")
    finally:
        if reader:
            # reader.close() was already called.
            # If an error occurred in reader.close() and was not handled, it would have propagated.
            # If it was handled, then self.con might be in an indeterminate state from RawLakeReader's perspective.
            pass
        monkeypatch.undo() # Important to restore original duckdb.connect

def test_get_raw_content_handles_unexpected_db_exception(setup_dummy_raw_lake_db, monkeypatch):
    """
    測試在資料庫查詢中發生非預期通用 Exception 時，get_raw_content 是否能處理並返回 None。
    """
    db_path = setup_dummy_raw_lake_db # 雖然 db_path 在此測試中不直接使用，但 fixture 提供了路徑
    reader = None

    class MockCursorFails:
        def fetchone(self):
            raise Exception("Simulated DB Error During Fetch")
        def execute(self, *args, **kwargs): # Mock cursor might also need an execute method
            return self
        def close(self): # Mock cursor might also need a close method
            pass


    class MockConnectionFailsExecute:
        def execute(self, *args, **kwargs):
            # The original code calls: self.con.execute(...).fetchone()
            # So, self.con.execute should return something that has a fetchone method.
            return MockCursorFails()

        def cursor(self): # If execute is called on a cursor
            return MockCursorFails()

        def close(self):
            pass # No-op for mock

    def mock_duckdb_connect_for_execute_error(database, read_only):
        return MockConnectionFailsExecute()

    monkeypatch.setattr(duckdb, "connect", mock_duckdb_connect_for_execute_error)

    try:
        # db_path 傳遞給 RawLakeReader，但 duckdb.connect 會被 mock
        reader = RawLakeReader(db_path=str(db_path))
        content = reader.get_raw_content('any_hash_will_do')
        assert content is None
    finally:
        if reader:
            reader.close() # 呼叫 reader.close()，它會呼叫 self.con.close()，即 MockConnectionFailsExecute.close()
        monkeypatch.undo() # 恢復 duckdb.connect

def test_get_raw_content_empty(setup_dummy_raw_lake_db):
    """
    Tests retrieving existing raw content that is an empty byte string.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        content = reader.get_raw_content('hash_empty_content')
        assert content == b''
    finally:
        if reader:
            reader.close()

def test_get_raw_content_not_exists(setup_dummy_raw_lake_db):
    """
    Tests retrieving non-existent raw content.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        content = reader.get_raw_content('hash_not_exists')
        assert content is None
    finally:
        if reader:
            reader.close()

def test_connection_error_invalid_path():
    """
    Tests that a ConnectionError is raised when an invalid database path is provided.
    Note: DuckDB might create a file if the directory exists, even in read_only.
    So, using a path where directory creation would fail is more robust for this test.
    """
    invalid_path = "/path/to/non_existent_dir/non_existent_and_uncreatable.db"
    # Ensure the parent directory does not exist and cannot be created by normal user

    with pytest.raises(ConnectionError) as excinfo:
        # RawLakeReader's __init__ catches duckdb.Error and raises ConnectionError
        RawLakeReader(db_path=invalid_path)

    assert "Could not connect to Raw Lake database" in str(excinfo.value)

def test_connection_error_unreadable_path(tmp_path):
    """
    Tests connection error for a path that exists but might be unreadable (e.g. a directory).
    DuckDB specific behavior for connecting to a directory path in read_only might vary,
    but generally it should fail if it's not a valid database file.
    """
    # Create a directory where a db file would be expected
    path_is_a_directory = tmp_path / "i_am_a_directory"
    path_is_a_directory.mkdir()

    with pytest.raises(ConnectionError) as excinfo:
        RawLakeReader(db_path=str(path_is_a_directory))

    assert "Could not connect to Raw Lake database" in str(excinfo.value)


def test_get_raw_content_after_close(setup_dummy_raw_lake_db):
    """
    Tests that attempting to get content after closing the reader returns None.
    """
    db_path = setup_dummy_raw_lake_db
    reader = RawLakeReader(db_path=str(db_path)) # Connection successful
    reader.close() # Close the connection

    # Try to get content after connection is closed
    content = reader.get_raw_content('hash_exists')
    assert content is None # Expect None as per RawLakeReader implementation

def test_close_idempotency(setup_dummy_raw_lake_db):
    """
    Tests that calling close() multiple times does not cause an error.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        reader.close()
        reader.close() # Call close again
    except Exception as e:
        pytest.fail(f"Calling close() multiple times raised an exception: {e}")
    finally:
        # Ensure connection is attempted to be closed if reader was initialized
        if reader and hasattr(reader, 'con') and reader.con: # Removed .is_closed() check
            reader.close()

def test_init_handles_non_string_path_conversion(tmp_path: pathlib.Path):
    """
    Tests that __init__ correctly handles path-like objects and also
    how it (indirectly via pathlib) rejects completely invalid path types.
    """
    # Test 1: Pass an integer as db_path, expecting TypeError from pathlib.Path()
    with pytest.raises(TypeError) as excinfo_type:
        RawLakeReader(db_path=12345)
    assert "argument should be a str or an os.PathLike object" in str(excinfo_type.value)

    # Test 2: Pass a pathlib.Path object for a non-existent database.
    # Expect ConnectionError from RawLakeReader, as DuckDB will fail to connect (read_only=True needs existing file).
    non_existent_db_path_obj = tmp_path / "data" / "non_existent_db_via_path_obj.db"
    # Ensure parent directory exists for a cleaner test of file non-existence vs path non-existence
    (tmp_path / "data").mkdir(exist_ok=True)

    with pytest.raises(ConnectionError) as excinfo_conn_non_existent:
        RawLakeReader(db_path=non_existent_db_path_obj)
    assert "Could not connect to Raw Lake database" in str(excinfo_conn_non_existent.value)

    # Test 3: Pass a pathlib.Path object for an existing (dummy) database.
    # Expect successful connection.
    existent_db_path_obj = tmp_path / "data" / "existent_db_via_path_obj.db"
    conn = None
    try:
        conn = duckdb.connect(str(existent_db_path_obj))
        conn.execute("CREATE TABLE IF NOT EXISTS dummy_table (id INTEGER);")
        conn.close()
        conn = None # Mark as closed for clarity

        reader = RawLakeReader(db_path=existent_db_path_obj)
        assert reader.db_path == existent_db_path_obj # Check path stored correctly
        reader.close()
    finally:
        if conn: # If conn is not None, it means it wasn't closed successfully above
             conn.close()
        if existent_db_path_obj.exists():
            existent_db_path_obj.unlink()

def test_get_raw_content_returns_none_for_non_existent_hash(setup_dummy_raw_lake_db):
    """
    測試當 file_hash 不存在時，get_raw_content 是否返回 None。
    此測試確保 'No content found' 的 print 語句路徑被執行。
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        # 使用一個保證不存在的 hash
        content = reader.get_raw_content('this_hash_definitely_does_not_exist_in_the_db')
        assert content is None
    finally:
        if reader:
            reader.close()
