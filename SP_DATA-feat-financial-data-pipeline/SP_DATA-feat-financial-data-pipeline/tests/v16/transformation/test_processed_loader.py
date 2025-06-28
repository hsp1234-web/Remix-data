import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
import builtins # 為了 mock builtins.print
from src.sp_data_v16.transformation.processed_loader import ProcessedDBLoader

@pytest.fixture
def mock_duckdb_connection(mocker):
    """Fixture 來模擬 duckdb 連線。"""
    mock_con = MagicMock()
    # 模擬 execute 方法，使其可以鏈式呼叫 fetchone() 或其他方法
    mock_con.execute.return_value.fetchone.return_value = None # 預設 table 不存在
    mocker.patch('duckdb.connect', return_value=mock_con)
    return mock_con

@pytest.fixture
def processed_loader_instance(mocker, mock_duckdb_connection):
    """Fixture 來建立 ProcessedDBLoader 的實例，並注入模擬的 duckdb 連線。"""
    # 模擬 os.makedirs，避免實際建立目錄
    mocker.patch('os.makedirs')
    loader = ProcessedDBLoader(db_path="dummy_path/test_processed.db")
    return loader

@pytest.fixture
def sample_dataframe():
    """Fixture 來提供一個範例 pandas DataFrame。"""
    data = {
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10.0, 20.0, 30.0]
    }
    return pd.DataFrame(data)

def test_processed_loader_init_success(processed_loader_instance, mock_duckdb_connection):
    """測試 ProcessedDBLoader 的 __init__ 方法是否成功初始化。"""
    assert processed_loader_instance.db_path == "dummy_path/test_processed.db"
    # duckdb.connect 已經在 fixture 中被 mock_duckdb_connection mock 並驗證
    # os.makedirs 也已經在 fixture 中被 mock
    # 驗證 connect 被呼叫
    import duckdb # 為了存取 duckdb.connect
    duckdb.connect.assert_called_once_with(database="dummy_path/test_processed.db", read_only=False)

def test_load_dataframe_append_mode_success(processed_loader_instance, mock_duckdb_connection, sample_dataframe):
    """測試 load_dataframe 在 schema_definition 未提供 unique_key 時，使用 append 模式成功載入 DataFrame。"""
    table_name = "test_table_append"
    schema_definition_no_unique_key = {
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"},
            "value": {"db_type": "DOUBLE"}
        }
    } # 沒有 unique_key

    with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:
        processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_no_unique_key)

        # 驗證 to_sql 被呼叫，並帶有 'append' 模式
        mock_to_sql.assert_called_once_with(
            name=table_name,
            con=mock_duckdb_connection,
            if_exists='append',
            index=False
        )

# --- Database Exception Hardening Tests ---
import duckdb # Required for duckdb.Error

@pytest.fixture
def processed_loader_with_mock_con(mocker):
    """
    Fixture to provide a ProcessedDBLoader instance where duckdb.connect
    is patched to return a MagicMock for the connection object.
    os.makedirs is also patched.
    """
    mock_con = mocker.MagicMock(spec=duckdb.DuckDBPyConnection)
    mocker.patch('duckdb.connect', return_value=mock_con)
    mocker.patch('os.makedirs') # Patch os.makedirs

    loader = ProcessedDBLoader(db_path="dummy_path/mock_con_processed.db")

    # Reset mocks that might have been called during init
    mock_con.execute.reset_mock()
    mock_con.commit.reset_mock() # Though ProcessedDBLoader's __init__ doesn't commit
    mock_con.register.reset_mock()
    mock_con.unregister.reset_mock()
    return loader, mock_con

def test_init_os_makedirs_error(mocker):
    """測試在 __init__ 中，如果 os.makedirs 拋出 OSError，則該錯誤會被正確拋出。"""
    mocker.patch('os.makedirs', side_effect=OSError("Mocked OS makedirs error"))
    # duckdb.connect should not be called if os.makedirs fails first
    mock_duckdb_connect = mocker.patch('duckdb.connect')

    with pytest.raises(OSError, match="Mocked OS makedirs error"):
        ProcessedDBLoader(db_path="some_dir/test.db")
    mock_duckdb_connect.assert_not_called()

def test_init_duckdb_connect_error_after_makedirs(mocker):
    """測試在 __init__ 中，如果 os.makedirs 成功但 duckdb.connect 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    mocker.patch('os.makedirs') # makedirs succeeds
    mocker.patch('duckdb.connect', side_effect=duckdb.Error("Mocked DB connect error in init"))

    with pytest.raises(duckdb.Error, match="Mocked DB connect error in init"):
        ProcessedDBLoader(db_path="dummy_path/fail_connect.db")

    # Verify os.makedirs was called
    assert os.makedirs.called

def test_load_dataframe_append_to_sql_error(processed_loader_with_mock_con, sample_dataframe, mocker):
    """測試在 append 模式下，如果 dataframe.to_sql 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    loader, mock_con = processed_loader_with_mock_con
    table_name = "test_append_sql_error"
    schema_no_unique_key = {"columns": {"id": {"db_type": "INTEGER"}}}

    # Mock the to_sql method on the DataFrame object itself
    with patch.object(sample_dataframe, 'to_sql', side_effect=duckdb.Error("Mocked to_sql DB Error")) as mock_to_sql:
        with pytest.raises(duckdb.Error, match="Mocked to_sql DB Error"):
            loader.load_dataframe(sample_dataframe, table_name, schema_no_unique_key)

        mock_to_sql.assert_called_once_with(
            name=table_name,
            con=mock_con, # The mock connection from the fixture
            if_exists='append',
            index=False
        )

def test_load_dataframe_upsert_register_error(processed_loader_with_mock_con, sample_dataframe, mocker):
    """測試在 upsert 模式下，如果 self.con.register 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    loader, mock_con = processed_loader_with_mock_con
    table_name = "test_upsert_register_error"
    schema_with_unique_key = {"unique_key": ["id"], "columns": {"id": {"db_type": "INTEGER"}}}

    mock_con.register.side_effect = duckdb.Error("Mocked DB register error")

    with pytest.raises(duckdb.Error, match="Mocked DB register error"):
        loader.load_dataframe(sample_dataframe, table_name, schema_with_unique_key)

    mock_con.register.assert_called_once_with(f"temp_view_{table_name}", sample_dataframe)
    # Ensure no execute calls for table creation or upsert happened
    mock_con.execute.assert_not_called()

def test_load_dataframe_upsert_create_table_as_select_error(processed_loader_with_mock_con, sample_dataframe, mocker):
    """測試在 upsert 模式下，如果 CREATE TABLE ... AS SELECT ... 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    loader, mock_con = processed_loader_with_mock_con
    table_name = "test_upsert_create_error"
    schema_with_unique_key = {
        "unique_key": ["id"],
        "columns": {"id": {"db_type": "INTEGER"}, "name": {"db_type": "VARCHAR"}}
    }

    # Simulate table does not exist
    mock_con.execute.return_value.fetchone.return_value = None

    # First execute is for table existence check, second for CREATE TABLE AS (empty)
    # Third for ALTER TABLE, Fourth for the final Upsert
    # We want the CREATE TABLE (second execute call in the if not table_exists_query block) to fail
    # This depends on the exact sequence of calls in load_dataframe
    # Let's make the call that contains "CREATE TABLE" fail
    def execute_side_effect_for_create_fail(query, *args):
        if "CREATE TABLE" in query and f"AS SELECT * FROM temp_view_{table_name}" in query : # This is the specific CREATE
            raise duckdb.Error("Mocked DB Error on CREATE TABLE AS SELECT")
        mock_result = mocker.MagicMock()
        mock_result.fetchone.return_value = None # For SELECT 1 FROM sqlite_master
        return mock_result

    mock_con.execute.side_effect = execute_side_effect_for_create_fail

    with pytest.raises(duckdb.Error, match="Mocked DB Error on CREATE TABLE AS SELECT"):
        loader.load_dataframe(sample_dataframe, table_name, schema_with_unique_key)

    # Verify register was called
    mock_con.register.assert_called_once()
    # Verify unregister was called due to error handling
    mock_con.unregister.assert_called_once()


def test_load_dataframe_upsert_alter_table_error(processed_loader_with_mock_con, sample_dataframe, mocker):
    """測試在 upsert 模式下，如果 ALTER TABLE 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    loader, mock_con = processed_loader_with_mock_con
    table_name = "test_upsert_alter_error"
    schema_with_unique_key = {
        "unique_key": ["id"],
        "columns": {"id": {"db_type": "INTEGER"}, "name": {"db_type": "VARCHAR"}}
    }

    # Simulate table does not exist
    mock_con.execute.return_value.fetchone.return_value = None

    def execute_side_effect_for_alter_fail(query, *args):
        if "ALTER TABLE" in query and f"ADD PRIMARY KEY" in query:
            raise duckdb.Error("Mocked DB Error on ALTER TABLE")
        mock_result = mocker.MagicMock()
        # For SELECT 1 from sqlite_master, make it return None (table doesn't exist)
        if "sqlite_master" in query:
             mock_result.fetchone.return_value = None
        else: # For other execute calls like CREATE TABLE, SELECT * FROM table WHERE 1=0
             mock_result.fetchone.return_value = None # Or some other appropriate mock
        return mock_result

    mock_con.execute.side_effect = execute_side_effect_for_alter_fail

    # The error from ALTER TABLE is caught and printed as a warning in the source code,
    # but the upsert attempt continues. If the final upsert then fails due to missing PK,
    # that error would be raised. Here, we test the specific ALTER TABLE failure.
    # The current code prints a warning and proceeds.
    # To test the raise, the source code would need to re-raise or not catch it.
    # For now, let's assert the warning is printed.
    mocker.patch('builtins.print')

    # Since the ALTER TABLE error is caught and printed, the function might not raise an error itself.
    # Instead, the subsequent INSERT ON CONFLICT might fail if it relies on the PK.
    # Let's make the final INSERT ON CONFLICT fail too, to ensure an error is raised from load_dataframe

    final_upsert_attempted = False
    def combined_execute_effect(query, *args):
        nonlocal final_upsert_attempted
        if "ALTER TABLE" in query and "ADD PRIMARY KEY" in query:
            raise duckdb.Error("Mocked DB Error on ALTER TABLE")
        if f"INSERT INTO \"{table_name}\"" in query and "ON CONFLICT" in query:
            final_upsert_attempted = True
            raise duckdb.Error("Mocked DB Error on final UPSERT after ALTER fail")

        mock_result = mocker.MagicMock()
        if "sqlite_master" in query:
             mock_result.fetchone.return_value = None
        else:
             mock_result.fetchone.return_value = None
        return mock_result

    mock_con.execute.side_effect = combined_execute_effect

    with pytest.raises(duckdb.Error, match="Mocked DB Error on final UPSERT after ALTER fail"):
         loader.load_dataframe(sample_dataframe, table_name, schema_with_unique_key)

    builtins.print.assert_any_call(f"警告：為資料表 '{table_name}' 添加 PRIMARY KEY 約束失敗: Mocked DB Error on ALTER TABLE。 Upsert 可能會失敗。")
    assert final_upsert_attempted, "Final upsert should have been attempted and failed."
    mock_con.unregister.assert_called_once() # Should be called in error handling path


def test_load_dataframe_upsert_final_insert_error(processed_loader_with_mock_con, sample_dataframe, mocker):
    """測試在 upsert 模式下，如果最後的 INSERT ... ON CONFLICT 拋出 duckdb.Error，則該錯誤會被正確拋出。"""
    loader, mock_con = processed_loader_with_mock_con
    table_name = "test_upsert_final_insert_error"
    schema_with_unique_key = {
        "unique_key": ["id"],
        "columns": {"id": {"db_type": "INTEGER"}, "name": {"db_type": "VARCHAR"}}
    }

    # Simulate table exists or is created successfully, and ALTER also succeeds
    # The first call to execute (fetchone) for table check can be None or a value.
    # Let's say table does not exist, so create/alter path is taken.
    create_alter_succeeded = False
    def execute_side_effect_for_final_insert_fail(query, *args):
        nonlocal create_alter_succeeded
        if f"INSERT INTO \"{table_name}\"" in query and "ON CONFLICT" in query:
            if not create_alter_succeeded: # Ensure this is after potential create/alter
                 # This state implies create/alter path was not fully mocked to succeed before this check
                 pass # Let it proceed to raise error
            raise duckdb.Error("Mocked DB Error on final INSERT")

        # Simulate successful CREATE and ALTER
        if "CREATE TABLE" in query or "ALTER TABLE" in query:
            create_alter_succeeded = True
            return mocker.MagicMock() # Does not raise error

        mock_result = mocker.MagicMock()
        if "sqlite_master" in query: # Table check
            mock_result.fetchone.return_value = None # Table doesn't exist initially
        else:
            mock_result.fetchone.return_value = None
        return mock_result

    mock_con.execute.side_effect = execute_side_effect_for_final_insert_fail

    with pytest.raises(duckdb.Error, match="Mocked DB Error on final INSERT"):
        loader.load_dataframe(sample_dataframe, table_name, schema_with_unique_key)

    assert mock_con.register.called
    assert mock_con.unregister.called # Called in the finally block of the exception

def test_close_db_error(processed_loader_with_mock_con, mocker):
    """測試在 close 中，如果 self.con.close 拋出 duckdb.Error，錯誤會被印出且不應向外拋出。"""
    loader, mock_con = processed_loader_with_mock_con
    mock_con.close.side_effect = duckdb.Error("Mocked DB Error on close")
    mocker.patch('builtins.print')

    loader.close() # Should not raise error based on current implementation

    mock_con.close.assert_called_once()
    builtins.print.assert_any_call(f"Error closing DuckDB connection: Mocked DB Error on close")

# Required for duckdb.Error
import os

def test_load_dataframe_upsert_mode_success(processed_loader_instance, mock_duckdb_connection, sample_dataframe):
    """測試 load_dataframe 在 schema_definition 提供 unique_key 時，使用 upsert 模式成功載入 DataFrame。"""
    table_name = "test_table_upsert"
    schema_definition_with_unique_key = {
        "unique_key": ["id"],
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"},
            "value": {"db_type": "DOUBLE"}
        }
    }

    # 模擬資料表不存在，這樣 CREATE TABLE 語句會被執行
    mock_duckdb_connection.execute.return_value.fetchone.return_value = None

    processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_with_unique_key)

    # 驗證 register 被呼叫以建立暫存視圖
    mock_duckdb_connection.register.assert_called_once()
    temp_view_name_arg = mock_duckdb_connection.register.call_args[0][0]
    assert temp_view_name_arg.startswith(f"temp_view_{table_name}")

    # 驗證 execute 被多次呼叫 (檢查資料表是否存在、CREATE TABLE、ALTER TABLE、INSERT ... ON CONFLICT)
    execute_calls = mock_duckdb_connection.execute.call_args_list

    # 1. 檢查資料表是否存在
    assert any(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table_name}'" in call[0][0] for call in execute_calls)

    # 2. CREATE TABLE (因為 fetchone 回傳 None)
    # 預期的 CREATE TABLE 語句可能很複雜，這裡簡化檢查
    # 根據 unique_key 和 columns 產生預期的 CREATE TABLE
    # "CREATE TABLE IF NOT EXISTS \"test_table_upsert\" (\"id\" INTEGER, \"name\" VARCHAR, \"value\" DOUBLE);"
    expected_create_table_sql = 'CREATE TABLE IF NOT EXISTS "test_table_upsert" ("id" INTEGER, "name" VARCHAR, "value" DOUBLE);'
    assert any(expected_create_table_sql in call[0][0] for call in execute_calls), \
        f"Expected CREATE TABLE call not found. Calls: {[call[0][0] for call in execute_calls]}"

    # 3. ALTER TABLE ADD PRIMARY KEY
    expected_alter_table_sql = 'ALTER TABLE "test_table_upsert" ADD PRIMARY KEY ("id");'
    assert any(expected_alter_table_sql in call[0][0] for call in execute_calls), \
        f"Expected ALTER TABLE call not found. Calls: {[call[0][0] for call in execute_calls]}"

    # 4. Upsert SQL
    # INSERT INTO "test_table_upsert" SELECT * FROM temp_view_test_table_upsert...
    # ON CONFLICT (id) DO UPDATE SET "id" = excluded."id", "name" = excluded."name", "value" = excluded."value";
    assert any(f'INSERT INTO "{table_name}"' in call[0][0] and \
               f'ON CONFLICT (id) DO UPDATE SET "id" = excluded."id", "name" = excluded."name", "value" = excluded."value"' in call[0][0]
               for call in execute_calls), \
        f"Expected Upsert SQL call not found. Calls: {[call[0][0] for call in execute_calls]}"

    # 驗證 unregister 被呼叫以清理暫存視圖
    mock_duckdb_connection.unregister.assert_called_once_with(temp_view_name_arg)

def test_load_dataframe_empty_dataframe(processed_loader_instance, mock_duckdb_connection):
    """測試當傳入空的 DataFrame 時，load_dataframe 是否能正確處理。"""
    empty_df = pd.DataFrame()
    table_name = "empty_table"
    schema_definition = {"columns": {"col1": {"db_type": "VARCHAR"}}} # 任意 schema

    processed_loader_instance.load_dataframe(empty_df, table_name, schema_definition)

    # 驗證沒有呼叫 to_sql 或 register/execute (因為 DataFrame 是空的)
    mock_duckdb_connection.execute.assert_not_called() # 除了可能的 init 中的 execute
    mock_duckdb_connection.register.assert_not_called()
    # 如果有 to_sql mock，也可以驗證 to_sql.assert_not_called()

def test_close_success(processed_loader_instance, mock_duckdb_connection):
    """測試 close 方法是否成功關閉資料庫連線。"""
    processed_loader_instance.close()
    # 驗證資料庫連線的 close 方法被呼叫
    mock_duckdb_connection.close.assert_called_once()

def test_init_db_connection_error(mocker):
    """測試資料庫連線失敗時，__init__ 是否會引發例外。"""
    mocker.patch('os.makedirs') # Mock os.makedirs 避免它失敗
    mocker.patch('duckdb.connect', side_effect=Exception("DB Connection Error"))
    with pytest.raises(Exception, match="DB Connection Error"):
        ProcessedDBLoader(db_path="dummy_path/test_processed.db")

def test_load_dataframe_append_mode_no_unique_key_in_schema(processed_loader_instance, mock_duckdb_connection, sample_dataframe):
    """測試 load_dataframe 在 schema_definition 中 'unique_key' 為空或不存在時，使用 append 模式。"""
    table_name = "test_table_append_no_key"
    schema_definition_no_key = {
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"}
        }
        # 'unique_key' 欄位不存在
    }

    with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:
        processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_no_key)
        mock_to_sql.assert_called_once_with(
            name=table_name,
            con=mock_duckdb_connection,
            if_exists='append',
            index=False
        )

    # 測試 'unique_key' 為空列表的情況
    schema_definition_empty_key = {
        "unique_key": [],
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"}
        }
    }
    with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:
        processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_empty_key)
        mock_to_sql.assert_called_once_with(
            name=table_name,
            con=mock_duckdb_connection,
            if_exists='append',
            index=False
        )
