# -*- coding: utf-8 -*-
import pytest
import pandas as pd
import json
import re # Added import
from unittest.mock import patch, MagicMock, mock_open

# Adjust the import path based on your project structure.
# Assuming 'src' is a top-level directory and your tests run from the project root.
from src.data_pipeline_v15.database_loader import DatabaseLoader

VALID_SCHEMAS_CONTENT = {
    "test_schema_A": {"db_table_name": "table_A"},
    "test_schema_B": {"db_table_name": "table_B"},
    "schema_missing_name": {} # Schema without db_table_name
}

MALFORMED_SCHEMAS_CONTENT = "this is not valid json"

@pytest.fixture
def mock_logger():
    """Provides a MagicMock instance for logger."""
    return MagicMock()

# Patch _connect for all load_data tests to avoid actual DB operations
# @patch.object(DatabaseLoader, '_connect', MagicMock())
# class TestLoadData:
#
#     @pytest.fixture
#     def loader_with_tables(self, mock_logger):
#         """
#         Provides a DatabaseLoader instance with pre-set allowed_table_names
#         and a mocked connection.
#         """
#         # Mock the schema loading part for these specific tests
#         with patch.object(DatabaseLoader, '_load_allowed_table_names', MagicMock()) as mock_load_names:
#             loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)
#             loader.allowed_table_names = {"table_A", "table_B"}
#             loader.connection = MagicMock() # Mock the connection object itself
#             mock_load_names.assert_called_once() # Ensure it was called during init
#             return loader
#
#     def test_load_data_allowed_table(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_A"], "data": [1]})
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_not_called()
#         loader_with_tables.connection.register.assert_called_once_with("temp_df_view", df)
#         assert loader_with_tables.connection.execute.call_count == 2
#         loader_with_tables.connection.execute.assert_any_call(
#             '\n                CREATE TABLE IF NOT EXISTS "table_A" AS SELECT * FROM temp_df_view LIMIT 0;\n            '
#         )
#         loader_with_tables.connection.execute.assert_any_call(
#             '\n                INSERT INTO "table_A" SELECT * FROM temp_df_view;\n            '
#         )
#
#     def test_load_data_table_not_in_whitelist(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_C"], "data": [1]}) # table_C is not allowed
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once()
#         assert "Table name 'table_C' (derived from schema_type) is not in the allowed list" in mock_logger.error.call_args[0][0]
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#
#     def test_load_data_missing_schema_type_column(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"data": [1]}) # Missing 'schema_type'
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once_with(
#             "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         )
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#     def test_load_data_multiple_schema_types_in_df(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_A", "table_B"], "data": [1, 2]})
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once_with(
#             "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         )
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#     def test_load_data_empty_dataframe_valid_schema_type(self, loader_with_tables, mock_logger):
#         # DataFrame with schema_type column but no rows
#         df = pd.DataFrame({"schema_type": pd.Series(dtype='str'), "data": pd.Series(dtype='int')})
#
#         loader_with_tables.load_data(df)
#
#         # This scenario (empty df but schema_type column exists) would lead to an error
#         # when trying df["schema_type"].iloc[0] because there's no row 0.
#         # The existing check for nunique() != 1 might catch it if column is empty,
#         # or iloc[0] will raise IndexError.
#         # The current code structure:
#         # 1. Checks for connection
#         # 2. Checks for "schema_type" column and nunique != 1
#         # If df["schema_type"] is empty, nunique() is 0, so it passes the "!=1" but might fail later.
#         # If the column exists but has no data, iloc[0] will fail.
#         # Let's assume the existing check `df["schema_type"].nunique() != 1` handles Series with no values.
#         # If nunique is 0, it's not 1.
#         # The error "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         # should be logged.
#         mock_logger.error.assert_called_once_with(
#             "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         )
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#     def test_load_data_connection_is_none(self, loader_with_tables, mock_logger):
#         loader_with_tables.connection = None # Simulate connection lost/not established
#         df = pd.DataFrame({"schema_type": ["table_A"], "data": [1]})
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once_with("資料庫連線不存在，無法載入資料。")
#         # Can't check execute on None, but it shouldn't be reached.
#
#     def test_load_data_db_operation_exception(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_A"], "data": [1]})
#         loader_with_tables.connection.execute.side_effect = Exception("DB write error")
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_any_call("❌ 載入資料至資料表 'table_A' 時發生錯誤: DB write error", exc_info=True)
#         loader_with_tables.connection.unregister.assert_called_once_with("temp_df_view") # finally block

# Example of how to run this with pytest:
# Ensure pytest is installed: pip install pytest
# Navigate to the directory containing 'tests' and 'src'
# Run: pytest
#
# To run with coverage:
# pip install pytest-cov
# pytest --cov=src.data_pipeline_v15.database_loader --cov-report=html
# (then open htmlcov/index.html in a browser)

# A test for _connect could also be added, but it's simple and mostly calls duckdb.connect
# Testing _connect would involve patching duckdb.connect itself.
@patch('src.data_pipeline_v15.database_loader.duckdb.connect')
def test_connect_success(mock_duckdb_connect, mock_logger):
    loader = DatabaseLoader(database_file="test.db", logger=mock_logger)
    # _connect is called in __init__
    mock_duckdb_connect.assert_called_once_with(database="test.db", read_only=False)
    mock_logger.info.assert_any_call("✅ DuckDB 資料庫連線成功。")
    assert loader.connection is not None


@patch('src.data_pipeline_v15.database_loader.duckdb.connect', side_effect=Exception("Connection Failed"))
def test_connect_failure(mock_duckdb_connect_failed, mock_logger):
    with pytest.raises(Exception, match="Connection Failed"): # Expecting it to re-raise
        DatabaseLoader(database_file="test.db", logger=mock_logger)

    mock_duckdb_connect_failed.assert_called_once_with(database="test.db", read_only=False)
    mock_logger.critical.assert_called_once_with(
        "❌ 無法連線至 DuckDB 資料庫！錯誤：Connection Failed", exc_info=True
    )

def test_close_connection_no_connection(mock_logger):
    with patch('src.data_pipeline_v15.database_loader.duckdb.connect'): # prevent __init__ from connecting
        loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)
        loader.connection = None # Ensure no connection

        loader.close_connection()
        # Assert logger was not called with "closing" or "closed" if there's no connection
        # This depends on the exact logging messages. Current code only logs if self.connection is truthy.
        # So, no calls to info about closing/closed.
        # Check that logger.info wasn't called with the closing messages
        for call_args in mock_logger.info.call_args_list:
            assert "正在關閉 DuckDB 資料庫連線..." not in call_args[0][0]
            assert "資料庫連線已成功關閉。" not in call_args[0][0]


# --- Tests for load_parquet with Idempotency ---

@pytest.fixture
def sample_parquet_file_for_idempotency(tmp_path):
    """Creates a sample Parquet file for idempotency testing."""
    data = {
        "trading_date": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-02"]),
        "product_id": ["TXF", "TXF", "MXF"],
        "expiry_month": ["202301", "202302", "202301"],
        "strike_price": [14000.0, 14000.0, 7000.0],
        "option_type": ["C", "P", "C"],
        "open": [100.0, 50.0, 200.0],
        "high": [110.0, 55.0, 210.0],
        "low": [90.0, 45.0, 190.0],
        "close": [105.0, 52.0, 205.0],
        "volume": [1000, 500, 800],
        "open_interest": [5000, 2000, 3000],
        "delta": [0.5, -0.4, 0.6]
    }
    df = pd.DataFrame(data)
    parquet_path = tmp_path / "sample_fact_daily_ohlc.parquet"
    df.to_parquet(parquet_path)
    return parquet_path
