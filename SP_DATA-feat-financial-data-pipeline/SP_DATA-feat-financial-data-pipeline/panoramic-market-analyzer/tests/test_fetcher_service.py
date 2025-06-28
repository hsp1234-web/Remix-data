# tests/test_fetcher_service.py
import pytest
import pandas as pd
import duckdb
from services.fetcher_service import fetch_and_store
import yfinance as yf # To allow monkeypatching yf.download
from unittest.mock import patch # Alternative to monkeypatch for more complex scenarios if needed

@pytest.fixture
def mock_yfinance_download(monkeypatch):
    """Mocks the yfinance.download function."""
    mock_data = {
        'Open': [150.0, 151.0, 152.0],
        'High': [153.0, 153.5, 154.0],
        'Low': [149.0, 150.5, 151.0],
        'Close': [152.5, 153.0, 153.5],
        'Adj Close': [152.5, 153.0, 153.5],
        'Volume': [1000000, 1200000, 1100000]
    }
    # yfinance returns a DataFrame with a DatetimeIndex
    mock_df = pd.DataFrame(mock_data, index=pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']))
    mock_df.index.name = 'Date'

    def mock_download(*args, **kwargs):
        print(f"Mock yf.download called with args: {args}, kwargs: {kwargs}")
        return mock_df

    monkeypatch.setattr(yf, "download", mock_download)
    return mock_df

def test_fetcher_saves_data_correctly(mock_yfinance_download, tmp_path):
    """
    Tests that the fetcher service correctly saves data from a mocked yf.download.
    """
    # 1. SETUP
    symbol = "TESTAAPL"
    start_date = "2023-01-01"
    end_date = "2023-01-03"
    db_file = tmp_path / "test_raw_data.db"

    # Expected data is the mocked DataFrame
    expected_df = mock_yfinance_download

    # 2. EXECUTION
    # Call the main function of the fetcher service
    fetch_and_store(symbol, start_date, end_date, str(db_file))

    # 3. VERIFICATION
    assert db_file.exists(), "DuckDB file was not created."

    con = duckdb.connect(str(db_file))
    table_name = f"{symbol.replace('-', '_')}_raw"

    try:
        result_df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    except duckdb.CatalogException:
        pytest.fail(f"Table '{table_name}' was not created in the database.")
    finally:
        con.close()

    # Convert 'Date' column back to datetime if it's not already, for proper comparison
    # DuckDB might store it as a string or a different date type.
    # yfinance.download returns a DataFrame with a DatetimeIndex.
    # When saving to DuckDB and reading back, the index 'Date' becomes a regular column.
    # We need to ensure the data types are comparable.
    # fetchdf() should infer types, but let's be explicit for 'Date'.
    # The sample data's index is DatetimeIndex. fetch_and_store does `SELECT * FROM df`
    # which includes the index as a column named "Date" (if df.index.name was "Date").
    # Let's ensure our mock_df had its index named. (Added to fixture)

    # Check if columns match (order might differ, so check as sets)
    assert set(result_df.columns) == set(expected_df.reset_index().columns), "Columns do not match."

    # Reset index for comparison if 'Date' is a column in result_df and index in expected_df
    expected_df_for_comparison = expected_df.reset_index()

    # Convert 'Date' column in result_df to datetime64[ns] if it's not already, to match pandas default
    if 'Date' in result_df.columns and not pd.api.types.is_datetime64_any_dtype(result_df['Date']):
        result_df['Date'] = pd.to_datetime(result_df['Date'])

    # Sort by date to ensure order doesn't affect comparison
    result_df = result_df.sort_values(by='Date').reset_index(drop=True)
    expected_df_for_comparison = expected_df_for_comparison.sort_values(by='Date').reset_index(drop=True)


    # Compare DataFrames
    # pd.testing.assert_frame_equal is very strict.
    # Allow for minor float differences if necessary, though not expected here.
    pd.testing.assert_frame_equal(result_df, expected_df_for_comparison, check_dtype=False)
    # Note: check_dtype=False because DuckDB float types might be float64 while pandas might use float32 sometimes,
    # or integer types might differ slightly (e.g. int32 vs int64). For this test, value equality is key.

def test_fetcher_handles_empty_data(monkeypatch, tmp_path, capsys):
    """
    Tests that the fetcher service handles empty DataFrame from yf.download gracefully.
    """
    # 1. SETUP
    symbol = "EMPTYDATA"
    start_date = "2023-01-01"
    end_date = "2023-01-03"
    db_file = tmp_path / "test_empty_raw_data.db"

    empty_df = pd.DataFrame()

    def mock_download_empty(*args, **kwargs):
        return empty_df

    monkeypatch.setattr(yf, "download", mock_download_empty)

    # 2. EXECUTION
    fetch_and_store(symbol, start_date, end_date, str(db_file))

    # 3. VERIFICATION
    # Check that the DB file was NOT created (or if it was, it's empty and no table for the symbol)
    # The current fetcher_service.py code for empty df:
    # if df.empty:
    #     print(f"[Fetcher] No data returned for {symbol}. Exiting.")
    #     return  <-- It returns, so os.makedirs and db connection won't happen if dirname was not pre-existing
    # If db_path's directory doesn't exist, it won't be created.
    # If it does exist, the file itself won't be created by duckdb.connect if it's not called.

    assert not db_file.exists(), "DB file was created for empty data, but should not have been."

    captured = capsys.readouterr()
    assert f"[Fetcher] No data returned for {symbol}. Exiting." in captured.out

    # Also ensure no table was attempted to be created if the directory/db somehow pre-existed
    # (though tmp_path should prevent this)
    if db_file.exists(): # Should not happen based on assert above
        con = duckdb.connect(str(db_file))
        table_name = f"{symbol.replace('-', '_')}_raw"
        try:
            count = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]
            assert count == 0, f"Table {table_name} was created for empty data."
        finally:
            con.close()
