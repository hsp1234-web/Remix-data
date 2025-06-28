# tests/test_processor_service.py
import pytest
import pandas as pd
import numpy as np # Import numpy
import duckdb
from services.processor_service import process_and_store
import os

@pytest.fixture
def mock_raw_db_path(tmp_path):
    """Create a mock raw DB in a temporary directory for testing."""
    db_path = tmp_path / "mock_raw.db"
    con = duckdb.connect(str(db_path))

    # Create some sample data
    data = {
        'Open': [100, 102, 101, 103, 105] * 10, # 50 days
        'High': [103, 104, 103, 105, 106] * 10,
        'Low': [99, 101, 100, 102, 104] * 10,
        'Close': [102, 103, 102, 104, 105] * 10,
        'Volume': [1000, 1100, 1200, 1300, 1400] * 10
    }
    # Add a 'Date' column as yfinance data typically includes it and it might be implicitly expected
    # by processor or later stages, even if not directly used in MA calculation.
    # Using a simple date range for the mock data.
    dates = pd.to_datetime("2023-01-01") + pd.to_timedelta(range(50), unit='D')
    df = pd.DataFrame(data, index=pd.Index(dates, name="Date"))

    con.execute("CREATE TABLE SPY_raw AS SELECT * FROM df")
    con.close()
    return str(db_path)

def test_processor_logic(mock_raw_db_path, tmp_path):
    """
    Test the processor service in a completely offline manner.
    """
    # 1. SETUP
    mock_feature_db_path = tmp_path / "mock_features.db"
    symbol = "SPY"

    # 2. EXECUTION
    # Directly call the function to test its logic
    process_and_store(mock_raw_db_path, str(mock_feature_db_path), symbol)

    # 3. VERIFICATION
    con = duckdb.connect(str(mock_feature_db_path))
    result_df = con.execute("SELECT * FROM SPY_features").fetchdf()
    con.close()

    # Check if MA20 column exists
    assert 'MA20' in result_df.columns
    # Check if NaN values from rolling window are dropped
    assert not result_df.isnull().values.any()
    # Check if the output has the correct number of rows after dropping NaNs
    # 50 rows input, MA60 needs 59 rows to warm up (window-1), so 50 - 59 is negative.
    # The processor uses dropna(), so the effective number of rows will be 50 - (60 - 1) = -9,
    # which means the DataFrame should be empty if MA60 is calculated.
    # However, the draft calculates MA20 and MA60. The largest window is 60.
    # So, 50 rows - (60 - 1) = -9. This implies the logic in the draft for dropna
    # with these window sizes on 50 data points will result in an empty dataframe.
    # Let's adjust the assertion based on the largest window (MA60).
    # df.dropna() will remove rows where ANY of the MA calculations result in NaN.
    # MA60 requires 59 prior data points. With 50 data points, all MA60 values will be NaN.
    # Thus, df.dropna() will remove all rows.

    # Re-evaluating the expected number of rows:
    # Input: 50 rows.
    # MA20 needs 19 prior rows. First valid MA20 is at index 19.
    # MA60 needs 59 prior rows. First valid MA60 is at index 59.
    # Since we only have 50 rows, ALL calculations for MA60 will be NaN.
    # df.dropna() will remove any row that has a NaN in *any* column.
    # Therefore, all rows will be dropped.
    assert len(result_df) == 0
    # This seems counter-intuitive for a test, perhaps the sample data or window size should be adjusted
    # in the test, or the processor logic itself for handling insufficient data.
    # For now, I will stick to what the current processor_service.py draft implies.
    # If the intention was to have some data remaining, the input data needs to be longer than 59 rows.

    # Let's create a new test case with enough data for MA60 to be non-NaN for some rows.
    # We'll need at least 60 data points. Let's use 70.

@pytest.fixture
def mock_raw_db_path_sufficient_data(tmp_path):
    """Create a mock raw DB with enough data for MA60 calculation."""
    db_path = tmp_path / "mock_raw_sufficient.db"
    con = duckdb.connect(str(db_path))

    num_days = 70
    data = {
        'Open': [100 + i*0.1 for i in range(num_days)],
        'High': [102 + i*0.1 for i in range(num_days)],
        'Low': [99 + i*0.1 for i in range(num_days)],
        'Close': [101 + i*0.1 for i in range(num_days)],
        'Volume': [1000 + i*10 for i in range(num_days)]
    }
    dates = pd.to_datetime("2023-01-01") + pd.to_timedelta(range(num_days), unit='D')
    df = pd.DataFrame(data, index=pd.Index(dates, name="Date"))

    con.execute("CREATE TABLE TESTausreichend_raw AS SELECT * FROM df") # Using a different symbol to avoid clashes
    con.close()
    return str(db_path)

def test_processor_logic_sufficient_data(mock_raw_db_path_sufficient_data, tmp_path):
    """Test processor with enough data for MA calculations."""
    mock_feature_db_path = tmp_path / "mock_features_sufficient.db"
    symbol = "TESTausreichend" # German for "sufficient"

    process_and_store(mock_raw_db_path_sufficient_data, str(mock_feature_db_path), symbol)

    con = duckdb.connect(str(mock_feature_db_path))
    # Table name uses symbol.replace('-', '_') - our symbol has no hyphen.
    result_df = con.execute(f"SELECT * FROM {symbol}_features").fetchdf()
    con.close()

    assert 'MA20' in result_df.columns
    assert 'MA60' in result_df.columns
    assert not result_df.isnull().values.any()
    # With 70 data points, MA60 is calculated from row index 59 onwards.
    # So, rows 0 to 58 will have NaN for MA60.
    # dropna() will keep rows from index 59.
    # Expected number of rows = 70 (total) - 59 (warm-up for MA60) = 11 rows.
    assert len(result_df) == 70 - (60 - 1)
    assert len(result_df) == 11

    # Verify some MA values if possible (optional, but good for robustness)
    # For simplicity, we'll just check the number of rows and non-NaN.
    # The first 'Close' value for the remaining 11 rows will be at original index 59.
    # Original Close values: 101 + i*0.1
    # Close at index 59: 101 + 59*0.1 = 101 + 5.9 = 106.9
    # Close at index 19 (for MA20): 101 + 19*0.1 = 102.9

    # Let's check the first valid MA20 value in the result_df
    # The first row of result_df corresponds to original index 59
    # MA20 for original index 59 is the average of Close from index 40 to 59
    original_closes = pd.Series([101 + i*0.1 for i in range(70)])
    expected_ma20_at_orig_idx_59 = original_closes.iloc[40:60].mean()
    assert np.isclose(result_df['MA20'].iloc[0], expected_ma20_at_orig_idx_59)

    expected_ma60_at_orig_idx_59 = original_closes.iloc[0:60].mean()
    assert np.isclose(result_df['MA60'].iloc[0], expected_ma60_at_orig_idx_59)

# Note: The original test_processor_logic still uses 50 data points,
# and correctly asserts that 0 rows will remain. This is important to test edge cases.
# The new test_processor_logic_sufficient_data ensures the logic works when data is produced.
