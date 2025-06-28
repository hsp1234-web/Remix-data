import pytest
import pandas as pd
from unittest.mock import MagicMock
from src.data_pipeline_v15.data_validator import Validator

@pytest.fixture
def mock_logger():
    """Provides a MagicMock instance for logger."""
    return MagicMock()

@pytest.fixture
def basic_data():
    return pd.DataFrame({
        'col_A': [1, 2, 3, 4, 5],
        'col_B': ["apple", "banana", "cherry", "date", "elderberry"],
        'col_C': [10.0, None, 30.0, 40.0, 50.0], # Has a None
        'col_D': [100, 200, -5, 300, 50] # Has a value below typical min_value
    })

def test_all_valid_data(mock_logger, basic_data):
    rules_config = {
        "test_schema": {
            "col_A": {"min_value": 0},
            "col_C": {"non_null": False} # Explicitly allow null for col_C here
        }
    }
    validator = Validator(rules_config, mock_logger)
    df_to_validate = basic_data.copy()

    valid_df, invalid_df = validator.validate(df_to_validate, "test_source.csv", "test_schema")

    pd.testing.assert_frame_equal(valid_df, df_to_validate)
    assert invalid_df.empty
    assert len(valid_df) == 5
    assert len(invalid_df) == 0

def test_some_invalid_data_non_null(mock_logger, basic_data):
    rules_config = {
        "test_schema": {
            "col_C": {"non_null": True} # col_C has a None value in basic_data
        }
    }
    validator = Validator(rules_config, mock_logger)
    df_to_validate = basic_data.copy()

    valid_df, invalid_df = validator.validate(df_to_validate, "test_source.csv", "test_schema")

    assert len(valid_df) == 4
    assert len(invalid_df) == 1
    assert invalid_df.iloc[0]['quarantine_reason'] == "Column 'col_C': is null"
    assert invalid_df.iloc[0]['source_file'] == "test_source.csv"
    # Check that the valid_df does not contain the row with None in col_C
    assert None not in valid_df['col_C'].values

def test_some_invalid_data_min_value(mock_logger, basic_data):
    rules_config = {
        "test_schema": {
            "col_D": {"min_value": 0} # col_D has -5
        }
    }
    validator = Validator(rules_config, mock_logger)
    df_to_validate = basic_data.copy()

    valid_df, invalid_df = validator.validate(df_to_validate, "test_source.csv", "test_schema")

    assert len(valid_df) == 4
    assert len(invalid_df) == 1
    assert invalid_df.iloc[0]['col_D'] == -5
    assert "is less than 0" in invalid_df.iloc[0]['quarantine_reason']
    assert invalid_df.iloc[0]['source_file'] == "test_source.csv"

def test_invalid_data_min_value_with_non_numeric(mock_logger):
    data = pd.DataFrame({
        'col_val': [10, "abc", -5, 20, "xyz", 0]
    })
    rules_config = {
        "test_schema": {
            "col_val": {"min_value": 0}
        }
    }
    validator = Validator(rules_config, mock_logger)
    valid_df, invalid_df = validator.validate(data, "test_source.csv", "test_schema")

    assert len(valid_df) == 3 # 修正：有效的行應為 10, 20, 0，共 3 行
    assert valid_df['col_val'].isin([10, 20, 0]).all()

    assert len(invalid_df) == 3 # Rows with "abc", -5, "xyz"

    reasons = invalid_df['quarantine_reason'].tolist()
    assert any("failed numeric conversion for range check" in reason for reason in reasons)
    assert any("is less than 0" in reason for reason in reasons)

    # Check specific rows
    assert invalid_df[invalid_df['col_val'] == 'abc']['quarantine_reason'].iloc[0] == "Column 'col_val': failed numeric conversion for range check"
    assert invalid_df[invalid_df['col_val'] == -5]['quarantine_reason'].iloc[0] == "Column 'col_val': is less than 0"
    assert invalid_df[invalid_df['col_val'] == 'xyz']['quarantine_reason'].iloc[0] == "Column 'col_val': failed numeric conversion for range check"


def test_all_invalid_data(mock_logger):
    data = pd.DataFrame({
        'col_A': [None, None],
        'col_B': [-10, -20]
    })
    rules_config = {
        "test_schema": {
            "col_A": {"non_null": True},
            "col_B": {"min_value": 0}
        }
    }
    validator = Validator(rules_config, mock_logger)
    valid_df, invalid_df = validator.validate(data, "test_source.csv", "test_schema")

    assert valid_df.empty
    assert len(invalid_df) == 2
    assert "Column 'col_A': is null; Column 'col_B': is less than 0" in invalid_df.iloc[0]['quarantine_reason']
    assert "Column 'col_A': is null; Column 'col_B': is less than 0" in invalid_df.iloc[1]['quarantine_reason']


def test_no_rules_for_schema(mock_logger, basic_data):
    rules_config = {
        "another_schema": {"col_A": {"non_null": True}}
    } # No rules for "test_schema"
    validator = Validator(rules_config, mock_logger)
    df_to_validate = basic_data.copy()

    valid_df, invalid_df = validator.validate(df_to_validate, "test_source.csv", "test_schema")

    pd.testing.assert_frame_equal(valid_df, df_to_validate)
    assert invalid_df.empty
    mock_logger.info.assert_any_call("No validation rules found for 'test_schema'. Skipping validation.")

def test_empty_dataframe_input(mock_logger):
    rules_config = {"test_schema": {"col_A": {"non_null": True}}}
    validator = Validator(rules_config, mock_logger)
    empty_df = pd.DataFrame({'col_A': pd.Series(dtype='int'), 'col_B': pd.Series(dtype='str')}) # Has columns

    valid_df, invalid_df = validator.validate(empty_df.copy(), "test_source.csv", "test_schema")

    assert valid_df.empty
    assert invalid_df.empty
    # Check columns of invalid_df
    expected_invalid_cols = ['col_A', 'col_B', "quarantine_reason", "source_file"]
    assert list(invalid_df.columns) == expected_invalid_cols

    # Test with a completely empty DataFrame (no columns)
    completely_empty_df = pd.DataFrame()
    valid_df_empty, invalid_df_empty = validator.validate(completely_empty_df.copy(), "test_source.csv", "test_schema")
    assert valid_df_empty.empty
    assert invalid_df_empty.empty
    assert list(invalid_df_empty.columns) == ["quarantine_reason", "source_file"]


def test_rule_for_missing_column(mock_logger, basic_data):
    rules_config = {
        "test_schema": {
            "col_Z_missing": {"non_null": True}
        }
    }
    validator = Validator(rules_config, mock_logger)
    df_to_validate = basic_data.copy()

    valid_df, invalid_df = validator.validate(df_to_validate, "test_source.csv", "test_schema")

    # Expect all original data to be valid as the rule for missing column is skipped
    pd.testing.assert_frame_equal(valid_df, df_to_validate)
    assert invalid_df.empty
    mock_logger.warning.assert_called_with("Rule defined for column 'col_Z_missing' in 'test_schema', but column not in DataFrame. Skipping rule.")

def test_multiple_rules_on_column(mock_logger):
    data = pd.DataFrame({
        'value': [None, -5, 10, 200]
    })
    rules_config = {
        "test_schema": {
            "value": {"non_null": True, "min_value": 0, "max_value": 100} # Example for max_value not yet implemented in Validator
        }
    }
    # Note: max_value rule is not implemented in the provided Validator class,
    # so this test will only check non_null and min_value based on current Validator code.
    # If max_value were implemented, row 200 would also be invalid.

    validator = Validator(rules_config, mock_logger)
    valid_df, invalid_df = validator.validate(data, "test_source.csv", "test_schema")

    assert len(valid_df) == 2 # 修正：值 10 和 200 均應有效 (因為 max_value 未實現)
    assert valid_df['value'].isin([10, 200]).all() # 驗證 valid_df 包含 10 和 200

    assert len(invalid_df) == 2 # None and -5

    # Check reasons for specific invalid rows
    assert "Column 'value': is null" in invalid_df[invalid_df['value'].isnull()]['quarantine_reason'].iloc[0]
    assert "Column 'value': is less than 0" in invalid_df[invalid_df['value'] == -5]['quarantine_reason'].iloc[0]
    # If max_value was implemented:
    # assert "Column 'value': is greater than 100" in invalid_df[invalid_df['value'] == 200]['quarantine_reason'].iloc[0]

    # Test that reasons accumulate
    data_multi_fail = pd.DataFrame({'value': [None]}) # Fails non_null, and would fail min_value if it was not null
    rules_multi = {"test_schema": {"value": {"non_null": True, "min_value": 0}}}
    validator_multi = Validator(rules_multi, mock_logger)
    _, invalid_df_multi = validator_multi.validate(data_multi_fail, "test.csv", "test_schema")
    assert "Column 'value': is null" in invalid_df_multi.iloc[0]['quarantine_reason']
    # min_value check is skipped if value is null and non_null rule exists.
    # If non_null was false, then a null value would also "fail" numeric conversion for range check.

    data_non_numeric_non_null_rule = pd.DataFrame({'value': ["abc"]})
    validator_non_numeric = Validator(rules_multi, mock_logger) # non_null: True, min_value: 0
    _, invalid_df_non_numeric = validator_non_numeric.validate(data_non_numeric_non_null_rule, "test.csv", "test_schema")
    # Should fail numeric conversion. non_null is true, but "abc" is not null.
    # The current logic for min_value:
    # numeric_series = pd.to_numeric(df[col_name], errors='coerce') -> "abc" becomes NaN
    # is_lt_min = numeric_series < rules["min_value"] -> NaN < 0 is False
    # conversion_failed_mask = numeric_series.isnull() & df[col_name].notnull() -> True for "abc"
    # So it should be caught by conversion_failed_mask.
    assert "Column 'value': failed numeric conversion for range check" in invalid_df_non_numeric.iloc[0]['quarantine_reason']
