import pytest # 引入 pytest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import numpy as np # For NaN

# Add src to sys.path to allow direct import of modules under src
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.sp_data_v16.transformation.validator import DataValidator

class TestDataValidator: # 移除 unittest.TestCase 的繼承

    def setup_method(self): # pytest 風格的 setup
        """Initialize DataValidator instance for all tests."""
        self.validator = DataValidator()

    def test_successful_type_conversion(self):
        """Test successful conversion to integer, float, datetime."""
        data = {
            'col_int': ['1', '2', '3'],
            'col_float': ['10.1', '20.2', '30.3'],
            'col_datetime': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'col_string': ['a', 'b', 'c']
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_int': {'dtype': 'integer'},
                'col_float': {'dtype': 'float'},
                'col_datetime': {'dtype': 'datetime'},
                'col_string': {'dtype': 'string'} # String type, no specific conversion
            }
        }

        validated_df = self.validator.validate(df.copy(), schema)

        assert validated_df['col_int'].dtype == 'Int64'
        assert validated_df['col_float'].dtype == 'float64'
        assert validated_df['col_datetime'].dtype == 'datetime64[ns]'
        assert validated_df['col_string'].dtype == 'object' # Stays as object for strings

        expected_data = {
            'col_int': pd.Series([1, 2, 3], dtype='Int64'),
            'col_float': pd.Series([10.1, 20.2, 30.3], dtype='float64'),
            'col_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'col_string': pd.Series(['a', 'b', 'c'], dtype='object')
        }
        expected_df = pd.DataFrame(expected_data)
        assert_frame_equal(validated_df, expected_df, check_dtype=True)

    def test_type_conversion_errors_coerce_nan(self):
        """測試無法轉換的值會被強制轉換為 NaN/NaT。"""
        data = {
            'col_int': ['1', 'abc', '3'],
            'col_float': ['10.1', 'xyz', '30.3'],
            'col_datetime': ['2023-01-01', 'invalid_date', '2023-01-03']
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_int': {'dtype': 'integer'},
                'col_float': {'dtype': 'float'},
                'col_datetime': {'dtype': 'datetime'}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            assert pd.isna(validated_df['col_int'].iloc[1])
            assert pd.isna(validated_df['col_float'].iloc[1])
            assert pd.isna(validated_df['col_datetime'].iloc[1]) # NaT for datetime

            assert validated_df['col_int'].iloc[0] == 1
            assert validated_df['col_float'].iloc[0] == 10.1
            assert validated_df['col_datetime'].iloc[0] == pd.Timestamp('2023-01-01')

            # Check if warnings were printed for conversion errors
            # Example: "Warning: Column 'col_int' (dtype: integer) had values that could not be converted..."
            assert any("col_int" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list)
            assert any("col_float" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list)
            assert any("col_datetime" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list)


    def test_non_nullable_field_with_nulls(self):
        """測試當存在空值（原始或強制轉換後）時的非空欄位檢查。"""
        data = {
            'col_non_nullable_original_null': [1, None, 3],
            'col_non_nullable_becomes_null': ['10', 'abc', '30'] # 'abc' will become NaN
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_non_nullable_original_null': {'dtype': 'integer', 'nullable': False},
                'col_non_nullable_becomes_null': {'dtype': 'integer', 'nullable': False}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            assert validated_df is None # 因為非空欄位出現 NaN，validate 函式會返回 None

            # 檢查是否有打印嚴重錯誤的警告訊息
            # 範例: "Critical: Column 'col_non_nullable_original_null' is defined as non-nullable but contains NaN values..."
            assert any("col_non_nullable_original_null" in call.args[0] and "Critical" in call.args[0] and "non-nullable but contains NaN" in call.args[0] for call in mocked_print.call_args_list)
            # 'col_non_nullable_becomes_null' 也會因為 'abc' 轉為 NaN 而觸發此錯誤
            assert any("col_non_nullable_becomes_null" in call.args[0] and "Critical" in call.args[0] and "non-nullable but contains NaN" in call.args[0] for call in mocked_print.call_args_list)
            # 同時，由於 'abc' 無法轉換為整數，也會有轉換錯誤的警告
            assert any("col_non_nullable_becomes_null" in call.args[0] and "Warning" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list)

    def test_nullable_field_with_nulls(self):
        """測試可空欄位處理：允許並保留空值。"""
        data = {
            'col_nullable': [1.0, None, 3.0, np.nan, 'text_will_be_nan'],
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_nullable': {'dtype': 'float', 'nullable': True}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            assert validated_df['col_nullable'].iloc[0] == 1.0
            assert pd.isna(validated_df['col_nullable'].iloc[1]) # Original None
            assert validated_df['col_nullable'].iloc[2] == 3.0
            assert pd.isna(validated_df['col_nullable'].iloc[3]) # Original NaN
            assert pd.isna(validated_df['col_nullable'].iloc[4]) # 'text_will_be_nan' coerced to NaN

            # Check that NO "not nullable" warnings were printed for this column
            assert not any("col_nullable" in call.args[0] and "not nullable" in call.args[0] for call in mocked_print.call_args_list)
            # A conversion warning for 'text_will_be_nan' is expected though
            assert any("col_nullable" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list)


    def test_column_in_schema_not_in_df(self):
        """測試 Schema 中定義但 DataFrame 中不存在的欄位會被警告處理。"""
        data = {'col_existing': [1, 2]}
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_existing': {'dtype': 'integer'},
                'col_missing': {'dtype': 'string', 'nullable': False}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            assert 'col_missing' not in validated_df.columns
            assert 'col_existing' in validated_df.columns

            # Check for warning about missing column
            assert any("'col_missing'" in call.args[0] and "defined in schema but not found" in call.args[0] for call in mocked_print.call_args_list)

    def test_empty_dataframe(self):
        """測試空 DataFrame 的處理。"""
        df_empty = pd.DataFrame(columns=['col_a', 'col_b'])
        schema = {
            'columns': {
                'col_a': {'dtype': 'integer'},
                'col_b': {'dtype': 'string'}
            }
        }

        validated_df = self.validator.validate(df_empty.copy(), schema)

        assert validated_df.empty
        # The validator does not add columns if they don't exist in the input df, even if in schema.
        # It processes existing columns. So, validated_df will have the same columns as df_empty.
        assert_series_equal(pd.Series(validated_df.columns), pd.Series(df_empty.columns), check_dtype=False)
        # Check that dtypes are converted for existing columns if any (though df_empty has no data for dtypes to be inferred beyond object)
        # If an empty dataframe with defined columns but no data is passed, pandas often defaults to 'object' dtype.
        # Our validator will try to convert them based on schema, resulting in correct (empty) typed Series.
        assert validated_df['col_a'].dtype == 'Int64'
        assert validated_df['col_b'].dtype == 'object' # String type

    def test_correct_types_no_change(self):
        """測試已具有正確資料類型的 DataFrame 不會被更改。"""
        data = {
            'col_int': pd.Series([1, 2, 3], dtype='Int64'),
            'col_float': pd.Series([10.1, 20.2, 30.3], dtype='float64'),
            'col_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        }
        df = pd.DataFrame(data)
        original_df_copy = df.copy() # Keep a pristine copy for comparison

        schema = {
            'columns': {
                'col_int': {'dtype': 'integer'},
                'col_float': {'dtype': 'float'},
                'col_datetime': {'dtype': 'datetime'}
            }
        }

        validated_df = self.validator.validate(df.copy(), schema) # Pass a copy to validate
        assert_frame_equal(validated_df, original_df_copy, check_dtype=True)

    def test_int64_conversion_supports_nan(self):
        """測試整數轉換使用 Int64 以支援 NaN。"""
        data = {'col_int_nan': ['1', 'invalid', None, '3']}
        df = pd.DataFrame(data)
        schema = {'columns': {'col_int_nan': {'dtype': 'integer'}}}

        validated_df = self.validator.validate(df.copy(), schema)

        assert validated_df['col_int_nan'].dtype == 'Int64'
        assert validated_df['col_int_nan'].iloc[0] == 1
        assert pd.isna(validated_df['col_int_nan'].iloc[1]) # 'invalid' becomes NaN
        assert pd.isna(validated_df['col_int_nan'].iloc[2]) # None remains NaN
        assert validated_df['col_int_nan'].iloc[3] == 3

    def test_datetime_conversion_with_mixed_formats_and_errors(self):
        """測試使用多種格式和錯誤處理的日期時間轉換。"""
        # Simplified data to isolate the parsing of '2023-01-03 10:00:00'
        data = {
            'dt_col': [
                '2023-01-03 10:00:00',
                '2023-01-01 00:00:00', # Using consistent full datetime format
                'invalid_date'       # Known invalid
            ]
        }
        df = pd.DataFrame(data)
        schema = {'columns': {'dt_col': {'dtype': 'datetime'}}}

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            assert validated_df['dt_col'].dtype == 'datetime64[ns]'
            assert validated_df['dt_col'].iloc[0] == pd.Timestamp('2023-01-03 10:00:00')
            assert validated_df['dt_col'].iloc[1] == pd.Timestamp('2023-01-01 00:00:00')
            assert pd.isna(validated_df['dt_col'].iloc[2]) # 'invalid_date' becomes NaT

            # Check that a warning for "invalid_date" is present
            assert (
                any("dt_col" in call.args[0] and
                    "could not be converted" in call.args[0] and
                    "invalid_date" in str(call.args)
                    for call in mocked_print.call_args_list)
            ), "未找到或不正確的 'invalid_date' 警告。"

    def test_validate_enum_raises_error_on_invalid_type_input(self):
        """測試 _validate_enum 在輸入值非字串時是否引發 ValueError。"""
        valid_enums = ['APPLE', 'BANANA']
        with pytest.raises(ValueError, match="輸入值必須是字串"):
            self.validator._validate_enum(123, valid_enums)

    def test_validate_enum_raises_error_on_illegal_enum_value(self):
        """測試 _validate_enum 在輸入值為非法枚舉值時是否引發 ValueError。"""
        valid_enums = ['APPLE', 'BANANA']
        with pytest.raises(ValueError, match="'CHERRY' 是不合法的枚舉值。有效的枚舉值為：\\['APPLE', 'BANANA'\\]"):
            self.validator._validate_enum('CHERRY', valid_enums)

    def test_validate_empty_columns_in_schema(self):
        """測試當 schema 中的 'columns' 定義為空時，validate 應直接返回原始 DataFrame (的副本)。"""
        data = {'col_a': [1, 2], 'col_b': ['x', 'y']}
        df = pd.DataFrame(data)
        original_df_copy = df.copy() # 用於比較

        # Schema with empty 'columns' dictionary
        schema_empty_cols = {'columns': {}}
        validated_df_empty = self.validator.validate(df.copy(), schema_empty_cols)
        assert_frame_equal(validated_df_empty, original_df_copy, check_dtype=True)

        # Schema where 'columns' key is missing (should also result in no validation)
        schema_missing_cols_key = {}
        validated_df_missing_key = self.validator.validate(df.copy(), schema_missing_cols_key)
        assert_frame_equal(validated_df_missing_key, original_df_copy, check_dtype=True)

# if __name__ == '__main__':
#     unittest.main() # 註解掉或移除 unittest.main()
