import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np # For np.nan
from src.sp_data_v16.transformation.parser import DataParser

@pytest.fixture(scope="module") # module scope is fine as DataParser is stateless
def data_parser():
    """
    Pytest fixture to provide an instance of DataParser.
    """
    return DataParser()

def test_parse_utf8_csv_success(data_parser):
    """
    Tests successful parsing of a standard UTF-8 encoded CSV.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    raw_content = b"val1,val2\nval3,val4"
    expected_df = pd.DataFrame([['val1', 'val2'], ['val3', 'val4']], columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_big5_piped_success(data_parser):
    """
    Tests successful parsing of BIG5 encoded, pipe-delimited data.
    """
    schema = {'encoding': 'big5', 'delimiter': '|', 'columns': ['名稱', '值']}
    raw_content = "測試1|值1\n測試2|值2".encode('big5')
    expected_df = pd.DataFrame([['測試1', '值1'], ['測試2', '值2']], columns=['名稱', '值'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_default_encoding_delimiter_success(data_parser):
    """
    Tests parsing using default encoding (utf-8) and delimiter (,).
    """
    schema = {'columns': ['X', 'Y']} # Encoding and delimiter not specified
    raw_content = b"default1,default2\ndefault3,default4"
    expected_df = pd.DataFrame([['default1', 'default2'], ['default3', 'default4']], columns=['X', 'Y'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_missing_columns_in_schema(data_parser):
    """
    Tests that parsing fails (returns None) if 'columns' key is missing in schema.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ','} # 'columns' is missing
    raw_content = b"a,b"

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_empty_columns_in_schema(data_parser):
    """
    Tests that parsing fails (returns None) if 'columns' list is empty in schema.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': []} # 'columns' is empty
    raw_content = b"a,b"

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_malformed_csv_pandas_error(data_parser):
    """
    Tests that parsing malformed CSV content returns None.
    DataParser should catch pd.errors.ParserError.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    # Malformed CSV: unclosed quote causing CParserError
    raw_content = b'val1,val2\n"val3,val4'

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_unicode_decode_error(data_parser):
    """
    Tests that parsing content with encoding different from schema returns None.
    (e.g., BIG5 content with a UTF-8 schema).
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A']}
    raw_content = "測試".encode('big5') # BIG5 encoded content

    result_df = data_parser.parse(raw_content, schema) # Schema expects UTF-8

    assert result_df is None

def test_parse_empty_raw_content(data_parser):
    """
    Tests that parsing empty raw content returns None.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A']}
    raw_content = b""

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_different_delimiter_than_data(data_parser):
    """
    Tests parsing when the schema's delimiter does not match the actual data delimiter.
    Pandas read_csv behavior: if the specified delimiter isn't found, it often reads
    each line as a single field into the first column specified in 'names'.
    Subsequent columns defined in 'names' will be filled with NaN.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ';', 'columns': ['A', 'B']}
    raw_content = b"val1,val2\nval3,val4" # Data is comma-separated

    # Expected behavior: 'val1,val2' becomes the value for column 'A', column 'B' is NaN
    # Similarly for the second row.
    expected_data = {
        'A': ['val1,val2', 'val3,val4'],
        'B': [np.nan, np.nan] # Use np.nan for NaNs
    }
    expected_df = pd.DataFrame(expected_data, columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    # When comparing DataFrames with NaNs, dtypes can be tricky.
    # assert_frame_equal is generally good but might need check_dtype=False
    # or ensuring expected_df has precisely matching dtypes for NaN columns (often 'object' or specific float).
    assert_frame_equal(result_df, expected_df, check_dtype=True) # Be strict initially, relax if needed due to NaN typing

def test_parse_skipinitialspace(data_parser):
    """
    Tests that skipinitialspace=True works as expected (handled by DataParser).
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    raw_content = b"val1, val2\nval3,val4 " # Note spaces around val2 and val4
    # Expected: "val2" (space trimmed) and "val4 " (trailing space may remain, depending on exact pandas handling for EOL)
    # pd.read_csv with skipinitialspace primarily affects space *after* delimiter.
    # Trailing spaces on the line for the last column are usually preserved unless other options like strip_ws are used.
    # The DataParser doesn't use strip_ws.
    expected_df = pd.DataFrame([['val1', 'val2'], ['val3', 'val4 ']], columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

@pytest.mark.filterwarnings("ignore:Length of header or names does not match length of data")
def test_parse_more_columns_in_data_than_schema(data_parser):
    """
    Tests parsing when data has more columns than defined in schema.
    Pandas should only read up to the number of columns specified in 'names'.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    raw_content = b"val1,val2,extra_col_data\nval3,val4,more_extra"
    expected_df = pd.DataFrame([['val1', 'val2'], ['val3', 'val4']], columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_fewer_columns_in_data_than_schema(data_parser):
    """
    Tests parsing when data has fewer columns than defined in schema.
    Pandas should fill the missing columns with NaN.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B', 'C']}
    raw_content = b"val1,val2\nval3" # Second row only has one value

    expected_data = {
        'A': ['val1', 'val3'],
        'B': ['val2', np.nan],
        'C': [np.nan, np.nan]
    }
    expected_df = pd.DataFrame(expected_data, columns=['A', 'B', 'C'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)
