import pandas as pd
import io

class DataParser:
    """
    A class to parse raw byte content into a pandas DataFrame based on a given schema.
    Currently focuses on CSV-like data.
    """

    def parse(self, raw_content: bytes, schema: dict) -> pd.DataFrame | None:
        """
        Parses raw byte content into a pandas DataFrame.

        Args:
            raw_content (bytes): The raw byte string to parse.
            schema (dict): A dictionary defining parsing parameters like
                           'encoding', 'delimiter', and 'columns'.
                           Example: {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['col1', 'col2']}

        Returns:
            pd.DataFrame | None: A pandas DataFrame if parsing is successful, otherwise None.
        """
        if not raw_content:
            print("Warning: Raw content is empty. Nothing to parse.")
            return None

        encoding = schema.get('encoding', 'utf-8')
        delimiter = schema.get('delimiter', ',')
        columns_definition = schema.get('columns')
        column_names = None

        if isinstance(columns_definition, dict):
            column_names = list(columns_definition.keys())
        elif isinstance(columns_definition, list):
            # Could be a list of strings (names) or list of dicts (more complex, not currently supported by this parser directly for names)
            # For now, assume if it's a list, it's a list of names, or the validator will handle richer structures.
            # The original check was just for list of strings.
            column_names = columns_definition


        if not column_names or not isinstance(column_names, list) or len(column_names) == 0:
            print("Error: Schema must define a non-empty list of 'columns' (or a dictionary of column definitions).")
            return None

        try:
            # Decode the byte string to a text string
            decoded_content = raw_content.decode(encoding)

            # Use StringIO to treat the string as a file
            content_io = io.StringIO(decoded_content)

            # Read into pandas DataFrame
            df = pd.read_csv(
                content_io,
                delimiter=delimiter,
                names=column_names, # Use the extracted/validated list of names
                header=None,  # We are providing column names via 'names'
                skipinitialspace=True, # Handles spaces after delimiter
                index_col=False # Explicitly prevent first column from becoming index
            )
            # Handle potential skipping of rows (e.g., for keywords or headers in the data file itself)
            # This should ideally be driven by the schema.
            rows_to_skip = schema.get('csv_skip_rows', 0)
            if rows_to_skip > 0 and not df.empty and len(df) > rows_to_skip :
                df = df.iloc[rows_to_skip:].reset_index(drop=True)
            elif rows_to_skip > 0 : # df might be empty or have fewer rows than skip_rows
                # If we skip more rows than exist, return an empty DataFrame with correct columns
                df = pd.DataFrame(columns=column_names)

            # 結構化補完 (Structural Completion)
            # 從 schema_definition 中，獲取所有目標欄位的列表。
            # 注意：我們已經在前面從 schema 中獲取了 column_names
            # 使用 parsed_df = parsed_df.reindex(columns=target_columns, fill_value=None) 來強制補完 DataFrame 的欄位。
            if column_names: # 確保 column_names 存在且不為空
                df = df.reindex(columns=column_names, fill_value=None)

            return df

        except UnicodeDecodeError as ude:
            print(f"Error: Could not decode raw_content using encoding '{encoding}'. Original error: {ude}")
            raise # Re-raise the exception to be handled by the pipeline
        except pd.errors.ParserError as pe:
            print(f"Error: Failed to parse CSV content. Pandas ParserError: {pe}")
            raise # Re-raise the exception to be handled by the pipeline
        except Exception as e:
            # For other unexpected errors during parsing, still print and return None,
            # or decide if these should also be re-raised.
            # For now, keeping the original behavior for truly unexpected 'Exception'.
            # If ValueErrors from within pandas operations (not ParserError itself) are an issue,
            # they might be caught here or by the pipeline's ValueError handler.
            print(f"Error: An unexpected error occurred during parsing: {e}")
            return None

if __name__ == '__main__':
    parser = DataParser()

    print("--- Test Case 1: Standard UTF-8 CSV ---")
    schema1 = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['col1', 'col2', 'col3']}
    content1 = b"apple,banana,cherry\n1,2,3\nalpha,beta,gamma"
    df1 = parser.parse(content1, schema1)
    if df1 is not None:
        print("Parsed DataFrame:")
        print(df1)
        print(f"DataFrame types:\n{df1.dtypes}\n")

    print("\n--- Test Case 2: BIG5 Encoded, Pipe Delimited ---")
    schema2 = {'encoding': 'big5', 'delimiter': '|', 'columns': ['姓名', '年齡']}
    content2 = '張三|20\n李四|25'.encode('big5')
    df2 = parser.parse(content2, schema2)
    if df2 is not None:
        print("Parsed DataFrame:")
        print(df2)
        print(f"DataFrame types:\n{df2.dtypes}\n")

    print("\n--- Test Case 3: Schema with Missing 'columns' ---")
    schema3 = {'encoding': 'utf-8', 'delimiter': ','} # Missing 'columns'
    content3 = b"should,not,parse"
    df3 = parser.parse(content3, schema3)
    if df3 is None:
        print("Parsing correctly returned None due to missing columns.\n")

    print("\n--- Test Case 4: Empty Raw Content ---")
    schema4 = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['a', 'b']}
    content4 = b""
    df4 = parser.parse(content4, schema4)
    if df4 is None:
        print("Parsing correctly returned None for empty content.\n")

    print("\n--- Test Case 5: UnicodeDecodeError (BIG5 content, UTF-8 schema) ---")
    # Using a simple schema that expects one column
    schema5 = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['data']}
    content5 = '測試中文'.encode('big5') # BIG5 encoded content
    df5 = parser.parse(content5, schema5) # Attempting to parse as UTF-8
    if df5 is None:
        print("Parsing correctly returned None due to UnicodeDecodeError.\n")

    print("\n--- Test Case 6: Malformed CSV (mismatched delimiters, for ParserError) ---")
    schema6 = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['item', 'value']}
    content6 = b"item1,value1\nitem2;value2\nitem3,value3" # Semicolon instead of comma
    # Note: Pandas might be robust enough to handle some errors.
    # A more direct way to cause ParserError might involve complex quoting issues or binary data.
    # This example *might* not always raise ParserError depending on pandas version and internal heuristics.
    # For a more guaranteed ParserError, one might need more complex malformed data.
    df6 = parser.parse(content6, schema6)
    if df6 is None: # Or if it has issues
        print("Parsing returned None or problematic DataFrame due to malformed CSV (ParserError expected).\n")
    else:
        print("Parsed DataFrame (Pandas might have handled this gracefully):")
        print(df6)
        print("Inspect if the parsing was as expected given the mixed delimiters.\n")

    print("\n--- Test Case 7: Schema with no delimiter (defaults to ',') ---")
    schema7 = {'encoding': 'utf-8', 'columns': ['colA', 'colB']} # Delimiter defaults to comma
    content7 = b"valueA,valueB\nvalueC,valueD"
    df7 = parser.parse(content7, schema7)
    if df7 is not None:
        print("Parsed DataFrame with default delimiter:")
        print(df7)
        print(f"DataFrame types:\n{df7.dtypes}\n")

    print("\n--- Test Case 8: Schema with no encoding (defaults to 'utf-8') ---")
    schema8 = {'delimiter': ';', 'columns': ['key', 'val']} # Encoding defaults to utf-8
    content8 = b"key1;val1\nkey2;val2"
    df8 = parser.parse(content8, schema8)
    if df8 is not None:
        print("Parsed DataFrame with default encoding:")
        print(df8)
        print(f"DataFrame types:\n{df8.dtypes}\n")
