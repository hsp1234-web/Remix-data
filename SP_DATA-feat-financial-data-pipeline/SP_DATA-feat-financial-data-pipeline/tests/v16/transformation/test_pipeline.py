import pytest
import pandas as pd
import duckdb
import yaml
import json
import pathlib
from src.sp_data_v16.transformation.pipeline import TransformationPipeline

@pytest.fixture(scope="function")
def transformation_pipeline_env(tmp_path):
    """
    Sets up a temporary environment for TransformationPipeline integration tests.
    Includes dummy config files, schema files, and databases with pre-populated data.
    """
    tmp_data_path = tmp_path / "data"
    tmp_config_path_dir = tmp_path / "config"
    tmp_data_path.mkdir()
    tmp_config_path_dir.mkdir()

    config_file_path = tmp_config_path_dir / "test_config_v16.yaml"
    schema_file_path = tmp_config_path_dir / "test_schemas.json"

    manifest_db_path = tmp_data_path / "manifest.db"
    raw_lake_db_path = tmp_data_path / "raw_lake.db"
    processed_db_path = tmp_data_path / "processed.db"

    # Create test_schemas.json
    schemas_content = {
        "csv_valid_data": {
            "table_name": "valid_data_table",
            "keywords": ["valid_data_keywords"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1, # Skip keyword line
            "unique_key": ["id"], # 加入 unique_key
            "columns": {
                "id": {"dtype": "integer", "nullable": False, "db_type": "INTEGER"},
                "name": {"dtype": "string", "nullable": True, "db_type": "VARCHAR"},
                "value": {"dtype": "float", "nullable": True, "db_type": "DOUBLE"}
            }
        },
        "csv_retry_succeeds_schema": { # New schema for retry_succeeds test
            "table_name": "retry_succeeds_table",
            "keywords": ["retry_succeeds_keywords"], # Unique keywords
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1,
            "unique_key": ["id"],
            "columns": { # Assuming same column structure as csv_valid_data for simplicity
                "id": {"dtype": "integer", "nullable": False, "db_type": "INTEGER"},
                "name": {"dtype": "string", "nullable": True, "db_type": "VARCHAR"},
                "value": {"dtype": "float", "nullable": True, "db_type": "DOUBLE"}
            }
        },
        "csv_generic_schema": { # New schema for fatal error test
            "table_name": "generic_table_for_fatal_error",
            "keywords": ["fatal_error_keywords"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1,
            "columns": {
                "data_col1": {"dtype": "string"},
                "data_col2": {"dtype": "integer"}
            }
        },
        "csv_validation_error": {
            "table_name": "validation_error_table",
            "keywords": ["validation_error_keywords"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1,
            "columns": {
                "id": {"dtype": "integer", "nullable": False}, # This field will cause validation error
                "description": {"dtype": "string", "nullable": True}
            }
        },
         "csv_parser_error_schema": { # Schema for the file that will cause a CParserError
            "table_name": "parser_error_table",
            "keywords": ["parser_error_keywords"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1,
            "columns": {
                "colA": {"dtype": "string"},
                "colB": {"dtype": "string"}
            }
        },
        "csv_bad_encoding_schema": { # Schema expects utf-8, data will be big5
            "table_name": "bad_encoding_table",
            "keywords": ["bad_encoding_keywords"],
            "file_type": "csv",
            "encoding": "utf-8", # Schema expects utf-8
            "delimiter": ",",
            "csv_skip_rows": 1,
            "columns": {"key": {"dtype":"string"}, "data":{"dtype":"string"}}
        }
    }
    with open(schema_file_path, 'w', encoding='utf-8') as f:
        json.dump(schemas_content, f)

    # Create test_config_v16.yaml
    config_content = {
        "database": {
            "manifest_db_path": str(manifest_db_path),
            "raw_lake_db_path": str(raw_lake_db_path),
            "processed_db_path": str(processed_db_path),
        },
        "paths": {
            "schema_config_path": str(schema_file_path),
            "input_directory": "dummy_input_not_used_by_pipeline_directly"
        }
    }
    with open(config_file_path, 'w', encoding='utf-8') as f:
        yaml.dump(config_content, f)

    # Setup manifest.db
    m_conn = duckdb.connect(str(manifest_db_path))
    m_conn.execute("""
        CREATE TABLE IF NOT EXISTS file_manifest (
            file_hash VARCHAR PRIMARY KEY,
            file_path VARCHAR,
            registration_timestamp TIMESTAMP DEFAULT current_timestamp,
            status VARCHAR DEFAULT 'registered'
        );
    """)
    manifest_test_data = [
        ('hash_valid_data_csv', '/fake/valid_data.csv', 'loaded_to_raw_lake'),
        ('hash_validation_err_csv', '/fake/validation_error.csv', 'loaded_to_raw_lake'),
        ('hash_parser_err_csv', '/fake/parser_error.csv', 'loaded_to_raw_lake'),
        ('hash_schema_not_found_txt', '/fake/no_schema.txt', 'loaded_to_raw_lake'),
        ('hash_bad_encoding_csv', '/fake/bad_encoding.csv', 'loaded_to_raw_lake'),
        ('hash_no_content_csv', '/fake/no_content.csv', 'loaded_to_raw_lake'),
        ('hash_already_processed_csv', '/fake/already_processed.csv', 'processed'),
        # New entries for fault tolerance tests
        ('hash_retry_succeeds_csv', '/fake/retry_succeeds.csv', 'loaded_to_raw_lake'),
        ('hash_retry_fails_csv', '/fake/retry_fails.csv', 'loaded_to_raw_lake'),
        ('hash_fatal_error_csv', '/fake/fatal_error.csv', 'loaded_to_raw_lake')
    ]
    for record in manifest_test_data:
        m_conn.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)", record)
    m_conn.close()

    # Setup raw_lake.db
    rl_conn = duckdb.connect(str(raw_lake_db_path))
    rl_conn.execute("CREATE TABLE IF NOT EXISTS raw_files (file_hash VARCHAR PRIMARY KEY, raw_content BLOB);")
    _raw_lake_test_data_list = [ # Renamed to avoid confusion with the dict below
        ('hash_valid_data_csv', b"valid_data_keywords\n1,Alice,100.5\n2,Bob,200.0\n3,Charlie,NaN"),
        ('hash_validation_err_csv', b"validation_error_keywords\nnot_an_int,Test Data"), # "not_an_int" for non-nullable integer
        ('hash_parser_err_csv', b"parser_error_keywords\n\"unterminated_quote,valueA\ncol2,valueB"), # Matches csv_parser_error_schema
        ('hash_schema_not_found_txt', b"some_random_text_content\nthat_matches_no_schema"),
        ('hash_bad_encoding_csv', "bad_encoding_keywords\n測試鍵,測試值".encode('big5')), # For csv_bad_encoding_schema (expects utf-8)
        # New entries for fault tolerance tests
        # Content for retry_succeeds should match 'csv_retry_succeeds_schema'
        ('hash_retry_succeeds_csv', b"retry_succeeds_keywords\n10,RetrySuccess,1000.1"),
        # Content for retry_fails and fatal_error, actual content might not matter much due to mocking,
        # but providing some basic content is good practice.
        ('hash_retry_fails_csv', b"keywords_for_retry_fails\ndata_that_will_keep_failing_io"),
        ('hash_fatal_error_csv', b"fatal_error_keywords\nwill_cause_parser_value_error,123") # Matches csv_generic_schema
    ]
    raw_lake_test_data_dict = {item[0]: item[1] for item in _raw_lake_test_data_list} # Create a dict version
    # hash_no_content_csv is intentionally omitted from raw_files table
    for record in _raw_lake_test_data_list: # Use the list for DB insertion
        rl_conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)", record)
    rl_conn.close()

    # Setup processed.db (empty, ProcessedDBLoader will create tables if needed)
    p_conn = duckdb.connect(str(processed_db_path)) # Ensure it's created for pipeline init
    p_conn.close()

    # Expected statuses after pipeline run
    expected_statuses = {
        'hash_valid_data_csv': 'processed',
        'hash_validation_err_csv': 'validation_error', # validator returns None/empty
        'hash_parser_err_csv': 'transformation_failed', # pd.errors.ParserError re-raised by DataParser, caught by pipeline
        'hash_schema_not_found_txt': 'parse_error_schema_not_identified', # schema identification fails
        'hash_bad_encoding_csv': 'transformation_failed', # UnicodeDecodeError re-raised by DataParser, caught by pipeline
        'hash_no_content_csv': 'parse_error_no_content', # raw_content is None
        'hash_already_processed_csv': 'processed', # Should remain unchanged

        # Expected statuses for new fault tolerance tests in an end-to-end run (no mocks from fault tolerance tests apply here):
        'hash_retry_succeeds_csv': 'processed', # Content is valid and should be processed.
        'hash_retry_fails_csv': 'parse_error_schema_not_identified', # Content does not match any schema keywords.
        'hash_fatal_error_csv': 'processed'     # Content is valid for its schema and should be processed.
    }

    # For the original test_find_pending_files
    # It expects 'expected_hashes' for files that are 'loaded_to_raw_lake'
    # Adding new files that are initially in 'loaded_to_raw_lake'
    expected_hashes_for_find_pending = sorted([
        'hash_valid_data_csv', 'hash_validation_err_csv', 'hash_parser_err_csv',
        'hash_schema_not_found_txt', 'hash_bad_encoding_csv', 'hash_no_content_csv',
        'hash_retry_succeeds_csv', 'hash_retry_fails_csv', 'hash_fatal_error_csv'
    ])

    yield {
        "config_path": str(config_file_path),
        "manifest_db_path": str(manifest_db_path),
        "processed_db_path": str(processed_db_path), # Added for verification
        "expected_statuses": expected_statuses,
        "expected_hashes_for_find_pending": expected_hashes_for_find_pending,
        "valid_data_table_name": schemas_content["csv_valid_data"]["table_name"], # Pass for verification
        "raw_lake_test_data_dict": raw_lake_test_data_dict # Expose the dict of raw content
    }
    # tmp_path fixture handles cleanup

def test_find_pending_files(transformation_pipeline_env):
    """
    Tests the find_pending_files method of TransformationPipeline to ensure it correctly
    identifies files that are in 'loaded_to_raw_lake' status.
    """
    config_path = transformation_pipeline_env["config_path"]
    # This test specifically uses the 'expected_hashes_for_find_pending' part of the fixture
    expected_hashes = transformation_pipeline_env["expected_hashes_for_find_pending"]

    pipeline = None
    try:
        pipeline = TransformationPipeline(config_path=config_path)

        pending_files_dicts = pipeline.find_pending_files()

        assert len(pending_files_dicts) == len(expected_hashes), \
            f"Expected {len(expected_hashes)} files, but got {len(pending_files_dicts)}"

        returned_hashes = sorted([item['file_hash'] for item in pending_files_dicts])

        assert returned_hashes == expected_hashes, \
            f"Returned file hashes do not match expected hashes.\nExpected: {expected_hashes}\nGot: {returned_hashes}"

        # Also check if other columns are present (as per the method's query)
        # and that status is correct for all files returned by find_pending_files
        for item in pending_files_dicts:
            assert 'file_path' in item
            assert 'status' in item
            assert item['status'] == 'loaded_to_raw_lake' # Verify status from query
            assert 'registration_timestamp' in item

    finally:
        if pipeline:
            pipeline.close()

def test_pipeline_run_updates_statuses(transformation_pipeline_env):
    """
    Tests the end-to-end run method of TransformationPipeline and verifies
    manifest statuses and data loaded into the processed database.
    """
    config_path = transformation_pipeline_env["config_path"]
    manifest_db_path = transformation_pipeline_env["manifest_db_path"]
    processed_db_path = transformation_pipeline_env["processed_db_path"]
    expected_statuses = transformation_pipeline_env["expected_statuses"]
    valid_data_table = transformation_pipeline_env["valid_data_table_name"]

    pipeline = None
    try:
        pipeline = TransformationPipeline(config_path=config_path)
        pipeline.run()
    except Exception as e:
        pytest.fail(f"Pipeline run failed with an exception: {e}")
    finally:
        if pipeline:
            pipeline.close()

    # 1. Verify statuses in manifest.db after run
    m_conn_check = None
    queried_statuses = {}
    try:
        m_conn_check = duckdb.connect(str(manifest_db_path), read_only=True)
        results = m_conn_check.execute("SELECT file_hash, status FROM file_manifest").fetchall()
        for row in results:
            queried_statuses[row[0]] = row[1]
    except Exception as e:
        pytest.fail(f"Failed to query manifest DB after pipeline run: {e}")
    finally:
        if m_conn_check:
            m_conn_check.close()

    assert queried_statuses == expected_statuses, \
        f"Mismatch in final manifest statuses.\nExpected: {expected_statuses}\nGot: {queried_statuses}"

    # 2. Verify content in processed.db for the successfully processed file
    p_conn_check = None
    try:
        p_conn_check = duckdb.connect(str(processed_db_path), read_only=True)

        # Check data for 'hash_valid_data_csv'
        loaded_df = p_conn_check.table(valid_data_table).df()

        assert len(loaded_df) == 3, f"Expected 3 rows in {valid_data_table}, got {len(loaded_df)}"

        # Verify dtypes (DuckDB types vs Pandas dtypes)
        # Example: DuckDB INTEGER maps to pandas int64 or Int64, VARCHAR to object/string, DOUBLE to float64
        # This requires knowing the schema of 'csv_valid_data'
        # id (integer, non-nullable), name (string, nullable), value (float, nullable)
        # For integer columns that are NOT NULL and contain no NaNs, pandas might read them back as int32 or int64 from DuckDB.
        # DataValidator prepares it as Int64, but after DB roundtrip, it can change if no NaNs.
        assert str(loaded_df['id'].dtype) in ('Int64', 'int64', 'int32'), \
            f"Expected id dtype Int64/int64/int32, got {loaded_df['id'].dtype}"
        assert str(loaded_df['name'].dtype) == 'object' or str(loaded_df['name'].dtype) == 'string', \
            f"Expected name dtype object/string, got {loaded_df['name'].dtype}"
        assert str(loaded_df['value'].dtype) in ('float64', 'float32'), \
            f"Expected value dtype float64/float32, got {loaded_df['value'].dtype}"

        # Verify content for a specific row (e.g., the first row)
        assert loaded_df['id'].iloc[0] == 1
        assert loaded_df['name'].iloc[0] == 'Alice'
        assert loaded_df['value'].iloc[0] == 100.5
        assert pd.isna(loaded_df['value'].iloc[2]) # For the "NaN" string in input

        # 3. Verify that tables for error files were not created or are empty
        # For this test, we'll just check that the specific validation_error_table is not in the list of tables
        # or if it exists, it's empty. A more robust check might be to list all tables.
        all_tables_query = p_conn_check.execute("SHOW TABLES;").fetchall()
        all_table_names = [table[0] for table in all_tables_query]

        # Table for validation error data should ideally not exist if validation happens before table creation attempt.
        # Or if schema identification fails, or parsing fails.
        # The ProcessedDBLoader.load_dataframe uses 'append', so table might be created by a previous successful run
        # if names were reused. Here, table names are distinct.
        assert "validation_error_table" not in all_table_names, \
            f"Table 'validation_error_table' should not exist for validation error case."
        assert "parser_error_table" not in all_table_names, \
            f"Table 'parser_error_table' should not exist for parser error case."
        # Add similar checks for tables related to other errorneous files if they have distinct table names in schema

    except Exception as e:
        pytest.fail(f"Failed to query or verify processed DB: {e}")
    finally:
        if p_conn_check:
            p_conn_check.close()

# --- Unit Tests for TransformationPipeline ---
from unittest.mock import MagicMock, patch # Added patch here

@pytest.mark.parametrize(
    "missing_key_info", # Changed to a more descriptive name
    [
        # Test case 1: Missing 'processed_db_path'
        ({"config_override": {"database": {"processed_db_path": None}}, "expected_missing_key_in_error": "processed_db_path"}),
        # Test case 2: Missing 'schema_config_path'
        ({"config_override": {"paths": {"schema_config_path": None}}, "expected_missing_key_in_error": "schema_config_path"}),
        # Test case 3: Missing 'manifest_db_path'
        ({"config_override": {"database": {"manifest_db_path": None}}, "expected_missing_key_in_error": "manifest_db_path"}),
        # Test case 4: Missing 'raw_lake_db_path'
        ({"config_override": {"database": {"raw_lake_db_path": None}}, "expected_missing_key_in_error": "raw_lake_db_path"}),
        # Test case 5: Top-level 'database' key missing entirely
        ({"config_override": "remove_database_key", "expected_missing_key_in_error": "manifest_db_path"}), # Will also miss raw_lake and processed
        # Test case 6: Top-level 'paths' key missing entirely
        ({"config_override": "remove_paths_key", "expected_missing_key_in_error": "schema_config_path"}),
    ]
)
def test_pipeline_init_raises_value_error_on_missing_config(
    monkeypatch, tmp_path, missing_key_info
):
    """測試 TransformationPipeline 初始化時，若設定檔缺少關鍵路徑，會引發 ValueError。"""
    config_override = missing_key_info["config_override"]
    expected_missing_key = missing_key_info["expected_missing_key_in_error"]

    base_config = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {
            "schema_config_path": str(tmp_path / "schemas.json")
        }
    }

    # Apply the override
    if isinstance(config_override, dict):
        if "database" in config_override:
            if base_config["database"] is None: base_config["database"] = {} # Ensure nested dict exists
            base_config["database"].update(config_override["database"])
        if "paths" in config_override:
            if base_config["paths"] is None: base_config["paths"] = {} # Ensure nested dict exists
            base_config["paths"].update(config_override["paths"])
    elif config_override == "remove_database_key":
        del base_config["database"]
    elif config_override == "remove_paths_key":
        del base_config["paths"]


    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: base_config)

    # Mock dependencies to isolate the config check
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())
    # Mock duckdb.connect at the source where it's imported in pipeline.py
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock())

    with pytest.raises(ValueError) as excinfo:
        TransformationPipeline(config_path="dummy_config.yaml")

    # The error message lists all missing keys. We check if our expected one is present.
    assert f"Missing required paths in configuration:" in str(excinfo.value)
    assert expected_missing_key in str(excinfo.value)

def test_pipeline_init_creates_schema_parent_directory(monkeypatch, tmp_path):
    """測試 TransformationPipeline 初始化時，會自動建立不存在的 schema 路徑的父目錄。"""
    non_existent_schema_dir = tmp_path / "non_existent_schemas_dir"
    schema_file_in_non_existent_dir = non_existent_schema_dir / "schemas.json"

    # Pre-condition: Ensure the directory does not exist
    assert not non_existent_schema_dir.exists()

    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {
            "schema_config_path": str(schema_file_in_non_existent_dir)
        }
    }
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)

    # Mock other mkdir calls for DB paths to avoid side effects if they also don't exist
    # but we are specifically not mocking the one for schema_config_path.parent
    # For simplicity in this specific test, we can let all mkdir run, or mock specific ones.
    # Let's allow mkdir to run for this test to verify its behavior.
    # However, we must mock duckdb.connect and other initializers that might fail if
    # their respective DB files/paths are not fully set up by this minimal config.

    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock())

    # Initialize the pipeline
    TransformationPipeline(config_path="dummy_config.yaml")

    # Post-condition: Assert the specific parent directory for schema_config_path was created
    assert non_existent_schema_dir.exists()
    assert non_existent_schema_dir.is_dir()

@pytest.mark.parametrize(
    "dependency_to_fail, error_to_raise, expected_exception_type, expected_error_message_part",
    [
        ("RawLakeReader", ConnectionError("Simulated RawLakeReader Connection Error"), ConnectionError, "Simulated RawLakeReader Connection Error"),
        ("ManifestManager", ConnectionError("Simulated ManifestManager Connection Error"), ConnectionError, "Simulated ManifestManager Connection Error"),
        ("ProcessedDBLoader", Exception("Simulated ProcessedDBLoader Init Error"), Exception, "Simulated ProcessedDBLoader Init Error"),
        ("SchemaManager", FileNotFoundError("Simulated SchemaManager: Schema config file not found"), FileNotFoundError, "Simulated SchemaManager: Schema config file not found"),
        # This case tests the direct duckdb.connect for self.manifest_con
        ("duckdb.connect_manifest_direct", duckdb.Error("Simulated direct duckdb.connect error for manifest"), duckdb.Error, "Simulated direct duckdb.connect error for manifest"),
        # This case tests if ProcessedDBLoader.__init__ itself raises an error that TransformationPipeline catches and re-raises
        ("ProcessedDBLoader_internal_fail", Exception("Simulated ProcessedDBLoader internal error, caught by pipeline"), Exception, "Simulated ProcessedDBLoader internal error, caught by pipeline"),
    ]
)
def test_pipeline_init_handles_dependency_errors(
    monkeypatch, tmp_path, dependency_to_fail, error_to_raise, expected_exception_type, expected_error_message_part
):
    """測試 TransformationPipeline 初始化時，若依賴項初始化失敗，會拋出相應的錯誤。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {
            "schema_config_path": str(tmp_path / "schemas.json")
        }
    }
    if dependency_to_fail != "SchemaManager":
        (tmp_path / "schemas.json").touch() # Ensure schema file exists

    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())

    # Default successful mocks
    mock_duckdb_connect_default = MagicMock(return_value=MagicMock(name="default_db_conn"))
    mock_schema_manager_init_default = MagicMock(return_value=MagicMock(name="schema_manager_instance"))
    mock_data_parser_init_default = MagicMock(return_value=MagicMock(name="data_parser_instance"))
    mock_data_validator_init_default = MagicMock(return_value=MagicMock(name="data_validator_instance"))
    mock_processed_db_loader_init_default = MagicMock(return_value=MagicMock(name="processed_loader_instance"))
    mock_raw_lake_reader_init_default = MagicMock(return_value=MagicMock(name="raw_lake_reader_instance"))
    mock_manifest_manager_init_default = MagicMock(return_value=MagicMock(name="manifest_manager_instance"))

    # Assign defaults first, then override the one that should fail
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", mock_duckdb_connect_default)
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", mock_schema_manager_init_default)
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", mock_data_parser_init_default)
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", mock_data_validator_init_default)
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", mock_processed_db_loader_init_default)
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", mock_raw_lake_reader_init_default)
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", mock_manifest_manager_init_default)

    # Apply failure mock
    if dependency_to_fail == "RawLakeReader":
        mock_raw_lake_reader_init_default.side_effect = error_to_raise
    elif dependency_to_fail == "ManifestManager":
        mock_manifest_manager_init_default.side_effect = error_to_raise
    elif dependency_to_fail == "ProcessedDBLoader": # This targets the direct __init__ of ProcessedDBLoader
        mock_processed_db_loader_init_default.side_effect = error_to_raise
    elif dependency_to_fail == "SchemaManager":
        mock_schema_manager_init_default.side_effect = error_to_raise
    elif dependency_to_fail == "duckdb.connect_manifest_direct":
        # This makes the duckdb.connect call for self.manifest_con fail
        # Need to ensure other duckdb.connect calls (e.g. inside other components if not mocked) don't also fail
        # For this test, other components' __init__ are mocked, so their internal connects won't happen.
        mock_duckdb_connect_default.side_effect = error_to_raise
    elif dependency_to_fail == "ProcessedDBLoader_internal_fail":
        # This is for the case where ProcessedDBLoader.__init__ itself raises an error,
        # and TransformationPipeline catches it and re-raises.
        mock_processed_db_loader_init_default.side_effect = error_to_raise


    with pytest.raises(expected_exception_type) as excinfo:
        TransformationPipeline(config_path="dummy_config.yaml")

    # The raised exception e, should be what we simulated.
    # The print statements in TransformationPipeline.__init__ are for logging/debugging,
    # the actual exception raised should be the one from the dependency.
    assert expected_error_message_part in str(excinfo.value)

def test_run_handles_no_pending_files(monkeypatch, tmp_path, capsys):
    """測試 TransformationPipeline.run() 在沒有待處理檔案時的行為。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {"schema_config_path": str(tmp_path / "schemas.json")}
    }
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock())

    pipeline = TransformationPipeline(config_path="dummy_config.yaml")

    # Mock find_pending_files to return an empty list
    monkeypatch.setattr(pipeline, "find_pending_files", lambda: [])

    # Mock close methods of dependencies that would be called in finally
    # These are instances on the pipeline object
    pipeline.manifest_con.close = MagicMock() # manifest_con is created by mocked duckdb.connect
    pipeline.raw_lake_reader.close = MagicMock()
    pipeline.manifest_manager.close = MagicMock()
    pipeline.processed_loader.close = MagicMock()


    pipeline.run()

    captured = capsys.readouterr()
    assert "目前沒有待處理的檔案。" in captured.out
    pipeline.manifest_con.close.assert_called_once()
    pipeline.raw_lake_reader.close.assert_called_once()
    pipeline.manifest_manager.close.assert_called_once()
    pipeline.processed_loader.close.assert_called_once()

def test_run_handles_validation_failure(monkeypatch, tmp_path, capsys):
    """測試 pipeline.run() 在 validator.validate() 返回 None (驗證失敗) 時的行為。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {"schema_config_path": str(tmp_path / "schemas.json")}
    }
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())
    # General duckdb.connect mock for __init__
    mock_initial_db_conn = MagicMock(name="initial_db_conn")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock(return_value=mock_initial_db_conn))

    # Mock dependencies' __init__ methods
    mock_rrl_instance = MagicMock(name="raw_lake_reader_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock(return_value=mock_rrl_instance))

    mock_sm_instance = MagicMock(name="schema_manager_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock(return_value=mock_sm_instance))

    mock_dp_instance = MagicMock(name="data_parser_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock(return_value=mock_dp_instance))

    mock_dv_instance = MagicMock(name="data_validator_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock(return_value=mock_dv_instance))

    mock_mm_instance = MagicMock(name="manifest_manager_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock(return_value=mock_mm_instance))

    mock_pdl_instance = MagicMock(name="processed_db_loader_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock(return_value=mock_pdl_instance))

    pipeline = TransformationPipeline(config_path="dummy_config.yaml")
    # Ensure close methods on the actual instances are mocked for the finally block in run()
    pipeline.manifest_con.close = MagicMock(name="manifest_con_close_mock")
    mock_rrl_instance.close = MagicMock(name="rrl_close_mock")
    mock_mm_instance.close = MagicMock(name="mm_close_mock")
    mock_pdl_instance.close = MagicMock(name="pdl_close_mock")


    # Setup for a single file to be processed
    test_file_hash = 'hash_validation_fail'
    test_file_path = '/fake/validation_fail.csv'
    mock_pending_files = [{'file_hash': test_file_hash, 'file_path': test_file_path, 'status': 'loaded_to_raw_lake'}]
    monkeypatch.setattr(pipeline, "find_pending_files", lambda: mock_pending_files)

    # Mock interactions within the loop for this file
    mock_rrl_instance.get_raw_content.return_value = b"raw,csv,data"
    mock_sm_instance.identify_schema_from_content.return_value = "test_schema"
    mock_schema_def = {"columns": {"col1": {"dtype": "string"}}} # Simplified schema
    mock_sm_instance.schemas = {"test_schema": mock_schema_def} # Make .get work

    mock_df = pd.DataFrame({'col1': ['data']}) # Dummy DataFrame
    mock_dp_instance.parse.return_value = mock_df

    # Key mock: validator.validate returns None
    mock_dv_instance.validate.return_value = None

    pipeline.run()

    # Assert that update_status was called with 'validation_error'
    mock_mm_instance.update_status.assert_called_once_with(test_file_hash, 'validation_error')

    # Assert that no attempt was made to load data
    mock_pdl_instance.load_dataframe.assert_not_called()

    captured = capsys.readouterr()
    # Updated log message to reflect the change in pipeline.py
    assert f"[進度] 資料驗證失敗或無有效數據 {test_file_path} (Hash: {test_file_hash[:8]})" in captured.out

    # Verify all relevant close methods were called
    pipeline.manifest_con.close.assert_called_once()
    mock_rrl_instance.close.assert_called_once()
    mock_mm_instance.close.assert_called_once() # This instance is pipeline.manifest_manager
    mock_pdl_instance.close.assert_called_once()

@pytest.mark.parametrize(
    "exception_to_raise, expected_status, expected_log_message_part",
    [
            # ValueError from parser.parse should now result in 'transformation_failed'
            (ValueError("Simulated ValueError in parse"), 'transformation_failed', "Simulated ValueError in parse"),
            # pd.errors.ParserError from parser.parse should now result in 'transformation_failed'
            (pd.errors.ParserError("Simulated ParserError"), 'transformation_failed', "Simulated ParserError"),
            # UnicodeDecodeError from parser.parse should now result in 'transformation_failed'
            (UnicodeDecodeError("utf-8", b"\x80", 0, 1, "invalid start byte"), 'transformation_failed', "'utf-8' codec can't decode byte 0x80 in position 0: invalid start byte"),
            # Generic Exception remains 'transformation_failed'
            (Exception("Simulated Generic Error in parse"), 'transformation_failed', "Simulated Generic Error in parse"),
    ]
)
def test_run_main_loop_exception_handling(
    monkeypatch, tmp_path, capsys,
    exception_to_raise, expected_status, expected_log_message_part
):
    """測試 pipeline.run() 在處理檔案迴圈中，對不同類型的例外進行處理並更新 manifest 狀態。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {"schema_config_path": str(tmp_path / "schemas.json")}
    }
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock(return_value=MagicMock(name="initial_db_conn")))

    mock_rrl_instance = MagicMock(name="raw_lake_reader_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock(return_value=mock_rrl_instance))
    mock_sm_instance = MagicMock(name="schema_manager_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock(return_value=mock_sm_instance))
    mock_dp_instance = MagicMock(name="data_parser_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock(return_value=mock_dp_instance))
    mock_dv_instance = MagicMock(name="data_validator_instance") # Not used in this path, but needed for init
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock(return_value=mock_dv_instance))
    mock_mm_instance = MagicMock(name="manifest_manager_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock(return_value=mock_mm_instance))
    mock_pdl_instance = MagicMock(name="processed_db_loader_instance") # Not used in this path, but needed for init
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock(return_value=mock_pdl_instance))

    pipeline = TransformationPipeline(config_path="dummy_config.yaml")
    # Mock close methods
    pipeline.manifest_con.close = MagicMock(name="manifest_con_close_mock")
    mock_rrl_instance.close = MagicMock(name="rrl_close_mock")
    mock_mm_instance.close = MagicMock(name="mm_close_mock")
    mock_pdl_instance.close = MagicMock(name="pdl_close_mock")

    test_file_hash = 'hash_exception_test'
    test_file_path = '/fake/exception_test.file'
    mock_pending_files = [{'file_hash': test_file_hash, 'file_path': test_file_path, 'status': 'loaded_to_raw_lake'}]
    monkeypatch.setattr(pipeline, "find_pending_files", lambda: mock_pending_files)

    mock_rrl_instance.get_raw_content.return_value = b"some content"
    mock_sm_instance.identify_schema_from_content.return_value = "some_schema"
    mock_sm_instance.schemas = {"some_schema": {"columns": {}}} # Simplified schema

    # Configure the DataParser's parse method to raise the specified exception
    mock_dp_instance.parse.side_effect = exception_to_raise

    pipeline.run()

    mock_mm_instance.update_status.assert_called_once_with(test_file_hash, expected_status)

    captured = capsys.readouterr()
    assert expected_log_message_part in captured.out

    # Ensure no data loading attempt was made
    mock_pdl_instance.load_dataframe.assert_not_called()

    # Verify all relevant close methods were called
    pipeline.manifest_con.close.assert_called_once()
    mock_rrl_instance.close.assert_called_once()
    mock_mm_instance.close.assert_called_once()
    mock_pdl_instance.close.assert_called_once()

def test_run_handles_error_on_final_connection_close(monkeypatch, tmp_path, capsys):
    """測試 pipeline.run() 在最後關閉 manifest_con 時發生錯誤，程式是否能優雅處理。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {"schema_config_path": str(tmp_path / "schemas.json")}
    }
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())

    # Mock duckdb.connect to return a connection object that will be later modified
    mock_db_conn_instance = MagicMock(name="db_conn_for_manifest")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock(return_value=mock_db_conn_instance))

    # Mock other dependencies for __init__
    mock_rrl_instance = MagicMock(name="raw_lake_reader_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock(return_value=mock_rrl_instance))
    mock_mm_instance = MagicMock(name="manifest_manager_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock(return_value=mock_mm_instance))
    mock_pdl_instance = MagicMock(name="processed_db_loader_instance")
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock(return_value=mock_pdl_instance))
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock())


    pipeline = TransformationPipeline(config_path="dummy_config.yaml")

    # Mock find_pending_files to return an empty list to quickly get to the finally block
    monkeypatch.setattr(pipeline, "find_pending_files", lambda: [])

    # Mock the close method of the specific manifest_con instance to raise an error
    pipeline.manifest_con.close = MagicMock(side_effect=duckdb.Error("Simulated error closing manifest_con"))

    # Mock other close methods to ensure they are still called
    mock_rrl_instance.close = MagicMock(name="rrl_close_mock")
    mock_mm_instance.close = MagicMock(name="mm_close_mock") # This is pipeline.manifest_manager.close()
    mock_pdl_instance.close = MagicMock(name="pdl_close_mock")


    try:
        pipeline.run() # Should not raise an unhandled exception
    except Exception as e:
        pytest.fail(f"pipeline.run() raised an unexpected exception during final close: {e}")

    captured = capsys.readouterr()
    assert "Error closing manifest_con: Simulated error closing manifest_con" in captured.out

    # Verify that other close methods were still attempted
    mock_rrl_instance.close.assert_called_once()
    mock_mm_instance.close.assert_called_once()
    mock_pdl_instance.close.assert_called_once()


def test_find_pending_files_handles_no_results(monkeypatch, tmp_path):
    """測試 find_pending_files 在資料庫查詢無結果時返回空列表。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {"schema_config_path": str(tmp_path / "schemas.json")}
    }
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())

    # Mock duckdb.connect to return a connection object with a mock execute method
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [] # No results
    mock_cursor.description = [('file_hash',), ('file_path',), ('status',), ('registration_timestamp',)] # Mock description

    mock_connection = MagicMock()
    mock_connection.execute.return_value = mock_cursor

    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock(return_value=mock_connection))

    # Mock other dependencies of TransformationPipeline.__init__
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock())


    pipeline = TransformationPipeline(config_path="dummy_config.yaml")
    # Overwrite the specific manifest_con instance after it's created in __init__
    # This is because __init__ sets self.manifest_con = duckdb.connect(...)
    # and we want to control the execute method on *that specific connection instance*
    # for find_pending_files.
    pipeline.manifest_con = mock_connection # Replace with our controlled mock

    result = pipeline.find_pending_files()
    assert result == []
    mock_connection.execute.assert_called_once_with(
        "SELECT file_hash, file_path, status, registration_timestamp FROM file_manifest WHERE status = 'loaded_to_raw_lake'"
    )

# --- Transformation Pipeline Fault Tolerance Tests ---

@pytest.mark.xfail(reason="TransformationPipeline 尚未實現 IO 錯誤的重試邏輯。此測試預期在該功能實現後通過。")
def test_pipeline_retries_on_transient_error_and_succeeds(
    transformation_pipeline_env, mocker, capsys
):
    """
    測試：當讀取原始內容遇到暫時性 IO 錯誤時，管線應進行重試，並最終成功處理檔案。
    - Mock find_pending_files 以隔離測試，只處理一個目標檔案。
    - Mock raw_lake_reader.get_raw_content 以模擬 IO 錯誤及後續的成功讀取。
    - 假設 Pipeline 內部有 MAX_RETRIES >= 2 的重試邏輯。
    - 假設成功重試後，狀態會更新為 'processed_after_retry'。
    """
    config_path = transformation_pipeline_env["config_path"]
    manifest_db_path = transformation_pipeline_env["manifest_db_path"]

    TARGET_FILE_HASH = 'hash_retry_succeeds_csv'
    TARGET_FILE_PATH = '/fake/retry_succeeds.csv'
    # This content should match the 'csv_valid_data' schema defined in the fixture
    MOCK_RAW_CONTENT_SUCCESS = transformation_pipeline_env["raw_lake_test_data_dict"][TARGET_FILE_HASH]


    pipeline = TransformationPipeline(config_path=config_path)

    # 1. 測試隔離：只處理目標檔案
    mock_find_pending = mocker.patch.object(pipeline, 'find_pending_files')
    mock_find_pending.return_value = [{
        'file_hash': TARGET_FILE_HASH,
        'file_path': TARGET_FILE_PATH,
        'status': 'loaded_to_raw_lake',
        'registration_timestamp': pd.Timestamp.now() # Mock timestamp
    }]

    # 2. 精準 Mocking：模擬 raw_lake_reader.get_raw_content 的行為
    # 前兩次呼叫失敗 (IOError)，第三次成功返回內容
    mock_get_raw_content = mocker.patch.object(
        pipeline.raw_lake_reader,
        'get_raw_content',
        side_effect=[
            IOError("Simulated IO Error 1"),
            IOError("Simulated IO Error 2"),
            MOCK_RAW_CONTENT_SUCCESS
        ]
    )

    # Mock manifest_manager.update_status to spy on its calls
    spy_update_status = mocker.spy(pipeline.manifest_manager, 'update_status')

    # 執行管線
    try:
        pipeline.run()
    except Exception as e:
        # 這裡的 `pytest.fail` 會立即終止測試並顯示錯誤訊息。
        # 這有助於捕獲在 `pipeline.run()` 內部未被預期處理的例外。
        pytest.fail(f"Pipeline run failed unexpectedly during retry test: {e}\nCaptured stdout:\n{capsys.readouterr().out}")
    finally:
        pipeline.close()

    # 3. 驗證 Mock 和狀態
    # 驗證 get_raw_content 被呼叫了3次 (2次失敗, 1次成功)
    assert mock_get_raw_content.call_count == 3, \
        f"Expected get_raw_content to be called 3 times, but was called {mock_get_raw_content.call_count} times."

    # 驗證 manifest 狀態是否更新為 'processed_after_retry'
    # 由於 TransformationPipeline 目前沒有內建重試邏輯會更新到這個特定狀態，
    # 此斷言將在 Pipeline 實現該功能前失敗。
    # 我們檢查最後一次對 update_status 的呼叫。
    # 注意：如果管線在重試期間更新了狀態（例如 'retry_attempt_1'），則需要調整此斷言。
    # 目前，我們假設它只在最終成功或失敗時更新。

    # 找到對目標檔案 hash 的最後一次狀態更新
    final_status_call = None
    for call in spy_update_status.call_args_list:
        if call.args[0] == TARGET_FILE_HASH:
            final_status_call = call

    assert final_status_call is not None, f"update_status was never called for {TARGET_FILE_HASH}"

    # *** 假設 Pipeline 更新後的狀態 ***
    # 這個 'processed_after_retry' 狀態是我們期望 Pipeline 未來會設定的狀態。
    # 如果 Pipeline 目前的邏輯是直接設為 'processed'，此測試會失敗，
    # 指出 Pipeline 需要增強以區分正常處理和重試後處理。
    EXPECTED_FINAL_STATUS = 'processed_after_retry'
    assert final_status_call.args[1] == EXPECTED_FINAL_STATUS, \
        f"Expected final status for {TARGET_FILE_HASH} to be '{EXPECTED_FINAL_STATUS}', but got '{final_status_call.args[1]}'. " \
        "This might indicate that the TransformationPipeline does not yet implement the specific status update for successful retries."

    # 輔助輸出，用於調試
    captured = capsys.readouterr()
    print("\nCaptured output for test_pipeline_retries_on_transient_error_and_succeeds:")
    print(captured.out)
    if captured.err:
        print("Captured stderr:")
        print(captured.err)

    # 也可以直接查詢 DB 確認最終狀態，如果 update_status mock 不夠用的話
    conn_check = None
    try:
        conn_check = duckdb.connect(str(manifest_db_path), read_only=True)
        db_status = conn_check.execute(
            "SELECT status FROM file_manifest WHERE file_hash = ?", (TARGET_FILE_HASH,)
        ).fetchone()
        assert db_status is not None, f"Manifest record for {TARGET_FILE_HASH} not found in DB."
        assert db_status[0] == EXPECTED_FINAL_STATUS, \
            f"Expected DB status for {TARGET_FILE_HASH} to be '{EXPECTED_FINAL_STATUS}', but got '{db_status[0]}'."
    finally:
        if conn_check:
            conn_check.close()


@pytest.mark.xfail(reason="TransformationPipeline 尚未實現 IO 錯誤的重試邏輯和最大重試次數後的特定狀態更新。此測試預期在該功能實現後通過。")
def test_pipeline_fails_after_max_retries_and_updates_manifest(
    transformation_pipeline_env, mocker, capsys
):
    """
    測試：當讀取原始內容持續遇到 IO 錯誤，達到最大重試次數後，管線應放棄並更新 Manifest 狀態。
    - Mock find_pending_files 以隔離測試。
    - Mock raw_lake_reader.get_raw_content 以持續模擬 IO 錯誤。
    - 假設 Pipeline 內部 MAX_RETRIES = 3 (總共嘗試 1 + 3 = 4 次)。
    - 假設達到最大重試次數後，狀態更新為 'failed_after_retries'。
    """
    config_path = transformation_pipeline_env["config_path"]
    manifest_db_path = transformation_pipeline_env["manifest_db_path"]

    TARGET_FILE_HASH = 'hash_retry_fails_csv'
    TARGET_FILE_PATH = '/fake/retry_fails.csv'
    MAX_ATTEMPTS = 4 # 1 initial + 3 retries

    pipeline = TransformationPipeline(config_path=config_path)

    # 1. 測試隔離
    mock_find_pending = mocker.patch.object(pipeline, 'find_pending_files')
    mock_find_pending.return_value = [{
        'file_hash': TARGET_FILE_HASH,
        'file_path': TARGET_FILE_PATH,
        'status': 'loaded_to_raw_lake',
        'registration_timestamp': pd.Timestamp.now()
    }]

    # 2. 精準 Mocking： raw_lake_reader.get_raw_content 持續失敗
    mock_get_raw_content = mocker.patch.object(
        pipeline.raw_lake_reader,
        'get_raw_content',
        side_effect=[IOError(f"Simulated IO Error attempt {i+1}") for i in range(MAX_ATTEMPTS)]
    )

    spy_update_status = mocker.spy(pipeline.manifest_manager, 'update_status')

    # 執行管線
    try:
        pipeline.run()
    except Exception as e:
        pytest.fail(f"Pipeline run failed unexpectedly during max retries test: {e}\nCaptured stdout:\n{capsys.readouterr().out}")
    finally:
        pipeline.close()

    # 3. 驗證 Mock 和狀態
    assert mock_get_raw_content.call_count == MAX_ATTEMPTS, \
        f"Expected get_raw_content to be called {MAX_ATTEMPTS} times, but was called {mock_get_raw_content.call_count} times."

    final_status_call = None
    for call in spy_update_status.call_args_list:
        if call.args[0] == TARGET_FILE_HASH:
            final_status_call = call

    assert final_status_call is not None, f"update_status was never called for {TARGET_FILE_HASH}"

    EXPECTED_FINAL_STATUS = 'failed_after_retries'
    assert final_status_call.args[1] == EXPECTED_FINAL_STATUS, \
        f"Expected final status for {TARGET_FILE_HASH} to be '{EXPECTED_FINAL_STATUS}', but got '{final_status_call.args[1]}'. "\
        "This might indicate that the TransformationPipeline does not yet implement the specific status update for failures after max retries."

    # 輔助輸出
    captured = capsys.readouterr()
    print("\nCaptured output for test_pipeline_fails_after_max_retries_and_updates_manifest:")
    print(captured.out)
    if captured.err:
        print("Captured stderr:")
        print(captured.err)

    # DB 確認
    conn_check = None
    try:
        conn_check = duckdb.connect(str(manifest_db_path), read_only=True)
        db_status = conn_check.execute(
            "SELECT status FROM file_manifest WHERE file_hash = ?", (TARGET_FILE_HASH,)
        ).fetchone()
        assert db_status is not None, f"Manifest record for {TARGET_FILE_HASH} not found in DB."
        assert db_status[0] == EXPECTED_FINAL_STATUS, \
            f"Expected DB status for {TARGET_FILE_HASH} to be '{EXPECTED_FINAL_STATUS}', but got '{db_status[0]}'."
    finally:
        if conn_check:
            conn_check.close()


def test_pipeline_skips_on_fatal_error_and_updates_manifest(
    transformation_pipeline_env, mocker, capsys
):
    """
    測試：當解析階段遇到致命錯誤 (如 ValueError) 時，管線應跳過該檔案，並更新 Manifest 狀態。
    - Mock find_pending_files 以隔離測試。
    - Mock parser.parse 以模擬致命的解析錯誤。
    - 狀態應更新為 'transformation_failed'。
    """
    config_path = transformation_pipeline_env["config_path"]
    manifest_db_path = transformation_pipeline_env["manifest_db_path"]

    TARGET_FILE_HASH = 'hash_fatal_error_csv'
    TARGET_FILE_PATH = '/fake/fatal_error.csv'
    # Raw content for this file is defined in the fixture and matches 'csv_generic_schema'
    # This allows the pipeline to reach the parser.parse step.
    MOCK_RAW_CONTENT_FATAL = transformation_pipeline_env["raw_lake_test_data_dict"][TARGET_FILE_HASH]


    pipeline = TransformationPipeline(config_path=config_path)

    # 1. 測試隔離
    mock_find_pending = mocker.patch.object(pipeline, 'find_pending_files')
    mock_find_pending.return_value = [{
        'file_hash': TARGET_FILE_HASH,
        'file_path': TARGET_FILE_PATH,
        'status': 'loaded_to_raw_lake',
        'registration_timestamp': pd.Timestamp.now()
    }]

    # Mock get_raw_content to successfully return content, so parsing is attempted
    mocker.patch.object(pipeline.raw_lake_reader, 'get_raw_content', return_value=MOCK_RAW_CONTENT_FATAL)

    # 2. Mocking parser.parse to raise a fatal error (ValueError)
    mock_parser_parse = mocker.patch.object(
        pipeline.parser,
        'parse',
        side_effect=ValueError("Simulated Fatal Parser Error")
    )

    spy_update_status = mocker.spy(pipeline.manifest_manager, 'update_status')
    spy_load_data = mocker.spy(pipeline.processed_loader, 'load_dataframe')


    # 執行管線
    try:
        pipeline.run()
    except Exception as e:
        # A ValueError from parser.parse should be caught by the pipeline's main loop exception handler.
        # If it's re-raised here, it means the pipeline didn't handle it as expected.
        pytest.fail(f"Pipeline run failed unexpectedly during fatal error test: {e}\nCaptured stdout:\n{capsys.readouterr().out}")
    finally:
        pipeline.close()

    # 3. 驗證 Mock 和狀態
    # parser.parse 應該被呼叫一次
    assert mock_parser_parse.call_count == 1, \
        f"Expected parser.parse to be called once, but was called {mock_parser_parse.call_count} times."

    # processed_loader.load_dataframe 不應該被呼叫
    spy_load_data.assert_not_called()

    final_status_call = None
    for call in spy_update_status.call_args_list:
        if call.args[0] == TARGET_FILE_HASH:
            final_status_call = call

    assert final_status_call is not None, f"update_status was never called for {TARGET_FILE_HASH}"

    # 根據 pipeline.py 的 except (ValueError, TypeError, KeyError) as val_err:
    # 它會將狀態更新為 'validation_error'。
    # 如果我們希望一個更特定的 'transformation_failed' 由致命的 parser error 引起，
    # pipeline.py 中的錯誤處理可能需要調整以區分。
    # 目前，我們遵循現有 pipeline 的行為。
    # 您的指示是 'STATUS_TRANSFORMATION_FAILED'。
    # 如果 pipeline.py 的 except ValueError 區塊設置的是 'validation_error', 則此處會不匹配。
    # 讓我們假設您的指示優先，或者 pipeline.py 的通用 Exception e 區塊會捕獲它並設為 'transformation_failed'
    # 經檢查 pipeline.py, ValueError 被映射到 'validation_error'.
    # Exception e 被映射到 'transformation_failed'.
    # ValueError 是 Exception 的子類，所以它會被更早的 except 塊捕獲。
    #
    # 根據您的指示 "斷言檔案的最終狀態為 STATUS_TRANSFORMATION_FAILED"
    # 這意味著我們期望 ValueError 被 pipeline.py 中的 `except Exception as e:` 區塊處理
    # 或者 `except (ValueError, TypeError, KeyError) as val_err:` 區塊應更新為 `transformation_failed`。
    #
    # 為了使測試與您的明確指示 "STATUS_TRANSFORMATION_FAILED" 一致，
    # 我將假設 ValueError 會導致 'transformation_failed'。
    # 這可能需要對 pipeline.py 的錯誤處理進行調整，或接受此處的測試可能與當前 pipeline 精確行為不符，
    # 而是測試期望的行為。
    #
    # 更新：根據您的任務描述第4點 "斷言檔案的最終狀態為 STATUS_TRANSFORMATION_FAILED。"
    # 我將以此為準。如果 pipeline.py 的 `except ValueError` 設定了不同的狀態，
    # 這個測試將會失敗，從而指出 pipeline.py 需要調整以符合此期望。
    EXPECTED_FINAL_STATUS = 'transformation_failed' # Per your instruction for fatal error

    assert final_status_call.args[1] == EXPECTED_FINAL_STATUS, \
        f"Expected final status for {TARGET_FILE_HASH} to be '{EXPECTED_FINAL_STATUS}', but got '{final_status_call.args[1]}'."

    # 輔助輸出
    captured = capsys.readouterr()
    print("\nCaptured output for test_pipeline_skips_on_fatal_error_and_updates_manifest:")
    print(captured.out)
    if captured.err:
        print("Captured stderr:")
        print(captured.err)

    # DB 確認
    conn_check = None
    try:
        conn_check = duckdb.connect(str(manifest_db_path), read_only=True)
        db_status = conn_check.execute(
            "SELECT status FROM file_manifest WHERE file_hash = ?", (TARGET_FILE_HASH,)
        ).fetchone()
        assert db_status is not None, f"Manifest record for {TARGET_FILE_HASH} not found in DB."
        assert db_status[0] == EXPECTED_FINAL_STATUS, \
            f"Expected DB status for {TARGET_FILE_HASH} to be '{EXPECTED_FINAL_STATUS}', but got '{db_status[0]}'."
    finally:
        if conn_check:
            conn_check.close()


def test_find_pending_files_handles_db_error(monkeypatch, tmp_path, capsys):
    """測試 find_pending_files 在資料庫查詢時發生 duckdb.Error，能返回空列表並記錄錯誤。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db"),
            "processed_db_path": str(tmp_path / "processed.db")
        },
        "paths": {"schema_config_path": str(tmp_path / "schemas.json")}
    }
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())

    mock_connection = MagicMock()
    mock_connection.execute.side_effect = duckdb.Error("Simulated DB query error")
    # duckdb.connect自體被mock以返回這個受控制的mock_connection
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.duckdb.connect", MagicMock(return_value=mock_connection))

    # Mock other dependencies of TransformationPipeline.__init__
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.SchemaManager", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataParser", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.DataValidator", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ProcessedDBLoader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.RawLakeReader", MagicMock())
    monkeypatch.setattr("src.sp_data_v16.transformation.pipeline.ManifestManager", MagicMock())

    pipeline = TransformationPipeline(config_path="dummy_config.yaml")
    # 確保 pipeline.manifest_con 是我們的 mock_connection
    pipeline.manifest_con = mock_connection

    result = pipeline.find_pending_files()
    assert result == []

    captured = capsys.readouterr()
    assert "Database error in find_pending_files: Simulated DB query error" in captured.out
    mock_connection.execute.assert_called_once_with(
        "SELECT file_hash, file_path, status, registration_timestamp FROM file_manifest WHERE status = 'loaded_to_raw_lake'"
    )
