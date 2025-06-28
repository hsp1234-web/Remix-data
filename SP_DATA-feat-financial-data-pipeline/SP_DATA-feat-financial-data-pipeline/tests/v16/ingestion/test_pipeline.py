import pytest
import pathlib
import yaml
import duckdb
import hashlib # Added for SHA256 calculation
from src.sp_data_v16.ingestion.pipeline import IngestionPipeline
from src.sp_data_v16.ingestion.manifest import ManifestManager # For direct DB check

@pytest.fixture
def temp_pipeline_env(tmp_path: pathlib.Path):
    """
    Sets up a temporary environment for pipeline integration testing:
    - Temporary input directory with some files.
    - Temporary data directory for the manifest.db.
    - Temporary config file pointing to these temporary paths.
    """
    # 1. Create temporary directories
    input_dir = tmp_path / "pipeline_input"
    input_dir.mkdir()

    data_dir = tmp_path / "pipeline_data" / "v16"
    data_dir.mkdir(parents=True, exist_ok=True)

    manifest_db_file = data_dir / "manifest.db"
    raw_lake_db_file = data_dir / "raw_lake.db" # Path for raw_lake.db

    # 2. Create some dummy files in the input directory
    (input_dir / "fileA.txt").write_text("Content of file A")
    (input_dir / "fileB.log").write_text("Log data for file B")
    sub_input_dir = input_dir / "subfolder"
    sub_input_dir.mkdir()
    (sub_input_dir / "fileC.dat").write_bytes(b"Binary data for C")

    expected_files_count = 3

    # 3. Create a temporary config_v16.yaml
    temp_config_path = tmp_path / "temp_config_v16.yaml"
    config_content = {
        "database": {
            "manifest_db_path": str(manifest_db_file),
            "raw_lake_db_path": str(raw_lake_db_file), # Use the defined path
            "processed_db_path": str(data_dir / "processed_data.db") # Placeholder
        },
        "logging": {
            "level": "DEBUG",
            "format": "[%(asctime)s] - %(message)s"
        },
        "paths": {
            "input_directory": str(input_dir)
        }
    }
    with open(temp_config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config_content, f)

    return temp_config_path, manifest_db_file, raw_lake_db_file, expected_files_count, input_dir

def count_manifest_records(db_path: pathlib.Path) -> int:
    """Helper function to count records in the manifest table."""
    if not db_path.exists():
        return 0
    # Use ManifestManager's connection logic, or connect directly for test simplicity
    con = None
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        count_result = con.execute("SELECT COUNT(*) FROM file_manifest;").fetchone()
        return count_result[0] if count_result else 0
    except Exception as e:
        print(f"Error counting records: {e}")
        return -1 # Indicate error
    finally:
        if con:
            con.close()

def test_pipeline_run(temp_pipeline_env):
    """
    Integration test for IngestionPipeline.run().
    Tests initial run (all files new) and a second run (all files should be skipped).
    """
    config_path, manifest_db_file, raw_lake_db_file, expected_files_count, input_dir = temp_pipeline_env

    # --- First Run ---
    # print(f"Starting first pipeline run. Config: {config_path}, DB: {manifest_db_file}") # Original line
    print(f"Starting first pipeline run. Config: {config_path}, Manifest DB: {manifest_db_file}, Raw Lake DB: {raw_lake_db_file}")
    pipeline_run1 = IngestionPipeline(config_path=str(config_path))
    pipeline_run1.run() # Errors within run should be caught by pytest if they occur

    # Verify manifest content after first run
    # All files should have been registered
    records_after_run1 = count_manifest_records(manifest_db_file)
    assert records_after_run1 == expected_files_count,         f"After first run, expected {expected_files_count} records in manifest, found {records_after_run1}"

    # Verify Raw Lake content after first run
    assert count_raw_lake_records(raw_lake_db_file) == expected_files_count, \
        "After first run, raw_files table record count mismatch"

    # Compare content of one file in Raw Lake
    file_a_path = input_dir / "fileA.txt"
    file_a_hash = get_file_sha256(file_a_path)
    original_content_a = file_a_path.read_bytes()

    con_raw_lake = None
    try:
        con_raw_lake = duckdb.connect(database=str(raw_lake_db_file), read_only=True)
        result = con_raw_lake.execute("SELECT raw_content FROM raw_files WHERE file_hash = ?", (file_a_hash,)).fetchone()
        assert result is not None, f"File with hash {file_a_hash} (fileA.txt) not found in raw_lake.db"
        assert result[0] == original_content_a, "Content of fileA.txt in raw_lake.db does not match original"
    finally:
        if con_raw_lake:
            con_raw_lake.close()

    print(f"Content of fileA.txt (hash: {file_a_hash[:8]}...) successfully verified in Raw Lake.")

    # Verify file statuses in manifest after first run
    con_manifest = None
    try:
        con_manifest = duckdb.connect(database=str(manifest_db_file), read_only=True)

        file_a_path = input_dir / "fileA.txt"
        file_b_path = input_dir / "fileB.log"
        file_c_path = input_dir / "subfolder" / "fileC.dat"

        expected_hashes_statuses = {
            get_file_sha256(file_a_path): 'loaded_to_raw_lake',
            get_file_sha256(file_b_path): 'loaded_to_raw_lake',
            get_file_sha256(file_c_path): 'loaded_to_raw_lake'
        }

        actual_statuses = {}
        records = con_manifest.execute("SELECT file_hash, status FROM file_manifest").fetchall()
        for record in records:
            actual_statuses[record[0]] = record[1]

        for f_hash, expected_status in expected_hashes_statuses.items():
            assert f_hash in actual_statuses, f"Hash {f_hash} not found in manifest."
            assert actual_statuses[f_hash] == expected_status, \
                f"For hash {f_hash}, expected status '{expected_status}', got '{actual_statuses[f_hash]}'."
        print("File statuses successfully verified in manifest after first run.")

    finally:
        if con_manifest:
            con_manifest.close()

    # Optional: Check specific statuses or file hashes if needed,
    # but count is a good primary indicator for this integration test.

    # --- Second Run ---
    print("\nStarting second pipeline run (expecting files to be skipped).")
    # Re-initialize pipeline to simulate a new execution context but using the same config/DB
    pipeline_run2 = IngestionPipeline(config_path=str(config_path))
    pipeline_run2.run()

    # Verify manifest content after second run
    # No new files should have been added, so count should remain the same
    records_after_run2 = count_manifest_records(manifest_db_file)
    assert records_after_run2 == expected_files_count,         f"After second run, expected {expected_files_count} records (no change), found {records_after_run2}"

    # Verify Raw Lake content after second run (should also be unchanged)
    assert count_raw_lake_records(raw_lake_db_file) == expected_files_count, \
        "After second run, raw_files table record count should not change"

    print("Pipeline integration test completed.")

# Helper function to count records in raw_lake.db
def count_raw_lake_records(db_path: pathlib.Path) -> int:
    """Helper function to count records in the raw_files table."""
    if not db_path.exists():
        return 0
    con = None
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        count_result = con.execute("SELECT COUNT(*) FROM raw_files;").fetchone()
        return count_result[0] if count_result else 0
    except Exception as e:
        print(f"Error counting raw_lake records: {e}")
        return -1 # Indicate error
    finally:
        if con:
            con.close()

# Helper function to calculate SHA256 hash (consistent with FileScanner)
def get_file_sha256(file_path: pathlib.Path) -> str:
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# --- Unit Tests for IngestionPipeline ---
from unittest.mock import MagicMock, call # 引入 MagicMock 和 call

def test_pipeline_initialization_success(monkeypatch, tmp_path):
    """測試 IngestionPipeline 使用有效設定成功初始化。"""
    mock_config_dict = { # 更名以避免與 pytest 的 config fixture 衝突
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db")
        },
        "paths": {
            "input_directory": str(tmp_path / "input")
        }
    }

    # Mock load_config
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.load_config", lambda x: mock_config_dict)

    # Mock pathlib.Path.mkdir
    mock_mkdir_method = MagicMock() # 更名以避免與 pytest 的 mkdir fixture 衝突
    monkeypatch.setattr(pathlib.Path, "mkdir", mock_mkdir_method)

    # Mock ManifestManager
    mock_mm_instance = MagicMock()
    mock_mm_init = MagicMock(return_value=mock_mm_instance)
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.ManifestManager", mock_mm_init)

    # Mock RawLakeLoader
    mock_rll_instance = MagicMock()
    mock_rll_init = MagicMock(return_value=mock_rll_instance)
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.RawLakeLoader", mock_rll_init)

    pipeline = IngestionPipeline(config_path="dummy_config.yaml")

    assert pipeline.manifest_db_path == mock_config_dict["database"]["manifest_db_path"]
    assert pipeline.raw_lake_db_path == mock_config_dict["database"]["raw_lake_db_path"]
    assert pipeline.input_directory == mock_config_dict["paths"]["input_directory"]

    # 驗證 mkdir 被呼叫以創建 manifest DB 和 raw lake DB 的父目錄
    # pathlib.Path("...").parent.mkdir(...)
    # The mock_mkdir_method is directly patching `pathlib.Path.mkdir`.
    # The calls would be on the Path objects representing the parent directories.
    # We expect it to be called for manifest_db_path.parent and raw_lake_db_path.parent
    assert mock_mkdir_method.call_count == 2
    # 檢查呼叫的參數 (parents=True, exist_ok=True)
    # 由於 mock_mkdir_method 會被不同的 Path 實例（即 parent 目錄）呼叫，
    # 我們可以檢查 calls 列表中的參數
    calls = [call(parents=True, exist_ok=True), call(parents=True, exist_ok=True)]
    mock_mkdir_method.assert_has_calls(calls, any_order=False) # 順序可能重要，取決於程式碼

    mock_mm_init.assert_called_once_with(db_path=mock_config_dict["database"]["manifest_db_path"])
    mock_rll_init.assert_called_once_with(db_path=mock_config_dict["database"]["raw_lake_db_path"])

@pytest.mark.parametrize(
    "invalid_config_dict, expected_error_msg_part",
    [
        (None, "Configuration could not be loaded."),
        ({}, "Configuration could not be loaded."), # An empty dict is False in boolean context
        ({"database": {}}, "Manifest DB path not found in configuration."),
        ({"database": {"manifest_db_path": "dummy_manifest.db"}}, "Input directory path not found in configuration."),
        ({"database": {"manifest_db_path": "dummy_manifest.db"}, "paths": {}}, "Input directory path not found in configuration."),
        ({ # Missing raw_lake_db_path
            "database": {"manifest_db_path": "dummy_manifest.db"},
            "paths": {"input_directory": "dummy_input"}
         }, "Raw Lake DB path (raw_lake_db_path) not found in configuration."),
    ]
)
def test_pipeline_initialization_raises_value_error_on_missing_keys(
    monkeypatch, tmp_path, invalid_config_dict, expected_error_msg_part
):
    """測試 IngestionPipeline 初始化時，若設定檔缺少關鍵鍵，會引發 ValueError。"""

    # Mock load_config to return the parameterized invalid config
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.load_config", lambda x: invalid_config_dict)

    # Mock pathlib.Path.mkdir as it might be called if some paths are present
    mock_mkdir_method = MagicMock()
    monkeypatch.setattr(pathlib.Path, "mkdir", mock_mkdir_method)

    # Mock ManifestManager and RawLakeLoader __init__ as they might be called if initialization proceeds partially
    mock_mm_init = MagicMock()
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.ManifestManager", mock_mm_init)
    mock_rll_init = MagicMock()
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.RawLakeLoader", mock_rll_init)

    with pytest.raises(ValueError) as excinfo:
        IngestionPipeline(config_path="dummy_config.yaml")

    assert expected_error_msg_part in str(excinfo.value)

def test_run_handles_file_not_found_during_scan(monkeypatch, tmp_path, capsys):
    """測試在掃描過程中 FileScanner.scan_directory 拋出 FileNotFoundError 時，pipeline.run 能正確處理。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db")
        },
        "paths": {"input_directory": str(tmp_path / "input")}
    }
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())

    # Mock ManifestManager and RawLakeLoader
    mock_mm_instance = MagicMock()
    mock_mm_init = MagicMock(return_value=mock_mm_instance)
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.ManifestManager", mock_mm_init)

    mock_rll_instance = MagicMock()
    mock_rll_init = MagicMock(return_value=mock_rll_instance)
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.RawLakeLoader", mock_rll_init)

    # Mock FileScanner.scan_directory to raise FileNotFoundError
    def mock_scan_directory_raises_fnf(directory_path):
        raise FileNotFoundError("Simulated FileScanner.scan_directory error")
        yield # This makes it a generator, which scan_directory is

    monkeypatch.setattr("src.sp_data_v16.ingestion.scanner.FileScanner.scan_directory", mock_scan_directory_raises_fnf)

    pipeline = IngestionPipeline(config_path="dummy_config.yaml")
    pipeline.run()

    captured = capsys.readouterr()
    assert "Error during scanning: Simulated FileScanner.scan_directory error" in captured.out
    assert "Ingestion process aborted." in captured.out
    # 確保 ManifestManager 和 RawLakeLoader 的 close 被呼叫
    mock_mm_instance.close.assert_called_once()
    mock_rll_instance.close.assert_called_once()

def test_run_handles_unexpected_exception_during_scan(monkeypatch, tmp_path, capsys):
    """測試在掃描過程中 FileScanner.scan_directory 拋出通用 Exception 時，pipeline.run 能正確處理。"""
    mock_config_dict = {
        "database": {
            "manifest_db_path": str(tmp_path / "manifest.db"),
            "raw_lake_db_path": str(tmp_path / "raw_lake.db")
        },
        "paths": {"input_directory": str(tmp_path / "input")}
    }
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.load_config", lambda x: mock_config_dict)
    monkeypatch.setattr(pathlib.Path, "mkdir", MagicMock())

    # Mock ManifestManager and RawLakeLoader
    mock_mm_instance = MagicMock()
    mock_mm_init = MagicMock(return_value=mock_mm_instance)
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.ManifestManager", mock_mm_init)

    mock_rll_instance = MagicMock()
    mock_rll_init = MagicMock(return_value=mock_rll_instance)
    monkeypatch.setattr("src.sp_data_v16.ingestion.pipeline.RawLakeLoader", mock_rll_init)

    # Mock FileScanner.scan_directory to raise a generic Exception
    def mock_scan_directory_raises_exception(directory_path):
        raise Exception("Simulated generic error in FileScanner.scan_directory")
        yield # This makes it a generator

    monkeypatch.setattr("src.sp_data_v16.ingestion.scanner.FileScanner.scan_directory", mock_scan_directory_raises_exception)

    pipeline = IngestionPipeline(config_path="dummy_config.yaml")
    pipeline.run()

    captured = capsys.readouterr()
    assert "An unexpected error occurred during the ingestion run: Simulated generic error in FileScanner.scan_directory" in captured.out
    assert "Ingestion process aborted." in captured.out
    # 確保 ManifestManager 和 RawLakeLoader 的 close 被呼叫
    mock_mm_instance.close.assert_called_once()
    mock_rll_instance.close.assert_called_once()
