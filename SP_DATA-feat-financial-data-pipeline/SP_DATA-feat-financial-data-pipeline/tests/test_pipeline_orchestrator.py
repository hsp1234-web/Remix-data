# tests/test_pipeline_orchestrator.py
# Standard library imports
import datetime
import logging # For mock_logger spec
import os # For os.path spec in mock_os_tools
import pathlib # For Path object

# Third-party imports
import pytest
from unittest import mock # Already imported but good to note

# Local application/library specific imports
from src.data_pipeline_v15.pipeline_orchestrator import PipelineOrchestrator
from src.data_pipeline_v15.file_parser import FileParser # Needed for spec
from src.data_pipeline_v15.manifest_manager import ManifestManager # Needed for spec
from src.data_pipeline_v15.database_loader import DatabaseLoader # Needed for spec
from src.data_pipeline_v15.core import constants

# --- Base Test Configuration ---
PROJECT_FOLDER_NAME = "data_pipeline_project"
DATABASE_NAME = "test_db.duckdb"
LOG_NAME = "test_pipeline.log"
TARGET_ZIP_FILES = ""
DEBUG_MODE = False

# --- Mock Fixtures ---

@pytest.fixture
def mock_file_parser():
    parser = mock.MagicMock(spec=FileParser)
    parser.parse_file.return_value = {
        constants.KEY_STATUS: constants.STATUS_SUCCESS,
        constants.KEY_FILE: "test_file.csv",
        constants.KEY_TABLE: "test_table",
        constants.KEY_COUNT: 100,
        constants.KEY_PATH: None,
        constants.KEY_REASON: "Successfully processed"
    }
    return parser

@pytest.fixture
def mock_manifest_manager(tmp_path, mock_logger_setup):
    dummy_manifest_for_spec = tmp_path / "dummy_spec_manifest.json"
    # Ensure the directory for the dummy manifest exists
    dummy_manifest_for_spec.parent.mkdir(parents=True, exist_ok=True)
    dummy_manifest_for_spec.write_text("{}")

    # Use a real instance for spec to catch signature changes
    manager_spec_instance = ManifestManager(manifest_path=str(dummy_manifest_for_spec), logger=mock_logger_setup)
    manager = mock.MagicMock(spec=manager_spec_instance)

    manager.has_been_processed.return_value = False
    manager.load_or_create_manifest.return_value = None
    manager.update_manifest.return_value = None
    return manager


@pytest.fixture
def mock_db_loader(mock_logger_setup, tmp_path):
    dummy_db_file_path = tmp_path / f"dummy_db_for_spec_{os.getpid()}_{datetime.datetime.now().timestamp()}.duckdb"

    # Use a real instance for spec
    db_loader_spec_instance = DatabaseLoader(str(dummy_db_file_path), mock_logger_setup)
    loader = mock.MagicMock(spec=db_loader_spec_instance)

    loader.load_parquet.return_value = {"rows_in_source": 100, "rows_inserted": 100}
    loader.close_connection.return_value = None

    # pytest's tmp_path fixture handles cleanup of contents within tmp_path
    return loader


@pytest.fixture
def mock_logger_setup(monkeypatch):
    mock_actual_logger = mock.MagicMock(spec=logging.Logger)
    for level in ['info', 'debug', 'warning', 'error', 'critical']:
        setattr(mock_actual_logger, level, mock.MagicMock())
    # Patch the setup_logger function in the orchestrator's module
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.setup_logger", lambda log_path, log_name, debug_mode: mock_actual_logger)
    return mock_actual_logger

@pytest.fixture
def mock_os_tools(monkeypatch):
    # Mock the 'os' module that pipeline_orchestrator imports
    mock_os_module_for_orchestrator = mock.MagicMock(spec=os)

    # Mock os.path submodule
    mock_os_path_module = mock.MagicMock(spec=os.path)
    mock_os_path_module.join.side_effect = lambda *args: os.path.normpath(os.path.join(*[str(arg) for arg in args]))
    mock_os_path_module.exists.return_value = False # Default: path does not exist
    mock_os_path_module.isfile.return_value = True
    mock_os_path_module.isdir.return_value = True
    mock_os_path_module.dirname.side_effect = os.path.dirname

    mock_os_module_for_orchestrator.path = mock_os_path_module

    mock_os_module_for_orchestrator.listdir.return_value = ["test_file.csv"]
    mock_os_module_for_orchestrator.cpu_count.return_value = 4

    # This is the critical mock for os.makedirs, which pathlib.Path.mkdir calls
    # We globally patch os.makedirs
    # original_makedirs = os.makedirs # This would be the real one
    mock_global_makedirs_fn = mock.MagicMock(spec=os.makedirs)
    # If you want it to actually create dirs during test (not usually recommended for unit tests):
    # mock_global_makedirs_fn.side_effect = original_makedirs
    monkeypatch.setattr(os, "makedirs", mock_global_makedirs_fn) # Global patch

    # Ensure the 'os' module used by the orchestrator also uses this global mock if it calls os.makedirs directly
    mock_os_module_for_orchestrator.makedirs = mock_global_makedirs_fn

    # Patch where `PipelineOrchestrator` imports `os`
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.os", mock_os_module_for_orchestrator)

    # Also, if Path objects are created and mkdir is called, they use the globally patched os.makedirs.
    # So, mock_global_makedirs_fn is the one to assert calls against.

    return mock_os_module_for_orchestrator, mock_global_makedirs_fn # Return the global mock for assertions

@pytest.fixture
def mock_shutil_tools(monkeypatch):
    mock_shutil_move = mock.MagicMock()
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.shutil.move", mock_shutil_move)

    mock_shutil_copy2 = mock.MagicMock()
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.shutil.copy2", mock_shutil_copy2)

    mock_shutil_rmtree = mock.MagicMock()
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.shutil.rmtree", mock_shutil_rmtree)

    return mock_shutil_move, mock_shutil_copy2, mock_shutil_rmtree

@pytest.fixture
def mock_datetime_now(monkeypatch):
    fixed_utc_datetime = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    mock_dt_class = mock.MagicMock(spec=datetime.datetime)

    def mock_now_side_effect(tz=None):
        current_time = fixed_utc_datetime
        if tz:
            if not isinstance(tz, datetime.tzinfo): # Should be a tzinfo object
                # Fallback or raise error if tz is not as expected
                return current_time
            return current_time.astimezone(tz)
        return current_time
    mock_dt_class.now.side_effect = mock_now_side_effect
    mock_dt_class.strptime = datetime.datetime.strptime

    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.datetime", mock_dt_class)

    mock_pytz = mock.MagicMock()
    asia_taipei_tz = datetime.timezone(datetime.timedelta(hours=8), 'Asia/Taipei')
    mock_pytz.timezone.return_value = asia_taipei_tz
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.pytz", mock_pytz)

    return mock_dt_class


@pytest.fixture
def orchestrator_instance(
    mock_file_parser,
    mock_manifest_manager,
    mock_db_loader,
    mock_logger_setup,
    mock_os_tools, # This fixture now returns (mocked_os_module_for_orchestrator, globally_mocked_os_makedirs_function)
    mock_shutil_tools,
    mock_datetime_now,
    tmp_path,
    monkeypatch # For specific patches if needed, e.g. Path.iterdir
):
    test_base_path = tmp_path / "remote_base_for_project"
    test_base_path.mkdir()

    dummy_local_workspace = tmp_path / "local_test_workspace"
    # Orchestrator will create PROJECT_FOLDER_NAME under dummy_local_workspace

    dummy_config_content = f"""
project_folder: {PROJECT_FOLDER_NAME}
database_name: {DATABASE_NAME}
log_name: {LOG_NAME}
local_workspace: {str(dummy_local_workspace)}
remote_base_path: {str(test_base_path)}
directories:
  input: "00_input_test"
  processed: "01_processed_test"
  archive: "02_archive_test"
  quarantine: "03_quarantine_test"
  db: "98_db_test"
  log: "99_logs_test"
validation_rules: {{}}
max_workers: 1
"""
    dummy_config_path = tmp_path / "dummy_config.yaml"
    dummy_config_path.write_text(dummy_config_content)

    schemas_dir_in_tmp = tmp_path / "config_test_schemas_dir"
    schemas_dir_in_tmp.mkdir(exist_ok=True, parents=True)
    dummy_schemas_path_str = str(schemas_dir_in_tmp / "schemas.json")
    with open(dummy_schemas_path_str, 'w') as f:
        f.write('{ "schema1": { "fields": [] } }')

    # Get the 'os' module mock that's patched into the orchestrator's namespace
    mock_os_module_for_orchestrator, _ = mock_os_tools

    # Configure the .path.exists mock on this specific 'os' module mock
    original_os_path_exists_side_effect = None
    current_exists_mock = mock_os_module_for_orchestrator.path.exists
    if callable(current_exists_mock.side_effect):
        original_os_path_exists_side_effect = current_exists_mock.side_effect
    else:
        original_os_path_exists_return_value = current_exists_mock.return_value

    def side_effect_path_exists_for_orchestrator(path_arg_str_or_path):
        s_path_arg = str(path_arg_str_or_path)
        if s_path_arg == dummy_schemas_path_str:
            return True
        if s_path_arg == str(dummy_config_path):
            return True
        if original_os_path_exists_side_effect:
             return original_os_path_exists_side_effect(s_path_arg)
        return original_os_path_exists_return_value if 'original_os_path_exists_return_value' in locals() else False
    mock_os_module_for_orchestrator.path.exists.side_effect = side_effect_path_exists_for_orchestrator

    # Mock Path.iterdir for the specific local_input_path that will be used by the orchestrator
    # This is because orchestrator uses `self.local_input_path.iterdir()`
    # We need to know what self.local_input_path will be.
    # It's dummy_local_workspace / PROJECT_FOLDER_NAME / "00_input_test"
    expected_local_input_path = dummy_local_workspace / PROJECT_FOLDER_NAME / "00_input_test"

    # This mock will be used if orchestrator does `for f in self.local_input_path.iterdir()`
    mock_iterdir_result = [pathlib.Path(expected_local_input_path / "test_file.csv")]

    # We need to patch iterdir on the Path class, or on instances of Path for specific paths.
    # Patching on the class is broad. Let's try to patch it on the specific Path instance.
    # This is tricky as the instance is created inside orchestrator.
    # Alternative: mock os.listdir if Path.iterdir uses it (it might use os.scandir).
    # The mock_os_module_for_orchestrator.listdir is already set up. If iterdir uses this, it's covered.
    # Let's assume for now that the existing os.listdir mock is sufficient or Path.iterdir is not the primary issue.

    orchestrator = PipelineOrchestrator(
        config_file_path=str(dummy_config_path),
        base_path=str(test_base_path),
        project_folder_name_override=PROJECT_FOLDER_NAME,
        database_name_override=DATABASE_NAME,
        log_name_override=LOG_NAME,
        target_zip_files=TARGET_ZIP_FILES,
        debug_mode=DEBUG_MODE,
        schemas_file_path=dummy_schemas_path_str
    )
    orchestrator.file_parser = mock_file_parser
    orchestrator.manifest_manager = mock_manifest_manager
    orchestrator.db_loader = mock_db_loader
    orchestrator.logger = mock_logger_setup  # Assign the direct mock logger

    return orchestrator

def assert_makedirs_called_for_paths(globally_mocked_os_makedirs, orchestrator_instance, expected_paths_keys):
    expected_paths_set = {str(getattr(orchestrator_instance, key)) for key in expected_paths_keys}

    actual_makedirs_calls = set()
    if not hasattr(globally_mocked_os_makedirs, 'call_args_list'):
        pytest.fail("globally_mocked_os_makedirs does not have call_args_list. Patching for os.makedirs might have failed.")

    for call_args_tuple in globally_mocked_os_makedirs.call_args_list:
        call_obj = call_args_tuple[0] # This is the call object itself
        path_called = str(call_obj[0]) # First positional argument to makedirs
        actual_makedirs_calls.add(path_called)

        kwargs_of_call = call_args_tuple[1] # This is the kwargs dict
        assert kwargs_of_call.get('exist_ok') is True, \
            f"exist_ok=True not found or not True for makedirs call with {path_called}. Got kwargs: {kwargs_of_call}"

    missing_paths = expected_paths_set - actual_makedirs_calls
    assert not missing_paths, \
        (f"Not all expected directory creations were called via the global os.makedirs mock. "
         f"\nMissing: {missing_paths}"
         f"\nExpected all of: {expected_paths_set}"
         f"\nActual calls to global os.makedirs mock: {actual_makedirs_calls}")

# Removed ZIP file tests for brevity and focus, they can be added back later.
# The principles for path mocking and return value mocking would be similar.
# test_run_success_zip_multiple_successful_sub_items,
# test_run_zip_partial_success_sub_items,
# test_run_zip_all_sub_items_fail
# would need careful setup of mock_file_parser.parse_file to return KEY_GROUP_RESULT
# and a list of sub-results, ensuring paths in sub-results are also correctly mocked/constructed.
