import pytest
import yaml
import os
from src.sp_data_v16.core.config import load_config

# Define a temporary config file name for testing
TEST_CONFIG_FILENAME = "test_config_v16.yaml"
MALFORMED_CONFIG_FILENAME = "malformed_config_v16.yaml"

@pytest.fixture(scope="function")
def temp_config_file(tmp_path):
    """ Creates a temporary valid config file for testing """
    config_content = {
        "database": {
            "manifest_db_path": "data/v16/manifest.db",
            "raw_lake_db_path": "data/v16/raw_lake.db",
            "processed_db_path": "data/v16/processed_data.db"
        },
        "logging": {
            "level": "DEBUG",
            "format": "[%(asctime)s] - %(message)s"
        },
        "paths": {
            "input_directory": "TEST_DATA/01_input_files_test"
        }
    }
    config_file_path = tmp_path / TEST_CONFIG_FILENAME
    with open(config_file_path, 'w', encoding='utf-8') as f:
        yaml.dump(config_content, f)
    return config_file_path

@pytest.fixture(scope="function")
def malformed_config_file(tmp_path):
    """ Creates a temporary malformed config file for testing """
    config_file_path = tmp_path / MALFORMED_CONFIG_FILENAME
    with open(config_file_path, 'w', encoding='utf-8') as f:
        f.write("database: manifest_db_path: data/v16/manifest.db\n  raw_lake_db_path: data/v16/raw_lake.db") # Intentionally malformed
    return config_file_path

def test_load_config_success(mocker):
    """ 測試成功載入設定檔，使用 mocker 注入虛構的 YAML 內容。 """
    # 定義虛構的 YAML 內容
    mock_yaml_content = """
database:
  manifest_db_path: "mock/data/v16/manifest.db"
  raw_lake_db_path: "mock/data/v16/raw_lake.db"
  processed_db_path: "mock/data/v16/processed_data.db"
logging:
  level: "INFO"
  format: "[%(asctime)s] - %(levelname)s - %(name)s - %(message)s"
paths:
  input_directory: "mock/input_files"
"""
    # 使用 mocker.patch 來模擬 open 函數
    mocker.patch('builtins.open', mocker.mock_open(read_data=mock_yaml_content))

    # 呼叫 load_config，此時它會使用我們 mock 的 open
    # 傳入的路徑字串是什麼在這裡不重要，因為 open 被 mock 了
    config = load_config("any_dummy_path.yaml")

    # 根據 mock_yaml_content 進行斷言
    assert "database" in config, "設定檔中缺少 'database' 區塊"
    assert config["database"]["manifest_db_path"] == "mock/data/v16/manifest.db"
    assert config["database"]["raw_lake_db_path"] == "mock/data/v16/raw_lake.db"
    assert config["database"]["processed_db_path"] == "mock/data/v16/processed_data.db"
    assert "logging" in config, "設定檔中缺少 'logging' 區塊"
    assert config["logging"]["level"] == "INFO"
    assert "paths" in config, "設定檔中缺少 'paths' 區塊"
    assert config["paths"]["input_directory"] == "mock/input_files"
    assert config is not None, "設定檔載入失敗，結果為 None"


def test_load_config_file_not_found():
    """ Tests that FileNotFoundError is raised for a non-existent config file. """
    with pytest.raises(FileNotFoundError) as excinfo:
        load_config("non_existent_config_v16.yaml")
    assert "Configuration file not found at: non_existent_config_v16.yaml" in str(excinfo.value)

# Optional: Test for malformed YAML, if load_config is expected to handle it gracefully
# or if PyYAML's behavior in such cases needs to be confirmed.
# The current load_config will likely let PyYAML raise its own error (e.g., yaml.YAMLError).
def test_load_malformed_config(malformed_config_file):
    """ Tests that yaml.YAMLError (or a more specific error) is raised for a malformed config file. """
    with pytest.raises(yaml.YAMLError):
        load_config(str(malformed_config_file))

def test_load_config_permission_error(mocker):
    """測試當開啟設定檔時發生 PermissionError 的情況。"""
    # 模擬 builtins.open 在被呼叫時拋出 PermissionError
    mocker.patch('builtins.open', side_effect=PermissionError("Permission denied"))

    with pytest.raises(PermissionError, match="Permission denied"):
        load_config("any_path.yaml")
