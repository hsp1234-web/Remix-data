import pytest
import json
import logging
import os
from pathlib import Path
import sys # 用於 logger_setup 中的 StreamHandler(sys.stdout)

from taifex_pipeline.core import config_loader, logger_setup
from taifex_pipeline.core.logger_setup import CustomJsonFormatter # 用於驗證 formatter 類型

# --- Fixtures ---

@pytest.fixture(autouse=True)
def clear_config_loader_cache_after_test():
    """確保每個 config_loader 測試後都清除快取"""
    yield
    config_loader.clear_config_cache()

@pytest.fixture
def temp_config_file(tmp_path: Path) -> Path:
    """建立一個位於臨時目錄的設定檔路徑"""
    return tmp_path / "format_catalog.json"

@pytest.fixture
def temp_log_file(tmp_path: Path) -> Path:
    """建立一個位於臨時目錄的日誌檔路徑"""
    log_dir = tmp_path / "logs"
    log_dir.mkdir(exist_ok=True)
    return log_dir / "test_pipeline.log"

# --- Tests for config_loader ---

class TestConfigLoader:
    def test_load_format_catalog_success(self, temp_config_file: Path):
        """測試成功讀取有效的 JSON 設定檔"""
        expected_data = {"key1": "value1", "numbers": [1, 2, 3]}
        temp_config_file.write_text(json.dumps(expected_data), encoding="utf-8")

        actual_data = config_loader.load_format_catalog(str(temp_config_file))
        assert actual_data == expected_data

    def test_load_format_catalog_file_not_found(self, temp_config_file: Path):
        """測試當設定檔不存在時拋出 FileNotFoundError"""
        # 確保檔案不存在 (temp_config_file fixture 只提供路徑，不建立檔案)
        assert not temp_config_file.exists()
        with pytest.raises(FileNotFoundError):
            config_loader.load_format_catalog(str(temp_config_file))

    def test_load_format_catalog_json_decode_error(self, temp_config_file: Path):
        """測試當設定檔內容為無效 JSON 時拋出 JSONDecodeError"""
        temp_config_file.write_text("{'invalid_json': True,,}", encoding="utf-8") # 無效 JSON
        with pytest.raises(json.JSONDecodeError):
            config_loader.load_format_catalog(str(temp_config_file))

    def test_load_format_catalog_caching(self, temp_config_file: Path, mocker):
        """測試快取機制"""
        initial_data = {"version": 1, "data": "initial"}
        temp_config_file.write_text(json.dumps(initial_data), encoding="utf-8")

        # Mock os.path.exists and open to ensure they are called only once
        # 注意：需要 mock 'taifex_pipeline.core.config_loader.open' 而不是內建的 'open'
        # 同樣地，mock 'taifex_pipeline.core.config_loader.os.path.exists'
        mocked_exists = mocker.patch("taifex_pipeline.core.config_loader.os.path.exists")
        mocked_exists.return_value = True # 假設檔案存在

        # 使用 mocker.patch 對 'builtins.open' 或特定模組的 'open' 進行 mock
        # 因為 open 是內建函式，通常 mock 'builtins.open'
        # 但如果 config_loader.py 中用 import open，則 mock config_loader.open
        # 目前它是直接用 open()，所以 mock 'taifex_pipeline.core.config_loader.open'
        mocked_open = mocker.patch("builtins.open", new_callable=mocker.mock_open, read_data=json.dumps(initial_data))

        # 第一次呼叫，應讀取檔案
        data1 = config_loader.load_format_catalog(str(temp_config_file))
        assert data1 == initial_data
        mocked_exists.assert_called_once_with(str(temp_config_file))
        mocked_open.assert_called_once_with(str(temp_config_file), 'r', encoding='utf-8')

        # 重設 mock counters 以便下次檢查
        mocked_exists.reset_mock()
        mocked_open.reset_mock()

        # 第二次呼叫，應從快取讀取
        data2 = config_loader.load_format_catalog(str(temp_config_file))
        assert data2 == initial_data
        assert data1 is data2, "第二次呼叫應返回與第一次相同的物件 (來自快取)"

        # 斷言檔案系統相關函式未被再次呼叫
        mocked_exists.assert_not_called()
        mocked_open.assert_not_called()

    def test_clear_config_cache(self, temp_config_file: Path, mocker):
        """測試清除快取後會重新讀取檔案"""
        data_v1 = {"version": 1}
        temp_config_file.write_text(json.dumps(data_v1), encoding="utf-8")

        mocked_open_v1 = mocker.patch("builtins.open", new_callable=mocker.mock_open, read_data=json.dumps(data_v1))
        mocker.patch("taifex_pipeline.core.config_loader.os.path.exists", return_value=True)

        # 第一次讀取
        config1 = config_loader.load_format_catalog(str(temp_config_file))
        assert config1 == data_v1
        mocked_open_v1.assert_called_once()

        # 清除快取
        config_loader.clear_config_cache()

        # 修改檔案內容 (模擬檔案在兩次讀取間被外部修改)
        data_v2 = {"version": 2}
        # 為了讓第二次 load_format_catalog 讀到新內容，我們需要讓 mock_open 返回新內容
        # 這裡我們重新 mock open，或者修改現有 mock 的 read_data (如果 mock_open 支援)
        # 簡單起見，重新 mock
        mocked_open_v2 = mocker.patch("builtins.open", new_callable=mocker.mock_open, read_data=json.dumps(data_v2))

        # 第二次讀取 (快取清除後)
        config2 = config_loader.load_format_catalog(str(temp_config_file))
        assert config2 == data_v2
        mocked_open_v2.assert_called_once() # 驗證 open 被再次呼叫

# --- Tests for logger_setup ---

class TestLoggerSetup:
    def test_setup_logger_creation(self, temp_log_file: Path):
        """測試 logger 物件的成功建立"""
        logger_name = "test_app"
        logger = logger_setup.setup_logger(logger_name, log_file_path=str(temp_log_file))

        assert isinstance(logger, logging.Logger), "應返回一個 Logger 實例"
        assert logger.name == logger_name, "Logger 名稱應與提供的一致"
        assert logger.level == logging.INFO, "預設日誌級別應為 INFO" # setup_logger 預設為 INFO

        # 清理: 移除 handlers 避免影響其他測試或產生不必要的輸出
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        # 確保日誌目錄下的檔案被清理 (如果 pytest 的 tmp_path 沒有自動處理)
        if temp_log_file.exists():
             temp_log_file.unlink()


    def test_setup_logger_dual_handlers(self, temp_log_file: Path):
        """測試 logger 是否有 console 和 file 兩個 handlers"""
        logger = logger_setup.setup_logger("dual_handler_test", log_file_path=str(temp_log_file))

        assert len(logger.handlers) == 2, "Logger 應有兩個 handlers"

        handler_types = [type(h) for h in logger.handlers]
        assert logging.StreamHandler in handler_types or sys.stdout.isatty(), "應包含一個 StreamHandler (主控台)"
        # StreamHandler 可能會根據 sys.stdout 指向的類型而變化，嚴格來說是 logging.StreamHandler

        assert any(isinstance(h, logging.handlers.RotatingFileHandler) for h in logger.handlers), \
               "應包含一個 RotatingFileHandler (檔案)"

        # 清理
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        if temp_log_file.exists():
             temp_log_file.unlink()


    def test_logger_json_output_structure_and_content(self, temp_log_file: Path):
        """測試 JSON 日誌的結構和內容"""
        logger_name = "json_logger_test"
        test_message = "這是一條JSON結構測試訊息"
        test_extra_val = "my_custom_value"

        # 在 setup_logger 之前重設可能的 execution_id，以確保此測試的 execution_id 是新的
        logger_setup._EXECUTION_ID = None
        logger = logger_setup.setup_logger(
            logger_name,
            log_level=logging.DEBUG,  # 設定為 DEBUG 以便記錄 debug 訊息
            log_file_path=str(temp_log_file)
        )

        # 取得 setup_logger 內部產生的 execution_id
        # logger_setup._EXECUTION_ID 是在 setup_logger 中設定的
        current_execution_id = logger_setup._EXECUTION_ID
        assert current_execution_id is not None

        logger.info(test_message, extra={"custom_field": test_extra_val})

        # 關閉 handlers 以確保內容寫入檔案
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler) # 移除以避免影響後續 getLogger

        # 讀取日誌檔案內容
        assert temp_log_file.exists(), "日誌檔案應已建立"
        log_content = temp_log_file.read_text(encoding="utf-8").strip()

        assert log_content, "日誌檔案不應為空"

        try:
            # 假設日誌檔案每行一個 JSON 物件
            # 我們的 logger 目前是每個記錄一個 JSON 物件在新的一行
            log_lines = log_content.splitlines()
            # 我們記錄了 setup_logger 的初始訊息，以及我們的測試訊息
            # 預期至少有兩條 INFO (setup logger 完成, 檔案日誌路徑) + 我們的測試訊息
            # 這裡我們只關注最後一條我們發送的測試訊息
            # 或者，我們可以找包含 test_message 的那一行

            log_entry = None
            for line in reversed(log_lines): # 從後往前找
                try:
                    current_entry = json.loads(line)
                    # 檢查是否是我們發送的測試訊息
                    if current_entry.get("message") == test_message and \
                       current_entry.get("module_name") == "test_core_modules": # 增加 module_name 判斷以更精準
                        log_entry = current_entry
                        break
                except json.JSONDecodeError:
                    # 忽略無法解析的行，繼續尋找
                    print(f"警告: 無法解析日誌行: {line}", file=sys.stderr) # 在測試中輸出警告
                    continue

            assert log_entry is not None, f"在日誌檔案中找不到 message 為 '{test_message}' 且 module 為 'test_core_modules' 的 JSON 記錄。\n完整日誌內容:\n{log_content}"

        except Exception as e: # 更通用的 Exception 捕捉，以防 json.loads 以外的問題
            pytest.fail(f"讀取或解析日誌時發生錯誤: {e}\nLog content:\n{log_content}")

        # 斷言必要欄位存在 (log_entry 已是解析後的字典)
        expected_fields = ["timestamp", "execution_id", "level", "module_name", "message"]
        for field in expected_fields:
            assert field in log_entry, f"JSON 日誌應包含欄位 '{field}'"

        # 斷言內容
        assert log_entry["level"] == "INFO"
        assert log_entry["message"] == test_message
        assert log_entry["module_name"] == "test_core_modules" # 測試是從這個模組執行的
        assert log_entry["execution_id"] == current_execution_id

        # 驗證 'extra' 欄位 (python-json-logger 會將 extra 的內容也放到頂層)
        assert log_entry.get("custom_field") == test_extra_val, "額外欄位 'custom_field' 未正確記錄"

        # 驗證時間戳 (格式和時區可能較複雜，這裡僅作基本檢查)
        assert isinstance(log_entry["timestamp"], str)
        # 可以嘗試解析時間戳來驗證其有效性
        try:
            from datetime import datetime
            # 我們的 CustomJsonFormatter 使用 isoformat()
            datetime.fromisoformat(log_entry["timestamp"])
        except ValueError:
            pytest.fail(f"時間戳 '{log_entry['timestamp']}' 不是有效的 ISO 格式")

        # 清理
        if temp_log_file.exists():
             temp_log_file.unlink()

    def test_logger_console_formatter(self, temp_log_file: Path):
        """測試 console handler 使用 TaipeiFormatter"""
        logger = logger_setup.setup_logger("console_formatter_test", log_file_path=str(temp_log_file))
        console_handler = None
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                console_handler = handler
                break

        assert console_handler is not None, "找不到 StreamHandler"
        assert isinstance(console_handler.formatter, logger_setup.TaipeiFormatter), \
               "Console handler 應使用 TaipeiFormatter"

        # 清理
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        if temp_log_file.exists():
             temp_log_file.unlink()

    def test_logger_file_formatter(self, temp_log_file: Path):
        """測試 file handler 使用 CustomJsonFormatter"""
        logger = logger_setup.setup_logger("file_formatter_test", log_file_path=str(temp_log_file))
        file_handler = None
        for handler in logger.handlers:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                file_handler = handler
                break

        assert file_handler is not None, "找不到 RotatingFileHandler"
        assert isinstance(file_handler.formatter, CustomJsonFormatter), \
               "File handler 應使用 CustomJsonFormatter"

        # 清理
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        if temp_log_file.exists():
             temp_log_file.unlink()

    def test_logger_execution_id_consistency(self, temp_log_file: Path):
        """測試多次取得 logger 時 execution_id 是否一致 (不重新呼叫 setup_logger)"""
        logger_name = "exec_id_consistency_test"

        # 第一次設定，取得 execution_id
        logger_setup._EXECUTION_ID = None # 重設以確保是新的
        logger1 = logger_setup.setup_logger(logger_name, log_file_path=str(temp_log_file))
        execution_id1 = logger_setup._EXECUTION_ID
        assert execution_id1 is not None

        # 第二次取得同名 logger (不應改變 execution_id)
        logger2 = logging.getLogger(logger_name)
        # 這裡我們需要一種方式從 logger2 的 handler 中提取 execution_id
        # 或者，我們信任 CustomJsonFormatter 會使用全域的 _EXECUTION_ID
        # 記錄一條訊息並檢查

        test_message = "Checking execution ID consistency"
        logger2.info(test_message)

        for handler in logger1.handlers[:]: # logger1 和 logger2 內部 handlers 應該是相同的
            handler.close()
            logger1.removeHandler(handler)

        log_content = temp_log_file.read_text(encoding="utf-8").strip()
        recorded_json_str = None
        for line in reversed(log_content.splitlines()):
            if test_message in line:
                recorded_json_str = line
                break
        assert recorded_json_str is not None
        log_entry = json.loads(recorded_json_str)

        assert log_entry["execution_id"] == execution_id1, "透過 getLogger 取得的 logger 應共享相同的 execution_id"

        # 清理
        if temp_log_file.exists():
             temp_log_file.unlink()

    def test_setup_logger_log_directory_creation(self, tmp_path: Path):
        """測試 setup_logger 是否會自動建立日誌目錄 (如果不存在)"""
        log_dir = tmp_path / "new_log_dir"
        log_file = log_dir / "app.log"

        assert not log_dir.exists(), "測試前，日誌目錄不應存在"

        logger = logger_setup.setup_logger("dir_creation_test", log_file_path=str(log_file))

        assert log_dir.exists(), "setup_logger 應已建立日誌目錄"
        assert log_dir.is_dir()

        # 清理
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        # tmp_path 會自動清理其內容
