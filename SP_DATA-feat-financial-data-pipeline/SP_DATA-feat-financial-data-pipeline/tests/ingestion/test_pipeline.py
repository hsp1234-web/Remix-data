import pytest
from unittest.mock import MagicMock, call, patch
from pathlib import Path
import hashlib
import logging

# 被測模組
from taifex_pipeline.ingestion.pipeline import IngestionPipeline
from taifex_pipeline.database.db_manager import DBManager # 用於 type hinting

# 設定一個測試用的 logger，避免影響到標準輸出或其他測試的 logger 設定
# 在 conftest.py 中可能有更通用的 logger 設定方式
# 這裡為了獨立性，簡單設定一下
logger = logging.getLogger("test_ingestion_pipeline")
logger.addHandler(logging.NullHandler()) # 避免 "No handler found" 警告


# --- Helper Functions ---
def create_dummy_file(directory: Path, filename: str, content: str = "dummy content") -> Path:
    """在指定目錄建立一個虛擬檔案並返回其路徑。"""
    file_path = directory / filename
    file_path.write_text(content, encoding='utf-8')
    return file_path

def calculate_sha256_str(content: str) -> str:
    """計算字串內容的 SHA256 雜湊值。"""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

# --- Fixtures ---

@pytest.fixture
def mock_db_manager() -> MagicMock:
    """提供一個 DBManager 的 MagicMock 實例。"""
    mock = MagicMock(spec=DBManager)
    # 預設 check_hash_exists 返回 False (表示檔案不存在)
    mock.check_hash_exists.return_value = False
    return mock

@pytest.fixture
def ingestion_pipeline(mock_db_manager: MagicMock, tmp_path: Path) -> IngestionPipeline:
    """提供一個使用 mock DBManager 和臨時來源目錄的 IngestionPipeline 實例。"""
    source_dir = tmp_path / "source_data"
    source_dir.mkdir()
    return IngestionPipeline(db_manager=mock_db_manager, source_directory=str(source_dir))

# --- Test Cases ---

class TestIngestionPipeline:

    def test_init_success(self, mock_db_manager: MagicMock, tmp_path: Path):
        """測試 IngestionPipeline 成功初始化。"""
        source_dir = tmp_path / "test_src"
        source_dir.mkdir()
        pipeline = IngestionPipeline(db_manager=mock_db_manager, source_directory=str(source_dir))
        assert pipeline.db_manager == mock_db_manager
        assert pipeline.source_directory == source_dir

    def test_init_db_manager_none_raises_value_error(self, tmp_path: Path):
        """測試 DBManager 為 None 時引發 ValueError。"""
        source_dir = tmp_path / "test_src"
        source_dir.mkdir()
        with pytest.raises(ValueError, match="DBManager 實例不能為 None。"):
            IngestionPipeline(db_manager=None, source_directory=str(source_dir))

    def test_init_source_dir_empty_raises_value_error(self, mock_db_manager: MagicMock):
        """測試來源目錄為空字串時引發 ValueError。"""
        with pytest.raises(ValueError, match="來源目錄路徑不能為空。"):
            IngestionPipeline(db_manager=mock_db_manager, source_directory="")

    def test_init_source_dir_not_exists_raises_file_not_found(self, mock_db_manager: MagicMock, tmp_path: Path):
        """測試來源目錄不存在時引發 FileNotFoundError。"""
        non_existent_dir = tmp_path / "non_existent"
        with pytest.raises(FileNotFoundError, match=f"指定的來源目錄不存在或不是一個目錄: {non_existent_dir}"):
            IngestionPipeline(db_manager=mock_db_manager, source_directory=str(non_existent_dir))

    def test_run_empty_directory(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試來源目錄為空時，不應有任何資料庫操作。"""
        ingestion_pipeline.run()

        mock_db_manager.check_hash_exists.assert_not_called()
        mock_db_manager.store_raw_file.assert_not_called()
        mock_db_manager.add_manifest_record.assert_not_called()
        # 可以進一步檢查 logger 的輸出，確認掃描檔案數為 0

    def test_run_single_new_file(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試處理單一個新檔案。"""
        file_content = "This is a new file."
        file_path = create_dummy_file(ingestion_pipeline.source_directory, "new_file.txt", file_content)
        file_hash = calculate_sha256_str(file_content)
        file_size = file_path.stat().st_size

        ingestion_pipeline.run()

        mock_db_manager.check_hash_exists.assert_called_once_with(file_hash)
        mock_db_manager.store_raw_file.assert_called_once_with(file_hash, file_content.encode('utf-8'))
        mock_db_manager.add_manifest_record.assert_called_once_with(
            file_hash=file_hash,
            original_path=str(file_path.resolve()),
            file_size_bytes=file_size,
            source_system="IngestionPipeline"
        )

    def test_run_single_existing_file(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試處理單一個已存在的檔案 (應被跳過)。"""
        file_content = "This is an existing file."
        file_path = create_dummy_file(ingestion_pipeline.source_directory, "existing_file.txt", file_content)
        file_hash = calculate_sha256_str(file_content)

        # 模擬檔案已存在於資料庫
        mock_db_manager.check_hash_exists.return_value = True

        ingestion_pipeline.run()

        mock_db_manager.check_hash_exists.assert_called_once_with(file_hash)
        mock_db_manager.store_raw_file.assert_not_called()
        mock_db_manager.add_manifest_record.assert_not_called()

    def test_run_mixed_files_new_and_existing(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試處理混合新舊檔案的目錄。"""
        # 新檔案
        new_file_content = "Content of new file."
        new_file_path = create_dummy_file(ingestion_pipeline.source_directory, "new.txt", new_file_content)
        new_file_hash = calculate_sha256_str(new_file_content)
        new_file_size = new_file_path.stat().st_size

        # 已存在的檔案
        existing_file_content = "Content of existing file."
        create_dummy_file(ingestion_pipeline.source_directory, "existing.txt", existing_file_content)
        existing_file_hash = calculate_sha256_str(existing_file_content)

        # 設定 mock 行為：new_file 不存在，existing_file 存在
        def check_hash_side_effect(h):
            if h == new_file_hash:
                return False
            if h == existing_file_hash:
                return True
            return False # 預設
        mock_db_manager.check_hash_exists.side_effect = check_hash_side_effect

        ingestion_pipeline.run()

        # 驗證 check_hash_exists 被呼叫兩次，參數正確
        mock_db_manager.check_hash_exists.assert_any_call(new_file_hash)
        mock_db_manager.check_hash_exists.assert_any_call(existing_file_hash)
        assert mock_db_manager.check_hash_exists.call_count == 2

        # 驗證只有新檔案被處理
        mock_db_manager.store_raw_file.assert_called_once_with(new_file_hash, new_file_content.encode('utf-8'))
        mock_db_manager.add_manifest_record.assert_called_once_with(
            file_hash=new_file_hash,
            original_path=str(new_file_path.resolve()),
            file_size_bytes=new_file_size,
            source_system="IngestionPipeline"
        )

    def test_run_file_read_error(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試當讀取檔案內容失敗時，管線能正確處理並繼續。"""
        file_path = create_dummy_file(ingestion_pipeline.source_directory, "problem_file.txt", "content")
        file_hash = calculate_sha256_str("content")

        # 模擬 open() 拋出異常
        # 需要 mock 'builtins.open'，因為它是 pipeline.py 中使用的 open
        with patch('builtins.open', side_effect=IOError("Simulated read error")):
            ingestion_pipeline.run()

        # check_hash_exists 仍然會被呼叫 (因為雜湊計算在讀取之前，但_calculate_sha256裡的open會失敗)
        # 實際上 _calculate_sha256 裡的 open 會先失敗
        # 讓我們調整一下，假設 _calculate_sha256 成功，但在 run() 中的 open 失敗
        # 或者，更簡單的是，讓 _calculate_sha256 本身拋出錯誤

        # 重設 mock 和 pipeline 以進行更精確的 mock
        source_dir = ingestion_pipeline.source_directory
        pipeline = IngestionPipeline(db_manager=mock_db_manager, source_directory=str(source_dir))
        file_path_again = source_dir / "problem_file.txt" # 檔案已由 fixture 建立

        # 模擬 _calculate_sha256 成功，但後續的 open(file_path, "rb") 失敗
        mock_db_manager.check_hash_exists.return_value = False # 假設是新檔案

        # Mock open in the context of IngestionPipeline's run method's specific read
        # This is tricky because _calculate_sha256 also opens the file.
        # Let's mock the read operation within the run method itself.
        # For simplicity, let's assume _calculate_sha256 works, but the f.read() in run() fails.

        # A better way: mock the `read_bytes` method of Path object if we were using it.
        # Or, more directly, mock the `open` call within the `run` method's scope for reading content.

        # Let's assume _calculate_sha256 fails due to read error
        with patch.object(pipeline, '_calculate_sha256', side_effect=IOError("Simulated SHA calc read error")):
            pipeline.run()

        mock_db_manager.check_hash_exists.assert_not_called() # 因為在計算雜湊時就失敗了
        mock_db_manager.store_raw_file.assert_not_called()
        mock_db_manager.add_manifest_record.assert_not_called()
        # 這裡可以檢查 logger 是否記錄了錯誤

    def test_run_store_raw_file_fails(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試當 db_manager.store_raw_file 失敗時，管線的行為。"""
        file_content = "content for store_raw_file fail test"
        file_path = create_dummy_file(ingestion_pipeline.source_directory, "store_fail.txt", file_content)
        file_hash = calculate_sha256_str(file_content)

        mock_db_manager.check_hash_exists.return_value = False # 是新檔案
        mock_db_manager.store_raw_file.side_effect = Exception("DB store_raw_file error")

        ingestion_pipeline.run()

        mock_db_manager.check_hash_exists.assert_called_once_with(file_hash)
        mock_db_manager.store_raw_file.assert_called_once_with(file_hash, file_content.encode('utf-8'))
        mock_db_manager.add_manifest_record.assert_not_called() # 因為 store_raw_file 失敗了
        # 檢查 logger 是否記錄了錯誤

    def test_run_add_manifest_record_fails(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試當 db_manager.add_manifest_record 失敗時，管線的行為。"""
        file_content = "content for add_manifest_record fail test"
        file_path = create_dummy_file(ingestion_pipeline.source_directory, "manifest_fail.txt", file_content)
        file_hash = calculate_sha256_str(file_content)
        file_size = file_path.stat().st_size

        mock_db_manager.check_hash_exists.return_value = False # 是新檔案
        mock_db_manager.store_raw_file.return_value = None # store_raw_file 成功
        mock_db_manager.add_manifest_record.side_effect = Exception("DB add_manifest_record error")

        ingestion_pipeline.run()

        mock_db_manager.check_hash_exists.assert_called_once_with(file_hash)
        mock_db_manager.store_raw_file.assert_called_once_with(file_hash, file_content.encode('utf-8'))
        mock_db_manager.add_manifest_record.assert_called_once_with(
            file_hash=file_hash,
            original_path=str(file_path.resolve()),
            file_size_bytes=file_size,
            source_system="IngestionPipeline"
        )
        # 檢查 logger 是否記錄了錯誤

    def test_run_with_subdirectories(self, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試管線是否能處理子目錄中的檔案。"""
        source_dir = ingestion_pipeline.source_directory
        sub_dir = source_dir / "subdir1"
        sub_dir.mkdir()

        file1_content = "file in root"
        file1_path = create_dummy_file(source_dir, "file1.txt", file1_content)
        file1_hash = calculate_sha256_str(file1_content)
        file1_size = file1_path.stat().st_size

        file2_content = "file in subdir"
        file2_path = create_dummy_file(sub_dir, "file2.txt", file2_content)
        file2_hash = calculate_sha256_str(file2_content)
        file2_size = file2_path.stat().st_size

        # 模擬所有檔案都是新的
        mock_db_manager.check_hash_exists.return_value = False

        ingestion_pipeline.run()

        assert mock_db_manager.check_hash_exists.call_count == 2
        mock_db_manager.check_hash_exists.assert_any_call(file1_hash)
        mock_db_manager.check_hash_exists.assert_any_call(file2_hash)

        assert mock_db_manager.store_raw_file.call_count == 2
        mock_db_manager.store_raw_file.assert_any_call(file1_hash, file1_content.encode('utf-8'))
        mock_db_manager.store_raw_file.assert_any_call(file2_hash, file2_content.encode('utf-8'))

        assert mock_db_manager.add_manifest_record.call_count == 2
        mock_db_manager.add_manifest_record.assert_any_call(
            file_hash=file1_hash,
            original_path=str(file1_path.resolve()),
            file_size_bytes=file1_size,
            source_system="IngestionPipeline"
        )
        mock_db_manager.add_manifest_record.assert_any_call(
            file_hash=file2_hash,
            original_path=str(file2_path.resolve()),
            file_size_bytes=file2_size,
            source_system="IngestionPipeline"
        )

    @patch('taifex_pipeline.ingestion.pipeline.logger') # Mock logger in pipeline.py
    def test_logging_summary(self, mock_pipeline_logger: MagicMock, ingestion_pipeline: IngestionPipeline, mock_db_manager: MagicMock):
        """測試管線結束時是否記錄正確的摘要資訊。"""
        # 1 個新檔案
        new_content = "new"
        new_path = create_dummy_file(ingestion_pipeline.source_directory, "new.txt", new_content)
        new_hash = calculate_sha256_str(new_content)

        # 1 個已存在檔案
        existing_content = "existing"
        create_dummy_file(ingestion_pipeline.source_directory, "existing.txt", existing_content)
        existing_hash = calculate_sha256_str(existing_content)

        # 1 個讀取失敗的檔案 (模擬 _calculate_sha256 失敗)
        fail_calc_path = create_dummy_file(ingestion_pipeline.source_directory, "fail_calc.txt", "fail_calc")

        # 1 個 store_raw_file 失敗的檔案
        fail_store_content = "fail_store"
        fail_store_path = create_dummy_file(ingestion_pipeline.source_directory, "fail_store.txt", fail_store_content)
        fail_store_hash = calculate_sha256_str(fail_store_content)

        # 1 個 add_manifest_record 失敗的檔案
        fail_manifest_content = "fail_manifest"
        fail_manifest_path = create_dummy_file(ingestion_pipeline.source_directory, "fail_manifest.txt", fail_manifest_content)
        fail_manifest_hash = calculate_sha256_str(fail_manifest_content)


        def check_hash_side_effect(h):
            if h == new_hash: return False
            if h == existing_hash: return True
            if h == fail_store_hash: return False
            if h == fail_manifest_hash: return False
            return False
        mock_db_manager.check_hash_exists.side_effect = check_hash_side_effect

        def store_raw_file_side_effect(h, content):
            if h == fail_store_hash:
                raise Exception("Simulated store_raw_file error")
            return None
        mock_db_manager.store_raw_file.side_effect = store_raw_file_side_effect

        def add_manifest_side_effect(file_hash, original_path, file_size_bytes, source_system):
            if file_hash == fail_manifest_hash:
                raise Exception("Simulated add_manifest_record error")
            return None
        mock_db_manager.add_manifest_record.side_effect = add_manifest_side_effect


        original_calculate_sha256 = ingestion_pipeline._calculate_sha256
        def calculate_sha256_side_effect_for_logging_test(file_path: Path):
            if file_path.name == "fail_calc.txt":
                raise IOError("Simulated SHA calc read error for logging test")
            return original_calculate_sha256(file_path)

        with patch.object(ingestion_pipeline, '_calculate_sha256', side_effect=calculate_sha256_side_effect_for_logging_test):
            ingestion_pipeline.run()

        # 驗證 logger.info 的呼叫
        # 我們關心的是摘要部分的日誌
        # 預期：總共 5 個檔案被掃描
        # 新增 1 個 (new.txt)
        # 跳過 1 個 (existing.txt)
        # 失敗 3 個 (fail_calc.txt, fail_store.txt, fail_manifest.txt)

        # 擷取對 logger.info 的所有呼叫
        info_calls = [args[0] for name, args, kwargs in mock_pipeline_logger.method_calls if name == 'info']

        # for c in info_calls: # Debugging: print all info logs
        #     print(c)

        assert "--- 汲取管線執行摘要 ---" in info_calls
        assert "總共掃描檔案數: 5" in info_calls # 5 個檔案被建立
        assert "新汲取的檔案數: 1" in info_calls # 只有 new.txt 成功
        assert "因已存在而跳過的檔案數: 1" in info_calls # existing.txt
        assert "處理失敗的檔案數: 3" in info_calls # fail_calc, fail_store, fail_manifest
        assert "--- 汲取管線執行完畢 ---" in info_calls

# TODO: 更多測試
# - 測試檔案路徑包含特殊字元 (如果 OS 和 pathlib 支援)
# - 測試極大的檔案 (可能需要 mock read，而不是真的建立大檔案)
# - 測試 source_directory 是 symlink 的情況
# - 測試 discovery_timestamp 和 last_modified_at_source (如果 IngestionPipeline 未來會處理這些)
# - 測試 DBManager 的其他異常情況
