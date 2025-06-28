import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call, ANY
from concurrent.futures import ProcessPoolExecutor # 可能不需要直接 mock ProcessPoolExecutor 內部
import importlib

# 被測模組
from taifex_pipeline.transformation.pipeline import TransformationPipeline
from taifex_pipeline.database.db_manager import DBManager # 用於 Mock 和 type hint
from taifex_pipeline.transformation.format_detector import FormatDetector # 用於 Mock 和 type hint
from taifex_pipeline.database.constants import FileStatus # 用於狀態比較

# --- Fixtures ---

@pytest.fixture
def mock_db_manager_cls():
    """提供一個 DBManager 類別的 Mock。用於在 worker 中模擬 DBManager 的創建。"""
    # 創建一個 mock 實例，當 DBManager(db_path) 被調用時返回這個 mock 實例
    mock_instance = MagicMock(spec=DBManager)
    mock_instance.db_path = ":memory:" # 讓 mock 實例也有 db_path

    # mock get_raw_file_content, load_dataframe_to_table, close
    mock_instance.get_raw_file_content.return_value = b"raw_bytes_content"
    mock_instance.load_dataframe_to_table.return_value = None
    mock_instance.close.return_value = None

    # mock get_manifest_records_by_status, update_manifest_transformation_status
    # 這些主要由主進程的 DBManager 使用
    mock_instance.get_manifest_records_by_status.return_value = []
    mock_instance.update_manifest_transformation_status.return_value = None

    mock_class = MagicMock(return_value=mock_instance)
    return mock_class


@pytest.fixture
def mock_format_detector_instance():
    """提供一個 FormatDetector 的 MagicMock 實例。"""
    mock = MagicMock(spec=FormatDetector)
    mock.try_encodings = ['utf-8'] # 給一個預設值
    return mock

@pytest.fixture
def sample_format_catalog() -> dict:
    """提供一個簡單的 format_catalog。"""
    return {
        "fingerprint1": {
            "name": "Recipe One",
            "parser_type": "csv",
            "parser_config": {"sep": ","},
            "cleaner_function": "clean_daily_ohlc", # 假設這是 cleaners.py 中的一個函式
            "target_table": "table_one",
            "load_options": {"if_exists": "append"}
        }
    }

@pytest.fixture
def sample_file_info() -> dict:
    """提供一個範例 file_info 字典。"""
    return {'file_hash': 'hash123', 'original_path': '/path/to/file.csv'}

@pytest.fixture
def sample_statuses_dict() -> dict:
    """提供一個狀態字典給 worker。"""
    return {
        'STATUS_QUARANTINED': FileStatus.QUARANTINED.value,
        'STATUS_TRANSFORMATION_FAILED': FileStatus.TRANSFORMATION_FAILED.value,
        'STATUS_TRANSFORMED_SUCCESS': FileStatus.TRANSFORMED_SUCCESS.value
    }

@pytest.fixture
def mock_cleaner_function():
    """Mock 一個清洗函式。"""
    mock_func = MagicMock(return_value=pd.DataFrame({'A': [1, 2], 'B': [3, 4]}))
    return mock_func

# --- Helper: Mock pd.read_csv (或其他 parser) ---
# 我們可能需要 mock pandas 的 read_csv 等，以避免實際的文件IO和解析邏輯
# 或者讓 _static_parse_raw_content 返回一個固定的 DataFrame

@pytest.fixture(autouse=True) # 自動使用這個 fixture，對所有測試生效
def mock_pandas_parsers(mocker):
    """Mock pandas parsing functions."""
    mock_df = pd.DataFrame({'col1': ['data1'], 'col2': ['data2']})
    mocker.patch('pandas.read_csv', return_value=mock_df.copy())
    mocker.patch('pandas.read_excel', return_value=mock_df.copy())
    mocker.patch('pandas.read_fwf', return_value=mock_df.copy())


# --- Test Cases for _process_file_worker (Static Method) ---

class TestTransformationPipelineProcessFileWorker:

    @patch('taifex_pipeline.transformation.pipeline.DBManager') # Mock DBManager class
    @patch('importlib.import_module')
    def test_process_file_worker_success(self,
                                       mock_import_module,
                                       mock_db_manager_constructor,
                                       mock_format_detector_instance,
                                       sample_format_catalog,
                                       sample_file_info,
                                       sample_statuses_dict,
                                       mock_cleaner_function):
        """測試 _process_file_worker 成功處理檔案的流程。"""
        # 設定 Mock DBManager 實例的行為
        mock_db_instance = mock_db_manager_constructor.return_value
        mock_db_instance.get_raw_file_content.return_value = b"some,csv,header\ndata,row,1"

        # 設定 Mock FormatDetector 的行為
        recipe = sample_format_catalog["fingerprint1"]
        mock_format_detector_instance.get_recipe.return_value = recipe

        # 設定 Mock importlib 和 cleaner
        mock_cleaners_module = MagicMock()
        mock_cleaners_module.clean_daily_ohlc = mock_cleaner_function
        mock_import_module.return_value = mock_cleaners_module

        # 執行 worker
        result = TransformationPipeline._process_file_worker(
            file_info,
            ":memory:",  # db_path
            mock_format_detector_instance,
            sample_format_catalog, # catalog instance
            sample_statuses_dict
        )

        # 驗證 DBManager 的創建和方法調用
        mock_db_manager_constructor.assert_called_once_with(":memory:")
        mock_db_instance.get_raw_file_content.assert_called_once_with(sample_file_info['file_hash'])

        # 驗證 FormatDetector 的調用
        mock_format_detector_instance.get_recipe.assert_called_once_with(b"some,csv,header\ndata,row,1", sample_format_catalog)

        # 驗證 Parser (這裡我們 mock 了 pd.read_csv，所以檢查其是否被間接調用)
        # TransformationPipeline._static_parse_raw_content 會調用 pd.read_csv
        # 這個驗證比較間接，可以考慮直接 mock _static_parse_raw_content

        # 驗證 Cleaner 的載入和調用
        mock_import_module.assert_called_once_with('taifex_pipeline.transformation.cleaners')
        mock_cleaner_function.assert_called_once() # 參數是 DataFrame，較難直接比較

        # 驗證 DBManager load_dataframe_to_table 的調用
        mock_db_instance.load_dataframe_to_table.assert_called_once_with(
            mock_cleaner_function.return_value, # 應傳入清洗後的 DataFrame
            recipe['target_table'],
            recipe['load_options']
        )

        # 驗證 DBManager close 的調用
        mock_db_instance.close.assert_called_once()

        # 驗證結果
        assert result['file_hash'] == sample_file_info['file_hash']
        assert result['status'] == sample_statuses_dict['STATUS_TRANSFORMED_SUCCESS']
        assert result['processed_rows'] == len(mock_cleaner_function.return_value)
        assert result['target_table'] == recipe['target_table']
        assert 'error_message' not in result # 成功時不應有 error_message

    @patch('taifex_pipeline.transformation.pipeline.DBManager')
    def test_process_file_worker_no_recipe(self,
                                           mock_db_manager_constructor,
                                           mock_format_detector_instance,
                                           sample_format_catalog,
                                           sample_file_info,
                                           sample_statuses_dict):
        """測試當 FormatDetector 找不到配方時，檔案被隔離。"""
        mock_db_instance = mock_db_manager_constructor.return_value
        mock_db_instance.get_raw_file_content.return_value = b"unknown,content"
        mock_format_detector_instance.get_recipe.return_value = None # 模擬找不到配方

        result = TransformationPipeline._process_file_worker(
            sample_file_info, ":memory:", mock_format_detector_instance,
            sample_format_catalog, sample_statuses_dict
        )

        assert result['status'] == sample_statuses_dict['STATUS_QUARANTINED']
        assert "No matching recipe found" in result['error_message']
        mock_db_instance.load_dataframe_to_table.assert_not_called() # 不應載入數據
        mock_db_instance.close.assert_called_once()


    @patch('taifex_pipeline.transformation.pipeline.DBManager')
    @patch('taifex_pipeline.transformation.pipeline.TransformationPipeline._static_parse_raw_content') # Mock 解析
    @patch('importlib.import_module')
    def test_process_file_worker_parsing_fails(self,
                                               mock_import_module,
                                               mock_static_parser,
                                               mock_db_manager_constructor,
                                               mock_format_detector_instance,
                                               sample_format_catalog,
                                               sample_file_info,
                                               sample_statuses_dict):
        """測試當 DataFrame 解析失敗時的情況。"""
        mock_db_instance = mock_db_manager_constructor.return_value
        mock_db_instance.get_raw_file_content.return_value = b"bad,csv,data"
        recipe = sample_format_catalog["fingerprint1"]
        mock_format_detector_instance.get_recipe.return_value = recipe

        mock_static_parser.side_effect = ValueError("Simulated parsing error")

        result = TransformationPipeline._process_file_worker(
            sample_file_info, ":memory:", mock_format_detector_instance,
            sample_format_catalog, sample_statuses_dict
        )

        assert result['status'] == sample_statuses_dict['STATUS_TRANSFORMATION_FAILED']
        assert "Simulated parsing error" in result['error_message']
        mock_import_module.assert_not_called() # 清洗不應被調用
        mock_db_instance.load_dataframe_to_table.assert_not_called()
        mock_db_instance.close.assert_called_once()

    @patch('taifex_pipeline.transformation.pipeline.DBManager')
    @patch('importlib.import_module') # Mock importlib
    def test_process_file_worker_cleaner_fails(self,
                                               mock_import_module,
                                               mock_db_manager_constructor,
                                               mock_format_detector_instance,
                                               sample_format_catalog,
                                               sample_file_info,
                                               sample_statuses_dict,
                                               mock_cleaner_function): # 使用 mock cleaner
        """測試當清洗函式執行失敗時的情況。"""
        mock_db_instance = mock_db_manager_constructor.return_value
        mock_db_instance.get_raw_file_content.return_value = b"data"
        recipe = sample_format_catalog["fingerprint1"]
        mock_format_detector_instance.get_recipe.return_value = recipe

        # 讓 mock cleaner 拋出異常
        mock_cleaner_function.side_effect = RuntimeError("Simulated cleaning error")

        mock_cleaners_module = MagicMock()
        mock_cleaners_module.clean_daily_ohlc = mock_cleaner_function
        mock_import_module.return_value = mock_cleaners_module

        result = TransformationPipeline._process_file_worker(
            sample_file_info, ":memory:", mock_format_detector_instance,
            sample_format_catalog, sample_statuses_dict
        )

        assert result['status'] == sample_statuses_dict['STATUS_TRANSFORMATION_FAILED']
        assert "Simulated cleaning error" in result['error_message']
        mock_db_instance.load_dataframe_to_table.assert_not_called()
        mock_db_instance.close.assert_called_once()

    @patch('taifex_pipeline.transformation.pipeline.DBManager')
    @patch('importlib.import_module')
    def test_process_file_worker_load_to_db_fails(self,
                                                  mock_import_module,
                                                  mock_db_manager_constructor,
                                                  mock_format_detector_instance,
                                                  sample_format_catalog,
                                                  sample_file_info,
                                                  sample_statuses_dict,
                                                  mock_cleaner_function):
        """測試當載入數據到資料庫失敗時的情況。"""
        mock_db_instance = mock_db_manager_constructor.return_value
        mock_db_instance.get_raw_file_content.return_value = b"data"
        mock_db_instance.load_dataframe_to_table.side_effect = Exception("DB load error") # 模擬DB載入失敗

        recipe = sample_format_catalog["fingerprint1"]
        mock_format_detector_instance.get_recipe.return_value = recipe

        mock_cleaners_module = MagicMock()
        mock_cleaners_module.clean_daily_ohlc = mock_cleaner_function
        mock_import_module.return_value = mock_cleaners_module

        result = TransformationPipeline._process_file_worker(
            sample_file_info, ":memory:", mock_format_detector_instance,
            sample_format_catalog, sample_statuses_dict
        )

        assert result['status'] == sample_statuses_dict['STATUS_TRANSFORMATION_FAILED']
        assert "DB load error" in result['error_message']
        mock_db_instance.close.assert_called_once()

    # TODO: 更多 _process_file_worker 的測試，例如：
    # - raw_content is None
    # - cleaner_function name not in recipe
    # - target_table not in recipe
    # - importlib.import_module 失敗 (找不到 cleaners 模組)
    # - getattr 失敗 (cleaner 函式名在 cleaners 模組中不存在)


# --- Test Cases for run Method ---
class TestTransformationPipelineRun:

    @patch('taifex_pipeline.transformation.pipeline.DBManager')
    @patch('concurrent.futures.ProcessPoolExecutor') # Mock ProcessPoolExecutor
    def test_run_no_files_to_process(self,
                                     mock_executor_cls,
                                     mock_db_manager_constructor,
                                     mock_format_detector_instance,
                                     sample_format_catalog):
        """測試當沒有 RAW_INGESTED 狀態的檔案時，run 方法的行為。"""
        mock_db_main_instance = mock_db_manager_constructor.return_value
        mock_db_main_instance.get_manifest_records_by_status.return_value = [] # 沒有檔案

        pipeline = TransformationPipeline(":memory:", mock_format_detector_instance, sample_format_catalog)
        pipeline.run()

        mock_db_main_instance.get_manifest_records_by_status.assert_called_once_with(FileStatus.RAW_INGESTED.value)
        mock_executor_cls.assert_not_called() # 不應建立進程池
        mock_db_main_instance.update_manifest_transformation_status.assert_not_called() # 不應有 manifest 更新
        mock_db_main_instance.close.assert_called_once() # 主DBM應被關閉

    @patch('taifex_pipeline.transformation.pipeline.DBManager')
    @patch('concurrent.futures.ProcessPoolExecutor')
    def test_run_processes_files_and_updates_manifest(self,
                                                       mock_executor_cls,
                                                       mock_db_manager_constructor,
                                                       mock_format_detector_instance,
                                                       sample_format_catalog,
                                                       sample_statuses_dict): # 使用 statuses fixture
        """測試 run 方法能查詢檔案、提交給 executor、並更新 manifest。"""
        mock_db_main_instance = mock_db_manager_constructor.return_value

        file_infos = [
            {'file_hash': 'hash1', 'original_path': 'p1'},
            {'file_hash': 'hash2', 'original_path': 'p2'}
        ]
        mock_db_main_instance.get_manifest_records_by_status.return_value = file_infos

        # 模擬 executor 和 future 的行為
        mock_executor_instance = mock_executor_cls.return_value.__enter__.return_value # __enter__ for context manager

        # 模擬 _process_file_worker 的返回結果
        results_from_worker = [
            {'file_hash': 'hash1', 'status': sample_statuses_dict['STATUS_TRANSFORMED_SUCCESS'], 'processed_rows': 100},
            {'file_hash': 'hash2', 'status': sample_statuses_dict['STATUS_QUARANTINED'], 'error_message': 'No recipe'}
        ]

        # 設定 submit 返回的 future 的 result()
        mock_futures = [MagicMock(), MagicMock()]
        mock_futures[0].result.return_value = results_from_worker[0]
        mock_futures[1].result.return_value = results_from_worker[1]

        # 讓 executor.submit 依序返回這些 mock_futures
        # 並且我們需要讓 as_completed 返回這些 futures
        mock_executor_instance.submit.side_effect = mock_futures

        # Mock as_completed to return our futures in order
        # This is a bit tricky. A simpler way might be to not mock ProcessPoolExecutor so deeply,
        # or to make _process_file_worker return results synchronously for this test.
        # For now, let's assume we can control what `as_completed` yields.
        # A common pattern is to patch `as_completed` itself.

        with patch('concurrent.futures.as_completed', return_value=mock_futures):
            pipeline = TransformationPipeline(":memory:", mock_format_detector_instance, sample_format_catalog, max_workers=2)
            pipeline.run()

        # 驗證 DBManager 查詢
        mock_db_main_instance.get_manifest_records_by_status.assert_called_once_with(FileStatus.RAW_INGESTED.value)

        # 驗證 executor submit 被呼叫
        assert mock_executor_instance.submit.call_count == len(file_infos)
        # 檢查 submit 的參數 (比較複雜，需要檢查每個 call)
        # 例如，第一個 call 的第一個 file_info:
        mock_executor_instance.submit.assert_any_call(
            TransformationPipeline._process_file_worker,
            file_infos[0],
            ":memory:", # db_path
            mock_format_detector_instance,
            sample_format_catalog,
            ANY # statuses dict
        )

        # 驗證 manifest 更新
        assert mock_db_main_instance.update_manifest_transformation_status.call_count == len(results_from_worker)
        mock_db_main_instance.update_manifest_transformation_status.assert_any_call(
            file_hash='hash1',
            status=sample_statuses_dict['STATUS_TRANSFORMED_SUCCESS'],
            error_message=None,
            processed_rows=100
        )
        mock_db_main_instance.update_manifest_transformation_status.assert_any_call(
            file_hash='hash2',
            status=sample_statuses_dict['STATUS_QUARANTINED'],
            error_message='No recipe',
            processed_rows=None # 或者 worker 返回 0
        )
        mock_db_main_instance.close.assert_called_once()

    @patch('taifex_pipeline.transformation.pipeline.DBManager')
    @patch('concurrent.futures.ProcessPoolExecutor')
    def test_run_handles_worker_exception(self,
                                          mock_executor_cls,
                                          mock_db_manager_constructor,
                                          mock_format_detector_instance,
                                          sample_format_catalog,
                                          sample_statuses_dict):
        """測試 run 方法在 worker process 拋出未捕獲異常時的處理。"""
        mock_db_main_instance = mock_db_manager_constructor.return_value
        file_infos = [{'file_hash': 'hash_ex', 'original_path': 'p_ex'}]
        mock_db_main_instance.get_manifest_records_by_status.return_value = file_infos

        mock_executor_instance = mock_executor_cls.return_value.__enter__.return_value

        # 模擬 future.result() 拋出異常
        mock_future_ex = MagicMock()
        mock_future_ex.result.side_effect = Exception("Worker crashed unexpectedly")
        mock_executor_instance.submit.return_value = mock_future_ex # 假設只有一個任務

        with patch('concurrent.futures.as_completed', return_value=[mock_future_ex]):
            pipeline = TransformationPipeline(":memory:", mock_format_detector_instance, sample_format_catalog)
            pipeline.run()

        # 驗證 manifest 更新為 FAILED
        mock_db_main_instance.update_manifest_transformation_status.assert_called_once_with(
            file_hash='hash_ex',
            status=sample_statuses_dict['STATUS_TRANSFORMATION_FAILED'],
            error_message=ANY, # 錯誤訊息包含 "平行處理異常: Worker crashed unexpectedly"
            processed_rows=0
        )
        # 檢查 error_message 的內容
        args, kwargs = mock_db_main_instance.update_manifest_transformation_status.call_args
        assert "平行處理異常: Worker crashed unexpectedly" in kwargs['error_message']
        mock_db_main_instance.close.assert_called_once()

    # TODO:
    # - 測試主 DBManager 操作 (get_manifest_records_by_status, update_manifest_transformation_status) 失敗的情況。
    # - 測試 DBManager(self.db_path) 在 run() 方法開始時創建失敗的情況。
    # - 測試 ProcessPoolExecutor 初始化失敗的情況 (較難模擬)。
    # - 測試當 format_detector 或 format_catalog 是 None 或無效時，__init__ 的行為 (已由 __init__ 保證)。
    # - 測試 FileStatus Enum 無法匯入時，狀態字串是否正確使用。
    # - 整合測試：一個更端到端的測試，可能不 mock ProcessPoolExecutor，而是用少量 worker 和真實的（但可能是 mock 的）_process_file_worker 邏輯。
    #   但這通常很複雜，且依賴於多進程環境的正確設定。

    def test_static_parse_raw_content(self, mock_format_detector_instance): # 測試輔助的靜態 parser
        """測試 _static_parse_raw_content 方法。"""
        recipe = {"parser_type": "csv", "parser_config": {"sep": ","}}
        raw_content = b"col1,col2\nval1,val2"
        df = TransformationPipeline._static_parse_raw_content(raw_content, recipe, mock_format_detector_instance.try_encodings)
        assert not df.empty
        assert list(df.columns) == ['col1', 'col2']
        assert len(df) == 1

        with pytest.raises(ValueError, match="不支援的 parser_type: unknown"):
            TransformationPipeline._static_parse_raw_content(raw_content, {"parser_type": "unknown"}, [])

        with pytest.raises(UnicodeDecodeError): # 如果用不正確的編碼
             TransformationPipeline._static_parse_raw_content(b'\xff\xfe', {"parser_type": "csv"}, ['ascii'])
