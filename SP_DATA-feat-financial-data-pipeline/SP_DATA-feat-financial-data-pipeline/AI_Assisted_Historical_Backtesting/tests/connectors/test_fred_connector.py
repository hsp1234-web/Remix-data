import unittest
import os
import json
import urllib.error
from unittest.mock import patch, MagicMock # Python 3.3+
from AI_Assisted_Historical_Backtesting.src.connectors.fred_connector import FredConnector, FRED_API_ENDPOINT
from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME

# 測試前配置日誌，以便查看連接器內部的日誌輸出
# setup_logger(logger_name=PROJECT_LOGGER_NAME, level="DEBUG") # 在測試運行時，通常由測試運行器或 conftest.py 控制日誌級別

# 使用一個固定的假 API 金鑰進行測試
FAKE_API_KEY = "test_api_key_12345"

class TestFredConnector(unittest.TestCase):

    def setUp(self):
        # 確保每次測試都有一個乾淨的 FredConnector 實例
        # 我們將通過 mock os.getenv 來避免真實的環境變量依賴
        self.patch_getenv = patch.dict(os.environ, {FredConnector.__init__.__defaults__[1]: FAKE_API_KEY}) # type: ignore
        self.patch_getenv.start()
        self.connector = FredConnector()

    def tearDown(self):
        self.patch_getenv.stop()

    def test_initialization_with_api_key_arg(self):
        connector = FredConnector(api_key="direct_key")
        self.assertEqual(connector.api_key, "direct_key")

    def test_initialization_with_env_var(self):
        self.assertEqual(self.connector.api_key, FAKE_API_KEY)

    def test_initialization_no_api_key_raises_value_error(self):
        # 停止 setUp 中啟動的 patch，以模擬環境變量不存在的情況
        self.patch_getenv.stop()
        # 再次 patch，但這次不提供 FRED_API_KEY
        with patch.dict(os.environ, {}, clear=True): # clear=True 確保環境是乾淨的
            with self.assertRaisesRegex(ValueError, "FRED API 金鑰未找到"):
                FredConnector()
        self.patch_getenv.start() # 恢復 setUp 的 patch，以防影響其他測試

    @patch('urllib.request.urlopen')
    def test_get_series_observations_success_json(self, mock_urlopen):
        # 準備 mock 返回值
        mock_response_data = {
            "realtime_start": "2024-01-01",
            "realtime_end": "2024-01-01",
            "observation_start": "2022-01-01",
            "observation_end": "2023-01-01",
            "units": "lin",
            "output_type": 1,
            "file_type": "json",
            "order_by": "observation_date",
            "sort_order": "asc",
            "count": 1,
            "offset": 0,
            "limit": 1,
            "observations": [{"date": "2023-01-01", "value": "100.0"}]
        }
        mock_response_content = json.dumps(mock_response_data).encode('utf-8')

        mock_http_response = MagicMock()
        mock_http_response.getcode.return_value = 200
        mock_http_response.read.return_value = mock_response_content
        mock_http_response.headers = {} # 添加 headers 屬性
        mock_http_response.fp = None # 添加 fp 屬性

        # 配置 urlopen 的上下文管理器 (__enter__ 和 __exit__)
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_http_response
        mock_cm.__exit__.return_value = None
        mock_urlopen.return_value = mock_cm

        series_id = "TESTSERIES"
        data = self.connector.get_series_observations(series_id=series_id, start_date_str="2023-01-01")

        self.assertIsNotNone(data)
        self.assertEqual(data["series_id"] if "series_id" in data else series_id, series_id) # API 響應通常不含 series_id
        self.assertEqual(len(data["observations"]), 1)
        self.assertEqual(data["observations"][0]["value"], "100.0")

        # 驗證 API URL 是否正確構建 (大致檢查)
        expected_url_part = f"{FRED_API_ENDPOINT}?series_id={series_id}&api_key={FAKE_API_KEY}&file_type=json"
        called_url = mock_urlopen.call_args[0][0]
        self.assertTrue(called_url.startswith(expected_url_part))
        self.assertIn("observation_start=2023-01-01", called_url)

    @patch('urllib.request.urlopen')
    def test_get_series_observations_success_non_json(self, mock_urlopen):
        mock_response_content_xml = b"<xml><observation date='2023-01-01' value='100.0'/></xml>"

        mock_http_response = MagicMock()
        mock_http_response.getcode.return_value = 200
        mock_http_response.read.return_value = mock_response_content_xml
        mock_http_response.headers = {}
        mock_http_response.fp = None

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_http_response
        mock_cm.__exit__.return_value = None
        mock_urlopen.return_value = mock_cm

        data = self.connector.get_series_observations(series_id="TESTSERIES", file_type="xml")
        self.assertEqual(data, mock_response_content_xml)

    @patch('urllib.request.urlopen')
    def test_get_series_observations_http_error_400(self, mock_urlopen):
        # 模擬 FRED API 返回的 400 錯誤 JSON
        error_response_data = {"error_code": 400, "error_message": "Bad Request. The value for variable limit is not an integer."}
        error_response_content = json.dumps(error_response_data).encode('utf-8')

        # urllib.error.HTTPError 需要 url, code, msg, hdrs, fp
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url=FRED_API_ENDPOINT,
            code=400,
            msg="Bad Request",
            hdrs={},
            fp=MagicMock(read=MagicMock(return_value=error_response_content)) # fp 需要有 read 方法
        )

        with self.assertRaises(urllib.error.HTTPError) as context:
            self.connector.get_series_observations(series_id="TESTSERIES", limit="not_an_int") # type: ignore

        self.assertEqual(context.exception.code, 400)
        # 可以在此處檢查日誌是否包含 API 返回的 error_message

    @patch('urllib.request.urlopen')
    def test_get_series_observations_http_error_non_json_response(self, mock_urlopen):
        # 模擬 API 返回 500 錯誤，但響應體不是 JSON
        error_response_content_html = b"<html><body>Internal Server Error</body></html>"

        mock_urlopen.side_effect = urllib.error.HTTPError(
            url=FRED_API_ENDPOINT,
            code=500,
            msg="Internal Server Error",
            hdrs={'Content-Type': 'text/html'},
            fp=MagicMock(read=MagicMock(return_value=error_response_content_html))
        )
        with self.assertRaises(urllib.error.HTTPError) as context:
            self.connector.get_series_observations(series_id="TESTSERIES")
        self.assertEqual(context.exception.code, 500)


    @patch('urllib.request.urlopen')
    def test_get_series_observations_url_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Name or service not known")
        with self.assertRaises(urllib.error.URLError):
            self.connector.get_series_observations(series_id="TESTSERIES")

    @patch('urllib.request.urlopen')
    def test_get_series_observations_timeout_error(self, mock_urlopen):
        mock_urlopen.side_effect = TimeoutError("Request timed out")
        with self.assertRaises(TimeoutError):
            self.connector.get_series_observations(series_id="TESTSERIES")

    @patch('urllib.request.urlopen')
    def test_get_series_observations_invalid_json_response(self, mock_urlopen):
        mock_response_content_invalid_json = b"{'date': '2023-01-01', 'value': '100.0'" # 缺少右花括號

        mock_http_response = MagicMock()
        mock_http_response.getcode.return_value = 200
        mock_http_response.read.return_value = mock_response_content_invalid_json
        mock_http_response.headers = {}
        mock_http_response.fp = None

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_http_response
        mock_cm.__exit__.return_value = None
        mock_urlopen.return_value = mock_cm

        with self.assertRaises(json.JSONDecodeError):
            self.connector.get_series_observations(series_id="TESTSERIES", file_type="json")

    def test_date_format_validation_warning(self):
        # 測試無效日期格式是否會產生警告 (需要 mock logger 來捕獲)
        # FredConnector 內部會記錄警告，但不會阻止請求（會忽略無效日期）
        with patch.object(FredConnector, 'get_series_observations', wraps=self.connector.get_series_observations) as wrapped_method:
            # 使用一個 mock 的 logger 來捕獲日誌輸出
            # logger_fred_connector = logging.getLogger(f"{PROJECT_LOGGER_NAME}.src.connectors.fred_connector")
            # stream_handler = logging.StreamHandler(io.StringIO())
            # logger_fred_connector.addHandler(stream_handler)
            # logger_fred_connector.setLevel(logging.WARNING)

            # 由於 retry 裝飾器的存在，直接 mock logger 可能比較複雜
            # 這裡我們假設 get_series_observations 內部會正確調用 logger.warning
            # 並且只驗證它是否會繼續執行（不拋出日期格式異常）
            # 實際的日誌驗證可能需要在集成測試中進行，或者更複雜的 mock 設置
            try:
                with patch('urllib.request.urlopen', MagicMock()) as mock_urlopen_date_test:
                    # 設置一個最小化的成功響應，以避免其他錯誤
                    mock_http_resp = MagicMock()
                    mock_http_resp.getcode.return_value = 200
                    mock_http_resp.read.return_value = json.dumps({"observations":[]}).encode()
                    mock_http_resp.headers = {}
                    mock_http_resp.fp = None
                    mock_cm_date = MagicMock()
                    mock_cm_date.__enter__.return_value = mock_http_resp
                    mock_urlopen_date_test.return_value = mock_cm_date

                    self.connector.get_series_observations(series_id="DUMMY", start_date_str="2023/01/01") # 無效格式
                    self.connector.get_series_observations(series_id="DUMMY", end_date_str="2023-13-01")   # 無效日期

                    # 驗證 URL 中是否不包含這些無效日期
                    call_args_list = mock_urlopen_date_test.call_args_list
                    self.assertNotIn("observation_start=2023/01/01", call_args_list[0][0][0])
                    self.assertNotIn("observation_end=2023-13-01", call_args_list[1][0][0])

            except Exception as e:
                self.fail(f"日期格式驗證測試不應拋出異常，但拋出了: {e}")


    @patch('time.sleep', return_value=None) # mock time.sleep 防止測試過慢
    @patch('urllib.request.urlopen')
    def test_retry_mechanism_works(self, mock_urlopen, mock_sleep):
        # 第一次拋出 URLError，第二次成功
        mock_success_response_data = {"observations": [{"date": "2023-01-01", "value": "100.0"}]}
        mock_success_content = json.dumps(mock_success_response_data).encode('utf-8')

        mock_http_success_response = MagicMock()
        mock_http_success_response.getcode.return_value = 200
        mock_http_success_response.read.return_value = mock_success_content
        mock_http_success_response.headers = {}
        mock_http_success_response.fp = None

        mock_cm_success = MagicMock()
        mock_cm_success.__enter__.return_value = mock_http_success_response

        mock_urlopen.side_effect = [
            urllib.error.URLError("Simulated network error"),
            mock_cm_success # 第二次調用返回成功的上下文管理器
        ]

        data = self.connector.get_series_observations(series_id="RETRYTEST")

        self.assertIsNotNone(data)
        self.assertEqual(len(data["observations"]), 1)
        self.assertEqual(mock_urlopen.call_count, 2) # 驗證 urlopen 被調用了兩次
        mock_sleep.assert_called_once() # 驗證 time.sleep 被調用了一次


if __name__ == '__main__':
    # 為了能直接運行此測試文件 (python tests/connectors/test_fred_connector.py)
    # 需要將項目根目錄的父目錄添加到 sys.path，以便導入
    # AI_Assisted_Historical_Backtesting.src.connectors 等。
    import sys
    import os
    # current_script_path = os.path.abspath(__file__) # .../tests/connectors/test_fred_connector.py
    # tests_connectors_dir = os.path.dirname(current_script_path) # .../tests/connectors
    # tests_dir = os.path.dirname(tests_connectors_dir) # .../tests
    # project_root_dir = os.path.dirname(tests_dir) # .../AI_Assisted_Historical_Backtesting
    # project_root_parent_dir = os.path.dirname(project_root_dir) # 父目錄
    # if project_root_parent_dir not in sys.path:
    #    sys.path.insert(0, project_root_parent_dir)

    # 更簡單的方式，如果從 AI_Assisted_Historical_Backtesting 目錄下執行
    # python -m unittest tests.connectors.test_fred_connector
    # 則不需要修改 sys.path，因為 AI_Assisted_Historical_Backtesting 會被視為頂層包（如果父目錄在 PYTHONPATH）
    # 或者，如果 tests 目錄本身是執行點，則需要把 AI_Assisted_Historical_Backtesting 的父目錄加進去

    # 假設執行時 PYTHONPATH 已經正確設置，或者使用 python -m unittest 執行
    unittest.main(verbosity=2)
