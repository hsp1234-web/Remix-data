import unittest
import os
import json
import urllib.error
from unittest.mock import patch, MagicMock
from AI_Assisted_Historical_Backtesting.src.connectors.finmind_connector import (
    FinMindConnector,
    FINMIND_API_BASE_URL,
    FinMindAPIError
)
# from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME

# setup_logger(PROJECT_LOGGER_NAME, level="DEBUG")

FAKE_FINMIND_TOKEN = "test_finmind_api_token_xyz"

class TestFinMindConnector(unittest.TestCase):

    def setUp(self):
        self.patch_getenv = patch.dict(os.environ, {FinMindConnector.__init__.__defaults__[1]: FAKE_FINMIND_TOKEN}) # type: ignore
        self.patch_getenv.start()
        self.connector = FinMindConnector()

    def tearDown(self):
        self.patch_getenv.stop()

    def test_initialization_with_token_arg(self):
        connector = FinMindConnector(api_token="direct_token")
        self.assertEqual(connector.api_token, "direct_token")

    def test_initialization_with_env_var(self):
        self.assertEqual(self.connector.api_token, FAKE_FINMIND_TOKEN)

    def test_initialization_no_token_logs_warning_but_does_not_raise(self):
        self.patch_getenv.stop()
        with patch.dict(os.environ, {}, clear=True):
            with patch('AI_Assisted_Historical_Backtesting.src.connectors.finmind_connector.logger.warning') as mock_log_warning:
                connector_no_token = FinMindConnector()
                self.assertIsNone(connector_no_token.api_token)
                mock_log_warning.assert_called_once()
                self.assertIn("FinMind API Token 未在環境變量", mock_log_warning.call_args[0][0])
        self.patch_getenv.start()

    @patch('urllib.request.urlopen')
    def test_fetch_finmind_data_success(self, mock_urlopen):
        mock_api_response_data = {
            "msg": "success",
            "status": 200,
            "data": [{"date": "2023-01-01", "value": 123}]
        }
        mock_response_content = json.dumps(mock_api_response_data).encode('utf-8')

        mock_http_response = MagicMock()
        mock_http_response.getcode.return_value = 200
        mock_http_response.read.return_value = mock_response_content

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_http_response
        mock_urlopen.return_value = mock_cm

        dataset = "TestDataset"
        data_id = "2330"
        start_date = "2023-01-01"

        result = self.connector._fetch_finmind_data(dataset, data_id, start_date)

        self.assertEqual(result, mock_api_response_data)
        called_url = mock_urlopen.call_args[0][0].full_url # urllib.request.Request object
        self.assertTrue(called_url.startswith(FINMIND_API_BASE_URL))
        self.assertIn(f"dataset={dataset}", called_url)
        self.assertIn(f"data_id={data_id}", called_url)
        self.assertIn(f"start_date={start_date}", called_url)
        self.assertIn(f"token={FAKE_FINMIND_TOKEN}", called_url)

    @patch('urllib.request.urlopen')
    def test_fetch_finmind_data_api_returns_error_status(self, mock_urlopen):
        mock_api_error_response = {
            "msg": "Invalid token or permission denied",
            "status": 403,
        }
        mock_response_content = json.dumps(mock_api_error_response).encode('utf-8')

        mock_http_response = MagicMock()
        mock_http_response.getcode.return_value = 200 # HTTP 狀態碼可能是 200，但業務邏輯是失敗的
        mock_http_response.read.return_value = mock_response_content

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_http_response
        mock_urlopen.return_value = mock_cm

        with self.assertRaises(FinMindAPIError) as context:
            self.connector._fetch_finmind_data("TestDataset", "2330", "2023-01-01")

        self.assertEqual(context.exception.status_code, 403)
        self.assertIn("Invalid token or permission denied", str(context.exception))

    @patch('urllib.request.urlopen')
    def test_fetch_finmind_data_http_error_401_with_api_message(self, mock_urlopen):
        # 模擬 API 返回 HTTP 401，且響應體是 FinMind 風格的 JSON 錯誤
        error_json_response = {"message": "token error", "status_code": 401}
        error_response_content = json.dumps(error_json_response).encode('utf-8')

        mock_urlopen.side_effect = urllib.error.HTTPError(
            url=FINMIND_API_BASE_URL,
            code=401,
            msg="Unauthorized",
            hdrs={},
            fp=MagicMock(read=MagicMock(return_value=error_response_content))
        )
        with self.assertRaises(FinMindAPIError) as context:
            self.connector._fetch_finmind_data("TestDataset", "2330", "2023-01-01")

        self.assertEqual(context.exception.status_code, 401)
        self.assertIn("HTTP Error from API: token error", str(context.exception))


    @patch('urllib.request.urlopen')
    def test_fetch_finmind_data_json_decode_error(self, mock_urlopen):
        mock_invalid_json_content = b"This is not valid JSON"
        mock_http_response = MagicMock()
        mock_http_response.getcode.return_value = 200
        mock_http_response.read.return_value = mock_invalid_json_content

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_http_response
        mock_urlopen.return_value = mock_cm

        with self.assertRaises(FinMindAPIError) as context: # FinMindAPIError wraps JSONDecodeError
            self.connector._fetch_finmind_data("TestDataset", "2330", "2023-01-01")
        self.assertIn("JSON 解析錯誤", str(context.exception))


    @patch.object(FinMindConnector, '_fetch_finmind_data')
    def test_get_financial_statements_success(self, mock_fetch):
        expected_data = [{"statement_key": "value1"}, {"statement_key": "value2"}]
        mock_fetch.return_value = {"msg": "success", "status": 200, "data": expected_data}

        result = self.connector.get_financial_statements("2330", "2022-01-01", "2022-12-31")
        self.assertEqual(result, expected_data)
        mock_fetch.assert_called_once_with(
            dataset="FinancialStatements", # 或者 "TaiwanStockFinancialStatements"
            data_id="2330",
            start_date="2022-01-01",
            end_date="2022-12-31"
        )

    @patch.object(FinMindConnector, '_fetch_finmind_data')
    def test_get_financial_statements_api_error(self, mock_fetch):
        mock_fetch.side_effect = FinMindAPIError("API failed", status_code=500)
        result = self.connector.get_financial_statements("2330", "2022-01-01")
        self.assertIsNone(result)

    # @patch.object(FinMindConnector, '_fetch_finmind_data') # 在 no_token 測試中不應該 mock _fetch
    def test_get_financial_statements_no_token(self): # 移除 mock_fetch 參數
        # 確保 os.getenv 在此測試中返回 None，以模擬 token 不存在的情況
        with patch.dict(os.environ, {}, clear=True): # 清空環境變量
             with patch('AI_Assisted_Historical_Backtesting.src.connectors.finmind_connector.os.getenv') as mock_os_getenv:
                mock_os_getenv.return_value = None
                connector_no_token = FinMindConnector(api_token=None) # api_token 參數也是 None
                self.assertIsNone(connector_no_token.api_token, "api_token 應該是 None")

                # 因為我們沒有 mock _fetch_finmind_data，所以需要確保它不會真的發起網絡請求
                # 但在這個 case，它根本不應該被調用
                # 我們可以通過 mock _fetch_finmind_data 來驗證它是否被調用
                with patch.object(connector_no_token, '_fetch_finmind_data') as mock_fetch_actual:
                    result = connector_no_token.get_financial_statements("2330", "2022-01-01")
                    self.assertIsNone(result)
                    mock_fetch_actual.assert_not_called()

    @patch.object(FinMindConnector, '_fetch_finmind_data')
    def test_get_chip_data_success(self, mock_fetch):
        expected_data = [{"chip_key": "value_chip"}]
        mock_fetch.return_value = {"msg": "success", "status": 200, "data": expected_data}
        chip_type = "InstitutionalInvestorsBuySell"

        result = self.connector.get_chip_data("2454", "2023-03-01", chip_type=chip_type)
        self.assertEqual(result, expected_data)
        mock_fetch.assert_called_once_with(
            dataset=chip_type,
            data_id="2454",
            start_date="2023-03-01",
            end_date=None
        )

    # @patch.object(FinMindConnector, '_fetch_finmind_data') # 在 no_token 測試中不應該 mock _fetch
    def test_get_chip_data_no_token(self): # 移除 mock_fetch 參數
        with patch.dict(os.environ, {}, clear=True):
            with patch('AI_Assisted_Historical_Backtesting.src.connectors.finmind_connector.os.getenv') as mock_os_getenv:
                mock_os_getenv.return_value = None
                connector_no_token = FinMindConnector(api_token=None)
                self.assertIsNone(connector_no_token.api_token, "api_token 應該是 None")

                with patch.object(connector_no_token, '_fetch_finmind_data') as mock_fetch_actual:
                    result = connector_no_token.get_chip_data("2454", "2023-03-01")
                    self.assertIsNone(result)
                    mock_fetch_actual.assert_not_called()

    @patch('time.sleep', return_value=None) # mock time.sleep
    @patch('urllib.request.urlopen')
    def test_retry_mechanism_for_fetch_finmind_data(self, mock_urlopen, mock_sleep):
        # 模擬 urlopen：第一次拋出 URLError，第二次成功
        mock_api_response_data = {"msg": "success", "status": 200, "data": [{"value": "ok"}]}
        mock_response_content = json.dumps(mock_api_response_data).encode('utf-8')

        mock_http_response_success = MagicMock()
        mock_http_response_success.getcode.return_value = 200
        mock_http_response_success.read.return_value = mock_response_content

        mock_cm_success = MagicMock()
        mock_cm_success.__enter__.return_value = mock_http_response_success

        mock_urlopen.side_effect = [
            urllib.error.URLError("Simulated network error for FinMind retry"),
            mock_cm_success
        ]

        result = self.connector._fetch_finmind_data("RetryDataset", "1101", "2023-01-01")

        self.assertEqual(result, mock_api_response_data)
        self.assertEqual(mock_urlopen.call_count, 2)
        mock_sleep.assert_called_once()


if __name__ == '__main__':
    import sys
    current_script_path = os.path.abspath(__file__)
    project_root_dir = os.path.abspath(os.path.join(current_script_path, "..", "..", ".."))
    if project_root_dir not in sys.path:
       sys.path.insert(0, project_root_dir)
    unittest.main(verbosity=2)
