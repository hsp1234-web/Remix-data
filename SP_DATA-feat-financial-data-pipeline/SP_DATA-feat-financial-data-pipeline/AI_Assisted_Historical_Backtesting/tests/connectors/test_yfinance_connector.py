import unittest
import os
import json
import time
import urllib.error
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime, timezone, timedelta

# 由於 YFinanceConnector 的 __init__ 中有相對路徑計算 cache_dir
# 為了讓測試時這個路徑計算正確，或者能夠穩定 mock，我們需要小心處理 sys.path
# 假設執行測試時，AI_Assisted_Historical_Backtesting 是可以被導入的頂層包
from AI_Assisted_Historical_Backtesting.src.connectors.yfinance_connector import YFinanceConnector, DEFAULT_CACHE_DIR, DEFAULT_CACHE_EXPIRY_SECONDS
from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME

# setup_logger(PROJECT_LOGGER_NAME, level="DEBUG")

# 測試用的快取目錄基礎路徑
TEST_CACHE_BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "test_data", "yf_cache_connector_test")

class TestYFinanceConnector(unittest.TestCase):

    def setUp(self):
        # 確保測試用的快取目錄存在且為空
        self.test_cache_full_path = TEST_CACHE_BASE_DIR
        if not os.path.exists(self.test_cache_full_path):
            os.makedirs(self.test_cache_full_path)
        else:
            # 清空目錄下的文件
            for f in os.listdir(self.test_cache_full_path):
                os.remove(os.path.join(self.test_cache_full_path, f))

        self.connector = YFinanceConnector(cache_dir_base=self.test_cache_full_path, cache_expiry_seconds=300)

    def tearDown(self):
        # 清理測試快取目錄中的文件
        if os.path.exists(self.test_cache_full_path):
            for f in os.listdir(self.test_cache_full_path):
                os.remove(os.path.join(self.test_cache_full_path, f))
            # 可以選擇是否刪除目錄本身
            # os.rmdir(self.test_cache_full_path)

    def test_initialization_creates_cache_dir(self):
        temp_cache_dir = os.path.join(os.path.dirname(__file__), "..", "test_data", "temp_yf_cache_init_test")
        if os.path.exists(temp_cache_dir):
            # 清理可能存在的舊目錄
            import shutil
            shutil.rmtree(temp_cache_dir)

        self.assertFalse(os.path.exists(temp_cache_dir))
        YFinanceConnector(cache_dir_base=temp_cache_dir)
        self.assertTrue(os.path.exists(temp_cache_dir))
        # 清理
        import shutil
        shutil.rmtree(temp_cache_dir)

    def test_cache_filepath_generation(self):
        # 測試 _get_cache_filepath 方法
        filepath = self.connector._get_cache_filepath("AAPL", "v7_csv_20230101_20230110_1d")
        expected_filename = "AAPL_v7_csv_20230101_20230110_1d.json" # 假設後綴是 .json
        self.assertIsNotNone(filepath)
        self.assertTrue(filepath.endswith(expected_filename))
        self.assertTrue(self.test_cache_full_path in filepath)

        filepath_special_chars = self.connector._get_cache_filepath("EURUSD=X", "v8_json_1h_20230101_20230102")
        expected_filename_special = "EURUSD_X_v8_json_1h_20230101_20230102.json"
        self.assertTrue(filepath_special_chars.endswith(expected_filename_special))

    @patch('os.path.getmtime')
    @patch('builtins.open', new_callable=mock_open, read_data='{"key": "value"}')
    def test_read_from_cache_valid_and_not_expired(self, mock_file_open, mock_getmtime):
        mock_getmtime.return_value = time.time() - 100 # 文件修改時間在過期時間內 (100s < 300s)
        test_filepath = os.path.join(self.test_cache_full_path, "test.json")

        # 模擬文件存在
        with patch('os.path.exists', return_value=True):
            content = self.connector._read_from_cache(test_filepath)

        self.assertEqual(content, '{"key": "value"}')
        mock_file_open.assert_called_once_with(test_filepath, 'r', encoding='utf-8')

    @patch('os.path.getmtime')
    @patch('os.remove')
    def test_read_from_cache_expired(self, mock_os_remove, mock_getmtime):
        mock_getmtime.return_value = time.time() - (DEFAULT_CACHE_EXPIRY_SECONDS + 100) # 文件已過期
        test_filepath = os.path.join(self.test_cache_full_path, "expired.json")

        with patch('os.path.exists', return_value=True):
            content = self.connector._read_from_cache(test_filepath)

        self.assertIsNone(content)
        mock_os_remove.assert_called_once_with(test_filepath) # 驗證過期文件被刪除

    @patch('builtins.open', new_callable=mock_open)
    def test_write_to_cache(self, mock_file_open):
        test_filepath = os.path.join(self.test_cache_full_path, "write_test.json")
        test_content = '{"data": "test data"}'
        self.connector._write_to_cache(test_filepath, test_content)
        mock_file_open.assert_called_once_with(test_filepath, 'w', encoding='utf-8')
        mock_file_open().write.assert_called_once_with(test_content)

    @patch('urllib.request.urlopen')
    def test_fetch_data_from_yahoo_success(self, mock_urlopen):
        mock_response_str = 'Date,Open,High,Low,Close,Adj Close,Volume\n2023-01-01,100,101,99,100.5,100.5,10000'
        mock_http_response = MagicMock()
        mock_http_response.getcode.return_value = 200
        mock_http_response.read.return_value = mock_response_str.encode('utf-8')
        mock_http_response.headers = {}
        mock_http_response.fp = None

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_http_response
        mock_urlopen.return_value = mock_cm

        url = "http://fake.yahoo.com/download/AAPL"
        params = {"period1": "0", "period2": "1000", "interval": "1d"}

        response_str = self.connector._fetch_data_from_yahoo(url, params)
        self.assertEqual(response_str, mock_response_str)
        mock_urlopen.assert_called_once()
        called_request = mock_urlopen.call_args[0][0]
        self.assertEqual(called_request.full_url, f"{url}?period1=0&period2=1000&interval=1d")
        self.assertEqual(called_request.headers["User-agent"], self.connector.user_agent)


    @patch('urllib.request.urlopen')
    def test_fetch_data_from_yahoo_http_error(self, mock_urlopen):
        error_response_body = b"Yahoo Finance is temporarily unavailable."
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "http://fake.yahoo.com", 503, "Service Unavailable",
            hdrs={}, fp=MagicMock(read=MagicMock(return_value=error_response_body)) # 修正: headers -> hdrs
        )
        with self.assertRaises(urllib.error.HTTPError):
            self.connector._fetch_data_from_yahoo("http://fake.yahoo.com/download/FAIL")


    @patch.object(YFinanceConnector, '_fetch_data_from_yahoo')
    def test_get_historical_data_csv_no_cache(self, mock_fetch):
        expected_csv_str = "Date,Open,High\n2023-01-01,10,11"
        mock_fetch.return_value = expected_csv_str

        ticker = "TEST"
        start_date = "2023-01-01"
        end_date = "2023-01-02"

        csv_data = self.connector.get_historical_data_csv(ticker, start_date, end_date)
        self.assertEqual(csv_data, expected_csv_str)
        mock_fetch.assert_called_once()

        # 驗證是否寫入快取
        params_str_for_cache = f"v7_csv_{start_date}_{end_date}_1d"
        cache_filepath = self.connector._get_cache_filepath(ticker, params_str_for_cache)
        self.assertTrue(os.path.exists(cache_filepath))
        with open(cache_filepath, 'r') as f:
            self.assertEqual(f.read(), expected_csv_str)

    @patch.object(YFinanceConnector, '_fetch_data_from_yahoo')
    def test_get_historical_data_csv_with_cache_hit(self, mock_fetch):
        ticker = "CACHEHIT"
        start_date = "2023-02-01"
        end_date = "2023-02-02"
        cached_csv_str = "Date,Open,High\n2023-02-01,20,21"

        params_str_for_cache = f"v7_csv_{start_date}_{end_date}_1d"
        cache_filepath = self.connector._get_cache_filepath(ticker, params_str_for_cache)
        self.connector._write_to_cache(cache_filepath, cached_csv_str) # 手動寫入快取

        csv_data = self.connector.get_historical_data_csv(ticker, start_date, end_date)
        self.assertEqual(csv_data, cached_csv_str)
        mock_fetch.assert_not_called() # 不應調用網絡獲取

    @patch.object(YFinanceConnector, '_fetch_data_from_yahoo')
    def test_get_historical_data_csv_invalid_ticker_returns_none(self, mock_fetch):
        # 模擬 _fetch_data_from_yahoo 返回一個類似 Yahoo 錯誤頁面的 HTML
        mock_fetch.return_value = "<html><body>Yahoo! - Lookup</body></html>"

        csv_data = self.connector.get_historical_data_csv("INVALIDTICKER", "2023-01-01", "2023-01-02")
        self.assertIsNone(csv_data)

    def test_get_historical_data_csv_invalid_date_format(self):
        csv_data = self.connector.get_historical_data_csv("AAPL", "2023/01/01", "2023-01-02")
        self.assertIsNone(csv_data) # 應返回 None，並在內部記錄錯誤

    @patch.object(YFinanceConnector, '_fetch_data_from_yahoo')
    def test_get_chart_data_json_success(self, mock_fetch):
        expected_json_dict = {"chart": {"result": [{"meta": {"symbol": "TESTJSON"}}]}}
        mock_fetch.return_value = json.dumps(expected_json_dict)

        ticker = "TESTJSON"
        json_data = self.connector.get_chart_data_json(ticker, range_str="1d")

        self.assertEqual(json_data, expected_json_dict)
        mock_fetch.assert_called_once()

        params_str_for_cache = "v8_json_1d_1d" # 默認 interval="1d"
        cache_filepath = self.connector._get_cache_filepath(ticker, params_str_for_cache)
        self.assertTrue(os.path.exists(cache_filepath))
        with open(cache_filepath, 'r') as f:
            self.assertEqual(json.loads(f.read()), expected_json_dict)

    @patch.object(YFinanceConnector, '_fetch_data_from_yahoo')
    def test_get_chart_data_json_api_error_in_response(self, mock_fetch):
        error_json_dict = {"chart": {"error": {"code": "Not Found", "description": "No data found"}}}
        mock_fetch.return_value = json.dumps(error_json_dict)

        json_data = self.connector.get_chart_data_json("ERRORJSON", range_str="1d")
        self.assertIsNone(json_data) # API 內部錯誤應返回 None

    def test_get_chart_data_json_no_range_or_timestamps(self):
        json_data = self.connector.get_chart_data_json("NORANGE")
        self.assertIsNone(json_data) # 缺少必要參數應返回 None

    @patch.object(YFinanceConnector, '_fetch_data_from_yahoo')
    def test_get_chart_data_json_with_timestamps(self, mock_fetch):
        expected_json_dict = {"chart": {"result": [{"meta": {"symbol": "TSTIME"}}]}}
        mock_fetch.return_value = json.dumps(expected_json_dict)

        start_ts = int(datetime(2023,1,1, tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime(2023,1,5, tzinfo=timezone.utc).timestamp())

        self.connector.get_chart_data_json("TSTIME", start_timestamp=start_ts, end_timestamp=end_ts)

        # 驗證傳遞給 _fetch_data_from_yahoo 的參數
        called_args, called_kwargs = mock_fetch.call_args
        self.assertIn("period1", called_args[1])
        self.assertEqual(called_args[1]["period1"], str(start_ts))
        self.assertIn("period2", called_args[1])
        self.assertEqual(called_args[1]["period2"], str(end_ts))

    @patch('time.sleep', return_value=None) # mock time.sleep
    @patch('urllib.request.urlopen') # Mock urlopen at the lowest level
    def test_retry_mechanism_in_get_historical_data(self, mock_urlopen, mock_sleep):
        # 模擬 urlopen 的行為：第一次拋出 URLError，第二次返回成功數據

        # 成功時的響應
        mock_csv_content_success = "Date,Open,High,Low,Close,Adj Close,Volume\n2023-01-01,100,101,99,100.5,100.5,10000".encode('utf-8')
        mock_http_response_success = MagicMock()
        mock_http_response_success.getcode.return_value = 200
        mock_http_response_success.read.return_value = mock_csv_content_success
        mock_http_response_success.headers = {}
        mock_http_response_success.fp = None

        mock_cm_success = MagicMock()
        mock_cm_success.__enter__.return_value = mock_http_response_success
        mock_cm_success.__exit__.return_value = None

        # 設置 urlopen 的 side_effect
        # 第一次調用 (在 _fetch_data_from_yahoo 內部) 拋出 URLError
        # 第二次調用 (在 retry 之後的 _fetch_data_from_yahoo 內部) 返回成功
        mock_urlopen.side_effect = [
            urllib.error.URLError("Simulated network error for retry"),
            mock_cm_success
        ]

        csv_data = self.connector.get_historical_data_csv("RETRYCSV", "2023-01-01", "2023-01-02")

        # 斷言結果是第二次成功獲取的數據
        self.assertEqual(csv_data, mock_csv_content_success.decode('utf-8'))
        # 驗證 urlopen 被調用了兩次 (一次失敗，一次成功)
        self.assertEqual(mock_urlopen.call_count, 2)
        # 驗證 time.sleep 被調用了一次 (在 retry 裝飾器內部)
        mock_sleep.assert_called_once()

if __name__ == '__main__':
    # 為了能直接運行此測試文件
    import sys
    current_script_path = os.path.abspath(__file__)
    # 假設此文件在 AI_Assisted_Historical_Backtesting/tests/connectors/test_yfinance_connector.py
    project_root_dir = os.path.abspath(os.path.join(current_script_path, "..", "..", ".."))
    if project_root_dir not in sys.path:
       sys.path.insert(0, project_root_dir)
    unittest.main(verbosity=2)
