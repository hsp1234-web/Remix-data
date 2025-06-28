# tests/test_fred_fetcher.py
# ----------------------------------------------------
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from modules.fetchers.fred_fetcher import FredFetcher

class TestFredFetcher(unittest.TestCase):

    @patch('fredapi.Fred') # 模擬 (Mock) 掉外部的 fredapi 庫
    def test_fetch_data_returns_dataframe(self, MockFred):
        """
        測試 fetch_data 是否在成功時回傳一個 DataFrame。
        """
        # 準備：設定模擬物件的行為
        mock_instance = MockFred.return_value
        mock_instance.get_series.return_value = pd.Series([1, 2, 3])

        # 執行：實例化我們的 fetcher 並調用被測試的方法
        fetcher = FredFetcher()
        result = fetcher.fetch_data("GNPCA")

        # 斷言：驗證結果是否符合預期
        self.assertIsInstance(result, pd.DataFrame)
        self.assertFalse(result.empty)
        # 驗證模擬的API是否被正確呼叫
        mock_instance.get_series.assert_called_once_with("GNPCA")

if __name__ == '__main__':
    unittest.main()
