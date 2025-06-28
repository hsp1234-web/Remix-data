import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import yaml # 雖然會被 mock，但 Commander.py 導入了它
import os

# 確保可以從父目錄導入 data_pipeline 中的模組
# 這在直接執行此腳本時可能需要
import sys
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# 考慮到執行環境，更可靠的方式是確保 PYTHONPATH 設置正確，或者在執行時從 panoramic-market-analyzer 目錄下執行 python -m data_pipeline.test_commander_mocked

from data_pipeline.commander import Commander
# from data_pipeline.fetchers.yfinance_fetcher import YFinanceFetcher # 用於 spec
from data_pipeline.interfaces import DataFetcherInterface # 用於 spec

# --- 測試用的假組態 ---
# 簡化版，只包含測試 fetch_single_symbol_data (equity) 所需的關鍵部分
TEST_CONFIG = {
    'fetcher_robustness': {
        'retries': 1,
        'backoff_factor': 0.1,
        'delay_min_seconds': 0.01,
        'delay_max_seconds': 0.05
    },
    'data_sources': {
        'equity': 'yfinance', # 指向 Commander 中 fetcher_factory 的鍵
        'macro': 'fred',
        'crypto': 'coingecko'
    },
    'database': {
        'type': 'duckdb',
        'path': ':memory:' # 使用內存數據庫進行測試，執行完畢即消失
    },
    'cache': { # Commander 初始化時會檢查的路徑
        'path': './mock_test_cache.sqlite', # 臨時路徑
        'expire_after_days': 1
    },
    'concurrency': {
        'max_workers': 1 # 測試時不需要並行
    },
    'api_keys': {}, # 假設 yfinance 不需要 key
    'processor_settings': {}
}

# --- 預期的模擬返回數據 ---
MOCK_AAPL_DATA = pd.DataFrame({
    'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
    'open': [150.0, 152.0],
    'high': [153.0, 154.0],
    'low': [149.0, 151.0],
    'close': [152.5, 153.0],
    'adj_close': [152.0, 152.5], # yfinance 通常有 adj_close
    'volume': [1000000, 1200000]
})

class TestCommanderSimulated(unittest.TestCase):

    def setUp(self):
        """測試開始前的準備工作"""
        # 確保 cache 路徑的目錄存在，Commander 初始化時會檢查
        cache_dir = os.path.dirname(TEST_CONFIG['cache']['path'])
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        self.test_db_path = TEST_CONFIG['database']['path'] # :memory:

    @patch('data_pipeline.commander.open', new_callable=unittest.mock.mock_open) # 模擬 open
    @patch('data_pipeline.commander.yaml.safe_load') # 模擬 yaml.safe_load
    @patch('data_pipeline.commander.YFinanceFetcher') # Patch Commander 模組中的 YFinanceFetcher
    def test_fetch_single_equity_simulated_success(self, MockYFinanceFetcherClass, mock_yaml_safe_load, mock_open_file):
        print("\nRunning: test_fetch_single_equity_simulated_success")

        # 1. 設定模擬對象的行為
        # 模擬 yaml.safe_load 的返回值
        mock_yaml_safe_load.return_value = TEST_CONFIG
        # mock_open_file 不需要特別設定 read_data，因為 yaml.safe_load 被 mock 了，不會真的去讀 file handle

        # 創建 YFinanceFetcher 的模擬實例
        mock_yfinance_fetcher_instance = MagicMock(spec=DataFetcherInterface) # spec 可以是類或接口
        mock_yfinance_fetcher_instance.fetch.return_value = MOCK_AAPL_DATA.copy() # 每次測試用數據的副本

        # 當 Commander 試圖創建 YFinanceFetcher 時，讓它返回我們預先準備好的模擬 fetcher 實例
        MockYFinanceFetcherClass.return_value = mock_yfinance_fetcher_instance

        # 2. 實例化 Commander
        # Commander 會使用 mock_yaml_safe_load 返回的 TEST_CONFIG
        # 並且當它需要 YFinanceFetcher 時，會得到 MockYFinanceFetcherClass.return_value
        commander = Commander(config_path="dummy_config.yaml") # config_path 只是個佔位符，因為載入被 mock 了
                                                            # db_path 和 cache_path 會從 TEST_CONFIG 中讀取

        # 3. 調用被測方法
        symbol_to_test = 'AAPL'
        start_date = '2023-01-01'
        end_date = '2023-01-02'
        target_table_name = 'ohlcv_equity_test'

        # 執行 Commander 的方法，這會觸發對模擬 fetcher 的調用
        returned_df = commander.fetch_single_symbol_data(
            symbol=symbol_to_test,
            start_date=start_date,
            end_date=end_date,
            source_key='equity', # 這會讓 Commander 選擇 'yfinance' fetcher
            table_name=target_table_name
        )

        # 4. 斷言和驗證
        # 4.1 驗證模擬的 fetcher 是否被正確調用
        mock_yfinance_fetcher_instance.fetch.assert_called_once_with(symbol_to_test, start_date, end_date)
        print(f"  Asserted: YFinanceFetcher.fetch called correctly for {symbol_to_test}.")

        # 4.2 驗證 Commander 返回的 DataFrame 是否與 fetcher 模擬返回的一致
        # 注意：Commander 的 fetch_single_symbol_data 內部會添加 'symbol' 列並做清洗
        # MOCK_AAPL_DATA 沒有 'symbol' 列，所以我們比較時要注意
        self.assertIsNotNone(returned_df, "Commander should return a DataFrame.")
        # 比較除了 'symbol' 之外的內容是否大致相符 (行數，部分數值)
        self.assertEqual(len(returned_df), len(MOCK_AAPL_DATA), "Returned DataFrame row count mismatch.")
        self.assertTrue('symbol' in returned_df.columns, "Returned DataFrame should have a 'symbol' column.")
        self.assertEqual(returned_df['symbol'].iloc[0], symbol_to_test, "Symbol column value mismatch.")
        pd.testing.assert_frame_equal(
            returned_df[['date', 'open', 'close']].reset_index(drop=True),
            MOCK_AAPL_DATA[['date', 'open', 'close']].reset_index(drop=True),
            check_dtype=False, # 類型可能因處理而略有差異
            atol=0.01 # 容忍浮點數的微小差異
        )
        print(f"  Asserted: Returned DataFrame content matches expectations for {symbol_to_test}.")

        # 4.3 驗證數據是否已存入 (內存) 數據庫
        # Commander 的 database 屬性是 DuckDBRepository 的實例
        # 我們可以調用它的 get_ohlcv 方法來檢查
        # 注意：由於是內存數據庫，Commander 實例銷毀後數據就沒了，所以要在 commander.close() 之前檢查
        db_data = commander.database.get_ohlcv(symbol_to_test, target_table_name, start_date, end_date)
        self.assertIsNotNone(db_data, "Data not found in database.")
        self.assertEqual(len(db_data), len(MOCK_AAPL_DATA), "Database row count mismatch.")
        self.assertEqual(db_data['close'].iloc[0], MOCK_AAPL_DATA['close'].iloc[0], "Database 'close' value mismatch.")
        self.assertEqual(db_data['symbol'].iloc[0], symbol_to_test, "Database 'symbol' value mismatch.")
        print(f"  Asserted: Data for {symbol_to_test} correctly stored and retrieved from in-memory DB.")

        # 5. 清理 (對於內存數據庫，主要是關閉連接)
        commander.close()
        print("  Commander closed.")
        print("Test: test_fetch_single_equity_simulated_success PASSED")


    @patch('data_pipeline.commander.open', new_callable=unittest.mock.mock_open)
    @patch('data_pipeline.commander.yaml.safe_load')
    @patch('data_pipeline.commander.YFinanceFetcher')
    def test_fetch_single_equity_fetcher_returns_none(self, MockYFinanceFetcherClass, mock_yaml_safe_load, mock_open_file):
        print("\nRunning: test_fetch_single_equity_fetcher_returns_none")
        mock_yaml_safe_load.return_value = TEST_CONFIG
        # mock_open_file 不需要特別設定

        mock_yfinance_fetcher_instance = MagicMock(spec=DataFetcherInterface)
        mock_yfinance_fetcher_instance.fetch.return_value = None # 模擬 fetcher 獲取失敗
        MockYFinanceFetcherClass.return_value = mock_yfinance_fetcher_instance

        commander = Commander(config_path="dummy_config.yaml")

        returned_df = commander.fetch_single_symbol_data(
            symbol='FAIL',
            start_date='2023-01-01',
            end_date='2023-01-02',
            source_key='equity',
            table_name='test_failure_equity'
        )

        mock_yfinance_fetcher_instance.fetch.assert_called_once_with('FAIL', '2023-01-01', '2023-01-02')
        self.assertIsNone(returned_df, "Commander should return None if fetcher returns None.")
        print("  Asserted: Commander correctly handles fetcher returning None.")

        # 驗證數據庫中沒有存入 'FAIL' 的數據 (可選，但好的實踐)
        db_data = commander.database.get_ohlcv('FAIL', 'test_failure_equity', '2023-01-01', '2023-01-02')
        self.assertTrue(db_data is None or db_data.empty, "Database should not contain data for 'FAIL' if fetch failed.")
        print("  Asserted: No data for 'FAIL' stored in DB.")

        commander.close()
        print("  Commander closed.")
        print("Test: test_fetch_single_equity_fetcher_returns_none PASSED")

def run_specific_tests():
    """
    一個輔助函數，用於運行此文件中的所有測試。
    這使得此文件可以直接被 python 解釋器執行以運行測試。
    """
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCommanderSimulated))

    runner = unittest.TextTestRunner(verbosity=2) # verbosity=2 會打印更多訊息
    result = runner.run(suite)

    # 根據測試結果決定退出碼，0表示成功，非0表示失敗
    # 這對於 CI/CD 環境判斷測試是否通過很重要
    if result.wasSuccessful():
        print("\nALL SIMULATED COMMANDER TESTS PASSED!")
        return 0
    else:
        print("\nSIMULATED COMMANDER TESTS FAILED.")
        return 1

if __name__ == '__main__':
    # 使此腳本可以直接執行以運行測試
    # 例如: python panoramic-market-analyzer/data_pipeline/test_commander_mocked.py
    # 或者從 panoramic-market-analyzer 目錄執行:
    # python -m data_pipeline.test_commander_mocked

    # 為了確保 Commander 和其他模組能被正確導入，最好從專案根目錄執行
    # 並將專案根目錄加入到 PYTHONPATH，或者使用 -m 選項。
    # 我們這裡假設執行時的當前工作目錄是 panoramic-market-analyzer
    # 或者 test_commander_mocked.py 能正確找到它的兄弟模組。
    # 在 setup 中調整 sys.path 是一種方法，但更推薦使用 Python 的模組化執行方式。

    # 將專案根目錄加到 sys.path，以便導入 data_pipeline
    # (假設此測試檔案在 data_pipeline 目錄下)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir) # 上一層是 data_pipeline 的父目錄，即專案根目錄
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        print(f"Added to sys.path for module resolution: {project_root}")

    # 重新導入 Commander，以確保它在 sys.path 更新後被查找
    # 這有點 hacky，更好的方式是確保執行環境的 PYTHONPATH 正確
    # 或者使用 `python -m data_pipeline.test_commander_mocked` 從根目錄執行
    try:
        from data_pipeline.commander import Commander
    except ImportError:
        print("Failed to re-import Commander after sys.path modification. Ensure you are running from project root or using -m.")
        sys.exit(1)


    exit_code = run_specific_tests()
    sys.exit(exit_code) # 將測試結果作為退出碼返回
