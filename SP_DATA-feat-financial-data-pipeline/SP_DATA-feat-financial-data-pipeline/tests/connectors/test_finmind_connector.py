import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timezone
import logging

# 假設 connector 和 config 的導入路徑
from src.connectors.finmind_connector import FinMindConnector
# 如果需要從 config 加載，則取消註釋
# from src.config_loader import load_app_config # 假設有這樣一個加載器

# 模擬的 config，至少包含 FinMind API token
MOCK_APP_CONFIG = {
    "api_keys": {
        "finmind_api_token": "TEST_FINMIND_TOKEN"
    }
    # 其他必要的配置...
}

class TestFinMindConnector(unittest.TestCase):
    """
    對 FinMindConnector 的單元測試。
    """

    def setUp(self):
        """
        在每個測試方法執行前，設置好必要的 mock 物件和 connector 實例。
        """
        self.patcher_finmind_logger = patch('src.connectors.finmind_connector.logger', spec=True)
        # 如果 BaseConnector 的 logger 也被 FinMindConnector 的 __init__ 間接使用，也需要 patch
        self.patcher_base_logger = patch('src.connectors.base_connector.logger', spec=True)

        self.mock_finmind_logger = self.patcher_finmind_logger.start()
        self.mock_base_logger = self.patcher_base_logger.start()

        self.addCleanup(self.patcher_finmind_logger.stop)
        self.addCleanup(self.patcher_base_logger.stop)

        # 模擬 FinMind.data.DataLoader 實例及其方法
        # 我們需要 patch DataLoader 的實例化以及 login_by_token
        self.mock_data_loader_instance = MagicMock()

        # Patch DataLoader 的 __init__ 使其返回我們的 mock 實例
        # Patch login_by_token 使其不執行任何操作 (或按需返回)
        self.patcher_data_loader = patch('FinMind.data.DataLoader', return_value=self.mock_data_loader_instance)
        self.patcher_login_by_token = patch.object(self.mock_data_loader_instance, 'login_by_token')

        self.mock_data_loader_constructor = self.patcher_data_loader.start()
        self.mock_login_by_token_method = self.patcher_login_by_token.start()

        self.addCleanup(self.patcher_data_loader.stop)
        self.addCleanup(self.patcher_login_by_token.stop)

        # 實例化我們要測試的 connector
        self.connector = FinMindConnector(config=MOCK_APP_CONFIG) # 傳入模擬的 config

        # 預期的 canonical financials columns (應與 FinMindConnector._get_canonical_financials_columns() 一致)
        self.expected_financials_columns = [
            "security_id", "fiscal_period", "announcement_date", "data_snapshot_date",
            "metric_name", "metric_value", "currency", "source_api",
            "last_updated_in_db_timestamp", "report_date", "fiscal_year", "statement_type"
        ]


    @patch.object(FinMindConnector, '_fetch_data_internal') # Patch 我們自己的內部方法
    def test_get_income_statement_happy_path(self, mock_fetch_data_internal):
        """
        測試情境 1 (財報): 成功獲取並轉換綜合損益表。
        """
        # 1. 準備 (Arrange)
        stock_id_to_test = "2330"
        start_date_to_test = "2023-01-01"
        end_date_to_test = "2023-12-31" # 雖然 get_income_statement 目前未使用 end_date

        # 模擬 _fetch_data_internal 返回的 "寬格式" DataFrame
        # 符合 FinMind().taiwan_stock_income_statement 的典型輸出
        mock_raw_income_statement_df = pd.DataFrame({
            'date': ['2023-03-31', '2023-06-30'], # report_date
            'stock_id': [stock_id_to_test, stock_id_to_test],
            'type': ['Q1', 'Q2'], # fiscal_period
            # 財報指標欄位 (這些將被 melt)
            'revenue': [1000, 1100],      # 營業收入
            'gross_profit': [400, 450],   # 營業毛利
            'net_income': [200, 220],     # 稅後淨利
            'origin_url': ['url1', 'url2'] # FinMind 可能返回的額外欄位
        })
        mock_fetch_data_internal.return_value = mock_raw_income_statement_df

        # 2. 執行 (Act)
        result_df, error = self.connector.get_income_statement(
            stock_id=stock_id_to_test,
            start_date=start_date_to_test,
            end_date=end_date_to_test
        )

        # 3. 斷言 (Assert)
        self.assertIsNone(error, "在成功情境下，錯誤訊息應該為 None")
        self.assertIsNotNone(result_df, "在成功情境下，DataFrame 不應為 None")
        self.assertIsInstance(result_df, pd.DataFrame, "返回的應為 Pandas DataFrame")
        self.assertFalse(result_df.empty, "返回的 DataFrame 不應為空")

        # b. 驗證返回的 DataFrame 格式是「長格式」，其行數應為 (原始行數 * 指標欄位數)。
        # 原始行數 = 2, 指標欄位數 = 3 (revenue, gross_profit, net_income)
        self.assertEqual(len(result_df), 2 * 3, "長格式 DataFrame 的行數不正確")

        # c. 驗證 DataFrame 的欄位 (`columns`) 完全符合我們 `fact_financial_statement` 的 schema
        self.assertListEqual(sorted(list(result_df.columns)), sorted(self.expected_financials_columns),
                             "DataFrame 的欄位名稱或順序與 schema 不符")

        # d. 驗證 `melt` 操作是否成功：檢查 `metric_name` 和 `metric_value`
        self.assertTrue('metric_name' in result_df.columns)
        self.assertTrue('metric_value' in result_df.columns)

        expected_metric_names = {'revenue', 'gross_profit', 'net_income'}
        self.assertEqual(set(result_df['metric_name'].unique()), expected_metric_names,
                         "metric_name 欄位包含的指標不正確")

        # 檢查一個特定指標的值
        revenue_q1 = result_df[
            (result_df['security_id'] == stock_id_to_test) &
            (result_df['fiscal_period'] == 'Q1') &
            (result_df['metric_name'] == 'revenue')
        ]['metric_value'].iloc[0]
        self.assertEqual(revenue_q1, 1000)

        net_income_q2 = result_df[
            (result_df['security_id'] == stock_id_to_test) &
            (result_df['fiscal_period'] == 'Q2') &
            (result_df['metric_name'] == 'net_income')
        ]['metric_value'].iloc[0]
        self.assertEqual(net_income_q2, 220)


        # e. 驗證 `report_date` 和 `security_id` 欄位是否被正確重命名和填充
        self.assertEqual(result_df['report_date'].iloc[0], date(2023, 3, 31))
        self.assertEqual(result_df['security_id'].iloc[0], stock_id_to_test)

        # 驗證其他元數據
        self.assertEqual(result_df['source_api'].iloc[0], 'finmind')
        self.assertEqual(result_df['statement_type'].iloc[0], 'income_statement')
        self.assertEqual(result_df['currency'].iloc[0], 'TWD')
        self.assertIsInstance(result_df['last_updated_in_db_timestamp'].iloc[0], datetime)
        self.assertEqual(result_df['last_updated_in_db_timestamp'].iloc[0].tzinfo, timezone.utc)
        self.assertEqual(result_df['fiscal_year'].iloc[0], 2023)
        self.assertEqual(result_df['fiscal_period'].iloc[0], 'Q1') # Check first entry after melt
        self.assertTrue(pd.isna(result_df['announcement_date'].iloc[0])) # 應為 NaT
        self.assertEqual(result_df['data_snapshot_date'].iloc[0], datetime.now(timezone.utc).date())


        # 驗證 mock 的 _fetch_data_internal 方法確實被呼叫了一次
        mock_fetch_data_internal.assert_called_once_with(
            api_method_name='taiwan_stock_income_statement',
            stock_id=stock_id_to_test,
            start_date=start_date_to_test
            # end_date 如果在 get_income_statement 中有條件加入，則這裡也要對應
        )

        # 驗證日誌記錄
        self.mock_finmind_logger.info.assert_any_call(
            f"FinMindConnector: 獲取股票 {stock_id_to_test} 從 {start_date_to_test} 開始的綜合損益表數據。"
        )
        self.mock_finmind_logger.info.assert_any_call(
            f"FinMindConnector: 成功轉換股票 {stock_id_to_test} 的 {len(result_df)} 筆 income_statement 標準化記錄。"
        )


# 這使得檔案可以直接被執行以進行測試
if __name__ == '__main__':
    unittest.main()
