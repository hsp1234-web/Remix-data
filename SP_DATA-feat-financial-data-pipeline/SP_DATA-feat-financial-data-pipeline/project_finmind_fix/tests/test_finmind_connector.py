import os
import pandas as pd
import pytest
from connectors.finmind_connector import FinMindConnector

# 從環境變數讀取 API Token
API_TOKEN = os.getenv("FINMIND_API_TOKEN")

@pytest.fixture(scope="module")
def connector():
    """提供一個 FinMindConnector 的實例。如果沒有 Token，則跳過所有測試。"""
    if not API_TOKEN:
        pytest.skip("FINMIND_API_TOKEN 未設定，跳過所有 FinMind 整合測試。")
    return FinMindConnector(api_token=API_TOKEN)

def test_get_financial_statements(connector):
    """測試獲取財報數據的功能 (台積電 2330)。"""
    print("\n執行測試: test_get_financial_statements")
    df = connector.get_financial_statements(stock_id="2330", start_date="2023-01-01", end_date="2023-03-31")
    assert isinstance(df, pd.DataFrame), "返回值應為 DataFrame"
    assert not df.empty, "返回的財報 DataFrame 不應為空"
    # 增加一個欄位檢查，確保獲取的是財報數據
    assert 'revenue' in df.columns, "財報數據中應包含 'revenue' 欄位"
    print("測試成功: 財報數據獲取成功，且格式正確。")

def test_get_institutional_investors(connector):
    """測試獲取三大法人籌碼數據的功能。"""
    print("\n執行測試: test_get_institutional_investors")
    df = connector.get_institutional_investors(start_date="2024-01-02", end_date="2024-01-02")
    assert isinstance(df, pd.DataFrame), "返回值應為 DataFrame"
    assert not df.empty, "返回的籌碼 DataFrame 不應為空"
    # 增加一個欄位檢查，確保獲取的是法人數據
    assert 'buy' in df.columns and 'sell' in df.columns, "法人數據中應包含 'buy' 和 'sell' 欄位"
    print("測試成功: 法人籌碼數據獲取成功，且格式正確。")
