from services.fetcher_service import save_to_duckdb
import duckdb
import os
import pandas as pd # 根據 conftest.py 的 mock_raw_data 返回類型添加

# conftest.py 中的 mock_raw_data fixture 會被 pytest 自動發現和注入
# 無需顯式導入

def test_save_to_duckdb(mock_raw_data: pd.DataFrame, tmp_path): # 添加類型提示
    """
    測試 save_to_duckdb 函數是否能成功創建資料庫並寫入數據。
    - mock_raw_data: 來自 conftest.py 的假數據。
    - tmp_path: pytest 提供的臨時目錄，確保測試不污染專案目錄。
    """
    symbol = "MOCK"
    # db_path 直接使用 tmp_path 對象，它會自動轉換為字串路徑
    db_file_path = tmp_path / "test_raw.db"

    # 執行待測函數
    save_to_duckdb(mock_raw_data, symbol, str(db_file_path)) # 確保傳遞字串路徑

    # 驗證結果
    assert os.path.exists(db_file_path)

    con = duckdb.connect(str(db_file_path)) # 連接時也使用字串路徑
    table_name = symbol.lower() # 根據 fetcher_service.py 中的邏輯
    count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()

    # 檢查 fetchone() 是否返回了 None
    assert count_result is not None, f"Query returned None for table {table_name}"
    count = count_result[0]

    con.close()

    assert count == len(mock_raw_data)
