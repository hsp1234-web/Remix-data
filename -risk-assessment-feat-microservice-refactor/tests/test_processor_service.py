from services.processor_service import process_data
import pandas as pd # 根據 conftest.py 的 mock_raw_data 和 process_data 的返回類型添加
import numpy as np # 用於 np.isclose 進行浮點數比較

# conftest.py 中的 mock_raw_data fixture 會被 pytest 自動發現和注入

def test_process_data(mock_raw_data: pd.DataFrame): # 添加類型提示
    """
    測試 process_data 函數是否能正確計算移動平均線。
    - mock_raw_data: 來自 conftest.py 的假數據。
    """
    # 執行待測函數
    processed_df = process_data(mock_raw_data)

    # 驗證結果
    # 1. 檢查新的欄位是否存在 (現在是小寫)
    assert 'ma20' in processed_df.columns
    assert 'ma60' in processed_df.columns

    # 2. 檢查因 dropna() 導致的行數減少
    # 由於 ma60 需要 60 個數據點，前 59 行會是 NaN
    assert len(processed_df) == len(mock_raw_data) - 59

    # 3. 抽樣檢查計算是否正確
    # 注意：mock_raw_data 的欄位名現在也是小寫的 (來自 conftest.py 的更新)

    # 為了精確比較，我們在 mock_raw_data 上計算 MA，然後取與 processed_df 對應的部分
    temp_df_for_ma_calculation = mock_raw_data.copy()
    # 使用小寫 'close' 和 'ma20_expected', 'ma60_expected'
    temp_df_for_ma_calculation['ma20_expected'] = temp_df_for_ma_calculation['close'].rolling(window=20).mean()
    temp_df_for_ma_calculation['ma60_expected'] = temp_df_for_ma_calculation['close'].rolling(window=60).mean()

    # 取 ma60 計算後非 NaN 的部分
    valid_expected_ma_df = temp_df_for_ma_calculation.iloc[59:]

    # 檢查最後一筆 ma20
    actual_ma20 = processed_df['ma20'].iloc[-1] # processed_df 中的欄位名也應為小寫
    expected_ma20 = valid_expected_ma_df['ma20_expected'].iloc[-1]
    assert np.isclose(actual_ma20, expected_ma20), f"ma20 mismatch: Actual {actual_ma20}, Expected {expected_ma20}"

    # 檢查最後一筆 ma60
    actual_ma60 = processed_df['ma60'].iloc[-1] # processed_df 中的欄位名也應為小寫
    expected_ma60 = valid_expected_ma_df['ma60_expected'].iloc[-1]
    assert np.isclose(actual_ma60, expected_ma60), f"ma60 mismatch: Actual {actual_ma60}, Expected {expected_ma60}"

    # 檢查 processed_df 的第一行
    first_valid_actual_ma20 = processed_df['ma20'].iloc[0] # processed_df 中的欄位名也應為小寫
    first_valid_expected_ma20 = valid_expected_ma_df['ma20_expected'].iloc[0]
    assert np.isclose(first_valid_actual_ma20, first_valid_expected_ma20)

    first_valid_actual_ma60 = processed_df['ma60'].iloc[0] # processed_df 中的欄位名也應為小寫
    first_valid_expected_ma60 = valid_expected_ma_df['ma60_expected'].iloc[0]
    assert np.isclose(first_valid_actual_ma60, first_valid_expected_ma60)
