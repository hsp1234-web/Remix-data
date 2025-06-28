import pytest
import pandas as pd
import numpy as np

@pytest.fixture(scope="session")
def mock_raw_data():
    """
    提供一份假的、用於測試的原始 DataFrame 數據。
    欄位名稱已改為小寫，以匹配 fetcher_service 的輸出。
    """
    dates = pd.to_datetime(pd.date_range(start="2023-01-01", periods=100))
    data = {
        'open': np.random.uniform(100, 102, size=100),
        'high': np.random.uniform(102, 104, size=100),
        'low': np.random.uniform(98, 100, size=100),
        'close': np.random.uniform(100, 103, size=100),
        'adj close': np.random.uniform(100, 103, size=100), # 保持 adj close 以模擬 yfinance auto_adjust=False 的情況，或如果 auto_adjust=True 但 repair=True 可能仍保留的某些情況
        'volume': np.random.randint(1_000_000, 5_000_000, size=100)
    }
    df = pd.DataFrame(data, index=dates)
    df.index.name = 'date' # 也將索引名稱改為小寫
    return df
