# modules/fetchers/fred_fetcher.py
# ----------------------------------------------------
import pandas as pd
from modules.fetchers.interface import DataFetcherInterface
from utils.config_loader import config

class FredFetcher(DataFetcherInterface):
    """
    實現從 FRED 獲取數據的具體 logique。
    """
    def __init__(self):
        self.api_key = config['api_keys']['fred']
        print("FredFetcher 已初始化。")

    def fetch_data(self, identifier: str) -> pd.DataFrame:
        print(f"正在從 FRED 獲取 {identifier} (使用 API Key: {self.api_key[:4]}...)")
        # 這裡應為真實的 API 請求邏輯
        # 為求範例簡潔，我們回傳一個模擬的 DataFrame
        dates = pd.to_datetime(['2025-01-01', '2025-01-02'])
        data = {'value': [1.5, 1.6]}
        return pd.DataFrame(data, index=dates)
