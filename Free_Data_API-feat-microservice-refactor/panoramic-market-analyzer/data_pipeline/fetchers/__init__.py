# data_pipeline/fetchers/__init__.py
# 這個檔案的存在使 Python 將 fetchers 目錄視為一個子套件。

# 匯出此子套件中的所有具體獲取器實現
from .yfinance_fetcher import YFinanceFetcher
# from .fred_fetcher import FredFetcher  # 待實作後取消註解
# from .crypto_fetcher import CryptoFetcher  # 待實作後取消註解

# 也可以考慮定義一個工廠函數或註冊表於此，如果 fetcher 種類繁多

# 目前僅匯出已有的 YFinanceFetcher。
