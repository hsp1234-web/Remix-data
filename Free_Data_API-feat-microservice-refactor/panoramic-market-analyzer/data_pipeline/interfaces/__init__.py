# data_pipeline/interfaces/__init__.py
# 這個檔案的存在使 Python 將 interfaces 目錄視為一個子套件。

# 匯出此子套件中的所有介面，方便外部直接從 interfaces 導入
from .data_fetcher_interface import DataFetcherInterface
from .database_interface import DatabaseInterface
