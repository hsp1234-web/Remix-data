# data_pipeline/database/__init__.py
# 這個檔案的存在使 Python 將 database 目錄視為一個子套件。

# 匯出此子套件中的所有具體倉儲實現
from .duckdb_repository import DuckDBRepository

# 也可以考慮定義一個工廠函數或註冊表於此，如果未來支援多種數據庫類型。

# 目前僅匯出已有的 DuckDBRepository。
