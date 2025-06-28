# modules/database/sqlite_repository.py
# ----------------------------------------------------
import sqlite3
import pandas as pd
from modules.database.interface import DatabaseInterface
from utils.config_loader import config

class SQLiteRepository(DatabaseInterface):
    """
    實現 SQLite 數據庫操作的具體邏輯。
    """
    def __init__(self):
        self.db_path = config['database']['path']
        print(f"SQLiteRepository 已連接到 {self.db_path}")
        # self.conn = sqlite3.connect(self.db_path) # 真實的連接邏輯

    def save_timeseries_data(self, identifier: str, data: pd.DataFrame):
        table_name = identifier.replace('/', '_') # 清理表名
        print(f"正在將數據儲存到 SQLite 表: {table_name}")
        # data.to_sql(table_name, self.conn, if_exists='replace') # 真實的儲存邏輯
        pass

    def get_timeseries_data(self, identifier: str) -> pd.DataFrame:
        print(f"正在從 SQLite 讀取數據...")
        # 真實的讀取邏輯
        pass
