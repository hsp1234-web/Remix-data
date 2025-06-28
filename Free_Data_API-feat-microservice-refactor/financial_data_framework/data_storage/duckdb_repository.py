# data_storage/duckdb_repository.py

import duckdb
import pandas as pd
import logging
from interfaces.database_interface import DatabaseInterface

logger = logging.getLogger(__name__)

class DuckDBRepository(DatabaseInterface):
    """
    使用 DuckDB 作為後端存儲的數據庫實現。
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
        self.connect()

    def connect(self):
        """建立到 DuckDB 文件的連接。"""
        try:
            self.connection = duckdb.connect(database=self.db_path, read_only=False)
            logger.info(f"成功連接到 DuckDB 數據庫: {self.db_path}")
        except Exception as e:
            logger.error(f"連接 DuckDB 失敗: {e}", exc_info=True)
            raise

    def disconnect(self):
        """關閉數據庫連接。"""
        if self.connection:
            self.connection.close()
            logger.info("DuckDB 數據庫連接已關閉。")

    def _create_ohlcv_table_if_not_exists(self, table_name: str):
        """私有輔助方法，用於創建 OCHLV 數據表。"""
        # 簡單的 schema，可根據需要擴展
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol VARCHAR,
            date TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            dividends DOUBLE,
            stock_splits DOUBLE,
            PRIMARY KEY (symbol, date)
        );
        """
        self.connection.execute(create_table_sql)
        logger.info(f"數據表 '{table_name}' 已檢查/創建。")

    def save_ohlcv(self, data: pd.DataFrame, table_name: str):
        """
        使用 UPSERT 邏輯保存 OCHLV 數據，避免重複。
        """
        if data is None or data.empty:
            logger.warning("接收到空的 DataFrame，無需保存。")
            return

        self._create_ohlcv_table_if_not_exists(table_name)

        # 使用 DuckDB 的 ON CONFLICT (UPSERT) 功能
        # 這要求目標表有主鍵或唯一約束
        # DuckDB 3.3.0+ for ON CONFLICT DO UPDATE
        upsert_sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM temp_df
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            dividends = EXCLUDED.dividends,
            stock_splits = EXCLUDED.stock_splits;
        """
        try:
            # 將 pandas DataFrame 註冊為臨時視圖
            self.connection.register('temp_df', data)
            self.connection.execute(upsert_sql)
            logger.info(f"成功將 {len(data)} 筆數據 UPSERT 到 '{table_name}'。")
        except Exception as e:
            logger.error(f"保存數據到 '{table_name}' 時出錯: {e}", exc_info=True)
        finally:
            # 取消註冊臨時視圖
            self.connection.unregister('temp_df')

    def get_ohlcv(self, table_name: str, symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """從 DuckDB 中查詢數據。"""
        query = f"""
        SELECT * FROM {table_name}
        WHERE symbol = ? AND date >= ? AND date <= ?
        ORDER BY date;
        """
        try:
            result_df = self.connection.execute(query, [symbol, start_date, end_date]).fetchdf()
            logger.info(f"從 '{table_name}' 查詢到 {len(result_df)} 筆 '{symbol}' 的數據。")
            return result_df
        except Exception as e:
            logger.error(f"從 '{table_name}' 查詢數據時出錯: {e}", exc_info=True)
            return None
