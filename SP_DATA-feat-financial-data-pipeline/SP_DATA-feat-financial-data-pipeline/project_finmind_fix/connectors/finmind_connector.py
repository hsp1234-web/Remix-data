import pandas as pd
from FinMind.data import FinMindApi
import logging

# 設定基礎日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FinMindConnector:
    """
    用於從 FinMind API 獲取台股深度數據的連接器。
    V2: 根據 FinMind SDK 1.8.1 的變更，修正了獲取財報與籌碼數據的方法。
    """
    def __init__(self, api_token: str):
        self.api = FinMindApi()
        try:
            self.api.login(token=api_token)
            logging.info("FinMind API 登入成功。")
        except Exception as e:
            logging.error(f"FinMind API 登入失敗: {e}")
            raise

    def get_financial_statements(self, stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        獲取指定股票的綜合損益表。
        修正：使用 api.get_stock_financial_statement 替換舊的泛用方法。
        """
        logging.info(f"正在獲取 {stock_id} 從 {start_date} 到 {end_date} 的財報...")
        try:
            df = self.api.get_stock_financial_statement(
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )
            logging.info(f"成功獲取 {len(df)} 筆財報數據。")
            return df
        except Exception as e:
            logging.error(f"從 FinMind 獲取財報時出錯: {e}")
            return pd.DataFrame() # 返回空的 DataFrame 以確保穩健性

    def get_institutional_investors(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        獲取三大法人的買賣超資訊。
        修正：使用 api.get_stock_chip_institutional_investors 替換舊的泛用方法。
        """
        logging.info(f"正在獲取從 {start_date} 到 {end_date} 的法人籌碼數據...")
        try:
            df = self.api.get_stock_chip_institutional_investors(
                start_date=start_date,
                end_date=end_date,
            )
            logging.info(f"成功獲取 {len(df)} 筆法人籌碼數據。")
            return df
        except Exception as e:
            logging.error(f"從 FinMind 獲取法人籌碼時出錯: {e}")
            return pd.DataFrame() # 返回空的 DataFrame
