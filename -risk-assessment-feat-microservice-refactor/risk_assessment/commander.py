# commander.py
# ----------------------------------------------------
from utils.config_loader import config
from modules.fetchers.interface import DataFetcherInterface
from modules.database.interface import DatabaseInterface
# ... 其他模組的接口

# 註：此處應使用工廠模式來創建實例，為求簡潔暫時直接導入
from modules.fetchers.fred_fetcher import FredFetcher
from modules.database.sqlite_repository import SQLiteRepository

class Commander:
    def __init__(self):
        # 面向接口編程：變數的型別是「接口」，而非具體實現
        self.fetcher: DataFetcherInterface = FredFetcher()
        self.database: DatabaseInterface = SQLiteRepository()
        print("指揮官已就緒。")

    def run_market_analysis_pipeline(self, asset_id: str):
        """
        執行一個完整的市場分析流程 (高階任務)
        """
        print(f"--- 開始為 {asset_id} 執行分析管線 ---")
        try:
            # 1. 指揮 Fetcher 獲取數據
            print("步驟 1: 指揮數據獲取器...")
            raw_data = self.fetcher.fetch_data(asset_id)
            print(f"成功獲取 {len(raw_data)} 筆數據。")

            # 2. 指揮 Database 儲存數據
            print("步驟 2: 指揮數據庫儲存...")
            self.database.save_timeseries_data(asset_id, raw_data)
            print("數據已存入數據庫。")

            # 3. ...指揮 Processor 進行處理...
            # 4. ...指揮 RiskModel 進行計算...

            print(f"--- 分析管線執行完畢 ---")
            return "報告生成成功"

        except Exception as e:
            print(f"錯誤：管線執行失敗 - {e}")
            return "報告生成失敗"

# 讓 Colab 可以輕易調用
def generate_report():
    cmd = Commander()
    return cmd.run_market_analysis_pipeline("FRED/T10Y2Y")
