import os
import sys

# 將專案根目錄添加到 sys.path，以便能夠導入 src 下的模組
# 這裡假設 main_orchestrator.py 位於專案根目錄 Financial_Forensics_Engine/
# 如果不是，需要調整路徑的計算方式
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.utils.config_loader import load_all_configs
from src.utils.logger import setup_logger
from src.database.duckdb_repository import DuckDBRepository
from src.services.taifex_service import TaifexService

class Orchestrator:
    """
    總協調器，負責初始化和驅動整個金融鑑識引擎的各個服務和流程。
    """
    def __init__(self, config_path: str, project_root_path: str):
        """
        初始化 Orchestrator。

        Args:
            config_path (str): 配置文件目錄的路徑 (例如 "config/")。
            project_root_path (str): 專案的絕對根路徑。
        """
        self.project_root = project_root_path
        self.config_dir_abs = os.path.join(self.project_root, config_path)

        # 1. 加載所有配置
        self.configs = load_all_configs(self.config_dir_abs)
        if not self.configs or 'project_config' not in self.configs:
            # 如果沒有 project_config，日誌無法正確初始化，拋出錯誤
            print("錯誤：無法加載 project_config.yaml 或配置目錄不正確。協調器無法啟動。")
            raise ValueError("project_config.yaml 未找到或加載失敗。")

        # 2. 初始化日誌記錄器
        log_config = self.configs.get('project_config', {}).get('logging', {})
        log_file_name = log_config.get('log_file_name', 'orchestrator.log')
        # 確保日誌檔案路徑是絕對的
        self.log_file_abs_path = os.path.join(self.project_root, "data_workspace", "logs", log_file_name)

        self.logger = setup_logger(
            name=self.configs.get('project_config', {}).get('project_name', 'Orchestrator'),
            log_file_path_str=self.log_file_abs_path,
            console_level_str=log_config.get('console_level', 'INFO'),
            file_level_str=log_config.get('file_level', 'DEBUG')
        )
        self.logger.info(f"Orchestrator 初始化開始。專案根目錄: {self.project_root}")
        self.logger.info(f"配置文件已從 {self.config_dir_abs} 加載。")
        self.logger.info(f"日誌將記錄到: {self.log_file_abs_path}")

        # 3. 初始化資料庫倉儲 (raw_lake 和 curated_mart)
        db_config = self.configs.get('project_config', {}).get('database', {})
        paths_config = self.configs.get('project_config', {}).get('paths', {})

        raw_lake_db_name = db_config.get('raw_lake_db_name', 'raw_lake.duckdb')
        raw_lake_dir = os.path.join(self.project_root, paths_config.get('raw_lake_dir', 'data_workspace/raw_lake'))
        self.raw_lake_db_path = os.path.join(raw_lake_dir, raw_lake_db_name)

        curated_mart_db_name = db_config.get('curated_mart_db_name', 'curated_mart.duckdb')
        curated_mart_dir = os.path.join(self.project_root, paths_config.get('curated_mart_dir', 'data_workspace/curated_mart'))
        self.curated_mart_db_path = os.path.join(curated_mart_dir, curated_mart_db_name)

        self.db_repo_raw = DuckDBRepository(db_path=self.raw_lake_db_path, logger=self.logger)

        # curated_mart 需要 schema 配置進行初始化
        database_schemas_config = self.configs.get('database_schemas') # 這是從 database_schemas.json 加載的內容
        if not database_schemas_config:
            self.logger.warning("database_schemas.json 未加載或為空，curated_mart 可能無法正確初始化 schema。")

        self.db_repo_curated = DuckDBRepository(
            db_path=self.curated_mart_db_path,
            schemas_config=database_schemas_config,
            logger=self.logger
        )
        # 初始化 curated_mart 的 schema
        try:
            self.db_repo_curated.initialize_schema() # 這裡會使用 database_schemas.json
            self.logger.info(f"Curated Mart ({self.curated_mart_db_path}) schema 初始化完成。")
        except Exception as e:
            self.logger.error(f"Curated Mart ({self.curated_mart_db_path}) schema 初始化失敗: {e}", exc_info=True)
            # 根據情況決定是否拋出異常，這裡選擇記錄並繼續，某些操作可能仍可進行

        # 4. 初始化服務 (目前只有 TaifexService)
        taifex_service_config = self.configs.get('project_config', {}).get('taifex_service', {})
        # 確保 taifex_service 使用絕對路徑
        taifex_input_unzipped_rel = taifex_service_config.get('input_dir_unzipped', 'data_workspace/input/taifex/unzipped/')
        taifex_service_config['input_dir_unzipped_abs'] = os.path.join(self.project_root, taifex_input_unzipped_rel)

        taifex_format_catalog_config = self.configs.get('taifex_format_catalog')
        if not taifex_format_catalog_config:
            self.logger.warning("taifex_format_catalog.json 未加載或為空，TaifexService 可能無法正確處理檔案。")

        self.taifex_svc = TaifexService(
            config=taifex_service_config,
            db_repo_raw=self.db_repo_raw,
            db_repo_curated=self.db_repo_curated,
            taifex_format_catalog=taifex_format_catalog_config if taifex_format_catalog_config else {},
            logger=self.logger
        )
        self.logger.info("TaifexService 初始化完成。")

        # TODO: 初始化其他服務 (IngestionService, FeatureService, etc.)
        # self.ingestion_svc = IngestionService(...)
        # self.feature_svc = FeatureService(...)

        self.logger.info("Orchestrator 初始化完畢。")

    def run_data_preparation_pipeline(self):
        """
        執行所有數據準備工作流程，創建或更新 curated_mart。
        """
        self.logger.info("===== 開始執行數據準備管道 =====")

        # 步驟 1: 處理 TAIFEX 數據 (汲取到 raw_lake，轉換到 curated_mart - 目前轉換是佔位)
        try:
            self.logger.info("--- [數據準備] 步驟 1: 執行 TAIFEX 數據處理 ---")
            taifex_results = self.taifex_svc.run_full_pipeline()
            self.logger.info(f"TAIFEX 數據處理完成。結果: {taifex_results}")
        except Exception as e:
            self.logger.error(f"TAIFEX 數據處理過程中發生錯誤: {e}", exc_info=True)
            # 根據嚴重性決定是否繼續

        # TODO: 步驟 2: 執行 API 數據採集 (ingestion_service)
        # self.logger.info("--- [數據準備] 步驟 2: 執行 API 數據採集 (尚未實現) ---")
        # self.ingestion_svc.fetch_all_sources()

        # TODO: 步驟 3: 執行特徵工程 (feature_service)
        # self.logger.info("--- [數據準備] 步驟 3: 執行特徵工程 (尚未實現) ---")
        # self.feature_svc.calculate_pillar1_features()
        # self.feature_svc.calculate_pillar3_features()

        self.logger.info("===== 數據準備管道執行完畢 =====")

    def run_150_week_analysis(self, target_weeks_list: list):
        """
        (佔位符) 為指定的週次列表生成 Gemini 分析報告。
        """
        self.logger.info(f"===== 開始為 {len(target_weeks_list)} 個目標週生成洞察報告 (佔位符) =====")
        if not target_weeks_list:
            self.logger.warning("目標週次列表為空，不執行分析。")
            return

        for week_str in target_weeks_list:
            self.logger.info(f"\n--- 正在處理目標週: {week_str} (佔位符) ---")
            # 1. 打包情境 (context_packet_service - 尚未實現)
            # context_packet = self.packet_svc.create_packet_for_week(week_str)
            # self.logger.info(f"情境包已為 {week_str} 生成 (佔位符)。")

            # 2. 生成報告 (gemini_analysis_service - 尚未實現)
            # self.gemini_svc.analyze_and_save_report(context_packet)
            # self.logger.info(f"{week_str} 的洞察報告已生成並保存 (佔位符)。")

        self.logger.info("===== 所有目標週的洞察報告生成完畢 (佔位符) =====")

    def close(self):
        """
        優雅地關閉 Orchestrator 控制的所有資源，例如資料庫連接。
        """
        self.logger.info("開始關閉 Orchestrator...")
        if self.db_repo_raw:
            self.db_repo_raw.disconnect()
        if self.db_repo_curated:
            self.db_repo_curated.disconnect()
        self.logger.info("Orchestrator 已成功關閉。")


if __name__ == "__main__":
    # 這裡的 PROJECT_ROOT 是 main_orchestrator.py 所在的目錄
    # 在我們的標準結構中，這就是 Financial_Forensics_Engine/

    # 配置文件目錄相對於 PROJECT_ROOT
    CONFIG_DIR = "config"

    orchestrator = None # 確保 orchestrator 在 try 外部定義
    try:
        print(f"主協調器啟動中... 專案根目錄: {PROJECT_ROOT}, 配置目錄: {CONFIG_DIR}")
        orchestrator = Orchestrator(config_path=CONFIG_DIR, project_root_path=PROJECT_ROOT)

        # 執行數據準備管道
        orchestrator.run_data_preparation_pipeline()

        # 執行 150 週分析 (目前是佔位符)
        # 實際使用時，target_weeks 可能來自一個 CSV 檔案或 Notebook 的輸入
        sample_target_weeks = ["2023-W50", "2023-W51"]
        orchestrator.run_150_week_analysis(sample_target_weeks)

    except Exception as e:
        # 如果 Orchestrator 初始化失敗，logger 可能還沒準備好
        if orchestrator and orchestrator.logger:
            orchestrator.logger.critical(f"Orchestrator 執行過程中發生未處理的致命錯誤: {e}", exc_info=True)
        else:
            print(f"Orchestrator 執行過程中發生未處理的致命錯誤 (logger 未初始化或初始化失敗): {e}")
            import traceback
            traceback.print_exc()

    finally:
        if orchestrator:
            orchestrator.close()
        print("主協調器執行完畢。")
