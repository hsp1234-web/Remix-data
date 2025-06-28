import os
import hashlib
import pandas as pd
from typing import Dict, Any, Optional, List

# 假設 DuckDBRepository 在上一層的 database 目錄中
# from ..database.duckdb_repository import DuckDBRepository
# 為了讓此模組在開發時能獨立運行一些基本測試，或者被 Orchestrator 調用，
# 我們使用更靈活的導入方式，或者依賴 sys.path 的正確設定。
# 在 Orchestrator 中，由於 sys.path.append(LOCAL_WORKSPACE) 的存在，
# 可以使用 from src.database.duckdb_repository import DuckDBRepository

class TaifexService:
    """
    處理台灣期貨交易所 (TAIFEX) 數據的服務。
    包括檔案指紋計算、格式識別、數據汲取到 raw_lake，以及轉換到 curated_mart (初步)。
    """
    def __init__(self,
                 config: Dict[str, Any],
                 db_repo_raw: Any, # DuckDBRepository for raw_lake
                 db_repo_curated: Any, # DuckDBRepository for curated_mart
                 taifex_format_catalog: Dict[str, Any],
                 logger: Any):
        """
        初始化 TaifexService。

        Args:
            config (Dict[str, Any]): TAIFEX 服務的特定配置 (來自 project_config.yaml)。
                                     應包含 input_dir_unzipped, fingerprint_lines 等。
            db_repo_raw (DuckDBRepository): 用於 raw_lake 的資料庫倉儲實例。
            db_repo_curated (DuckDBRepository): 用於 curated_mart 的資料庫倉儲實例。
            taifex_format_catalog (Dict[str, Any]): 從 taifex_format_catalog.json 加載的內容。
            logger: 日誌記錄器實例。
        """
        self.config = config
        self.db_repo_raw = db_repo_raw
        self.db_repo_curated = db_repo_curated # 目前主要用於 schema 參考，實際寫入在後續步驟
        self.format_catalog = taifex_format_catalog
        self.logger = logger

        # 從配置中獲取路徑， Orchestrator 應確保這些路徑是絕對的或相對於正確的基準
        # 這裡假設 config 中的路徑是相對於 data_workspace
        # project_root = self.config.get("project_root_path", os.getcwd()) # Orchestrator 應提供
        # self.input_dir_unzipped = os.path.join(project_root, self.config.get("input_dir_unzipped", "data_workspace/input/taifex/unzipped/"))

        # 簡化：假設 Orchestrator 傳入的 config 包含已經解析好的絕對路徑
        self.input_dir_unzipped = self.config.get("input_dir_unzipped_abs")
        if not self.input_dir_unzipped:
             self.logger.error("TaifexService: 未在配置中找到 'input_dir_unzipped_abs'。")
             raise ValueError("TaifexService: input_dir_unzipped_abs is required in config.")

        self.fingerprint_lines = self.config.get("fingerprint_lines", 5)
        self.logger.info(f"TaifexService 初始化完成。輸入目錄: {self.input_dir_unzipped}, 指紋行數: {self.fingerprint_lines}")

    def _calculate_file_fingerprint(self, filepath: str) -> Optional[str]:
        """
        計算檔案內容的 SHA256 指紋。
        讀取檔案的前 N 行，去除日期、數字和空白，然後計算雜湊。

        Args:
            filepath (str): 檔案路徑。

        Returns:
            Optional[str]: 計算得到的 SHA256 指紋字串，如果失敗則返回 None。
        """
        try:
            with open(filepath, 'r', encoding=self.config.get("default_file_encoding", "big5"), errors='ignore') as f:
                lines_for_fingerprint = []
                for i, line in enumerate(f):
                    if i >= self.fingerprint_lines:
                        break
                    # 正規化：轉小寫，移除數字和常見分隔符引起的變動
                    # 這部分可以根據實際 TAIFEX 檔案的特點調整以提高指紋的穩定性
                    normalized_line = line.lower()
                    normalized_line = ''.join(filter(lambda x: x.isalpha() or x.isspace(), normalized_line)) # 只保留字母和空格
                    normalized_line = ' '.join(normalized_line.split()) # 壓縮多餘空格
                    lines_for_fingerprint.append(normalized_line)

            if not lines_for_fingerprint:
                self.logger.warning(f"檔案 {filepath} 為空或無法讀取指紋行。")
                return None

            fingerprint_content = "\n".join(lines_for_fingerprint).encode('utf-8')
            sha256_hash = hashlib.sha256(fingerprint_content).hexdigest()
            self.logger.debug(f"檔案 {filepath} 的指紋計算內容 (前 {self.fingerprint_lines} 行正規化後):\n{lines_for_fingerprint}\n指紋: {sha256_hash}")
            return sha256_hash
        except Exception as e:
            self.logger.error(f"計算檔案 {filepath} 指紋失敗: {e}")
            return None

    def _get_recipe_for_file(self, filepath: str) -> Optional[Dict[str, Any]]:
        """
        根據檔案指紋從 format_catalog 中獲取處理配方。

        Args:
            filepath (str): 檔案路徑。

        Returns:
            Optional[Dict[str, Any]]: 對應的處理配方，如果找不到則返回 None。
        """
        fingerprint = self._calculate_file_fingerprint(filepath)
        if not fingerprint:
            return None

        recipe = self.format_catalog.get(fingerprint)
        if recipe:
            self.logger.info(f"檔案 {os.path.basename(filepath)} (指紋: {fingerprint}) 匹配到配方: {recipe.get('description', '未命名配方')}")
        else:
            self.logger.warning(f"檔案 {os.path.basename(filepath)} (指紋: {fingerprint}) 未在 format_catalog 中找到匹配配方。")
        return recipe

    def ingest_single_file(self, filepath: str) -> bool:
        """
        汲取單個 TAIFEX 檔案到 raw_lake。

        1. 獲取檔案配方。
        2. 根據配方的 parser_config 讀取 CSV/TXT 到 Pandas DataFrame。
        3. 將 DataFrame 寫入 raw_lake 的 DuckDB 中。

        Args:
            filepath (str): 要汲取的檔案的完整路徑。

        Returns:
            bool: 如果汲取成功則返回 True，否則 False。
        """
        self.logger.info(f"開始汲取檔案: {filepath}")
        recipe = self._get_recipe_for_file(filepath)

        if not recipe:
            self.logger.warning(f"檔案 {filepath} 沒有找到處理配方，跳過汲取。")
            return False

        parser_config = recipe.get("parser_config", {})
        raw_table_name = recipe.get("target_table_raw")

        if not raw_table_name:
            self.logger.error(f"配方 {recipe.get('description')} 未定義 'target_table_raw'，無法汲取檔案 {filepath}。")
            return False

        try:
            # 讀取檔案時使用配方中指定的 encoding，預設為 utf-8
            encoding = parser_config.get("encoding", "utf-8")
            skiprows = parser_config.get("skiprows", 0)
            thousands = parser_config.get("thousands") # 可能為 None

            self.logger.debug(f"使用 Pandas 讀取檔案 {filepath}。配置: encoding={encoding}, skiprows={skiprows}, thousands='{thousands}'")

            # 檢查檔案是否為空或過小
            if os.path.getsize(filepath) < 10: # 隨意設定一個閾值，例如10字節
                self.logger.warning(f"檔案 {filepath} 過小或為空，可能無法正確解析，跳過。")
                return False

            df = pd.read_csv(
                filepath,
                encoding=encoding,
                skiprows=skiprows,
                thousands=thousands,
                on_bad_lines='warn', # 對於壞行，發出警告而不是停止
                low_memory=False # 避免 DtypeWarning
            )
            self.logger.info(f"檔案 {filepath} 成功讀取到 DataFrame，共 {len(df)} 行，{len(df.columns)} 欄。")

            if df.empty:
                self.logger.warning(f"從檔案 {filepath} 讀取的 DataFrame 為空，不進行存儲。")
                return False

            # 可選：根據 recipe 中的 column_mapping_raw 重命名欄位
            column_mapping = recipe.get("column_mapping_raw")
            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)
                self.logger.info(f"已根據配方對 DataFrame 欄位進行重命名。新欄位: {df.columns.tolist()}")

            # 可選：添加原始檔案名和汲取時間戳等元數據欄位
            df['raw_source_file'] = os.path.basename(filepath)
            df['ingested_at_raw'] = pd.Timestamp.now(tz='UTC')


            # 將 DataFrame 寫入 raw_lake 的 DuckDB
            # DuckDBRepository 的 insert_df 會處理表創建 (如果不存在)
            self.db_repo_raw.insert_df(raw_table_name, df, overwrite=False, create_table_if_not_exists=True)
            self.logger.info(f"DataFrame (來自 {filepath}) 已成功寫入到 raw_lake 的表 {raw_table_name}。")
            return True

        except pd.errors.EmptyDataError:
            self.logger.warning(f"檔案 {filepath} 為空或不包含數據，跳過。")
            return False
        except FileNotFoundError:
            self.logger.error(f"汲取檔案失敗：檔案 {filepath} 未找到。")
            return False
        except Exception as e:
            self.logger.error(f"汲取檔案 {filepath} 到表 {raw_table_name} 失敗: {e}", exc_info=True)
            return False

    def run_ingestion(self) -> int:
        """
        遍歷 input_dir_unzipped 目錄下的所有檔案，對每個檔案調用 ingest_single_file。

        Returns:
            int: 成功汲取的檔案數量。
        """
        if not os.path.isdir(self.input_dir_unzipped):
            self.logger.error(f"TAIFEX 未壓縮檔案輸入目錄 {self.input_dir_unzipped} 不存在或不是一個目錄。")
            return 0

        self.logger.info(f"開始 TAIFEX 數據汲取流程，掃描目錄: {self.input_dir_unzipped}")
        successful_ingestions = 0
        processed_files = 0

        for filename in os.listdir(self.input_dir_unzipped):
            # 這裡可以添加過濾邏輯，例如只處理 .csv 或 .txt 檔案
            if filename.lower().endswith(('.csv', '.txt')):
                filepath = os.path.join(self.input_dir_unzipped, filename)
                if os.path.isfile(filepath):
                    processed_files += 1
                    self.logger.debug(f"處理檔案: {filepath}")
                    if self.ingest_single_file(filepath):
                        successful_ingestions += 1
                else:
                    self.logger.warning(f"路徑 {filepath} 不是一個檔案，跳過。")
            else:
                self.logger.debug(f"檔案 {filename} 非 CSV/TXT 檔案，跳過。")

        self.logger.info(f"TAIFEX 數據汲取流程完成。共處理 {processed_files} 個潛在檔案，成功汲取 {successful_ingestions} 個檔案。")
        return successful_ingestions

    def transform_single_raw_table(self, raw_table_name: str, recipe: Dict[str, Any]) -> bool:
        """
        （佔位符）轉換單個 raw_lake 中的表到 curated_mart。
        未來會從 raw_lake 讀取數據，調用清理函數，驗證 schema，然後寫入 curated_mart。

        Args:
            raw_table_name (str): raw_lake 中的原始表名。
            recipe (Dict[str, Any]): 與此原始表相關的處理配方。

        Returns:
            bool: 轉換是否成功（目前總是 True）。
        """
        curated_table_name = recipe.get("target_table_curated")
        cleaner_function_name = recipe.get("cleaner_function")
        schema_curated_ref = recipe.get("schema_curated_ref")

        self.logger.info(f"[佔位符] 準備轉換原始表 {raw_table_name} 到精選表 {curated_table_name}。")
        self.logger.info(f"  將使用清理函數: {cleaner_function_name} (如果實現)。")
        self.logger.info(f"  將參考精選 schema: {schema_curated_ref} (來自 database_schemas.json)。")

        if not self.db_repo_raw.table_exists(raw_table_name):
            self.logger.warning(f"原始表 {raw_table_name} 在 raw_lake 中不存在，無法進行轉換。")
            return False

        # TODO (未來實現):
        # 1. 從 self.db_repo_raw 讀取 raw_table_name 的數據到 DataFrame。
        #    df_raw = self.db_repo_raw.fetch_data(f"SELECT * FROM {raw_table_name}")
        # 2. 如果 df_raw 不為空：
        #    a. 實現並動態調用 cleaner_function_name 對 df_raw 進行清理和轉換。
        #       (可能需要一個清理函數的註冊表或 dispatcher)
        #    b. 根據 database_schemas.json 中 schema_curated_ref 定義的 schema，
        #       對清理後的 DataFrame 進行欄位選擇、類型轉換、驗證。
        #    c. 將處理好的 DataFrame 寫入到 self.db_repo_curated 的 curated_table_name 中。
        #       (注意 curated_mart 的 schema 是嚴格的，需確保 DataFrame 匹配)
        self.logger.info(f"[佔位符] 表 {raw_table_name} 的轉換邏輯尚未完全實現。")
        return True


    def run_transformation(self) -> int:
        """
        （佔位符）遍歷 format_catalog 中的所有條目，對每個條目執行轉換。

        Returns:
            int: 成功觸發轉換的表的數量。
        """
        self.logger.info("開始 TAIFEX 數據轉換流程 (目前主要為佔位符)。")
        triggered_transformations = 0
        if not self.format_catalog:
            self.logger.warning("TAIFEX 格式目錄為空，無法執行轉換。")
            return 0

        for fingerprint, recipe in self.format_catalog.items():
            raw_table_name = recipe.get("target_table_raw")
            if raw_table_name:
                self.logger.info(f"觸發對原始表 {raw_table_name} (來自指紋 {fingerprint}) 的轉換。")
                if self.transform_single_raw_table(raw_table_name, recipe):
                    triggered_transformations +=1
            else:
                self.logger.warning(f"指紋 {fingerprint} 的配方中缺少 'target_table_raw'，跳過轉換。")

        self.logger.info(f"TAIFEX 數據轉換流程完成。共觸發 {triggered_transformations} 個表的轉換（佔位）。")
        return triggered_transformations


    def run_full_pipeline(self) -> Dict[str, int]:
        """
        依次調用數據汲取和數據轉換流程。

        Returns:
            Dict[str, int]: 包含成功汲取和觸發轉換數量的字典。
        """
        self.logger.info("執行 TAIFEX 完整數據管道...")
        ingested_count = self.run_ingestion()
        transformed_count = self.run_transformation() # 目前是佔位

        results = {
            "files_ingested_to_raw_lake": ingested_count,
            "tables_triggered_for_transformation": transformed_count
        }
        self.logger.info(f"TAIFEX 完整數據管道執行完畢。結果: {results}")
        return results

if __name__ == '__main__':
    # --- 簡易測試設置 ---
    import logging
    from ..database.duckdb_repository import DuckDBRepository # 假設可以這樣導入

    # 1. 模擬 Logger
    test_logger = logging.getLogger("TaifexServiceTest")
    test_logger.setLevel(logging.DEBUG)
    if not test_logger.hasHandlers():
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        test_logger.addHandler(ch)

    # 2. 模擬專案根目錄和配置
    # 假設此腳本在 src/services/ 下，專案根目錄是上兩層
    mock_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    test_logger.info(f"模擬專案根目錄: {mock_project_root}")

    mock_config_taifex = {
        "input_dir_unzipped_abs": os.path.join(mock_project_root, "data_workspace", "input", "taifex", "unzipped"),
        "fingerprint_lines": 3, # 減少指紋行數以匹配測試檔案
        "default_file_encoding": "utf-8" # 測試檔案使用 utf-8
    }
    test_logger.info(f"模擬 TAIFEX 服務配置: {mock_config_taifex}")

    # 3. 模擬 format_catalog (從 project_config.yaml 和 taifex_format_catalog.json 提取)
    # 這裡我們手動創建一個簡化的 catalog
    mock_format_catalog = {
        # 指紋需要根據下面的測試檔案實際生成
        # "test_fingerprint_123": {
        #     "description": "每日行情測試檔 (CSV)",
        #     "target_table_raw": "raw_test_daily_quotes",
        #     "target_table_curated": "fact_test_daily_quotes", # for transformation placeholder
        #     "parser_config": {
        #         "skiprows": 0, # 測試檔案沒有表頭跳過
        #         "encoding": "utf-8"
        #     },
        #     "column_mapping_raw": {"col1": "Date", "col2": "Open", "col3": "Close"},
        #     "cleaner_function": "clean_test_data", # for transformation placeholder
        #     "schema_curated_ref": "fact_test_daily_quotes" # for transformation placeholder
        # }
    }

    # 4. 創建模擬的 DuckDBRepository 實例
    raw_db_path = os.path.join(mock_project_root, "data_workspace", "raw_lake", "test_raw_taifex.duckdb")
    curated_db_path = os.path.join(mock_project_root, "data_workspace", "curated_mart", "test_curated_taifex.duckdb")

    # 清理舊的測試資料庫
    if os.path.exists(raw_db_path): os.remove(raw_db_path)
    if os.path.exists(curated_db_path): os.remove(curated_db_path)

    mock_db_repo_raw = DuckDBRepository(raw_db_path, logger=test_logger)
    mock_db_repo_curated = DuckDBRepository(curated_db_path, logger=test_logger) # Schema 初始化可選

    # 5. 準備測試用的 TAIFEX 檔案
    test_input_dir = mock_config_taifex["input_dir_unzipped_abs"]
    os.makedirs(test_input_dir, exist_ok=True)

    test_file_1_content = "col1,col2,col3\n20230101,100,102\n20230102,102,103"
    test_file_1_path = os.path.join(test_input_dir, "test_daily_A.csv")
    with open(test_file_1_path, "w", encoding="utf-8") as f:
        f.write(test_file_1_content)
    test_logger.info(f"創建測試檔案: {test_file_1_path}")

    test_file_2_content = "header1,header2\nval1,val2\nval3,val4" # 不同格式
    test_file_2_path = os.path.join(test_input_dir, "test_summary_B.txt")
    with open(test_file_2_path, "w", encoding="utf-8") as f:
        f.write(test_file_2_content)
    test_logger.info(f"創建測試檔案: {test_file_2_path}")

    empty_file_path = os.path.join(test_input_dir, "empty.csv")
    with open(empty_file_path, "w", encoding="utf-8") as f:
        pass # 創建空檔案
    test_logger.info(f"創建空檔案: {empty_file_path}")


    # 6. 實例化 TaifexService
    # 首先，我們需要為 test_file_1_path 計算一個指紋並添加到 mock_format_catalog
    temp_service_for_fingerprint = TaifexService(mock_config_taifex, mock_db_repo_raw, mock_db_repo_curated, {}, test_logger)
    fingerprint_file1 = temp_service_for_fingerprint._calculate_file_fingerprint(test_file_1_path)

    if fingerprint_file1:
        test_logger.info(f"測試檔案 {os.path.basename(test_file_1_path)} 的指紋是: {fingerprint_file1}")
        mock_format_catalog[fingerprint_file1] = {
            "description": "每日行情測試檔 (CSV)",
            "target_table_raw": "raw_test_daily_quotes",
            "target_table_curated": "fact_test_daily_quotes",
            "parser_config": {"skiprows": 0, "encoding": "utf-8"}, # header is on first line
            "column_mapping_raw": {"col1": "Date", "col2": "Open", "col3": "Close"},
            "cleaner_function": "clean_test_data",
            "schema_curated_ref": "fact_test_daily_quotes"
        }
        test_logger.info(f"已將指紋 {fingerprint_file1} 的配方添加到 mock_format_catalog。")
    else:
        test_logger.error(f"無法為測試檔案 {os.path.basename(test_file_1_path)} 計算指紋，測試可能不完整。")

    service = TaifexService(mock_config_taifex, mock_db_repo_raw, mock_db_repo_curated, mock_format_catalog, test_logger)

    # 7. 執行測試
    test_logger.info("\n--- 開始測試 TaifexService ---")

    # 測試單個檔案汲取 (預期成功)
    test_logger.info(f"\n--- 測試 ingest_single_file ({os.path.basename(test_file_1_path)}) ---")
    service.ingest_single_file(test_file_1_path)

    # 驗證數據是否寫入 raw_lake
    if mock_db_repo_raw.table_exists("raw_test_daily_quotes"):
        df_raw_check = mock_db_repo_raw.fetch_data("SELECT * FROM raw_test_daily_quotes")
        test_logger.info(f"從 raw_lake.raw_test_daily_quotes 讀取的數據:\n{df_raw_check}")
        if df_raw_check is not None and not df_raw_check.empty:
             test_logger.info("單個檔案汲取成功，數據已存入 raw_lake。")
        else:
            test_logger.error("單個檔案汲取後，raw_lake 中未找到數據或數據為空。")
    else:
        test_logger.error("單個檔案汲取後，表 raw_test_daily_quotes 未在 raw_lake 中創建。")

    # 測試單個檔案汲取 (預期因無配方而跳過)
    test_logger.info(f"\n--- 測試 ingest_single_file ({os.path.basename(test_file_2_path)}) ---")
    service.ingest_single_file(test_file_2_path) # 此檔案指紋應不在 catalog 中

    # 測試 run_ingestion (應處理 test_file_1, 跳過 test_file_2 和 empty.csv)
    test_logger.info("\n--- 測試 run_ingestion ---")
    # 清理一下 raw_test_daily_quotes，避免重複插入導致行數翻倍
    mock_db_repo_raw.execute_query("DROP TABLE IF EXISTS raw_test_daily_quotes;")
    ingested_count = service.run_ingestion()
    test_logger.info(f"run_ingestion 完成，成功汲取 {ingested_count} 個檔案。")
    if ingested_count == 1: # 只有 test_file_1 應該被汲取
        test_logger.info("run_ingestion 汲取數量符合預期。")
        df_raw_check_run = mock_db_repo_raw.fetch_data("SELECT * FROM raw_test_daily_quotes")
        if df_raw_check_run is not None and len(df_raw_check_run) == 2 : # test_file_1 有兩行數據
             test_logger.info(f"run_ingestion 後 raw_lake.raw_test_daily_quotes 數據行數正確:\n{df_raw_check_run}")
        else:
            test_logger.error(f"run_ingestion 後 raw_lake.raw_test_daily_quotes 數據行數不正確或為空。")
    else:
        test_logger.error(f"run_ingestion 汲取數量 ({ingested_count}) 不符合預期 (應為 1)。")


    # 測試 run_transformation (佔位符)
    test_logger.info("\n--- 測試 run_transformation (佔位符) ---")
    transformed_count = service.run_transformation()
    test_logger.info(f"run_transformation 完成，觸發 {transformed_count} 個轉換 (佔位)。")
    if transformed_count == (1 if fingerprint_file1 else 0) : # 如果 test_file_1 的指紋成功加入 catalog
        test_logger.info("run_transformation 觸發數量符合預期。")
    else:
        test_logger.error(f"run_transformation 觸發數量 ({transformed_count}) 不符合預期。")

    # 測試 run_full_pipeline
    test_logger.info("\n--- 測試 run_full_pipeline ---")
    # 再次清理
    mock_db_repo_raw.execute_query("DROP TABLE IF EXISTS raw_test_daily_quotes;")
    pipeline_results = service.run_full_pipeline()
    test_logger.info(f"run_full_pipeline 完成，結果: {pipeline_results}")
    if pipeline_results["files_ingested_to_raw_lake"] == 1 and \
       pipeline_results["tables_triggered_for_transformation"] == (1 if fingerprint_file1 else 0):
        test_logger.info("run_full_pipeline 結果符合預期。")
    else:
        test_logger.error(f"run_full_pipeline 結果不符合預期。")


    # 8. 清理測試檔案和目錄
    test_logger.info("\n--- 清理測試環境 ---")
    try:
        os.remove(test_file_1_path)
        os.remove(test_file_2_path)
        os.remove(empty_file_path)
        # 如果目錄為空則刪除 (避免意外刪除其他檔案)
        if not os.listdir(test_input_dir):
            os.rmdir(test_input_dir)
        test_logger.info("測試檔案和目錄已清理。")
    except OSError as e:
        test_logger.error(f"清理測試檔案時發生錯誤: {e}")

    if os.path.exists(raw_db_path): os.remove(raw_db_path)
    if os.path.exists(curated_db_path): os.remove(curated_db_path)
    test_logger.info("測試資料庫檔案已清理。")

    test_logger.info("\nTaifexService 測試完畢。")
