# -*- coding: utf-8 -*-
"""
汲取管線 (Ingestion Pipeline) 實現模組

本模組定義並實現了數據汲取管線 (`IngestionPipeline`)。
其主要職責是掃描指定的來源資料夾，識別新檔案或需要重新處理的檔案，
將這些檔案的原始二進位內容存儲到 `raw_lake.db`（由 `db_manager` 管理的 `raw_files` 表），
並在 `manifest.db`（由 `db_manager` 管理的 `file_processing_log` 表）中創建或更新
相應的處理記錄，標記其狀態為 `RAW_INGESTED`。

主要功能：
- 掃描一個或多個來源資料夾（包括子目錄）。
- 為每個掃描到的檔案計算 SHA256 內容雜湊值。
- 根據檔案雜湊值查詢 `manifest.db`，以避免重複汲取已成功處理的檔案，
  並能識別先前處理失敗或被隔離的檔案以進行可能的重新處理。
- 將新檔案或需要更新的檔案的原始內容存入 `raw_files` 表。
- 在 `file_processing_log` 表中為每個處理的檔案創建或更新一條記錄，
  包含檔案雜湊、原始路徑、汲取時間戳、當前執行ID以及 `RAW_INGESTED` 狀態。
  如果檔案是重新汲取（例如先前失敗），則會清除舊的轉換相關欄位。
"""
import time
from pathlib import Path
from typing import List, Union, Optional, Tuple # Tuple 從 typing 導入
import os
import duckdb # 導入 duckdb 以便類型提示

from taifex_pipeline.core.logger_setup import get_logger, EXECUTION_ID
from taifex_pipeline.core.utils import calculate_file_sha256
from taifex_pipeline.database import db_manager

logger = get_logger(__name__)

DEFAULT_INPUT_DIRS: List[Path] = [
    Path("data/01_input_files/"),
    Path("data/00_landing_zone/")
]
"""
預設的輸入資料夾列表 (相對於專案根目錄)。
- `data/01_input_files/`: 通常用於存放從期交所網站直接下載的原始檔案 (例如 ZIP 壓縮檔)。
- `data/00_landing_zone/`: 通常用於存放由 API 採集器或網路爬蟲獲取的數據檔案。
"""

class IngestionPipeline:
    """
    數據汲取管線類。

    封裝了掃描來源、識別新檔案、將原始數據存入數據湖，
    以及在處理清單 (manifest) 中登記檔案狀態等核心邏輯。
    """
    def __init__(self,
                 source_directories: Optional[List[Union[str, Path]]] = None):
        """
        初始化汲取管線。

        Args:
            source_directories (Optional[List[Union[str, Path]]]):
                一個包含多個來源資料夾路徑的列表。路徑可以是字串或 `pathlib.Path` 物件，
                且應為相對於專案根目錄的相對路徑。
                如果此參數為 `None` 或空列表，則管線將使用 `DEFAULT_INPUT_DIRS` 中定義的預設來源目錄。

        Raises:
            # 此處不直接拋出，而是在 run() 或 process_file() 中處理目錄不存在的情況。
            # FileNotFoundError: 如果任何指定的 source_directory 不存在。 (目前的實現是跳過)
        """
        # self.project_root 用於將相對路徑轉為絕對路徑，並在記錄 original_file_path 時使用
        # 此檔案位於 src/taifex_pipeline/ingestion/pipeline.py
        self.project_root: Path = Path(__file__).resolve().parents[3]

        effective_source_dirs: List[Union[str, Path]]
        if source_directories:
            effective_source_dirs = source_directories
            logger.info(f"汲取管線將使用使用者指定的來源目錄。")
        else:
            effective_source_dirs = DEFAULT_INPUT_DIRS
            logger.info(f"汲取管線將使用預設的來源目錄。")

        self.source_directories: List[Path] = []
        for d in effective_source_dirs:
            abs_path = (self.project_root / Path(d)).resolve()
            # 在初始化時不檢查目錄是否存在，而是在 run() 中掃描時檢查
            self.source_directories.append(abs_path)

        logger.info(f"汲取管線已初始化。將掃描以下絕對路徑目錄：")
        for d_path in self.source_directories:
            logger.info(f"  - {d_path}")

        # 確保資料庫 (特別是 raw_lake 和 manifest 表) 已初始化
        # db_manager.initialize_databases() 會處理連接獲取和表創建
        db_manager.initialize_databases()

    def _scan_single_directory(self, dir_path: Path) -> List[Path]:
        """
        遞歸掃描指定的單個目錄及其所有子目錄，返回找到的所有檔案的路徑列表。
        會過濾掉常見的隱藏檔案或系統臨時檔案。

        Args:
            dir_path (Path): 要掃描的目錄的絕對路徑。

        Returns:
            List[Path]: 在該目錄下找到的所有有效檔案的絕對路徑列表。
                        如果目錄不存在或不是一個目錄，則返回空列表。
        """
        found_files: List[Path] = []
        if not dir_path.is_dir():
            logger.warning(f"來源目錄 '{dir_path}' 不存在或不是一個有效目錄，將跳過此目錄的掃描。")
            return found_files

        logger.info(f"正在掃描目錄: '{dir_path}'...")
        for root_str, _, files_in_root in os.walk(dir_path):
            root_path = Path(root_str)
            for file_name in files_in_root:
                file_path = root_path / file_name
                # 排除常見的系統隱藏檔案 (如 .DS_Store) 或臨時檔案 (如 Thumbs.db)
                if file_name.startswith('.') or file_name.lower() == 'thumbs.db':
                    logger.debug(f"跳過隱藏/系統檔案: {file_path}")
                    continue
                if file_path.is_file(): # 確保它確實是一個檔案
                    found_files.append(file_path)
        logger.info(f"在目錄 '{dir_path}' 中掃描到 {len(found_files)} 個潛在檔案。")
        return found_files

    def _process_single_file(self, file_path: Path) -> bool:
        """
        處理單個掃描到的檔案。

        執行流程：
        1. 計算檔案內容的 SHA256 雜湊值。
        2. 檢查 `manifest.db`：
           - 如果檔案雜湊已存在且狀態為 `RAW_INGESTED` 或 `TRANSFORMATION_SUCCESS`，則跳過。
           - 如果已存在但狀態為失敗/隔離等，則繼續處理以更新記錄（特別是 `ingestion_timestamp` 和 `pipeline_execution_id`）。
        3. 讀取檔案的原始二進位內容。
        4. 將原始內容存儲到 `raw_lake.db` 的 `raw_files` 表（`db_manager.store_raw_file`）。
        5. 更新 `manifest.db` 中的記錄（`db_manager.update_manifest_record`）：
           - 設定狀態為 `RAW_INGESTED`。
           - 記錄原始檔案路徑（相對於專案根目錄）、汲取時間戳、當前執行ID。
           - 清除任何舊的轉換相關欄位（如 `fingerprint_hash`, `error_message` 等），因為這是新的汲取事件。

        Args:
            file_path (Path): 要處理的檔案的絕對路徑。

        Returns:
            bool: 如果檔案被新汲取或其 manifest 記錄被成功更新為 `RAW_INGESTED`，則返回 `True`。
                  在其他情況下（例如，檔案被跳過，或在處理過程中發生錯誤），返回 `False`。
        """
        logger.debug(f"開始處理檔案: '{file_path}'")
        file_hash = calculate_file_sha256(file_path)

        if not file_hash:
            logger.error(f"無法計算檔案 '{file_path}' 的 SHA256 雜湊值，跳過此檔案。")
            return False

        existing_record = db_manager.get_manifest_record(file_hash)
        should_skip_ingestion = False
        if existing_record:
            status = existing_record.get("status")
            if status in ["RAW_INGESTED", "TRANSFORMATION_SUCCESS"]:
                logger.info(f"檔案 '{file_path.name}' (Hash: {file_hash[:10]}...) 已存在於 Manifest "
                            f"且狀態為 '{status}'，無需重新汲取。")
                should_skip_ingestion = True # 技術上是跳過，但對於計數來說不算新汲取
                # 如果需要，即使跳過也可以更新 last_seen 或類似的欄位，但目前設計中沒有
                return False # 表示不是新汲取
            else: # 例如 TRANSFORMATION_FAILED, QUARANTINED, UNKNOWN
                 logger.info(f"檔案 '{file_path.name}' (Hash: {file_hash[:10]}...) 已存在於 Manifest "
                            f"但狀態為 '{status}'。將重新確認原始內容並更新汲取記錄。")

        try:
            with open(file_path, "rb") as f:
                raw_content = f.read()
        except IOError as e:
            logger.error(f"讀取檔案 '{file_path}' 內容時發生 IO 錯誤: {e}", exc_info=True)
            return False # 讀取失敗，無法繼續

        # 存儲到 Raw Lake。db_manager.store_raw_file 使用 INSERT OR REPLACE 語義 (ON CONFLICT DO UPDATE)
        # 所以如果 hash 已存在，它會更新 raw_content 和 first_seen_timestamp
        if not db_manager.store_raw_file(file_hash, raw_content):
            logger.error(f"儲存檔案 '{file_path.name}' (Hash: {file_hash[:10]}...) 到 Raw Lake 失敗。")
            return False

        try:
            relative_path_str = str(file_path.relative_to(self.project_root))
        except ValueError: # 如果 file_path 不在 project_root 下 (例如是絕對路徑且非子路徑)
            relative_path_str = str(file_path)

        current_ingestion_time = time.time()
        update_success = db_manager.update_manifest_record(
            file_hash=file_hash,
            original_file_path=relative_path_str,
            status="RAW_INGESTED", # 無論先前狀態如何，都更新為 RAW_INGESTED
            ingestion_timestamp_epoch=current_ingestion_time,
            pipeline_execution_id=EXECUTION_ID,
            # 重置轉換相關的欄位，因為這是一個新的汲取/重新汲取事件
            fingerprint_hash=None,
            transformation_timestamp_epoch=None,
            target_table_name=None,
            processed_row_count=None,
            error_message=None
        )

        if not update_success:
            logger.error(f"更新檔案 '{file_path.name}' (Hash: {file_hash[:10]}...) 的 Manifest 記錄失敗。")
            return False

        logger.info(f"檔案 '{file_path.name}' (Hash: {file_hash[:10]}...) 已成功汲取，"
                    f"Manifest 狀態更新為 RAW_INGESTED。")
        return True # 表示新汲取或成功更新了汲取記錄

    def run(self) -> Tuple[int, int]:
        """
        執行完整的汲取管線流程。

        掃描所有配置的來源資料夾，處理找到的每個檔案，
        將新數據汲取到原始數據湖並更新處理清單。

        Returns:
            Tuple[int, int]: 一個元組，包含：
                - `successfully_processed_count` (int): 成功汲取或其 Manifest 記錄被更新為
                  `RAW_INGESTED` 的檔案數量。
                - `total_files_scanned` (int): 在所有來源目錄中掃描到的檔案總數
                  （不包括被過濾的隱藏/系統檔案）。
        """
        logger.info(f"===== 汲取管線啟動 (Execution ID: {EXECUTION_ID}) =====")
        overall_start_time = time.time()

        all_found_files: List[Path] = []
        for dir_to_scan in self.source_directories:
            all_found_files.extend(self._scan_single_directory(dir_to_scan))

        total_files_scanned = len(all_found_files)
        if not all_found_files:
            logger.info("在所有配置的來源目錄中均未掃描到任何有效檔案。")
            duration_empty = time.time() - overall_start_time
            logger.info(f"汲取管線執行完畢 (耗時: {duration_empty:.2f} 秒)。掃描到 0 個檔案，新汲取 0 個。")
            return 0, 0

        logger.info(f"總共掃描到 {total_files_scanned} 個檔案，開始逐一處理汲取...")

        successfully_processed_count = 0 # 計數實際被汲取或manifest被更新的檔案
        for i, file_to_process in enumerate(all_found_files):
            logger.info(f"--- 正在處理第 {i+1}/{total_files_scanned} 個檔案: '{file_to_process.name}' ---")
            if self._process_single_file(file_to_process):
                successfully_processed_count += 1

        overall_duration = time.time() - overall_start_time
        logger.info(f"===== 汲取管線執行完畢 (耗時: {overall_duration:.2f} 秒) =====")
        logger.info(f"  總共掃描到檔案數: {total_files_scanned}")
        logger.info(f"  成功汲取/更新汲取記錄的檔案數: {successfully_processed_count}")

        return successfully_processed_count, total_files_scanned

# --- 範例使用 (通常由 run.py 主啟動腳本調用) ---
if __name__ == "__main__":
    # 初始化日誌 (如果尚未在應用程式入口處初始化)
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # import logging
    # setup_global_logger(log_level_console=logging.DEBUG) # 使用 DEBUG 以查看詳細流程

    logger.info("開始執行 ingestion_pipeline.py 範例...")

    # 測試前準備：創建一些測試用的來源目錄和檔案
    # 假設此檔案位於 src/taifex_pipeline/ingestion/pipeline.py
    # 則專案根目錄是 Path(__file__).resolve().parents[3]
    current_project_root = Path(__file__).resolve().parents[3]

    # 定義測試用的輸入目錄 (相對於專案根目錄)
    test_source_dir1_relative = Path("data") / "01_input_files" / "ingestion_test_s1"
    test_source_dir2_relative = Path("data") / "00_landing_zone" / "ingestion_test_s2"

    test_input_dir1_abs = current_project_root / test_source_dir1_relative
    test_input_dir2_abs = current_project_root / test_source_dir2_relative

    test_input_dir1_abs.mkdir(parents=True, exist_ok=True)
    test_input_dir2_abs.mkdir(parents=True, exist_ok=True)

    # 清理可能存在的舊資料庫檔案，以便從乾淨的狀態開始測試
    db_manager.close_all_connections() # 確保沒有鎖定
    db_path_raw_manifest_test = current_project_root / db_manager.DEFAULT_DATA_DIR / db_manager.RAW_LAKE_SUBDIR / db_manager.RAW_LAKE_DB_NAME
    if db_path_raw_manifest_test.exists():
        db_path_raw_manifest_test.unlink()
        logger.info(f"已刪除舊的 '{db_manager.RAW_LAKE_DB_NAME}' 以便進行汲取測試。")

    # 創建測試檔案
    (test_input_dir1_abs / "file_A.txt").write_text("內容：這是檔案A。", encoding="utf-8")
    (test_input_dir1_abs / "file_B.csv").write_text("col_one,col_two\nval_1,val_2", encoding="utf-8")
    (test_input_dir2_abs / "file_C.json").write_text("{\"data_key\": \"data_value\"}", encoding="utf-8")
    (test_input_dir2_abs / ".DS_Store").write_text("macOS system file", encoding="utf-8") # 應被忽略

    # 手動在 Manifest 中插入一條 file_A.txt 的記錄，模擬它之前已被成功轉換
    # 這需要先初始化一次資料庫以創建表結構
    db_manager.initialize_databases()
    file_A_hash_manual = calculate_file_sha256(test_input_dir1_abs / "file_A.txt")
    if file_A_hash_manual:
        # 先存入 raw_lake，因為 manifest 有外鍵
        db_manager.store_raw_file(file_A_hash_manual, (test_input_dir1_abs / "file_A.txt").read_bytes())
        db_manager.update_manifest_record(
            file_hash=file_A_hash_manual,
            original_file_path=str(test_source_dir1_relative / "file_A.txt"), # 使用相對路徑
            status="TRANSFORMATION_SUCCESS", # 標記為已成功轉換
            ingestion_timestamp_epoch=time.time() - (24 * 3600), # 模擬一天前汲取
            pipeline_execution_id="test_exec_id_prior"
        )
        logger.info(f"手動為 file_A.txt (Hash: {file_A_hash_manual[:10]}) 插入 'TRANSFORMATION_SUCCESS' 記錄。")

    # 指定測試用的來源目錄列表 (使用相對於專案根的字串路徑)
    test_source_dirs_for_pipeline = [
        str(test_source_dir1_relative),
        str(test_source_dir2_relative)
    ]

    # 實例化並運行汲取管線
    ingestion_pipeline_instance = IngestionPipeline(source_directories=test_source_dirs_for_pipeline)
    ingested_files_count, scanned_files_count = ingestion_pipeline_instance.run()

    logger.info(f"\n汲取管線範例執行結果：")
    logger.info(f"  總共掃描到檔案數: {scanned_files_count}")
    logger.info(f"  成功汲取/更新汲取記錄的檔案數: {ingested_files_count}")

    # 預期結果：
    # - 掃描到 3 個檔案 (file_A.txt, file_B.csv, file_C.json)，.DS_Store 被忽略。
    # - file_A.txt 因狀態為 TRANSFORMATION_SUCCESS 而被跳過（不計入新汲取）。
    # - file_B.csv 和 file_C.json 應被新汲取。
    # 所以, scanned_files_count == 3, ingested_files_count == 2
    assert scanned_files_count == 3, f"預期掃描到 3 個檔案，實際為 {scanned_files_count}"
    assert ingested_files_count == 2, f"預期新汲取 2 個檔案，實際為 {ingested_files_count}"

    logger.info("\n--- 驗證 Manifest 資料庫內容 ---")
    file_B_hash = calculate_file_sha256(test_input_dir1_abs / "file_B.csv")
    file_C_hash = calculate_file_sha256(test_input_dir2_abs / "file_C.json")

    record_A = db_manager.get_manifest_record(file_A_hash_manual) # type: ignore[arg-type]
    record_B = db_manager.get_manifest_record(file_B_hash) # type: ignore[arg-type]
    record_C = db_manager.get_manifest_record(file_C_hash) # type: ignore[arg-type]

    assert record_A is not None and record_A["status"] == "TRANSFORMATION_SUCCESS", \
        f"file_A.txt 狀態應保持 TRANSFORMATION_SUCCESS, 實際為 {record_A.get('status') if record_A else 'None'}"
    assert record_B is not None and record_B["status"] == "RAW_INGESTED", \
        f"file_B.csv 狀態應為 RAW_INGESTED, 實際為 {record_B.get('status') if record_B else 'None'}"
    assert record_C is not None and record_C["status"] == "RAW_INGESTED", \
        f"file_C.json 狀態應為 RAW_INGESTED, 實際為 {record_C.get('status') if record_C else 'None'}"

    logger.info("Manifest 中各檔案狀態符合預期。")

    # 驗證 raw_lake 中的內容 (抽樣檢查 file_B.csv)
    content_B_from_db = db_manager.get_raw_file_content(file_B_hash) # type: ignore[arg-type]
    assert content_B_from_db == (test_input_dir1_abs / "file_B.csv").read_bytes(), \
        "從 Raw Lake 中讀取的 file_B.csv 內容與原始不符"
    logger.info("file_B.csv 的內容已正確存儲並從 Raw Lake 中取回。")

    logger.info("\ningestion_pipeline.py 範例執行完畢並通過基本驗證。")

    # 測試完畢後清理 (可選，但在自動化測試中建議清理)
    # (test_input_dir1_abs / "file_A.txt").unlink(missing_ok=True)
    # (test_input_dir1_abs / "file_B.csv").unlink(missing_ok=True)
    # (test_input_dir2_abs / "file_C.json").unlink(missing_ok=True)
    # (test_input_dir2_abs / ".DS_Store").unlink(missing_ok=True)
    # if test_input_dir1_abs.exists(): test_input_dir1_abs.rmdir()
    # if test_input_dir2_abs.exists(): test_input_dir2_abs.rmdir()
    # logger.info("已清理測試時創建的檔案和目錄。")

    db_manager.close_all_connections()

[end of MyTaifexDataProject/src/taifex_pipeline/ingestion/pipeline.py]

[start of MyTaifexDataProject/src/taifex_pipeline/transformation/format_detector.py]
# -*- coding: utf-8 -*-
"""
格式指紋計算模組 (Format Detector)

本模組負責根據檔案內容（主要是標頭行）自動生成一個唯一的「格式指紋」。
這個指紋隨後可用於從「格式指紋目錄」 (`format_catalog.json`) 中查找
對應的處理配方（parser config, cleaner function 等）。

主要實現流程：
1.  **多行嗅探 (Multi-line Sniffing)**：讀取檔案開頭的若干行作為標頭候選。
2.  **標頭定位 (Header Localization)**：
    - 嘗試使用多種常見編碼 (UTF-8, UTF-8 with BOM, MS950/BIG5) 解碼這些行。
    - 從解碼後的行中，根據啟發式規則（例如，欄位數量、是否包含常見關鍵字、
      非數字欄位比例等）找到最可能代表標頭的那一行。
    - 採用「首個匹配但優先選擇欄位更多且符合條件的行」策略。
3.  **標頭正規化 (Header Normalization)**：
    - 提取選定的標頭行中的所有欄位名稱。
    - 對每個欄位名稱進行正規化處理：
        a. 移除所有內部及首尾的空白字元。
        b. 轉換為全小寫。
    - 將正規化後的欄位名列表按字母順序（Unicode碼點順序）排序。
4.  **指紋計算 (Fingerprint Calculation)**：
    - 使用固定的單一分隔符（例如 `|`）將排序後的正規化欄位名合併成一個字串。
    - 計算該合併字串的 SHA256 雜湊值，此即為該檔案格式的指紋。

這種方法旨在確保即使檔案的欄位順序、編碼（在支援範圍內）、
欄位名的大小寫或內部空格有所不同，只要其「欄位集合」的本質相同，
就能產生一致的格式指紋。
"""
import hashlib
import io
import re # re 模組通常用於更複雜的文本處理，此處可能較少用到
from typing import Optional, List, Tuple

from taifex_pipeline.core.logger_setup import get_logger

logger = get_logger(__name__)

# --- 常數設定 ---
MAX_HEADER_CANDIDATE_LINES: int = 20
"""在檔案開頭讀取多少行作為標頭候選行的最大數量。"""

MIN_COLUMNS_FOR_HEADER: int = 2
"""一行至少需要包含多少個逗號分隔的欄位，才被初步考慮為可能的標頭行。"""

COMMON_HEADER_KEYWORDS: List[str] = [
    "日期", "代號", "契約", "價格", "成交", "時間", "數量", "序列",
    "開盤", "最高", "最低", "收盤", "結算", "未平倉", "買賣權",
    "身份別", "期貨", "選擇權", "金額", "口數", "比率", "商品",
    "月份", "週別", "履約價", "漲跌", "名稱", "合計"
]
"""
一些在期交所數據檔案標頭中常見的中文關鍵字。
這些關鍵字（小寫，無空格）用於輔助判斷某行是否為標頭行。
此列表可根據實際遇到的檔案格式進行擴充。
"""

def _normalize_column_name(name: str) -> str:
    """
    正規化單個原始欄位名稱。

    執行以下操作：
    1. 移除字串首尾的空白字元。
    2. 移除字串內部所有類型的空白字元（包括空格、製表符等）。
    3. 將整個字串轉換為小寫。

    Args:
        name (str): 原始的欄位名稱字串。

    Returns:
        str: 正規化處理後的欄位名稱字串。
             例如：" 商品 代號 " -> "商品代號", "Trade Date" -> "tradedate"。
    """
    name = name.strip()  # 移除首尾空白
    name = "".join(name.split())  # 移除所有內部空白 (split無參數時會按所有空白分割)
    return name.lower()  # 轉換為小寫

def _extract_potential_header_fields(line: str) -> List[str]:
    """
    從單一行文本中提取潛在的、未經正規化的欄位名稱列表。

    此函式採用簡化的策略，主要基於逗號 `,` 作為分隔符來切分行內容。
    每個切分出的部分會移除其首尾空白。空的或僅包含空白的欄位會被忽略。

    注意：這不是一個完整的CSV解析器（例如，它不處理引號內的逗號）。
    其目的是在格式指紋計算的早期階段，快速從原始文本行中識別出可能的欄位字串。
    更複雜的CSV解析應由 Pandas 在後續的 `parsers` 模組中處理。

    Args:
        line (str): 從檔案中讀取的單一行原始文本。

    Returns:
        List[str]: 一個包含該行中所有潛在欄位字串的列表。
                   如果行為空或無法提取有效欄位，則返回空列表。
    """
    cleaned_line = line.strip() # 移除行首尾的換行符、空白等
    if not cleaned_line:
        return []

    # 簡單地以逗號分割。對於期交所的CSV，這通常足夠用於標頭行的初步識別。
    potential_fields = cleaned_line.split(',')

    # 清理每個潛在欄位，並過濾掉完全是空白的欄位
    extracted_fields = [field.strip() for field in potential_fields if field.strip()]

    return extracted_fields


def _is_likely_header(fields: List[str]) -> bool:
    """
    根據一組啟發式規則，判斷提取出的欄位列表是否「像」一個有效的數據標頭。

    規則包括：
    - 欄位數量是否達到最小閾值 (`MIN_COLUMNS_FOR_HEADER`)。
    - 是否包含一定數量的常見標頭關鍵字 (`COMMON_HEADER_KEYWORDS`)。
    - 非數字欄位的比例是否足夠高（標頭通常主要由文本構成）。

    Args:
        fields (List[str]): 從單一行中提取並初步清理過的潛在欄位名稱列表。

    Returns:
        bool: 如果該欄位列表符合標頭的特徵，則返回 `True`；否則返回 `False`。
    """
    if len(fields) < MIN_COLUMNS_FOR_HEADER:
        return False # 欄位太少，不太可能是標頭

    keyword_hits = 0
    non_numeric_fields = 0

    for raw_field in fields: # 這裡的 field 是 _extract_potential_header_fields 返回的，僅 strip 過
        # 為了匹配關鍵字，我們對其進行與關鍵字列表一致的正規化 (小寫、無內部空格)
        normalized_field_for_keyword_check = "".join(raw_field.split()).lower()

        for keyword in COMMON_HEADER_KEYWORDS:
            if keyword in normalized_field_for_keyword_check: # 關鍵字是子字串即可
                keyword_hits += 1
                break # 每個欄位命中一個關鍵字即可

        # 判斷欄位是否主要為非數字 (一個簡單的檢查方法)
        # .isnumeric() or .isdigit() 可能對 "12.34" 或 "-5" 返回 False
        # 此處用一個更寬鬆的檢查：如果移除一個小數點後仍然不是純數字，則認為是非數字
        if not raw_field.replace('.', '', 1).replace('-', '', 1).isdigit():
            non_numeric_fields +=1

    # 啟發式判斷條件 (可根據實際情況調整閾值)：
    # 1. 關鍵字命中數達到一定標準 (例如，至少2個，或者達到總欄位數的一半)。
    # OR
    # 2. 非數字欄位的比例很高 (例如，超過70%的欄位不是純數字)。
    # 這有助於區分標頭行和純數據行。
    if keyword_hits >= min(2, len(fields) // 2 + 1) or \
       (len(fields) > 0 and non_numeric_fields / len(fields) >= 0.7):
        logger.debug(f"候選標頭通過檢查: {fields} (關鍵字命中: {keyword_hits}, 非數字欄位: {non_numeric_fields}/{len(fields)})")
        return True

    logger.debug(f"候選標頭未通過檢查: {fields} (關鍵字命中: {keyword_hits}, 非數字欄位: {non_numeric_fields}/{len(fields)})")
    return False

def find_header_row(
    file_stream: io.BytesIO,
    file_name_for_log: str
) -> Tuple[Optional[List[str]], Optional[int], Optional[str]]:
    """
    從給定的檔案位元組串流中定位標頭行，並提取、正規化、排序標頭欄位。

    實現「多行嗅探，首個匹配（但優先選欄位最多的）」策略。
    會嘗試多種常用編碼來解碼檔案內容。

    Args:
        file_stream (io.BytesIO): 包含檔案內容的位元組串流。函式會從頭部 (seek(0)) 開始讀取。
        file_name_for_log (str): 檔案的原始名稱，主要用於日誌記錄，方便追蹤。

    Returns:
        Tuple[Optional[List[str]], Optional[int], Optional[str]]:
            - `best_header_fields` (Optional[List[str]]): 如果找到標頭，則為已正規化
              （小寫、去空格）並按字母順序排序的欄位名稱列表；否則為 `None`。
            - `header_line_num` (Optional[int]): 如果找到標頭，則為其在檔案中的行號
              (0-based)；否則為 `None`。
            - `detected_encoding` (Optional[str]): 成功讀取標頭時使用的編碼；否則為 `None`。
    """
    file_stream.seek(0) # 確保從檔案開頭讀取

    encodings_to_try: List[str] = ['utf-8-sig', 'utf-8', 'ms950', 'big5']
    candidate_lines: List[str] = []
    detected_encoding: Optional[str] = None

    # 步驟 1: 嘗試用不同編碼讀取檔案開頭的若干行
    for encoding in encodings_to_try:
        try:
            file_stream.seek(0) # 每次嘗試新編碼時都重置串流位置
            # 使用 TextIOWrapper 將 BytesIO 包裹成文本串流，指定編碼和錯誤處理
            # errors='strict' 表示如果遇到無法解碼的字節，會拋出 UnicodeDecodeError
            reader = io.TextIOWrapper(file_stream, encoding=encoding, errors='strict', newline='')
            candidate_lines = [reader.readline() for _ in range(MAX_HEADER_CANDIDATE_LINES)]
            # 移除列表末尾可能因檔案行數不足 MAX_HEADER_CANDIDATE_LINES 而產生的 None 或空字串
            candidate_lines = [line for line in candidate_lines if line is not None]
            detected_encoding = encoding
            logger.debug(f"檔案 '{file_name_for_log}': 成功使用編碼 '{encoding}' 讀取前 {len(candidate_lines)} 行。")
            break # 一旦成功讀取，就使用此編碼和行列表
        except UnicodeDecodeError:
            logger.debug(f"檔案 '{file_name_for_log}': 使用編碼 '{encoding}' 解碼標頭候選行失敗，嘗試下一個編碼。")
            continue
        except Exception as e:
            logger.error(f"檔案 '{file_name_for_log}': 讀取標頭候選行時發生非預期錯誤 (編碼: {encoding}): {e}", exc_info=True)
            return None, None, None # 發生嚴重錯誤，不再嘗試

    if not candidate_lines or detected_encoding is None:
        logger.warning(f"檔案 '{file_name_for_log}': 未能使用任何常用編碼成功讀取標頭候選行，或檔案行數不足。")
        return None, None, None

    # 步驟 2: 逐行嗅探，找到最像標頭的行
    best_header_fields_normalized_sorted: Optional[List[str]] = None
    best_header_line_num: Optional[int] = None
    max_field_count_for_best_header: int = 0

    for i, line_text in enumerate(candidate_lines):
        if not line_text.strip(): # 跳過完全是空白的行
            continue

        potential_fields_raw = _extract_potential_header_fields(line_text)

        if len(potential_fields_raw) < MIN_COLUMNS_FOR_HEADER:
            logger.debug(f"檔案 '{file_name_for_log}', 行 {i}: 欄位數过少 ({len(potential_fields_raw)})，跳過。")
            continue

        logger.debug(f"檔案 '{file_name_for_log}', 行 {i} (編碼 '{detected_encoding}'): "
                     f"提取到潛在標頭欄位 (原始): {potential_fields_raw}")

        if _is_likely_header(potential_fields_raw):
            # 如果此行被認為是標頭，且其欄位數多於之前找到的最佳標頭，則更新
            # （或如果這是第一個找到的可能標頭）
            if best_header_line_num is None or len(potential_fields_raw) > max_field_count_for_best_header:
                # 對提取的原始欄位進行正規化和排序
                current_normalized_fields = [_normalize_column_name(f) for f in potential_fields_raw]
                # 過濾掉正規化後可能為空的欄位 (例如，原始欄位僅包含特殊空白符)
                current_normalized_fields = [f for f in current_normalized_fields if f]

                if len(current_normalized_fields) >= MIN_COLUMNS_FOR_HEADER:
                    best_header_fields_normalized_sorted = sorted(current_normalized_fields)
                    best_header_line_num = i
                    max_field_count_for_best_header = len(potential_fields_raw) # 用原始欄位數比較
                    logger.info(f"檔案 '{file_name_for_log}': 在第 {i} 行 (0-based) 使用編碼 '{detected_encoding}' "
                                f"找到更優的候選標頭。欄位數: {len(potential_fields_raw)}, "
                                f"正規化並排序後: {best_header_fields_normalized_sorted}")

    if best_header_fields_normalized_sorted and best_header_line_num is not None:
        logger.info(f"檔案 '{file_name_for_log}': 最終選擇第 {best_header_line_num} 行 (0-based, 編碼 '{detected_encoding}') 作為標頭。 "
                    f"正規化並排序後的欄位: {best_header_fields_normalized_sorted}")
        return best_header_fields_normalized_sorted, best_header_line_num, detected_encoding
    else:
        logger.warning(f"檔案 '{file_name_for_log}': 在前 {len(candidate_lines)} 行 ({MAX_HEADER_CANDIDATE_LINES} 行上限) "
                       f"中未能使用編碼 '{detected_encoding}' 定位到明確的標頭。")
        return None, None, detected_encoding # 即使未找到標頭，也返回檢測到的編碼（如果有）


def calculate_format_fingerprint(
    file_stream: io.BytesIO,
    file_name_for_log: str = "UnknownFile"
) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    計算給定檔案位元組串流的格式指紋、標頭行號及使用的編碼。

    Args:
        file_stream (io.BytesIO): 包含檔案內容的位元組串流。函式會從頭部 (seek(0)) 開始讀取。
        file_name_for_log (str): 檔案的原始名稱，主要用於日誌記錄。

    Returns:
        Tuple[Optional[str], Optional[int], Optional[str]]:
            - `fingerprint` (Optional[str]): 計算得到的 SHA256 格式指紋。
                                            如果無法確定標頭，則為 `None`。
            - `header_line_num` (Optional[int]): 檢測到的標頭行號 (0-based)。
                                                如果無法確定標頭，則為 `None`。
            - `detected_encoding` (Optional[str]): 成功讀取標頭時檢測並使用的編碼。
                                                  如果無法讀取，則為 `None`。
    """
    logger.debug(f"開始為檔案 '{file_name_for_log}' 計算格式指紋...")

    header_fields, header_line_num, detected_encoding = find_header_row(file_stream, file_name_for_log)

    if not header_fields:
        logger.warning(f"檔案 '{file_name_for_log}': 未能提取有效標頭欄位，無法計算指紋。")
        return None, header_line_num, detected_encoding # header_line_num 可能為 None

    # 使用管道符 | 合併排序後的正規化欄位名
    combined_header_string = "|".join(header_fields)
    logger.debug(f"檔案 '{file_name_for_log}': 用於計算指紋的合併標頭字串: '{combined_header_string}'")

    # 計算 SHA256 雜湊值
    fingerprint = hashlib.sha256(combined_header_string.encode('utf-8')).hexdigest() # 標準化為 UTF-8 計算雜湊
    logger.info(f"檔案 '{file_name_for_log}': 計算得到的格式指紋為: {fingerprint} "
                f"(基於第 {header_line_num} 行的標頭，使用編碼 '{detected_encoding}')")

    return fingerprint, header_line_num, detected_encoding

# --- 範例使用 ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # import logging
    # setup_global_logger(log_level_console=logging.DEBUG)

    logger.info("開始執行 format_detector.py 範例...")

    test_cases = [
        ("std_csv_utf8", "交易日期,商品代號, 開盤價 ,最高價,最低價,收盤價\n20230101,TXF,14000,14050,13980,14020", 'utf-8', True),
        ("reordered_bom_utf8", "商品代號,交易日期,收盤價,最低價,最高價,開盤價\nTXF,20230101,14020,13980,14050,14000", 'utf-8-sig', True),
        ("mixed_case_space", " 交易日期 ,商品代號,OpenPrice,HighPrice,lowprice,CLOSEPRICE\n", 'utf-8', True),
        ("with_comments", "# Comment Line 1\n# Comment Line 2\n交易日期,商品代號,成交價\n20230102,TXF,14100", 'utf-8', True),
        ("few_cols", "日期,值\n2023,100", 'utf-8', True),
        ("empty_file", "", 'utf-8', False), # 預期無指紋
        ("numeric_only_row", "123,456,789", 'utf-8', True), # 依賴 _is_likely_header 的判斷
        ("options_daily_sim_ms950", " 成交日期, 商品代號, 履約價格, 到期月份(週別), 買賣權別, 成交時間, 成交價格, 成交數量(B or S), 開盤集合競價 \n---\n2023,data", 'ms950', True),
        ("header_at_line_3", "Junk Line 1\nJunk Line 2\nActual,Header,Here\nVal1,Val2,Val3", "utf-8", True)
    ]

    fingerprints_generated: Dict[str, Optional[str]] = {}

    for name, content_str, encoding, expect_fingerprint in test_cases:
        logger.info(f"\n--- 測試案例: {name} (編碼: {encoding}) ---")
        file_bytes: bytes
        if encoding == 'utf-8-sig':
            file_bytes = b'\xef\xbb\xbf' + content_str.encode('utf-8')
        elif encoding in ['ms950', 'big5']:
            try:
                file_bytes = content_str.encode(encoding)
            except UnicodeEncodeError as uee:
                logger.warning(f"無法使用編碼 {encoding} 對內容字串進行編碼 (可能包含該編碼不支持的字符): {uee}。將使用UTF-8替代進行此測試。")
                file_bytes = content_str.encode('utf-8') # Fallback for test environment
        else:
            file_bytes = content_str.encode('utf-8') # Default to utf-8 for test if not specified for other encodings

        stream = io.BytesIO(file_bytes)
        fingerprint, h_line, enc = calculate_format_fingerprint(stream, f"{name}.csv")
        fingerprints_generated[name] = fingerprint

        if expect_fingerprint:
            assert fingerprint is not None, f"案例 '{name}' 預期應產生指紋，但結果為 None。"
            logger.info(f"案例 '{name}': 指紋={fingerprint}, 標頭行={h_line}, 編碼={enc}")
        else:
            assert fingerprint is None, f"案例 '{name}' 預期不產生指紋，但結果為 '{fingerprint}'。"
            logger.info(f"案例 '{name}': 正確地未產生指紋 (標頭行={h_line}, 編碼={enc})")

    # 驗證欄位集相同但順序或BOM不同的情況，指紋應相同
    if fingerprints_generated.get("std_csv_utf8") and fingerprints_generated.get("reordered_bom_utf8"):
        assert fingerprints_generated["std_csv_utf8"] == fingerprints_generated["reordered_bom_utf8"], \
            "std_csv_utf8 和 reordered_bom_utf8 的指紋應相同。"
        logger.info("UTF-8 標準CSV與順序不同/帶BOM的CSV指紋一致性通過。")

    # 驗證註解行被跳過
    if fingerprints_generated.get("with_comments"):
        # 預期 "交易日期|商品代號|成交價" (排序後)
        # 實際: sorted(["交易日期","商品代號","成交價"]) -> ['交易日期', '商品代號', '成交價']
        # -> "交易日期|商品代號|成交價"
        expected_comment_fp_str = "|".join(sorted([_normalize_column_name(f) for f in ["交易日期","商品代號","成交價"]]))
        expected_comment_fp = hashlib.sha256(expected_comment_fp_str.encode('utf-8')).hexdigest()
        assert fingerprints_generated["with_comments"] == expected_comment_fp, \
            f"with_comments 指紋不符預期。得到: {fingerprints_generated['with_comments']}, 預期: {expected_comment_fp}"
        logger.info("帶註解行的檔案指紋計算通過。")

    logger.info("\nformat_detector.py 範例執行完畢。")

[end of MyTaifexDataProject/src/taifex_pipeline/transformation/format_detector.py]

[start of MyTaifexDataProject/src/taifex_pipeline/transformation/parsers.py]
# -*- coding: utf-8 -*-
"""
數據解析器模組 (Parsers)

本模組提供了一個通用的函式 `parse_file_stream_to_dataframe`，用於將
檔案的位元組串流 (`io.BytesIO`) 根據提供的解析設定 (`parser_config`)
轉換為 pandas DataFrame。

主要設計：
- **基於 Pandas**: 主要依賴 `pandas.read_csv()` 進行實際的解析工作。
- **配置驅動**: 解析的具體行為（如分隔符、跳過行、編碼、欄位類型等）
  完全由傳入的 `parser_config` 字典控制。該字典的鍵應對應
  `pandas.read_csv()` 的參數。
- **支持分塊讀取**: 如果 `parser_config` 中包含 `chunksize` 參數，
  函式將返回一個 DataFrame 的迭代器，允許對大型檔案進行流式處理。
- **錯誤處理**: 包含對空檔案、編碼錯誤以及其他解析過程中可能發生的
  常見 Pandas 錯誤的處理邏輯。

此模組旨在被轉換管線 (`TransformationPipeline`) 中的 worker 調用，
在格式被識別（獲得指紋和對應配方）之後，用來將原始數據加載到 DataFrame 中
以進行後續的清洗和驗證。
"""
import pandas as pd
import io
from typing import Dict, Any, Optional, Union, Iterator # Iterator, Union 從 typing 導入

from taifex_pipeline.core.logger_setup import get_logger

logger = get_logger(__name__)

def parse_file_stream_to_dataframe(
    file_stream: io.BytesIO,
    parser_config: Dict[str, Any],
    file_name_for_log: str = "UnknownFile"
) -> Union[Optional[pd.DataFrame], Optional[Iterator[pd.DataFrame]]]:
    """
    根據提供的 `parser_config`，將檔案的位元組串流解析為 pandas DataFrame 或 DataFrame 迭代器。

    主要使用 `pandas.read_csv()` 進行解析。`parser_config` 字典中的鍵值對
    會直接作為關鍵字參數傳遞給 `pd.read_csv()`。

    Args:
        file_stream (io.BytesIO): 包含檔案內容的位元組串流。函式會從串流的
                                  起始位置 (seek(0)) 開始讀取。
        parser_config (Dict[str, Any]): 一個字典，包含了傳遞給 `pd.read_csv()` 的
                                      解析參數。例如：
                                      `{"sep": ",", "encoding": "utf-8", "skiprows": 1, ...}`
        file_name_for_log (str): 檔案的原始名稱，主要用於日誌記錄。

    Returns:
        Union[Optional[pd.DataFrame], Optional[Iterator[pd.DataFrame]]]:
            - 如果 `parser_config` 中未指定 `chunksize` 或 `chunksize` 為 `None`，
              則返回一個包含整個檔案內容的 pandas DataFrame。如果解析失敗或檔案為空，
              則可能返回 `None` 或一個空的 DataFrame (取決於錯誤類型)。
            - 如果 `parser_config` 中指定了 `chunksize` (一個整數)，則返回一個
              `pandas.io.parsers.readers.TextFileReader` 物件，它是一個可以迭代
              產生多個 DataFrame (數據塊) 的迭代器。如果檔案為空或解析失敗，
              迭代器可能是空的或在首次迭代時引發錯誤 (Pandas 行為)。
              此函式會直接返回該迭代器。
            - 如果發生嚴重解析錯誤 (如編碼錯誤)，則返回 `None`。
    """
    file_stream.seek(0) # 確保從檔案/串流的開頭讀取

    # 從 parser_config 中獲取 encoding，如果未提供，pandas read_csv 會嘗試推斷，
    # 但通常建議明確指定。'utf-8' 作為一個常見的預設值。
    encoding_to_use = parser_config.get('encoding', 'utf-8')
    chunksize_to_use = parser_config.get('chunksize')

    # 複製 parser_config 以避免意外修改傳入的字典。
    # pd.read_csv 可以直接處理 BytesIO 輸入。
    read_csv_kwargs = parser_config.copy()

    logger.info(f"開始解析檔案 '{file_name_for_log}'。使用 parser_config: {read_csv_kwargs}")

    try:
        # 直接將 BytesIO 和所有參數傳遞給 pd.read_csv
        df_or_iterator = pd.read_csv(file_stream, **read_csv_kwargs)

        if chunksize_to_use is not None:
            # 如果指定了 chunksize，pd.read_csv 返回一個 TextFileReader (DataFrame 迭代器)
            logger.info(f"檔案 '{file_name_for_log}' 以 chunksize={chunksize_to_use} "
                        f"進行分塊讀取，將返回 DataFrame 迭代器。")
            return df_or_iterator # type: ignore[return-value] # Mypy 可能抱怨類型不匹配
        else:
            # 如果未指定 chunksize，pd.read_csv 返回一個單一的 DataFrame
            if not isinstance(df_or_iterator, pd.DataFrame):
                # 這是一個不太可能發生的情況，如果 chunksize is None，read_csv 應該返回 DataFrame。
                # 但作為防禦性程式碼，檢查一下。
                logger.error(f"檔案 '{file_name_for_log}': pd.read_csv 在 chunksize=None 時 "
                               f"未按預期返回 DataFrame。實際返回類型: {type(df_or_iterator)}")
                return None

            logger.info(f"檔案 '{file_name_for_log}' 解析成功，共 {len(df_or_iterator)} 行。"
                        f"欄位: {df_or_iterator.columns.tolist() if not df_or_iterator.empty else 'N/A (空檔案)'}")
            return df_or_iterator

    except pd.errors.EmptyDataError:
        # 當檔案為空，或者跳過所有行後沒有數據可解析時，Pandas 會引發此錯誤。
        logger.warning(f"檔案 '{file_name_for_log}' 為空或不包含可解析的數據。返回一個空的 DataFrame。")
        return pd.DataFrame() # 標準化行為：為空數據返回空 DataFrame
    except UnicodeDecodeError as ude:
        logger.error(f"檔案 '{file_name_for_log}' 使用編碼 '{encoding_to_use}' 解碼失敗: {ude}。 "
                       f"請檢查 parser_config 中的 'encoding' 設定與檔案實際編碼是否一致。", exc_info=False) # exc_info=False 避免過多重複日誌
        return None
    except Exception as e:
        # 捕獲其他所有可能的 Pandas 解析錯誤或意外錯誤
        logger.error(f"檔案 '{file_name_for_log}' 解析過程中發生未預期錯誤: {e}", exc_info=True)
        return None

# --- 範例使用 (通常由轉換管線中的 worker 調用) ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # import logging
    # setup_global_logger(log_level_console=logging.DEBUG) # 開啟 DEBUG 以查看詳細日誌

    logger.info("開始執行 parsers.py 範例...")

    # 範例1: 基本 CSV (UTF-8)
    csv_content_s1 = "col_A,col_B,col_C\n1,apple,true\n2,banana,false\n3,cherry,true"
    stream_s1 = io.BytesIO(csv_content_s1.encode('utf-8'))
    config_s1 = {"sep": ",", "encoding": "utf-8", "header": 0}
    df_s1 = parse_file_stream_to_dataframe(stream_s1, config_s1, "sample1.csv")
    if df_s1 is not None and isinstance(df_s1, pd.DataFrame):
        logger.info(f"範例1 DataFrame (shape: {df_s1.shape}):\n{df_s1}\nTypes:\n{df_s1.dtypes}")
        assert len(df_s1) == 3
        assert df_s1['col_B'].tolist() == ['apple', 'banana', 'cherry']

    # 範例2: 模擬 MS950 編碼 (實際用UTF-8字串)，有跳過行和指定數據類型
    csv_content_s2_sim = ("# 這是檔案的註解行1\n"
                          "# 這是檔案的註解行2\n"
                          "商品代號,商品名稱,參考價格\n"
                          "TXF,臺股期貨,15000.50\n"
                          "MXF,小型臺指,15000.25")
    stream_s2 = io.BytesIO(csv_content_s2_sim.encode('utf-8')) # 內容是UTF-8
    config_s2 = {
        "sep": ",",
        "encoding": "utf-8", # 測試時使用 utf-8，實際應用中若檔案為MS950則應設為ms950
        "skiprows": 2,
        "header": 0,
        "dtype": {"商品代號": str, "商品名稱": str, "參考價格": float}
    }
    df_s2 = parse_file_stream_to_dataframe(stream_s2, config_s2, "sample2_ms950_sim.csv")
    if df_s2 is not None and isinstance(df_s2, pd.DataFrame):
        logger.info(f"範例2 DataFrame (shape: {df_s2.shape}):\n{df_s2}\nTypes:\n{df_s2.dtypes}")
        assert len(df_s2) == 2
        assert df_s2['參考價格'].dtype == float
        assert df_s2['參考價格'].iloc[0] == 15000.50

    # 範例3: 使用 chunksize 進行分塊讀取
    csv_content_s3_chunked = "ID,Value\n1,10\n2,20\n3,30\n4,40\n5,50"
    stream_s3 = io.BytesIO(csv_content_s3_chunked.encode('utf-8'))
    config_s3 = {"sep": ",", "encoding": "utf-8", "header": 0, "chunksize": 2}
    iterator_s3 = parse_file_stream_to_dataframe(stream_s3, config_s3, "sample3_chunked.csv")

    if iterator_s3 is not None and not isinstance(iterator_s3, pd.DataFrame): # 應為迭代器
        logger.info("範例3: 成功返回 DataFrame 迭代器。開始迭代處理分塊數據...")
        all_chunks_list = []
        for i, chunk_df_s3 in enumerate(iterator_s3):
            logger.info(f"  Chunk {i+1} (shape: {chunk_df_s3.shape}):\n{chunk_df_s3}")
            assert isinstance(chunk_df_s3, pd.DataFrame)
            all_chunks_list.append(chunk_df_s3)

        if all_chunks_list:
            combined_df_s3 = pd.concat(all_chunks_list)
            logger.info(f"範例3 合併所有數據塊後的 DataFrame (shape: {combined_df_s3.shape}):\n{combined_df_s3}")
            assert len(combined_df_s3) == 5 # 2 + 2 + 1 chunks
        else:
            logger.warning("範例3 (chunked) 未能成功處理任何數據塊。")
    elif iterator_s3 is not None: # 如果返回了 DataFrame 而不是迭代器
        logger.error("範例3 錯誤: parse_file_stream_to_dataframe 在 chunksize 設定時未返回迭代器。")


    # 範例4: 空檔案處理
    stream_s4_empty = io.BytesIO(b"")
    config_s4 = {"sep": ",", "encoding": "utf-8", "header": 0}
    df_s4 = parse_file_stream_to_dataframe(stream_s4_empty, config_s4, "sample4_empty.csv")
    if df_s4 is not None and isinstance(df_s4, pd.DataFrame):
        logger.info(f"範例4 (空檔案) DataFrame (shape: {df_s4.shape}), 應為空:\n{df_s4}")
        assert df_s4.empty, "空檔案解析結果應為空 DataFrame"

    logger.info("\nparsers.py 範例執行完畢。")

[end of MyTaifexDataProject/src/taifex_pipeline/transformation/parsers.py]

[start of MyTaifexDataProject/src/taifex_pipeline/transformation/cleaners/example_cleaners.py]
# -*- coding: utf-8 -*-
"""
範例數據清洗函式模組 (Example Cleaners)

本模組提供了一些遵循標準化清洗函式介面的範例清洗函式。
每個清洗函式都應接收一個 pandas DataFrame 作為輸入，並返回一個
經過清洗和轉換處理後的新的 pandas DataFrame。

這些函式的名稱將在 `format_catalog.json` 設定檔中被引用，
以便轉換管線可以動態地導入並執行它們。

清洗函式介面約定：
`def cleaner_function_name(df: pd.DataFrame) -> pd.DataFrame:`
"""
import pandas as pd
import numpy as np # Numpy 通常與 Pandas 一起使用，用於數值操作或 np.nan

from taifex_pipeline.core.logger_setup import get_logger

logger = get_logger(__name__)

# --- 標準清洗函式介面 (註解形式，供參考) ---
# def specific_cleaner_function(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     對特定格式的數據 DataFrame 進行清洗和轉換。
#
#     Args:
#         df (pd.DataFrame): 從 parser 解析得到的原始 DataFrame。
#
#     Returns:
#         pd.DataFrame: 清洗和轉換後的 DataFrame。如果發生嚴重錯誤或
#                       清洗後無有效數據，可能返回空的 DataFrame。
#     """
#     # ... 清洗邏輯 ...
#     return cleaned_df
# --- ------------------------------------ ---

def clean_daily_ohlcv_example_v1(df: pd.DataFrame) -> pd.DataFrame:
    """
    一個針對假設的「每日行情數據」(OHLCV - 開高低收量) 的範例清洗函式。

    此函式演示了常見的數據清洗操作，例如：
    - 複製輸入 DataFrame 以避免原地修改。
    - 標準化欄位名稱（例如，將中文欄位名映射到預定義的英文名）。
    - 處理日期欄位：將多種可能的日期字串格式（包括民國年）轉換為 pandas datetime 物件。
    - 處理數值型欄位：移除千分位逗號，將字串轉換為數值類型 (float 或 Int64)，
      並將無法轉換的異常值（如 '-'）處理為 NaN/NA。
    - (可選) 移除完全重複的數據行。
    - (可選) 根據預期順序選擇和重新排列欄位。

    Args:
        df (pd.DataFrame): 輸入的原始 DataFrame，通常來自 `parsers.parse_file_stream_to_dataframe()`。
                           預期包含如 '交易日期', '商品代號', '開盤價' 等欄位。

    Returns:
        pd.DataFrame: 清洗和轉換後的 DataFrame。如果輸入 DataFrame 為空，則直接返回空 DataFrame。
                      如果處理過程中發生嚴重錯誤（雖然此範例中會盡量處理），
                      理想情況下應記錄錯誤並可能返回部分處理的數據或空 DataFrame，
                      具體行為取決於錯誤處理策略。
    """
    logger.info(f"開始執行清洗函式 'clean_daily_ohlcv_example_v1'，輸入 DataFrame (shape: {df.shape})")
    if df.empty:
        logger.warning("輸入 DataFrame 為空，'clean_daily_ohlcv_example_v1' 直接返回空 DataFrame。")
        return df.copy() # 返回副本以保持一致性

    cleaned_df = df.copy()

    # 步驟 1: 標準化欄位名稱
    # 實際應用中，欄位名映射可能更複雜，或部分在 parser_config 的 'names' 中處理。
    column_rename_map = {
        "交易日期": "trade_date", "契約": "product_id", "商品代號": "product_id",
        "開盤價": "open", "最高價": "high", "最低價": "low", "收盤價": "close",
        "成交量": "volume", "成交筆數": "ticks", "結算價": "settlement_price",
        "未平倉合約數": "open_interest", "未平倉量": "open_interest"
    }
    actual_rename_map = {k: v for k, v in column_rename_map.items() if k in cleaned_df.columns}
    if actual_rename_map:
        cleaned_df.rename(columns=actual_rename_map, inplace=True)
        logger.debug(f"欄位已重命名。映射關係: {actual_rename_map}")

    # 步驟 2: 處理 'trade_date' 日期欄位
    if 'trade_date' in cleaned_df.columns:
        try:
            original_dates = cleaned_df['trade_date'].astype(str)
            # 替換分隔符，統一處理 YYYYMMDD 和民國年 YYYMMDD / YYMMDD
            processed_dates = original_dates.str.replace(r'[/|-]', '', regex=True)

            converted_dates = pd.Series(index=cleaned_df.index, dtype='datetime64[ns]')

            # 逐行轉換以處理混合格式和民國年
            for idx, date_str in processed_dates.items():
                if pd.isna(date_str) or not date_str.strip():
                    converted_dates.loc[idx] = pd.NaT
                    continue

                # 嘗試民國年 (YYYMMDD or YYMMDD)
                if date_str.match(r'^\d{6,7}$'): # type: ignore[attr-defined] # Pandas Series str accessor
                    year_val: int
                    month_val: int
                    day_val: int
                    if len(date_str) == 7: # YYYMMDD (e.g., 1120315)
                        year_val = int(date_str[:3]) + 1911
                        month_val = int(date_str[3:5])
                        day_val = int(date_str[5:7])
                    elif len(date_str) == 6: # YYMMDD (e.g., 980315)
                        year_val = int(date_str[:2]) + 1911
                        month_val = int(date_str[2:4])
                        day_val = int(date_str[4:6])
                    else: # 不應發生，因為上面有長度檢查
                        converted_dates.loc[idx] = pd.NaT
                        continue
                    try:
                        converted_dates.loc[idx] = pd.Timestamp(year=year_val, month=month_val, day=day_val)
                        continue # 已成功轉換，跳到下一行
                    except ValueError:
                        # 如果民國年轉換失敗，則留給後續的 pd.to_datetime 處理
                        pass

                # 嘗試標準西元年格式 (YYYYMMDD) 或其他 Pandas 可推斷的格式
                # 如果前面的民國年轉換已填充，則此處的 pd.NaT 不會被覆蓋
                if pd.isna(converted_dates.loc[idx]):
                     # 使用原始未去除分隔符的日期字串進行 pd.to_datetime 推斷
                    converted_dates.loc[idx] = pd.to_datetime(original_dates.loc[idx], errors='coerce')

            cleaned_df['trade_date'] = converted_dates
            logger.info("已轉換 'trade_date' 欄位為 datetime 物件。")
            if cleaned_df['trade_date'].isna().any():
                logger.warning(f"轉換 'trade_date' 後仍有 {cleaned_df['trade_date'].isna().sum()} 個 NaT 值。")
        except Exception as e:
            logger.warning(f"轉換 'trade_date' 欄位時發生錯誤: {e}. 該欄位可能保留部分原始格式或充滿 NaT。", exc_info=True)
    else:
        logger.warning("欄位 'trade_date' 不存在於 DataFrame 中，跳過日期處理。")

    # 步驟 3: 處理數值型欄位
    numeric_cols_candidate = ['open', 'high', 'low', 'close', 'volume',
                              'settlement_price', 'open_interest', 'ticks']
    for col_name in numeric_cols_candidate:
        if col_name in cleaned_df.columns:
            try:
                series = cleaned_df[col_name]
                if series.dtype == 'object': # 只有當欄位是物件類型 (通常是字串) 時才嘗試轉換
                    # 移除千分位逗號 (,)
                    series = series.astype(str).str.replace(',', '', regex=False)
                    # 將常見的無效符號 (如 '-') 替換為 NaN，以便 pd.to_numeric 正確處理
                    series = series.replace('-', np.nan)

                # 轉換為數值類型
                if col_name in ['volume', 'open_interest', 'ticks']:
                    # 對於交易量、未平倉量等，應為整數，使用可空整數類型 Int64
                    cleaned_df[col_name] = pd.to_numeric(series, errors='coerce').astype('Int64')
                else:
                    # 對於價格欄位，應為浮點數
                    cleaned_df[col_name] = pd.to_numeric(series, errors='coerce').astype(float)
                logger.debug(f"已轉換欄位 '{col_name}' 為數值類型。")
            except Exception as e:
                logger.warning(f"轉換欄位 '{col_name}' 為數值類型時發生錯誤: {e}. "
                               f"該欄位可能保留部分原始數據或包含 NaN/NA。", exc_info=False) # info=False 避免過多堆疊
                # 出錯時，可以選擇將整列設為 np.nan 或 pd.NA，或讓 to_numeric 的 errors='coerce' 自行處理
                # cleaned_df[col_name] = pd.NA # 或 np.nan，取決於 Pandas 版本和偏好

    logger.info(f"清洗函式 'clean_daily_ohlcv_example_v1' 執行完畢。輸出 DataFrame (shape: {cleaned_df.shape})")
    return cleaned_df


def another_cleaner_example(df: pd.DataFrame) -> pd.DataFrame:
    """
    另一個範例清洗函式，可用於演示處理不同結構或需求的數據。

    Args:
        df (pd.DataFrame): 輸入的原始 DataFrame。

    Returns:
        pd.DataFrame: 清洗和轉換後的 DataFrame。
    """
    logger.info(f"開始執行清洗函式 'another_cleaner_example'，輸入 DataFrame (shape: {df.shape})")
    if df.empty:
        logger.warning("輸入 DataFrame 為空，'another_cleaner_example' 直接返回空 DataFrame。")
        return df.copy()

    cleaned_df = df.copy()

    # 範例：假設此清洗器處理包含 '契約月份' 和 '成交時間' 的數據
    if '契約月份' in cleaned_df.columns:
        # 實際的契約月份轉到期日邏輯會比較複雜，可能需要參考結算日曆等
        # 此處僅為示意，不作實際轉換
        logger.info("偵測到 '契約月份' 欄位，此處應有將其轉換為標準到期日 (expiry_date) 的邏輯 (在此範例中未實現)。")
        cleaned_df['expiry_date_placeholder'] = pd.NaT # 添加一個佔位符欄位

    if '成交時間' in cleaned_df.columns and cleaned_df['成交時間'].dtype == 'object':
        try:
            # 嘗試將 HHMMSS 或 HH:MM:SS 格式的時間字串轉換為 datetime.time 物件
            # 先嘗試 HHMMSS
            time_series = pd.to_datetime(cleaned_df['成交時間'], format='%H%M%S', errors='coerce').dt.time
            # 對於轉換失敗的 (NaT)，再嘗試 HH:MM:SS
            if time_series.isna().any():
                mask_nat = time_series.isna()
                time_series.loc[mask_nat] = pd.to_datetime(cleaned_df.loc[mask_nat, '成交時間'], format='%H:%M:%S', errors='coerce').dt.time
            cleaned_df['trade_time_obj'] = time_series
            logger.info("已嘗試轉換 '成交時間' 欄位為 time 物件。")
        except Exception as e:
            logger.warning(f"轉換 '成交時間' 欄位時發生錯誤: {e}", exc_info=True)

    logger.info(f"清洗函式 'another_cleaner_example' 執行完畢。輸出 DataFrame (shape: {cleaned_df.shape})")
    return cleaned_df

# --- 範例使用 (通常由轉換管線動態調用) ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger # 使用相對導入
    # import logging
    # setup_global_logger(log_level_console=logging.DEBUG)

    logger.info("開始執行 example_cleaners.py 範例...")

    # 測試 clean_daily_ohlcv_example_v1
    sample_data_ohlcv_test = {
        '交易日期': ["112/03/15", "2023/03/16", "20230317", "100/1/1", "invalid-date"],
        '商品代號': ["TXF", "MXF", "TXF", "TEF", "FFF"],
        '開盤價': ["15,000", "14,900.5", "15100", "7000.0", "abc"],
        '最高價': ["15,100", "-", "15150", "7050", "100.0"],
        '最低價': ["14,950", "14,880", "15050", "6990", ""], # 空字串
        '收盤價': ["15,050.00", "14,950", "15120", "7020", "50.50"],
        '成交量': ["120,000", "80000", "150000", "500", "N/A"],
        '結算價': ["15,055", "14,955", np.nan, "7025", "60.0"],
        '未平倉合約數': ["80,000", "40000", "85000", "2000", "100"],
        '一個多餘欄位': ["junk1", "junk2", "junk3", "junk4", "junk5"]
    }
    df_ohlcv_raw_test = pd.DataFrame(sample_data_ohlcv_test)

    logger.info(f"\n--- 測試 clean_daily_ohlcv_example_v1 ---")
    logger.info(f"原始 DataFrame (shape: {df_ohlcv_raw_test.shape}):\n{df_ohlcv_raw_test}\n原始 Dtypes:\n{df_ohlcv_raw_test.dtypes}")

    cleaned_df_ohlcv_test = clean_daily_ohlcv_example_v1(df_ohlcv_raw_test.copy()) # 傳入副本
    logger.info(f"清洗後 DataFrame (shape: {cleaned_df_ohlcv_test.shape}):\n{cleaned_df_ohlcv_test}\n清洗後 Dtypes:\n{cleaned_df_ohlcv_test.dtypes}")

    # 驗證 trade_date 轉換
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df_ohlcv_test['trade_date']), \
        f"trade_date 應為 datetime64 類型，實際為 {cleaned_df_ohlcv_test['trade_date'].dtype}"
    assert cleaned_df_ohlcv_test['trade_date'].iloc[0] == pd.Timestamp(2023, 3, 15)
    assert cleaned_df_ohlcv_test['trade_date'].iloc[3] == pd.Timestamp(2011, 1, 1)
    assert pd.isna(cleaned_df_ohlcv_test['trade_date'].iloc[4]), "無效日期 'invalid-date' 應轉換為 NaT"

    # 驗證數值欄位轉換和異常值處理
    assert cleaned_df_ohlcv_test['open'].dtype == float, f"open 欄位類型應為 float"
    assert cleaned_df_ohlcv_test['open'].iloc[0] == 15000.0
    assert pd.isna(cleaned_df_ohlcv_test['open'].iloc[4]), "無效開盤價 'abc' 應轉換為 NaN"

    assert cleaned_df_ohlcv_test['high'].dtype == float
    assert pd.isna(cleaned_df_ohlcv_test['high'].iloc[1]), "異常值 '-' 在 high 欄位應轉換為 NaN"

    assert cleaned_df_ohlcv_test['low'].dtype == float
    assert pd.isna(cleaned_df_ohlcv_test['low'].iloc[4]), "空字串在 low 欄位應轉換為 NaN"

    assert cleaned_df_ohlcv_test['volume'].dtype == 'Int64', f"volume 欄位類型應為 Int64"
    assert cleaned_df_ohlcv_test['volume'].iloc[0] == 120000
    assert pd.isna(cleaned_df_ohlcv_test['volume'].iloc[4]), "無效成交量 'N/A' 應轉換為 NA (Int64)"

    assert '一個多餘欄位' in cleaned_df_ohlcv_test.columns, "未被指定移除的欄位應保留"

    logger.info("clean_daily_ohlcv_example_v1 範例的斷言檢查通過。")
    logger.info("\nexample_cleaners.py 範例執行完畢。")

[end of MyTaifexDataProject/src/taifex_pipeline/transformation/cleaners/example_cleaners.py]

[start of MyTaifexDataProject/src/taifex_pipeline/transformation/pipeline.py]
# -*- coding: utf-8 -*-
"""
轉換管線 (Transformation Pipeline) 實現模組

本模組定義並實現了數據轉換管線 (`TransformationPipeline`)。
其核心職責是協調整個數據轉換流程：
1.  從 `manifest.db` 讀取待處理的檔案任務列表（通常是狀態為 `RAW_INGESTED`
    或 `QUARANTINED` 的檔案）。
2.  使用 `ProcessPoolExecutor` 平行地為每個檔案分派一個 worker 任務。
3.  每個 worker 任務 (`process_single_file_worker`) 負責：
    a. 從 `raw_lake.db` 讀取原始檔案內容。
    b. 計算檔案的格式指紋 (`format_detector`)。
    c. 根據指紋查詢 `format_catalog.json` 獲取處理配方 (`config_loader`)。
    d. 若無配方，則將檔案標記為 `QUARANTINED`。
    e. 若有配方，則使用配方中的 `parser_config` 解析數據 (`parsers`)。
    f. 對解析後的 DataFrame（或數據塊）驗證 `required_columns`。
    g. 動態導入並執行配方中指定的 `cleaner_function` 進行數據清洗。
    h. 將清洗後的數據載入到 `processed_data.duckdb` 的目標表格中 (`db_manager`)。
    i. 處理過程中發生的任何錯誤，並將檔案標記為 `TRANSFORMATION_FAILED`。
4.  主流程收集所有 worker 的處理結果，並更新 `manifest.db` 中對應檔案的
    狀態、時間戳、處理行數、錯誤訊息等元數據。

此模組是整個數據處理流程的核心引擎。
"""
import io
import time
import os
import importlib
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed, Future
from typing import List, Dict, Any, Tuple, Optional, Callable, Iterator # Iterator, Future 從 typing 導入

from taifex_pipeline.core.logger_setup import get_logger, EXECUTION_ID
from taifex_pipeline.core.config_loader import get_format_catalog, clear_config_cache # clear_config_cache 用於測試
from taifex_pipeline.core.utils import calculate_bytes_sha256 # 雖然原始hash已有，但可備用
from taifex_pipeline.database import db_manager
from taifex_pipeline.transformation.format_detector import calculate_format_fingerprint
from taifex_pipeline.transformation.parsers import parse_file_stream_to_dataframe
# 清洗函式 (cleaner functions) 將被動態導入

logger = get_logger(__name__)

# --- 清洗函式動態導入輔助 ---
CLEANER_MODULE_BASE_PATH: str = "taifex_pipeline.transformation.cleaners"
"""清洗函式模組的基礎 Python 路徑。"""

def get_cleaner_function(function_name_str: str) -> Optional[Callable[[pd.DataFrame], pd.DataFrame]]:
    """
    根據函式名稱字串，動態地從指定基礎路徑下載入並返回清洗函式。

    清洗函式的名稱在 `format_catalog.json` 中的格式應為 'module_name.function_name'。
    例如，如果 `function_name_str` 是 'example_cleaners.clean_daily_ohlcv_example_v1'，
    此函式會嘗試從 `taifex_pipeline.transformation.cleaners.example_cleaners` 模組中
    導入名為 `clean_daily_ohlcv_example_v1` 的函式。

    Args:
        function_name_str (str): 要導入的清洗函式的完整名稱字串
                                 （格式：'模組名.函式名'）。

    Returns:
        Optional[Callable[[pd.DataFrame], pd.DataFrame]]:
            如果成功導入，則返回可呼叫的清洗函式物件。
            如果導入失敗（例如，模組不存在、函式不存在、或函式不可呼叫），
            則記錄錯誤並返回 `None`。
    """
    if '.' not in function_name_str:
        logger.error(f"清洗函式名稱 '{function_name_str}' 格式不正確。 "
                       f"預期格式為 'module_name.function_name' (例如 'example_cleaners.clean_data')。")
        return None

    module_name, func_name = function_name_str.rsplit('.', 1)
    full_module_path = f"{CLEANER_MODULE_BASE_PATH}.{module_name}"

    try:
        module = importlib.import_module(full_module_path)
        cleaner_func = getattr(module, func_name)
        if not callable(cleaner_func):
            logger.error(f"在模組 '{full_module_path}' 中找到的屬性 '{func_name}' 不是一個可呼叫的函式。")
            return None
        logger.debug(f"成功動態導入清洗函式: {full_module_path}.{func_name}")
        return cleaner_func # type: ignore[return-value] # Mypy 可能需要更精確的 Callable 類型
    except ImportError:
        logger.error(f"無法導入清洗函式所在的模組: '{full_module_path}'。", exc_info=True)
        return None
    except AttributeError:
        logger.error(f"在模組 '{full_module_path}' 中未找到名為 '{func_name}' 的清洗函式。", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"動態導入清洗函式 '{function_name_str}' 時發生未預期錯誤: {e}", exc_info=True)
        return None


# --- 單一檔案處理函式 (在 ProcessPoolExecutor 的 worker 中執行) ---
def process_single_file_worker(
    file_hash: str,
    file_name_for_log_hint: str = "UnknownFileInWorker"
) -> Dict[str, Any]:
    """
    在獨立的工作進程中處理單個檔案的完整轉換流程。

    此函式被設計為由 `ProcessPoolExecutor` 調用。它負責從讀取原始檔案內容開始，
    到最終將清洗後的數據載入目標資料庫（或標記為隔離/失敗）的整個過程。

    Args:
        file_hash (str): 要處理的檔案的 SHA256 雜湊值。此雜湊值用於從 `raw_lake.db`
                         中檢索檔案內容，並在 `manifest.db` 中更新處理狀態。
        file_name_for_log_hint (str): 檔案的原始名稱或路徑提示，主要用於日誌記錄，
                                      以便更容易地追蹤特定檔案的處理過程。

    Returns:
        Dict[str, Any]: 一個包含處理結果的字典。該字典的結構應包含以下鍵：
            - 'file_hash' (str): 輸入的檔案雜湊值。
            - 'status' (str): 處理後的最終狀態 ("TRANSFORMATION_SUCCESS",
                              "TRANSFORMATION_FAILED", 或 "QUARANTINED")。
            - 'fingerprint_hash' (Optional[str]): 計算得到的格式指紋，如果成功。
            - 'target_table_name' (Optional[str]): 數據被載入的目標表名，如果成功。
            - 'processed_row_count' (Optional[int]): 成功處理並載入的數據行數。
            - 'error_message' (Optional[str]): 如果處理失敗或被隔離，則包含錯誤或原因描述。
            - 'transformation_timestamp_epoch' (float): 轉換操作完成時的時間戳 (Unix epoch)。
    """
    # 為每個 worker 進程使用帶有 PID 的獨立 logger 名稱，方便追蹤平行任務的日誌
    worker_logger = get_logger(f"worker.pid_{os.getpid()}")
    worker_logger.info(f"開始處理檔案 (Hash: {file_hash[:10]}..., Hint: {file_name_for_log_hint})")

    # 初始化結果字典，預設為失敗狀態
    result: Dict[str, Any] = {
        "file_hash": file_hash,
        "status": "TRANSFORMATION_FAILED", # 預設為失敗
        "fingerprint_hash": None,
        "target_table_name": None,
        "processed_row_count": 0,
        "error_message": "Worker process 啟動但未成功完成轉換。", # 預設錯誤訊息
        "transformation_timestamp_epoch": time.time() # 記錄開始處理的時間，稍後更新為完成時間
    }

    try:
        # 1. 從 Raw Lake 讀取原始檔案內容
        raw_content = db_manager.get_raw_file_content(file_hash)
        if raw_content is None:
            result["error_message"] = f"無法從 Raw Lake 中讀取檔案內容 (Hash: {file_hash[:10]}...)"
            worker_logger.error(result["error_message"])
            return result # 直接返回，狀態為預設的 FAILED

        file_stream = io.BytesIO(raw_content)

        # 2. 計算格式指紋 (同時獲取標頭行號和檢測到的編碼，雖然配方中可能已指定編碼)
        fingerprint, header_line_num, detected_encoding = calculate_format_fingerprint(
            file_stream, file_name_for_log_hint
        )
        result["fingerprint_hash"] = fingerprint # 記錄計算出的指紋，即使後面找不到配方

        if fingerprint is None:
            result["status"] = "QUARANTINED"
            result["error_message"] = (f"無法為檔案 (Hash: {file_hash[:10]}...) 計算有效的格式指紋。"
                                       f"檢測到的編碼: {detected_encoding or '未知'}。檔案將被隔離。")
            worker_logger.warning(result["error_message"])
            return result

        # 3. 查詢格式指紋目錄獲取處理配方
        # 每個 worker process 會在其首次調用 get_format_catalog 時建立自己的 config_loader 快取
        format_catalog = get_format_catalog()
        recipe = format_catalog.get(fingerprint)

        if recipe is None:
            result["status"] = "QUARANTINED"
            result["error_message"] = (f"在格式目錄中找不到指紋 '{fingerprint}' "
                                       f"(Hash: {file_hash[:10]}...) 對應的處理配方。檔案將被隔離。")
            worker_logger.warning(result["error_message"])
            return result

        worker_logger.info(f"檔案 (Hash: {file_hash[:10]}...) 匹配到指紋 '{fingerprint}'. "
                           f"使用配方: '{recipe.get('description', 'N/A')}'")
        result["target_table_name"] = recipe.get("target_table") # 記錄目標表名

        # 4. 解析數據 (Parser)
        parser_config = recipe.get("parser_config", {})
        # 如果 find_header_row 返回了 header_line_num，且 parser_config 未指定 'header'，
        # 可以考慮將其動態加入 parser_config，例如 parser_config['header'] = header_line_num
        # 但更穩健的做法是讓 format_catalog.json 中的 parser_config 明確指定 header 行號或 skiprows
        # 此處假設 parser_config 已包含所有必要的 pandas reader 參數

        df_or_iter: Union[Optional[pd.DataFrame], Optional[Iterator[pd.DataFrame]]] = \
            parse_file_stream_to_dataframe(file_stream, parser_config, file_name_for_log_hint)

        if df_or_iter is None:
            result["error_message"] = f"檔案解析失敗 (Hash: {file_hash[:10]}...)，詳見 parser 日誌。"
            worker_logger.error(result["error_message"])
            return result # 狀態保持 FAILED

        # 5. 驗證、清洗、載入
        cleaner_func_name = recipe.get("cleaner_function")
        if not cleaner_func_name:
            result["error_message"] = f"處理配方中未指定有效的 'cleaner_function' (Hash: {file_hash[:10]}...)."
            worker_logger.error(result["error_message"])
            return result

        cleaner_function = get_cleaner_function(cleaner_func_name)
        if cleaner_function is None:
            result["error_message"] = f"無法動態導入或找到清洗函式 '{cleaner_func_name}' (Hash: {file_hash[:10]}...)."
            worker_logger.error(result["error_message"])
            return result

        required_cols = recipe.get("required_columns", [])
        target_db_table = result["target_table_name"]
        if not target_db_table:
            result["error_message"] = f"處理配方中未指定 'target_table' (Hash: {file_hash[:10]}...)."
            worker_logger.error(result["error_message"])
            return result

        total_rows_processed_for_this_file = 0

        # 統一處理單一 DataFrame 或 DataFrame 迭代器
        data_input_iterator: Iterator[pd.DataFrame]
        if isinstance(df_or_iter, pd.DataFrame):
            data_input_iterator = iter([df_or_iter]) # 將單一 DataFrame 包裝成迭代器
        else: # 已經是迭代器
            data_input_iterator = df_or_iter # type: ignore[assignment]

        for chunk_num, current_chunk_df in enumerate(data_input_iterator):
            worker_logger.debug(f"開始處理檔案 (Hash: {file_hash[:10]}...) 的第 {chunk_num + 1} 個數據塊...")
            if not isinstance(current_chunk_df, pd.DataFrame):
                 result["error_message"] = f"解析器為檔案 (Hash: {file_hash[:10]}...) 的數據塊 {chunk_num + 1} 返回了非 DataFrame 物件 (類型: {type(current_chunk_df)})。"
                 worker_logger.error(result["error_message"])
                 return result

            # 5a. 驗證必要欄位 (在清洗函式調用 *之後* 驗證，因為清洗函式可能重命名或生成欄位)
            # 或者，如果 required_columns 指的是原始解析後的欄位，則應在此處驗證。
            # 根據 Program_Development_Project.txt，required_columns 用於驗證 *解析後* 的 DataFrame。
            # 所以在這裡驗證。
            if required_cols:
                # 注意：這裡的 required_columns 應該是清洗前的欄位名，除非 cleaner 會保持它們不變或 catalog 裡是清洗後的欄位名
                # 我們的設計是 required_columns 是 *最終* 需要的欄位，通常在 cleaner 之後檢查
                # 但如果 parser_config 中有 `usecols`，那麼解析後就應該有這些欄位。
                # 為了清晰，假設 required_columns 是針對 *清洗後* 的 DataFrame 的。
                # 因此，欄位驗證移到 cleaner_func 調用之後。
                pass # 驗證移後

            # 5b. 執行清洗函式
            worker_logger.debug(f"對數據塊 {chunk_num + 1} 執行清洗函式 '{cleaner_func_name}'...")
            cleaned_df_chunk = cleaner_function(current_chunk_df) # cleaner_function 保證返回 DataFrame

            if not isinstance(cleaned_df_chunk, pd.DataFrame):
                result["error_message"] = (f"清洗函式 '{cleaner_func_name}' 未返回 DataFrame "
                                           f"為檔案 (Hash: {file_hash[:10]}...) 的數據塊 {chunk_num + 1}。")
                worker_logger.error(result["error_message"])
                return result

            # 5a (續). 驗證清洗後的 DataFrame 是否包含必要欄位
            if required_cols:
                missing_columns = [col for col in required_cols if col not in cleaned_df_chunk.columns]
                if missing_columns:
                    result["error_message"] = (f"清洗後的數據塊 {chunk_num + 1} 中缺失必要欄位: {missing_columns} "
                                               f"(Hash: {file_hash[:10]}...).")
                    worker_logger.error(result["error_message"])
                    return result

            if cleaned_df_chunk.empty:
                worker_logger.info(f"數據塊 {chunk_num + 1} 在清洗後為空 (Hash: {file_hash[:10]}...)，此塊不載入。")
                continue # 處理下一個塊

            # 5c. 載入到 processed_data.duckdb
            worker_logger.debug(f"將清洗後的數據塊 {chunk_num + 1} ({len(cleaned_df_chunk)}行) 載入到目標表 '{target_db_table}'...")
            # 載入策略：如果這是第一個數據塊，且表格不存在或需要替換，則創建/替換；否則追加。
            # db_manager.load_dataframe_to_processed_db 應能處理此邏輯。
            # 為了處理分塊，第一塊通常決定表的創建 (if_exists='replace' or 'fail' if exists)，後續塊用 'append'。
            # 簡化：讓 load_dataframe_to_processed_db 總是嘗試創建表 (如果不存在) 並追加。
            # 或者，如果 worker 知道這是第一個 chunk，可以傳遞不同的 if_exists。
            # 目前 db_manager.load_dataframe_to_processed_db 的 if_exists 預設為 "append"，
            # 並且會在表不存在時創建它。這對於分塊追加是合適的。
            # 如果需要 replace 語義，應在管線層面，處理第一個塊之前執行 DROP TABLE。
            # 但這會使 worker 變複雜。
            # 我們的設計是：配方本身不指定 if_exists，由管線運行時決定。
            # 此處，由於是 worker，它只負責載入自己處理的塊，所以 "append" 是合理的。
            # 表的初始創建/替換應由更高層次的邏輯（或首次調用時的特殊處理）決定。
            # 暫定：load_dataframe_to_processed_db 預設 "append" 能處理好首次創建和後續追加。
            if not db_manager.load_dataframe_to_processed_db(cleaned_df_chunk, target_db_table, if_exists="append"):
                result["error_message"] = (f"將清洗後的數據塊 {chunk_num + 1} 載入到表 '{target_db_table}' 失敗 "
                                           f"(Hash: {file_hash[:10]}...).")
                worker_logger.error(result["error_message"])
                return result

            total_rows_processed_for_this_file += len(cleaned_df_chunk)

        # 如果所有數據塊都成功處理完畢
        result["status"] = "TRANSFORMATION_SUCCESS"
        result["processed_row_count"] = total_rows_processed_for_this_file
        result["error_message"] = None # 清除預設的錯誤訊息或之前塊的錯誤
        worker_logger.info(f"檔案 (Hash: {file_hash[:10]}...) 已成功轉換，"
                           f"共 {total_rows_processed_for_this_file} 行數據載入到目標表 '{target_db_table}'。")

    except Exception as e_worker:
        # 捕獲在 worker 內部發生的所有其他未預期錯誤
        error_msg_worker = f"處理檔案 (Hash: {file_hash[:10]}...) 的 worker 內部發生未預期嚴重錯誤: {e_worker}"
        worker_logger.critical(error_msg_worker, exc_info=True)
        result["status"] = "TRANSFORMATION_FAILED" # 確保狀態是失敗
        result["error_message"] = str(e_worker)[:1000] # 限制錯誤訊息長度

    result["transformation_timestamp_epoch"] = time.time() # 更新為實際完成時間
    return result


class TransformationPipeline:
    """
    數據轉換管線類。

    負責協調從 `manifest.db` 讀取待處理任務、平行分派任務給 worker 進程、
    收集處理結果並更新 `manifest.db` 的整個轉換流程。
    """
    def __init__(self,
                 reprocess_quarantined: bool = False,
                 max_workers: Optional[int] = None):
        """
        初始化轉換管線。

        Args:
            reprocess_quarantined (bool): 如果為 `True`，管線將查詢並處理狀態為
                                          `QUARANTINED` 的檔案。預設為 `False`，
                                          此時管線處理狀態為 `RAW_INGESTED` 的檔案。
            max_workers (Optional[int]): 用於平行處理的 `ProcessPoolExecutor` 的最大
                                         工作進程數量。如果為 `None` (預設)，
                                         則使用 `os.cpu_count()` 自動獲取系統 CPU 核心數。
        """
        self.reprocess_quarantined: bool = reprocess_quarantined

        default_cpu_count = os.cpu_count()
        if max_workers is None:
            self.max_workers: Optional[int] = default_cpu_count # os.cpu_count() 可能返回 None
        elif max_workers <= 0:
            logger.warning(f"指定的最大工作進程數 ({max_workers}) 無效，將使用預設的 CPU 核心數 ({default_cpu_count})。")
            self.max_workers = default_cpu_count
        else:
            self.max_workers = max_workers

        # 如果 os.cpu_count() 返回 None (某些非常受限的環境)，則預設為1個worker
        if self.max_workers is None:
            logger.warning("無法偵測 CPU 核心數 (os.cpu_count() 返回 None)，將預設使用 1 個工作進程。")
            self.max_workers = 1

        mode_str = "重新處理隔離檔案" if self.reprocess_quarantined else "處理新汲取檔案"
        logger.info(f"轉換管線已初始化。處理模式: {mode_str}。最大工作進程數: {self.max_workers}")

        # 確保資料庫 (特別是 manifest 表和 processed_data 庫) 已初始化
        db_manager.initialize_databases()

    def run(self) -> Tuple[int, int, int, int]:
        """
        執行完整的數據轉換管線流程。

        Returns:
            Tuple[int, int, int, int]: 一個元組，包含：
                - `total_files_attempted` (int): 嘗試處理的檔案總數。
                - `success_count` (int): 成功轉換並載入的檔案數量。
                - `failed_count` (int): 在轉換過程中失敗的檔案數量。
                - `quarantined_count` (int): 被隔離的檔案數量（例如，格式未知或無法處理）。
        """
        logger.info(f"===== 轉換管線啟動 (Execution ID: {EXECUTION_ID}) =====")
        overall_start_time = time.time()

        status_to_query = "QUARANTINED" if self.reprocess_quarantined else "RAW_INGESTED"
        files_to_process_hashes: List[str] = db_manager.get_files_by_status(status_to_query)

        total_files_attempted = len(files_to_process_hashes)
        if not files_to_process_hashes:
            logger.info(f"在 Manifest 中未找到狀態為 '{status_to_query}' 的檔案需要進行轉換。")
            duration_empty = time.time() - overall_start_time
            logger.info(f"轉換管線執行完畢 (耗時: {duration_empty:.2f} 秒)。無檔案處理。")
            return 0, 0, 0, 0

        logger.info(f"共找到 {total_files_attempted} 個狀態為 '{status_to_query}' 的檔案待轉換。")

        success_count: int = 0
        failed_count: int = 0
        quarantined_count: int = 0

        # 使用 ProcessPoolExecutor 進行平行處理
        # 注意：在 Windows 上，多進程的初始化和資料序列化/反序列化開銷可能較大。
        # 對於大量小檔案，有時單線程或基於執行緒的並行可能反而更快。
        # 但對於 CPU 密集型的清洗操作，多進程通常是合適的。
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # 創建 future 到 file_hash 和原始路徑提示的映射，以便在完成時識別
            future_to_fileinfo: Dict[Future[Dict[str, Any]], Tuple[str, str]] = {}

            for file_hash_to_process in files_to_process_hashes:
                manifest_entry = db_manager.get_manifest_record(file_hash_to_process)
                original_path_hint_for_log = manifest_entry.get("original_file_path", "N/A") if manifest_entry else "N/A"

                future = executor.submit(process_single_file_worker, file_hash_to_process, original_path_hint_for_log)
                future_to_fileinfo[future] = (file_hash_to_process, original_path_hint_for_log)

            processed_count_display = 0
            for future_instance in as_completed(future_to_fileinfo):
                processed_count_display += 1
                file_hash_done, path_hint_done = future_to_fileinfo[future_instance]
                logger.info(f"轉換進度: {processed_count_display}/{total_files_attempted} "
                            f"(檔案 Hash: {file_hash_done[:10]}..., Path: {path_hint_done})")
                try:
                    worker_result: Dict[str, Any] = future_instance.result()

                    # 安全地獲取 worker_result 中的值，提供預設
                    final_status = worker_result.get("status", "TRANSFORMATION_FAILED")
                    error_msg = worker_result.get("error_message")
                    if final_status != "TRANSFORMATION_SUCCESS" and not error_msg:
                        error_msg = "Worker 未返回明確錯誤訊息但狀態非成功。"

                    # 更新 Manifest 記錄
                    db_manager.update_manifest_record(
                        file_hash=worker_result["file_hash"], # 應與 file_hash_done 相同
                        status=final_status,
                        fingerprint_hash=worker_result.get("fingerprint_hash"),
                        transformation_timestamp_epoch=worker_result["transformation_timestamp_epoch"],
                        target_table_name=worker_result.get("target_table_name"),
                        processed_row_count=worker_result.get("processed_row_count"),
                        error_message=error_msg,
                        pipeline_execution_id=EXECUTION_ID # 使用主流程的 EXECUTION_ID
                    )

                    if final_status == "TRANSFORMATION_SUCCESS":
                        success_count += 1
                    elif final_status == "QUARANTINED":
                        quarantined_count += 1
                    else: # TRANSFORMATION_FAILED
                        failed_count += 1

                except Exception as exc_future: # 捕獲 worker 本身執行時的嚴重錯誤 (例如 worker crash)
                    failed_count += 1
                    logger.error(f"處理檔案 (Hash: {file_hash_done[:10]}...) 的 worker 引發了未捕獲的嚴重例外: {exc_future}",
                                 exc_info=True)
                    # 嘗試更新 manifest 為失敗狀態
                    db_manager.update_manifest_record(
                        file_hash=file_hash_done,
                        status="TRANSFORMATION_FAILED",
                        error_message=f"Worker進程崩潰或引發未處理異常: {str(exc_future)[:500]}",
                        transformation_timestamp_epoch=time.time(), # 記錄錯誤發生時間
                        pipeline_execution_id=EXECUTION_ID
                    )

        overall_duration = time.time() - overall_start_time
        logger.info(f"===== 轉換管線執行完畢 (耗時: {overall_duration:.2f} 秒) =====")
        logger.info(f"  總共嘗試處理檔案數: {total_files_attempted}")
        logger.info(f"  成功轉換檔案數: {success_count}")
        logger.info(f"  轉換失敗檔案數: {failed_count}")
        logger.info(f"  被隔離檔案數: {quarantined_count}")

        return total_files_attempted, success_count, failed_count, quarantined_count


# --- 範例使用 (通常由 run.py 主啟動腳本調用) ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # import logging
    # setup_global_logger(log_level_console=logging.DEBUG) # 開啟 DEBUG 以查看詳細流程

    logger.info("開始執行 transformation_pipeline.py 範例...")

    # --- 準備測試環境與數據 ---
    current_project_root_tp_main = Path(__file__).resolve().parents[3]

    # 1. 清理並初始化所有相關資料庫
    db_manager.close_all_connections() # 確保沒有鎖定的DB檔案
    db_path_raw_manifest_tp_main = current_project_root_tp_main / db_manager.DEFAULT_DATA_DIR / db_manager.RAW_LAKE_SUBDIR / db_manager.RAW_LAKE_DB_NAME
    db_path_processed_tp_main = current_project_root_tp_main / db_manager.DEFAULT_DATA_DIR / db_manager.PROCESSED_SUBDIR / db_manager.PROCESSED_DB_NAME
    if db_path_raw_manifest_tp_main.exists(): db_path_raw_manifest_tp_main.unlink()
    if db_path_processed_tp_main.exists(): db_path_processed_tp_main.unlink()
    db_manager.initialize_databases() # 創建所有表結構

    # 2. 準備一個包含多種格式配方的 format_catalog.json
    catalog_file_path_tp_main = current_project_root_tp_main / "config" / "format_catalog.json"
    catalog_file_path_tp_main.parent.mkdir(parents=True, exist_ok=True) # 確保 config 目錄存在

    # 指紋計算輔助：根據預期標頭計算指紋
    def get_fp_for_test(header_list: List[str]) -> str:
        normalized = sorted([_normalize_column_name(h) for h in header_list]) # type: ignore[name-defined] # _normalize_column_name 來自 format_detector
        return hashlib.sha256("|".join(normalized).encode('utf-8')).hexdigest()

    FINGERPRINT_OHLCV_TEST = get_fp_for_test(["date","product_id","open","high","low","close","volume"])
    FINGERPRINT_INST_TEST = get_fp_for_test(["日期","機構類別","買方口數","賣方口數"])
    FINGERPRINT_QUARANTINED_REPROC = get_fp_for_test(["col_x","col_y","col_z"])


    sample_catalog_content = {
        FINGERPRINT_OHLCV_TEST: {
            "description": "範例 OHLCV CSV (用於 transformation_pipeline 測試)",
            "target_table": "fact_ohlcv_transformed",
            "parser_config": {"sep": ",", "header": 0, "encoding": "utf-8",
                              "dtype": {"open":str, "high":str, "low":str, "close":str, "volume":str}},
            "cleaner_function": "example_cleaners.clean_daily_ohlcv_example_v1",
            "required_columns": ["trade_date", "product_id", "open", "high", "low", "close", "volume"] # 清洗後的欄位名
        },
        FINGERPRINT_INST_TEST: {
            "description": "範例三大法人CSV (用於 transformation_pipeline 測試)",
            "target_table": "fact_inst_trades_transformed",
            "parser_config": {"sep": ",", "header": 0, "encoding": "utf-8"},
            "cleaner_function": "example_cleaners.another_cleaner_example",
            "required_columns": ["日期", "機構類別", "買方口數", "賣方口數"]
        }
        # FINGERPRINT_QUARANTINED_REPROC 的配方初始不包含，後面再添加用於測試重處理
    }
    with open(catalog_file_path_tp_main, 'w', encoding='utf-8') as f_cat:
        json.dump(sample_catalog_content, f_cat, indent=2, ensure_ascii=False)
    logger.info(f"已創建範例 format_catalog.json 於: {catalog_file_path_tp_main}")
    clear_config_cache() # 確保 TransformationPipeline 讀取的是這個新檔案

    # 3. 在 raw_lake.db 和 manifest.db 中準備測試數據記錄
    files_to_prepare = [
        {"name": "ohlcv_data.csv", "header": "date,product_id,open,high,low,close,volume",
         "rows": ["20230101,TXF,14000,14050,13980,14020,12345", "112/03/15,MXF,14010,14060,13990,14030,54321"]},
        {"name": "institutional_trades.csv", "header": "日期,機構類別,買方口數,賣方口數",
         "rows": ["2023/01/01,自營商,1000,500", "2023/01/01,投信,200,800"]},
        {"name": "unknown_format.csv", "header": "unknown_col1,unknown_col2", "rows": ["val1,val2"]},
        {"name": "bad_data_for_ohlcv_cleaner.csv", "header": "date,product_id,open_price,high_price,low_price,close_price,volume", # 欄位名不匹配cleaner期望
         "rows": ["20230102,XYZ,10,11,9,10,100"]}
    ]

    for file_info in files_to_prepare:
        content = file_info["header"] + "\n" + "\n".join(file_info["rows"])
        content_bytes = content.encode('utf-8')
        file_h = calculate_bytes_sha256(content_bytes)
        db_manager.store_raw_file(file_h, content_bytes)
        db_manager.update_manifest_record(
            file_hash=file_h,
            original_file_path=f"/test_data/{file_info['name']}",
            status="RAW_INGESTED",
            ingestion_timestamp_epoch=time.time()
        )
    logger.info(f"已準備 {len(files_to_prepare)} 個測試檔案記錄在資料庫中。")

    # --- 執行轉換管線 (首次運行) ---
    logger.info("\n===== 開始執行轉換管線 (首次運行) =====")
    tp_instance = TransformationPipeline(max_workers=os.cpu_count() or 1) # 使用所有核心或至少1個
    total_att, suc_cnt, fail_cnt, quar_cnt = tp_instance.run()

    logger.info(f"首次轉換結果: 嘗試處理={total_att}, 成功={suc_cnt}, 失敗={fail_cnt}, 隔離={quar_cnt}")
    # 預期: ohlcv_data.csv -> SUCCESS, institutional_trades.csv -> SUCCESS
    #       unknown_format.csv -> QUARANTINED (無配方)
    #       bad_data_for_ohlcv_cleaner.csv -> FAILED (cleaner 可能因欄位名不對而拿不到必要欄位，導致 required_columns 驗證失敗)
    assert total_att == 4
    assert suc_cnt == 2
    assert fail_cnt == 1
    assert quar_cnt == 1

    # --- 驗證首次運行結果 ---
    logger.info("\n--- 驗證首次運行後的 Manifest 和 Processed DB ---")
    conn_proc_main_test = db_manager.get_processed_data_connection()

    ohlcv_hash_test = calculate_bytes_sha256( (files_to_prepare[0]["header"] + "\n" + "\n".join(files_to_prepare[0]["rows"])).encode('utf-8') )
    ohlcv_rec_test = db_manager.get_manifest_record(ohlcv_hash_test)
    assert ohlcv_rec_test and ohlcv_rec_test["status"] == "TRANSFORMATION_SUCCESS"
    assert ohlcv_rec_test["target_table_name"] == "fact_ohlcv_transformed"
    assert conn_proc_main_test.execute("SELECT COUNT(*) FROM fact_ohlcv_transformed").fetchone()[0] == 2 # type: ignore
    logger.info("ohlcv_data.csv 首次轉換成功並驗證。")

    # ... (可以添加對 institutional_trades.csv, unknown_format.csv, bad_data_for_ohlcv_cleaner.csv 的 manifest 狀態驗證) ...

    # --- 模擬更新 catalog 並重新處理隔離檔案 ---
    logger.info("\n===== 測試重新處理隔離檔案 (更新 catalog 後) =====")
    unknown_hash_test = calculate_bytes_sha256( (files_to_prepare[2]["header"] + "\n" + "\n".join(files_to_prepare[2]["rows"])).encode('utf-8') )
    unknown_fp_test = calculate_format_fingerprint(io.BytesIO( (files_to_prepare[2]["header"] + "\n" + "\n".join(files_to_prepare[2]["rows"])).encode('utf-8')), "unknown_format.csv")[0]

    if unknown_fp_test:
        current_catalog = get_format_catalog() # 獲取當前 catalog
        current_catalog[unknown_fp_test] = {
            "description": "先前未知的格式 (現已註冊)",
            "target_table": "fact_other_data",
            "parser_config": {"sep": ",", "header": 0, "encoding": "utf-8"},
            "cleaner_function": "example_cleaners.another_cleaner_example", # 假設可用
            "required_columns": ["unknown_col1", "unknown_col2"]
        }
        with open(catalog_file_path_tp_main, 'w', encoding='utf-8') as f_cat_upd:
            json.dump(current_catalog, f_cat_upd, indent=2, ensure_ascii=False)
        clear_config_cache()
        logger.info(f"已更新 format_catalog.json，為指紋 '{unknown_fp_test}' 添加了配方。")

        tp_reprocess = TransformationPipeline(reprocess_quarantined=True, max_workers=1)
        total_re, suc_re, fail_re, quar_re = tp_reprocess.run()
        logger.info(f"隔離檔案重處理結果: 嘗試={total_re}, 成功={suc_re}, 失敗={fail_re}, 隔離={quar_re}")
        assert total_re == 1 # 只有 unknown_format.csv 是 QUARANTINED
        assert suc_re == 1

        unknown_rec_reprocessed = db_manager.get_manifest_record(unknown_hash_test)
        assert unknown_rec_reprocessed and unknown_rec_reprocessed["status"] == "TRANSFORMATION_SUCCESS"
        assert conn_proc_main_test.execute("SELECT COUNT(*) FROM fact_other_data").fetchone()[0] == 1 # type: ignore
        logger.info("先前隔離的 unknown_format.csv 已成功重新處理。")
    else:
        logger.error("未能為 unknown_format.csv 計算出指紋，無法測試重處理。")

    logger.info("\ntransformation_pipeline.py 範例執行成功！")

    db_manager.close_all_connections()

[end of MyTaifexDataProject/src/taifex_pipeline/transformation/pipeline.py]

[start of MyTaifexDataProject/run.py]
# -*- coding: utf-8 -*-
"""
數據管道主啟動腳本 (Main Execution Script)

提供命令行介面，用於啟動和控制數據汲取與轉換管線。
支援的操作包括：
- `ingest`: 僅執行數據汲取。
- `transform`: 僅執行數據轉換，可選擇重新處理隔離檔案。
- `run_all`: 完整執行汲取與轉換流程。
- `init_db`: 初始化資料庫結構。
- `show_config`: 顯示當前的格式目錄設定。

可透過命令行參數配置日誌級別、來源目錄、工作進程數等。
"""
import argparse
import sys
import time
from pathlib import Path
import logging # 用於設定日誌級別的常數
import json # 用於 show_config

# 設定 sys.path 以便從根目錄執行時能找到 src 下的模組
PROJECT_ROOT_RUNPY = Path(__file__).resolve().parent
SRC_DIR_RUNPY = PROJECT_ROOT_RUNPY / "src"
if str(SRC_DIR_RUNPY) not in sys.path:
    sys.path.insert(0, str(SRC_DIR_RUNPY))

try:
    from taifex_pipeline.core.logger_setup import setup_global_logger, get_logger, EXECUTION_ID
    from taifex_pipeline.database import db_manager
    from taifex_pipeline.ingestion.pipeline import IngestionPipeline
    from taifex_pipeline.transformation.pipeline import TransformationPipeline
    from taifex_pipeline.core.config_loader import get_format_catalog, clear_config_cache
except ImportError as e:
    print(f"[CRITICAL] 核心模組導入失敗，無法啟動管道。請檢查環境與 PYTHONPATH。錯誤: {e}", file=sys.stderr)
    sys.exit(1)

logger: Optional[logging.Logger] = None # 將在 main 中初始化

def main():
    """
    主函式，解析命令行參數並執行相應的管道操作。
    """
    global logger

    parser = argparse.ArgumentParser(
        description="TAIFEX 數據管道主啟動腳本。\n"
                    "提供數據汲取、轉換、資料庫初始化及設定查看等功能。",
        formatter_class=argparse.RawTextHelpFormatter # 允許在 help 字串中使用換行
    )

    parser.add_argument(
        "action",
        choices=["ingest", "transform", "run_all", "show_config", "init_db"],
        help="要執行的操作:\n"
             "  ingest        - 只執行汲取管線。\n"
             "  transform     - 只執行轉換管線。\n"
             "  run_all       - 依次執行汲取和轉換管線 (最常用)。\n"
             "  show_config   - 顯示當前加載的格式目錄設定內容。\n"
             "  init_db       - 初始化資料庫 (建表等)。"
    )
    parser.add_argument(
        "--reprocess-quarantined",
        action="store_true",
        help="在執行 'transform' 或 'run_all' 操作時，\n重新處理狀態為 'QUARANTINED' 的檔案。"
    )
    parser.add_argument(
        "--source-dirs",
        nargs="+",
        metavar="DIR",
        help="指定一個或多個要掃描的來源資料夾路徑列表 (覆蓋預設值)。\n路徑相對於專案根目錄。"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        metavar="N",
        help="轉換管線平行處理的最大工作進程數 (覆蓋預設的 CPU 核心數)。"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="設定主控台日誌的輸出級別 (預設: INFO)。"
    )
    parser.add_argument(
        "--log-file-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="DEBUG",
        help="設定檔案日誌的輸出級別 (預設: DEBUG)。"
    )
    parser.add_argument(
        "--config-dir",
        default="config",
        metavar="PATH",
        help="設定檔 (format_catalog.json) 所在的目錄名稱\n(相對於專案根目錄, 預設: config)。"
    )
    parser.add_argument(
        "--catalog-file",
        default="format_catalog.json",
        metavar="FILENAME",
        help="格式目錄設定檔的名稱 (預設: format_catalog.json)。"
    )

    args = parser.parse_args()

    # 設定日誌
    console_log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    file_log_level = getattr(logging, args.log_file_level.upper(), logging.DEBUG)
    logs_dir_main = Path("logs")

    logger = setup_global_logger(
        log_level_console=console_log_level,
        log_level_file=file_log_level,
        log_dir=logs_dir_main
    )

    logger.info(f"*** TAIFEX 數據管道啟動 (Execution ID: {EXECUTION_ID}) ***")
    logger.info(f"執行操作: {args.action}")
    if args.action in ["transform", "run_all"]:
        logger.info(f"  重新處理隔離檔案: {'是' if args.reprocess_quarantined else '否'}")
        if args.max_workers is not None: # 只有當使用者明確指定時才記錄
            logger.info(f"  最大工作進程數 (使用者指定): {args.max_workers}")
    if args.source_dirs and args.action in ["ingest", "run_all"]:
        logger.info(f"  指定來源目錄: {args.source_dirs}")
    logger.info(f"  主控台日誌級別: {args.log_level}, 檔案日誌級別: {args.log_file_level}")
    logger.info(f"  設定檔目錄: {args.config_dir}, 格式目錄檔案: {args.catalog_file}")

    overall_start_time = time.time()

    try:
        if args.action == "init_db":
            logger.info("正在執行資料庫初始化...")
            db_manager.initialize_databases()
            logger.info("資料庫初始化完成。")

        elif args.action == "show_config":
            logger.info(f"正在顯示 '{args.config_dir}/{args.catalog_file}' 內容...")
            clear_config_cache()
            try:
                catalog = get_format_catalog(config_file_name=args.catalog_file, config_dir_name=args.config_dir)
                # 使用 logger 輸出 JSON 可能會因換行符導致格式不佳，直接 print 可能更好
                # 或將 JSON 格式化後逐行 logger.info
                formatted_catalog_str = json.dumps(catalog, indent=2, ensure_ascii=False)
                logger.info(f"\n--- Format Catalog ({args.config_dir}/{args.catalog_file}) ---\n"
                            f"{formatted_catalog_str}\n"
                            f"--- End of Format Catalog ---")
            except FileNotFoundError:
                logger.error(f"錯誤：設定檔 '{PROJECT_ROOT_RUNPY / args.config_dir / args.catalog_file}' 未找到。")
            except json.JSONDecodeError:
                logger.error(f"錯誤：設定檔 '{PROJECT_ROOT_RUNPY / args.config_dir / args.catalog_file}' JSON 格式無效。")


        elif args.action == "ingest":
            ingest_pipeline = IngestionPipeline(source_directories=args.source_dirs) # source_dirs可以是None
            ingest_pipeline.run()

        elif args.action == "transform":
            transform_pipeline = TransformationPipeline(
                reprocess_quarantined=args.reprocess_quarantined,
                max_workers=args.max_workers # max_workers可以是None
            )
            transform_pipeline.run()

        elif args.action == "run_all":
            logger.info("--- 階段一：執行汲取管線 ---")
            ingest_pipeline = IngestionPipeline(source_directories=args.source_dirs)
            ingested_count, scanned_count = ingest_pipeline.run()
            logger.info(f"--- 汲取管線完成。掃描 {scanned_count} 檔案，新汲取 {ingested_count} 檔案。 ---")

            logger.info("\n--- 階段二：執行轉換管線 ---")
            transform_pipeline = TransformationPipeline(
                reprocess_quarantined=args.reprocess_quarantined,
                max_workers=args.max_workers
            )
            transform_pipeline.run()
            logger.info("--- 轉換管線完成。 ---")

    except FileNotFoundError as fnf_err:
        logger.critical(f"嚴重錯誤：找不到執行所需的檔案或目錄: {fnf_err}", exc_info=True)
        sys.exit(2)
    except Exception as e:
        logger.critical(f"管道執行過程中發生未預期的嚴重錯誤: {e}", exc_info=True)
        sys.exit(3)
    finally:
        db_manager.close_all_connections()
        overall_duration = time.time() - overall_start_time
        logger.info(f"*** TAIFEX 數據管道執行完畢 (總耗時: {overall_duration:.2f} 秒, Execution ID: {EXECUTION_ID}) ***")

if __name__ == "__main__":
    main()

[end of MyTaifexDataProject/run.py]

[start of MyTaifexDataProject/scripts/register_format.py]
# -*- coding: utf-8 -*-
"""
格式註冊輔助腳本 (Format Registration Helper Script)

本命令行工具旨在協助使用者為新的或無法識別的檔案格式生成指紋，
並在專案的「格式指紋目錄」 (`config/format_catalog.json`) 中
添加或更新其對應的處理配方。

主要流程：
1.  接收一個範例檔案的路徑作為輸入。
2.  使用 `format_detector` 模組計算該範例檔案的格式指紋。
3.  如果指紋有效：
    a.  讀取現有的 `format_catalog.json`。
    b.  檢查該指紋是否已存在於目錄中。
    c.  如果已存在，顯示現有配方並詢問使用者是否更新（除非使用 `--force-update`）。
    d.  引導使用者輸入或修改配方的各個組成部分：
        - `description`: 對此格式的人類可讀描述。
        - `target_table`: 清洗後數據應存入的目標資料庫表名。
        - `parser_config`: Pandas reader (如 `pd.read_csv`) 所需的參數字典。
        - `cleaner_function`: 對應的清洗函式名稱 (格式：`module_name.function_name`)。
        - `required_columns`: 一個欄位列表，用於驗證清洗後 DataFrame 的完整性。
    e.  將新的或更新後的配方寫回到 `format_catalog.json` 檔案。

使用方法：
從專案根目錄執行，例如：
`python scripts/register_format.py path/to/your/sample_file.csv`
`python scripts/register_format.py path/to/another_sample.txt --force-update`
"""
import argparse
import json
import io
from pathlib import Path
import sys
import logging # 導入 logging 以便設定日誌級別
from typing import Dict, Any, Optional, List # 從 typing 導入

# 確保能導入 src 下的 taifex_pipeline 模組
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from taifex_pipeline.transformation.format_detector import calculate_format_fingerprint
    from taifex_pipeline.core.config_loader import get_format_catalog, clear_config_cache
    from taifex_pipeline.core.logger_setup import setup_global_logger, get_logger
except ImportError as e:
    print(f"[CRITICAL] 核心模組導入失敗，無法啟動格式註冊腳本。請檢查環境與 PYTHONPATH。錯誤: {e}", file=sys.stderr)
    sys.exit(1)

# 此腳本是命令行工具，其日誌輸出主要用於引導使用者，INFO 級別通常足夠
# setup_global_logger 會在 get_logger 首次調用時以預設值初始化，
# 如果需要更精細的控制，可以在 main() 開頭明確調用 setup_global_logger。
logger_script = get_logger(__name__) # 使用此模組名獲取 logger

CONFIG_DIR_NAME: str = "config"
CATALOG_FILE_NAME: str = "format_catalog.json"
CONFIG_FILE_PATH: Path = PROJECT_ROOT / CONFIG_DIR_NAME / CATALOG_FILE_NAME

def prompt_for_value(prompt_message: str, default_value: Optional[str] = None) -> str:
    """
    一個通用的輔助函式，用於向使用者顯示提示訊息並獲取其輸入。
    支持提供預設值，如果使用者直接按 Enter，則返回預設值。
    會持續提示直到獲得非空輸入或接受預設值。

    Args:
        prompt_message (str): 顯示給使用者的提示文字。
        default_value (Optional[str]): 如果使用者未輸入任何內容，則返回此預設值。

    Returns:
        str: 使用者輸入的字串值（已移除首尾空白），或預設值。
    """
    full_prompt = prompt_message
    if default_value is not None:
        full_prompt += f" (預設: '{default_value}')"
    full_prompt += ": "

    while True:
        try:
            value = input(full_prompt).strip()
            if value:
                return value
            if default_value is not None:
                return default_value
            # 如果沒有預設值且輸入為空，則提示重新輸入
            logger_script.warning("輸入不能為空，請重新輸入。")
        except KeyboardInterrupt:
            logger_script.warning("\n操作被使用者中斷。")
            sys.exit(1)


def prompt_for_list(prompt_message: str, default_value: Optional[List[str]] = None) -> List[str]:
    """
    提示使用者輸入一個以逗號分隔的字串列表。
    支持提供預設列表值。

    Args:
        prompt_message (str): 顯示給使用者的提示文字。
        default_value (Optional[List[str]]): 預設的字串列表。

    Returns:
        List[str]: 使用者輸入的字串列表，每個元素都移除了首尾空白。
                   如果使用者接受空的預設值或輸入空字串，則返回空列表。
    """
    default_str = ", ".join(default_value) if default_value else ""
    raw_input_str = prompt_for_value(f"{prompt_message} (請用逗號分隔各項)", default_str)
    if not raw_input_str: # 如果使用者接受了空的預設值（例如，預設就是空列表）
        return []
    return [item.strip() for item in raw_input_str.split(',') if item.strip()]

def prompt_for_parser_config(default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    引導使用者逐條輸入或修改 `parser_config` 字典的內容。
    `parser_config` 用於配置 Pandas reader (如 `pd.read_csv`) 的行為。

    Args:
        default_config (Optional[Dict[str, Any]]): 現有的 `parser_config` 作為預設值。
                                                   如果為 `None`，則從空配置開始。

    Returns:
        Dict[str, Any]: 使用者配置完成的 `parser_config` 字典。
    """
    logger_script.info("\n--- 設定 Parser Config ---")
    logger_script.info("請逐項提供 Pandas CSV Reader (pd.read_csv) 所需的參數。")
    logger_script.info("常用參數範例及預設值將會提示。按 Enter 可接受預設值。")
    logger_script.info("對於 JSON 格式的值 (如 dtype 的字典)，請直接輸入有效的 JSON 字串。")

    config: Dict[str, Any] = default_config.copy() if default_config else {}

    # 常用且重要的參數
    config['sep'] = prompt_for_value("分隔符 (sep)", config.get('sep', ','))

    skiprows_default_str = str(config.get('skiprows', 0))
    skiprows_str = prompt_for_value("跳過檔案開頭的行數 (skiprows)", skiprows_default_str)
    try:
        config['skiprows'] = int(skiprows_str)
    except ValueError:
        logger_script.warning(f"無效的 skiprows 值 '{skiprows_str}'，將使用原值或預設值 '{skiprows_default_str}'。")
        config['skiprows'] = config.get('skiprows', 0) # 保留原值或預設

    config['encoding'] = prompt_for_value("檔案編碼 (encoding)", config.get('encoding', 'utf-8'))

    header_default_str = str(config.get('header', 'infer')) # 'infer' 是 pandas 的選項之一
    header_val_str = prompt_for_value("標頭行號 (header, 'infer' 或 數字 或 'None')", header_default_str)
    if header_val_str.strip().lower() == 'none':
        config['header'] = None
        # 如果 header is None，通常需要提供 names
        names_default_list = config.get('names', [])
        config['names'] = prompt_for_list("欄位名稱列表 (names, 逗號分隔, 僅當 header=None 時需要)", names_default_list)
    elif header_val_str.strip().lower() == 'infer':
        config['header'] = 'infer'
        if 'names' in config: # 如果之前有 names (例如從舊配方讀取)，且現在改成 infer，則移除 names
            del config['names']
    else:
        try:
            config['header'] = int(header_val_str)
            if 'names' in config: # 指定 header 行號時，通常不需要 names
                del config['names']
        except ValueError:
            logger_script.warning(f"無效的 header 值 '{header_val_str}'，將使用原值或預設值 '{header_default_str}'。")
            config['header'] = config.get('header', 'infer')


    logger_script.info("\n您可以繼續添加或修改其他 `pd.read_csv` 參數 (例如 `dtype`, `usecols`, `na_values` 等)。")
    logger_script.info("輸入參數名後，再輸入其值。如果值是 JSON (如字典或列表)，請確保輸入有效的 JSON 字串。")
    while True:
        add_more_choice = prompt_for_value("是否要添加/修改其他 parser_config 參數? (y/n)", "n").lower()
        if add_more_choice != 'y':
            break

        param_name = prompt_for_value("請輸入參數名稱 (例如 'dtype')")
        if not param_name: # 使用者可能直接按 Enter
            continue

        current_param_value_str = str(config.get(param_name, '')) # 顯示當前值作為預設
        param_value_input_str = prompt_for_value(f"請輸入參數 '{param_name}' 的值", current_param_value_str)

        try:
            # 嘗試將輸入的值解析為 JSON，這樣可以支持數字、布林、列表、字典等複雜類型
            # 例如，使用者可以輸入 '{"colA": "str", "colB": "int"}' 給 dtype
            # 或者 '[0, 1, 3]' 給 usecols
            # 或者 'true'/'false' 給 keep_default_na
            # 或者 '123' 給一個數值型參數
            # 如果就是想輸入一個普通字串，它不會被 json.loads 成功解析，則會進入 except
            if param_value_input_str.lower() == 'true':
                 param_value = True
            elif param_value_input_str.lower() == 'false':
                 param_value = False
            elif param_value_input_str.lower() == 'null' or param_value_input_str.lower() == 'none':
                 param_value = None
            elif param_value_input_str.isdigit(): # 純數字，嘗試轉為 int
                param_value = int(param_value_input_str)
            elif (param_value_input_str.replace('.', '', 1).isdigit() and param_value_input_str.count('.') <= 1): # 嘗試轉為 float
                 param_value = float(param_value_input_str)
            elif (param_value_input_str.startswith('{') and param_value_input_str.endswith('}')) or \
                 (param_value_input_str.startswith('[') and param_value_input_str.endswith(']')):
                param_value = json.loads(param_value_input_str) # 嘗試解析為 JSON dict 或 list
            else: # 其他情況視為普通字串
                param_value = param_value_input_str
        except json.JSONDecodeError:
            # 如果不是有效的JSON，則視為普通字串 (例如 encoding='utf-8')
            param_value = param_value_input_str

        config[param_name] = param_value
        logger_script.info(f"已設定參數: '{param_name}': {json.dumps(config[param_name], ensure_ascii=False)}")

    logger_script.info(f"\n最終確認的 Parser Config: {json.dumps(config, indent=2, ensure_ascii=False)}")
    return config

def run_registration_tool():
    """
    執行格式註冊工具的主邏輯。
    """
    parser = argparse.ArgumentParser(
        description="TAIFEX 數據管道 - 格式註冊與更新輔助工具。",
        epilog="請提供一個範例檔案路徑以開始。腳本會計算其格式指紋，並引導您完成處理配方的設定。"
    )
    parser.add_argument(
        "sample_file_path",
        type=str,
        help="用於計算格式指紋的範例檔案的完整路徑。"
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="如果格式指紋已存在於目錄中，則強制更新其配方而不進行提示。"
    )

    args = parser.parse_args()
    sample_file = Path(args.sample_file_path).resolve() # 轉換為絕對路徑

    if not sample_file.is_file():
        logger_script.error(f"錯誤：提供的範例檔案路徑無效或不是一個檔案: '{sample_file}'")
        sys.exit(1)

    logger_script.info(f"正在為檔案 '{sample_file.name}' (路徑: '{sample_file}') 計算格式指紋...")
    try:
        with open(sample_file, "rb") as f_sample:
            file_content_stream = io.BytesIO(f_sample.read())
    except IOError as e_io:
        logger_script.error(f"讀取範例檔案 '{sample_file.name}' 時發生 IO 錯誤: {e_io}", exc_info=True)
        sys.exit(1)

    fingerprint, header_line, detected_enc = calculate_format_fingerprint(file_content_stream, sample_file.name)

    if not fingerprint:
        logger_script.error(f"未能為檔案 '{sample_file.name}' 計算出有效的格式指紋。")
        logger_script.error(f"可能原因：檔案格式無法識別、標頭不清晰、或編碼問題 (檢測到的編碼: {detected_enc})。")
        logger_script.error("請檢查檔案內容，確保其包含可識別的標頭行。")
        sys.exit(1)

    logger_script.info(f"檔案 '{sample_file.name}' 的格式指紋為: {fingerprint}")
    if header_line is not None:
        logger_script.info(f"(該指紋基於在第 {header_line + 1} 行偵測到的標頭，使用編碼 '{detected_enc}')")

    # 讀取或初始化 format_catalog.json
    CONFIG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True) # 確保 config 目錄存在
    clear_config_cache() # 確保讀取的是最新檔案內容
    try:
        catalog_data: Dict[str, Any] = get_format_catalog(
            config_file_name=CATALOG_FILE_NAME,
            config_dir_name=CONFIG_DIR_NAME
        )
    except FileNotFoundError:
        logger_script.info(f"設定檔 '{CONFIG_FILE_PATH}' 不存在，將創建一個新的。")
        catalog_data = {}
    except json.JSONDecodeError:
        logger_script.error(f"設定檔 '{CONFIG_FILE_PATH}' 內容已損毀，無法解析 JSON。 "
                            "請檢查檔案內容，或備份後刪除該檔案再重試。")
        sys.exit(1)

    existing_recipe: Optional[Dict[str, Any]] = catalog_data.get(fingerprint)
    is_update_mode = False

    if existing_recipe:
        logger_script.info(f"\n--- 此格式指紋 '{fingerprint}' 已存在於格式目錄中 ---")
        logger_script.info("目前的處理配方內容如下:")
        logger_script.info(f"{json.dumps(existing_recipe, indent=2, ensure_ascii=False)}")
        if not args.force_update:
            user_choice = prompt_for_value("是否要更新此現有配方? (y/n)", "n").lower()
            if user_choice != 'y':
                logger_script.info("操作已取消，未對格式目錄作任何修改。")
                sys.exit(0)
        is_update_mode = True
        logger_script.info("將開始更新現有的配方...")
    else:
        logger_script.info(f"\n--- 為新的格式指紋 '{fingerprint}' 創建處理配方 ---")
        existing_recipe = {} # 為新配方提供空的預設字典，以便 prompt 函式使用

    # 引導使用者輸入配方的詳細資訊
    new_recipe: Dict[str, Any] = {}
    new_recipe['description'] = prompt_for_value(
        "請輸入此格式的描述 (例如 '期交所每日行情CSV v2')",
        existing_recipe.get('description', f"自動生成描述：{sample_file.name} 的格式")
    )
    new_recipe['target_table'] = prompt_for_value(
        "清洗後數據應存入的目標資料庫表名",
        existing_recipe.get('target_table', 'default_target_table')
    )

    new_recipe['parser_config'] = prompt_for_parser_config(existing_recipe.get('parser_config'))

    new_recipe['cleaner_function'] = prompt_for_value(
        "對應的清洗函式名稱 (格式: '模組名.函式名', 例如 'example_cleaners.clean_data_v1')",
        existing_recipe.get('cleaner_function', 'example_cleaners.default_cleaner_placeholder')
    )
    new_recipe['required_columns'] = prompt_for_list(
        "清洗後 DataFrame 中必須存在的欄位列表",
        existing_recipe.get('required_columns', [])
    )

    # 更新 catalog_data 中的對應條目
    catalog_data[fingerprint] = new_recipe

    # 將更新後的 catalog_data 寫回到 JSON 檔案
    try:
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f_out:
            json.dump(catalog_data, f_out, indent=2, ensure_ascii=False)
        action_str = "更新後的" if is_update_mode else "新的"
        logger_script.info(f"\n成功將{action_str}配方寫入到 '{CONFIG_FILE_PATH}'。")
        clear_config_cache() # 清除快取，以便後續 get_format_catalog 能讀到更新
        logger_script.debug("Config loader 快取已清除。")

    except IOError as e_io_write:
        logger_script.error(f"將配方寫回到 '{CONFIG_FILE_PATH}' 時發生 IO 錯誤: {e_io_write}", exc_info=True)
        sys.exit(1)

    logger_script.info("\n格式註冊/更新操作完成。")
    logger_script.info("請確保 `format_catalog.json` 中指定的清洗函式已在相應的 cleaner 模組中實現。")

if __name__ == "__main__":
    # 確保腳本執行時，日誌可以被正確初始化並寫入到預期位置
    logs_dir_for_this_script = PROJECT_ROOT / "logs"
    logs_dir_for_this_script.mkdir(parents=True, exist_ok=True)

    # 對於命令行工具，INFO 級別的主控台日誌通常比較合適
    setup_global_logger(
        log_level_console=logging.INFO,
        log_level_file=logging.DEBUG, # 檔案日誌可以更詳細
        log_dir=logs_dir_for_this_script.relative_to(PROJECT_ROOT) # 相對路徑給 setup
    )
    run_registration_tool()

[end of MyTaifexDataProject/scripts/register_format.py]
