# -*- coding: utf-8 -*-
"""
資料庫互動模組 (Database Manager)

本模組封裝了與專案所使用的所有資料庫（主要是 DuckDB 實例）的互動邏輯。
它負責管理資料庫連接、初始化資料表結構、以及提供對以下核心資料存儲的
增、刪、改、查 (CRUD) 操作介面：

1.  **原始數據湖 (`raw_lake_and_manifest.duckdb` 中的 `raw_files` 表)**:
    儲存從各種來源獲取的原始檔案的二進位內容及其 SHA256 雜湊值。

2.  **處理清單/審計日誌 (`raw_lake_and_manifest.duckdb` 中的 `file_processing_log` 表)**:
    即 `manifest.db` 的功能，記錄每個檔案從汲取到最終處理狀態的完整生命週期，
    包括時間戳、狀態標籤、錯誤訊息、處理結果等元數據。

3.  **已處理數據倉庫 (`processed_data.duckdb`)**:
    儲存經過清洗、轉換和標準化後的最終數據，這些數據已準備好用於分析或進一步使用。
    其中的資料表通常是根據 `format_catalog.json` 中的 `target_table` 動態創建的。

主要特性：
- 使用 DuckDB 作為主要的資料庫引擎。
- 連接快取機制以複用資料庫連接。
- 在首次使用時自動初始化資料庫和必要的資料表結構。
- 提供清晰、類型安全的函式介面進行資料庫操作。
"""
import duckdb # type: ignore # DuckDB 可能沒有完全的類型存根，或 MyPy 配置需要調整
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Union # 添加 Union

from taifex_pipeline.core.logger_setup import get_logger

logger = get_logger(__name__)

# --- 資料庫檔案路徑與名稱設定 ---
RAW_LAKE_DB_NAME: str = "raw_lake_and_manifest.duckdb"
"""包含原始檔案內容和處理清單 (manifest) 的 DuckDB 資料庫檔案名稱。"""
PROCESSED_DB_NAME: str = "processed_data.duckdb"
"""儲存已處理和清洗後數據的 DuckDB 資料庫檔案名稱。"""

RAW_FILES_TABLE: str = "raw_files"
"""在 RAW_LAKE_DB_NAME 中儲存原始檔案內容的資料表名稱。"""
MANIFEST_TABLE: str = "file_processing_log"
"""在 RAW_LAKE_DB_NAME 中儲存檔案處理日誌 (manifest) 的資料表名稱。"""

DEFAULT_DATA_DIR: Path = Path("data")
"""專案中存放所有數據相關檔案的預設根目錄名稱。"""
RAW_LAKE_SUBDIR: str = "01_raw_lake"
"""在 DEFAULT_DATA_DIR 下存放 RAW_LAKE_DB_NAME 的子目錄名稱。"""
PROCESSED_SUBDIR: str = "02_processed"
"""在 DEFAULT_DATA_DIR 下存放 PROCESSED_DB_NAME 的子目錄名稱。"""

# --- 連接管理 ---
_connections: Dict[str, duckdb.DuckDBPyConnection] = {}
"""模組級的快取，用於存儲已建立的 DuckDB 連接，以路徑為鍵。"""

def _get_project_root() -> Path:
    """內部輔助函式，獲取專案的根目錄路徑。"""
    # 此檔案位於 src/taifex_pipeline/database/db_manager.py
    return Path(__file__).resolve().parents[3]

def get_db_connection(db_name: str, data_subdir_name: str) -> duckdb.DuckDBPyConnection:
    """
    獲取或建立一個到指定 DuckDB 資料庫檔案的連接。

    此函式會快取已建立的連接。如果請求的資料庫連接已存在於快取中且仍然有效，
    則直接返回快取的連接。否則，它會建立一個新的連接，配置必要的設定
    （如記憶體限制、物件快取），然後將新連接存入快取並返回。

    Args:
        db_name (str): 資料庫檔案的名稱 (例如, "raw_lake_and_manifest.duckdb")。
        data_subdir_name (str): 存放該資料庫檔案的 `data` 子目錄名稱
                               (例如, "01_raw_lake")。

    Returns:
        duckdb.DuckDBPyConnection: DuckDB 的連接物件。

    Raises:
        Exception: 如果連接到資料庫失敗。
    """
    project_root = _get_project_root()
    db_dir = project_root / DEFAULT_DATA_DIR / data_subdir_name
    db_dir.mkdir(parents=True, exist_ok=True) # 確保資料庫目錄存在
    db_path_str = str(db_dir / db_name)

    if db_path_str in _connections:
        conn_candidate = _connections[db_path_str]
        try:
            # 檢查快取的連接是否仍然有效
            if conn_candidate and not conn_candidate.isclosed(): # type: ignore[attr-defined]
                conn_candidate.execute("SELECT 1") # 簡單查詢以測試活性
                logger.debug(f"返回已快取的 DuckDB 連接: {db_path_str}")
                return conn_candidate
        except (duckdb.ConnectionException, duckdb.InvalidInputException, AttributeError) as e: # AttributeError for isclosed if already closed by other means
            logger.warning(f"快取的 DuckDB 連接 {db_path_str} 已失效或關閉 ({e})，將重新建立。")
            del _connections[db_path_str] # 從快取中移除失效連接

    logger.info(f"正在建立新的 DuckDB 連接: {db_path_str}")
    try:
        # TODO: 記憶體限制應從外部設定檔讀取或作為參數傳入
        # 注意：memory_limit 是每個 DuckDB 程序（實例）的限制。
        con = duckdb.connect(database=db_path_str, read_only=False)
        con.execute("SET memory_limit='2GB'") # 預設值，可調整
        con.execute("SET enable_object_cache=true;")
        _connections[db_path_str] = con
        return con
    except Exception as e: # 捕獲所有可能的 duckdb.Error 子類及其他錯誤
        logger.error(f"連接到 DuckDB '{db_path_str}' 失敗: {e}", exc_info=True)
        raise # 重新拋出異常，讓上層調用者處理

def get_raw_lake_connection() -> duckdb.DuckDBPyConnection:
    """獲取到 `raw_lake_and_manifest.duckdb` 資料庫的連接。"""
    return get_db_connection(RAW_LAKE_DB_NAME, RAW_LAKE_SUBDIR)

def get_processed_data_connection() -> duckdb.DuckDBPyConnection:
    """獲取到 `processed_data.duckdb` 資料庫的連接。"""
    return get_db_connection(PROCESSED_DB_NAME, PROCESSED_SUBDIR)

def close_all_connections() -> None:
    """
    安全地關閉所有由本模組管理和快取的 DuckDB 連接。
    建議在應用程式結束時調用此函式。
    """
    global _connections
    closed_count = 0
    for db_path, con in list(_connections.items()): # 使用 list(_connections.items()) 以允許在迭代中刪除
        try:
            if con and not con.isclosed(): # type: ignore[attr-defined]
                con.close()
                logger.info(f"已關閉 DuckDB 連接: {db_path}")
                closed_count +=1
            del _connections[db_path] # 從快取中移除
        except (duckdb.ConnectionException, AttributeError) as e: # AttributeError for isclosed if already closed
            logger.warning(f"關閉 DuckDB 連接 {db_path} 時發生錯誤或連接已關閉: {e}", exc_info=False) # info=False 避免過多堆疊
            if db_path in _connections: # 確保在迭代中安全刪除
                del _connections[db_path]
    if closed_count > 0:
        logger.info(f"共 {closed_count} 個資料庫連接已關閉。")
    else:
        logger.info("沒有需要關閉的活動資料庫連接。")
    _connections = {} # 確保快取被清空


# --- 初始化函式 ---
def initialize_databases() -> None:
    """
    初始化所有相關的資料庫，確保其中定義的必要資料表結構已存在。
    此函式應在管道啟動時或首次使用資料庫前調用。
    """
    logger.info("開始資料庫初始化程序...")
    _initialize_raw_lake_and_manifest()
    _initialize_processed_data()
    logger.info("資料庫初始化程序完成。")

def _initialize_raw_lake_and_manifest() -> None:
    """初始化 `raw_lake_and_manifest.duckdb` 中的資料表 (`raw_files`, `file_processing_log`)。"""
    con = get_raw_lake_connection()
    try:
        logger.debug(f"正在於 '{RAW_LAKE_DB_NAME}' 中初始化資料表...")
        # 創建 raw_files 資料表
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_FILES_TABLE} (
            file_hash TEXT PRIMARY KEY,
            raw_content BLOB NOT NULL,
            first_seen_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp
        );
        """)
        logger.info(f"資料表 '{RAW_FILES_TABLE}' 已在 '{RAW_LAKE_DB_NAME}' 中確認/創建。")

        # 創建 file_processing_log (manifest) 資料表
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
            file_hash TEXT PRIMARY KEY,
            original_file_path TEXT,
            status TEXT,
            fingerprint_hash TEXT,
            ingestion_timestamp TIMESTAMP WITH TIME ZONE,
            transformation_timestamp TIMESTAMP WITH TIME ZONE,
            target_table_name TEXT,
            processed_row_count INTEGER,
            error_message TEXT,
            pipeline_execution_id TEXT,
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
            CONSTRAINT fk_raw_file FOREIGN KEY (file_hash) REFERENCES {RAW_FILES_TABLE}(file_hash) ON DELETE CASCADE ON UPDATE CASCADE
        );
        """)
        # 創建索引以加速常用查詢
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_manifest_status ON {MANIFEST_TABLE} (status);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_manifest_fingerprint ON {MANIFEST_TABLE} (fingerprint_hash);")
        logger.info(f"資料表 '{MANIFEST_TABLE}' (含索引) 已在 '{RAW_LAKE_DB_NAME}' 中確認/創建。")

    except Exception as e: # 捕獲所有可能的 duckdb.Error 子類及其他錯誤
        logger.error(f"初始化 '{RAW_LAKE_DB_NAME}' 中的資料表失敗: {e}", exc_info=True)
        raise

def _initialize_processed_data() -> None:
    """
    初始化 `processed_data.duckdb`。
    目前此函式僅記錄連接資訊，因為實際的資料表是根據轉換結果動態創建的。
    未來可在此處添加通用設定或輔助結構的創建。
    """
    _ = get_processed_data_connection() # 僅獲取連接以確保DB檔案可被創建
    logger.info(f"資料庫 '{PROCESSED_DB_NAME}' 已連接。其資料表通常由轉換管線動態創建。")


# --- Raw Lake 操作 ---
def store_raw_file(file_hash: str, raw_content: bytes, conn: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """
    將原始檔案的二進位內容及其 SHA256 雜湊值存儲到 `raw_files` 表中。
    如果具有相同 `file_hash` 的記錄已存在，則此操作通常會替換現有記錄（取決於DB行為，DuckDB INSERT OR REPLACE）。

    Args:
        file_hash (str): 檔案內容的 SHA256 雜湊值，作為主鍵。
        raw_content (bytes): 檔案的原始二進位內容。
        conn (Optional[duckdb.DuckDBPyConnection]): 可選的資料庫連接。若未提供，則使用預設連接。

    Returns:
        bool: 如果儲存操作成功，返回 `True`；否則記錄錯誤並返回 `False`。
    """
    db_conn = conn or get_raw_lake_connection()
    try:
        # INSERT OR REPLACE 語法在 DuckDB 中是 INSERT ... ON CONFLICT DO UPDATE/NOTHING
        # 為了簡單和明確替換，可以先 DELETE 再 INSERT，或使用 CREATE OR REPLACE TABLE (如果適用於單行)
        # DuckDB 的 Python API register + insert 更為方便，或直接SQL
        # 此處使用 INSERT OR IGNORE 語義，如果存在則不操作，依賴 first_seen_timestamp。
        # 或者，更符合 "store" 語義的是 REPLACE:
        db_conn.execute(
            f"INSERT INTO {RAW_FILES_TABLE} (file_hash, raw_content) VALUES (?, ?) "
            f"ON CONFLICT(file_hash) DO UPDATE SET raw_content = EXCLUDED.raw_content, first_seen_timestamp = current_timestamp",
            (file_hash, raw_content)
        )
        logger.info(f"原始檔案 (Hash: {file_hash[:10]}...) 已儲存/更新到 '{RAW_FILES_TABLE}'。")
        return True
    except Exception as e:
        logger.error(f"儲存原始檔案 (Hash: {file_hash[:10]}...) 到 '{RAW_FILES_TABLE}' 失敗: {e}", exc_info=True)
        return False

def get_raw_file_content(file_hash: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[bytes]:
    """
    根據檔案的 SHA256 雜湊值從 `raw_files` 表中檢索其原始二進位內容。

    Args:
        file_hash (str): 要檢索檔案的 SHA256 雜湊值。
        conn (Optional[duckdb.DuckDBPyConnection]): 可選的資料庫連接。

    Returns:
        Optional[bytes]: 檔案的原始二進位內容。如果未找到具有該雜湊值的檔案，
                         或在讀取過程中發生錯誤，則返回 `None`。
    """
    db_conn = conn or get_raw_lake_connection()
    try:
        result = db_conn.execute(f"SELECT raw_content FROM {RAW_FILES_TABLE} WHERE file_hash = ?", (file_hash,)).fetchone()
        if result and result[0] is not None:
            logger.debug(f"從 '{RAW_FILES_TABLE}' 讀取到檔案內容 (Hash: {file_hash[:10]}...) 。")
            return bytes(result[0]) # 確保返回的是 bytes
        else:
            logger.warning(f"在 '{RAW_FILES_TABLE}' 中未找到檔案 (Hash: {file_hash[:10]}...) 或內容為 NULL。")
            return None
    except Exception as e:
        logger.error(f"從 '{RAW_FILES_TABLE}' 讀取檔案 (Hash: {file_hash[:10]}...) 失敗: {e}", exc_info=True)
        return None

# --- Manifest 操作 ---
def update_manifest_record(
    file_hash: str,
    original_file_path: Optional[str] = None,
    status: Optional[str] = None,
    fingerprint_hash: Optional[str] = None,
    ingestion_timestamp_epoch: Optional[float] = None,
    transformation_timestamp_epoch: Optional[float] = None,
    target_table_name: Optional[str] = None,
    processed_row_count: Optional[int] = None,
    error_message: Optional[str] = None,
    pipeline_execution_id: Optional[str] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None
) -> bool:
    """
    新增或更新 `file_processing_log` (manifest) 表中的檔案處理記錄。

    此函式實現了 "UPSERT" (Update or Insert) 邏輯：
    - 如果具有給定 `file_hash` 的記錄已存在，則更新提供的欄位。
    - 如果記錄不存在，則插入一條新記錄。
    `last_updated` 欄位會在每次成功操作時自動更新為當前時間戳。

    Args:
        file_hash (str): 檔案內容的 SHA256 雜湊值 (主鍵)。
        original_file_path (Optional[str]): 檔案的原始來源路徑。
        status (Optional[str]): 檔案的當前處理狀態 (例如, "RAW_INGESTED", "TRANSFORMATION_SUCCESS")。
        fingerprint_hash (Optional[str]): 檔案成功識別後的格式指紋。
        ingestion_timestamp_epoch (Optional[float]): 檔案汲取時間戳 (秒級 Unix epoch time)。
        transformation_timestamp_epoch (Optional[float]): 檔案轉換完成（或失敗）的時間戳 (秒級 Unix epoch time)。
        target_table_name (Optional[str]): 若轉換成功，數據存入的目標資料庫表名。
        processed_row_count (Optional[int]): 若轉換成功，從該檔案轉換出的數據行數。
        error_message (Optional[str]): 若處理過程中發生錯誤，記錄詳細的錯誤訊息。
        pipeline_execution_id (Optional[str]): 執行此處理的管線運行ID。
        conn (Optional[duckdb.DuckDBPyConnection]): 可選的資料庫連接。

    Returns:
        bool: 如果操作成功（新增或更新），返回 `True`；否則記錄錯誤並返回 `False`。
    """
    db_conn = conn or get_raw_lake_connection()

    fields_to_update: Dict[str, Any] = {}
    # 逐一檢查傳入的參數，只有非 None 的才加入到更新字典中
    if original_file_path is not None: fields_to_update["original_file_path"] = original_file_path
    if status is not None: fields_to_update["status"] = status
    if fingerprint_hash is not None: fields_to_update["fingerprint_hash"] = fingerprint_hash
    if ingestion_timestamp_epoch is not None:
        fields_to_update["ingestion_timestamp"] = pd.to_datetime(ingestion_timestamp_epoch, unit='s', utc=True)
    if transformation_timestamp_epoch is not None:
        fields_to_update["transformation_timestamp"] = pd.to_datetime(transformation_timestamp_epoch, unit='s', utc=True)
    if target_table_name is not None: fields_to_update["target_table_name"] = target_table_name
    if processed_row_count is not None: fields_to_update["processed_row_count"] = processed_row_count
    if error_message is not None: fields_to_update["error_message"] = error_message
    if pipeline_execution_id is not None: fields_to_update["pipeline_execution_id"] = pipeline_execution_id

    try:
        # 使用 DuckDB 的 INSERT ... ON CONFLICT ... DO UPDATE ... 語法實現 UPSERT
        # 首先準備要插入的欄位和值的列表 (用於 INSERT 部分)
        # 必須包含主鍵 file_hash
        all_insert_cols_map = fields_to_update.copy()
        all_insert_cols_map["file_hash"] = file_hash
        # last_updated 會由資料庫 DEFAULT 或在 UPDATE 中設定
        # 如果是新插入，也需要設定 last_updated
        all_insert_cols_map["last_updated"] = pd.Timestamp.now(tz='UTC').floor('ms')


        insert_cols_list = list(all_insert_cols_map.keys())
        insert_values_tuple = tuple(all_insert_cols_map.get(col) for col in insert_cols_list) # 保持順序

        cols_str = ", ".join(insert_cols_list)
        placeholders_str = ", ".join(["?"] * len(insert_cols_list))

        # 準備 UPDATE 部分的 SET 子句
        # 不更新 file_hash (主鍵) 和 ingestion_timestamp (通常只在汲取時設定一次)
        # 如果是重新汲取，ingestion_timestamp 應在汲取階段的 update_manifest_record 中更新
        update_set_parts: List[str] = []
        update_values: List[Any] = []

        # last_updated 總是會更新
        fields_to_update["last_updated"] = all_insert_cols_map["last_updated"]

        for key, value in fields_to_update.items():
            update_set_parts.append(f"{key} = ?")
            update_values.append(value)

        if not update_set_parts: # 如果除了 file_hash 外沒有其他欄位需要更新
            # 這種情況通常是只想確保記錄存在，或者是一個無操作的更新
            # 可以簡化為只做 INSERT OR IGNORE (如果 file_hash 已存在則不執行)
            # 或如果只想更新 last_updated:
            # db_conn.execute(f"UPDATE {MANIFEST_TABLE} SET last_updated = ? WHERE file_hash = ?", (all_insert_cols_map["last_updated"], file_hash))
            logger.debug(f"Manifest 記錄 (Hash: {file_hash[:10]}...): 無實質欄位更新，僅可能更新 last_updated (如果存在)。")
            # 為了確保外鍵約束及記錄存在，如果記錄不存在，則插入一個最小化的記錄
            # (已在 store_raw_file 之後的 update_manifest_record 初始調用中處理)
            # 此處假設如果沒有 fields_to_update，則不需要執行複雜的UPSERT
            # 但至少要確保 last_updated 被更新 (如果記錄存在)
            # 或者，如果記錄不存在，則插入帶有 file_hash 和 status 的記錄 (如果 status 被提供)
            # 簡化：如果 fields_to_update 為空，且記錄已存在，則不執行SQL，避免不必要的寫入
            #       如果記錄不存在，則下面的 ON CONFLICT 會處理插入
            if db_conn.execute(f"SELECT 1 FROM {MANIFEST_TABLE} WHERE file_hash = ?", (file_hash,)).fetchone() and not fields_to_update:
                 return True


        update_set_sql = ", ".join(update_set_parts)

        # UPSERT SQL
        # EXCLUDED.column_name 用於引用試圖插入的值
        upsert_sql = (
            f"INSERT INTO {MANIFEST_TABLE} ({cols_str}) VALUES ({placeholders_str}) "
            f"ON CONFLICT(file_hash) DO UPDATE SET {update_set_sql}"
        )

        final_params = list(insert_values_tuple) + update_values

        db_conn.execute(upsert_sql, final_params)
        logger.info(f"Manifest 記錄 (Hash: {file_hash[:10]}...) 已成功新增或更新。狀態: {status or '(未變更)'}")
        return True

    except Exception as e:
        logger.error(f"更新 Manifest 記錄 (Hash: {file_hash[:10]}...) 失敗: {e}", exc_info=True)
        return False


def get_manifest_record(file_hash: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[Dict[str, Any]]:
    """
    根據檔案的 SHA256 雜湊值從 `file_processing_log` (manifest) 表中檢索單條處理記錄。

    Args:
        file_hash (str): 要檢索記錄的檔案 SHA256 雜湊值。
        conn (Optional[duckdb.DuckDBPyConnection]): 可選的資料庫連接。

    Returns:
        Optional[Dict[str, Any]]: 包含記錄所有欄位內容的字典。
                                  如果未找到該記錄，則返回 `None`。
                                  Timestamp 欄位會被轉換為 Python `datetime.datetime` 物件 (naive)。
    """
    db_conn = conn or get_raw_lake_connection()
    try:
        # 使用 fetch_arrow_table().to_pandas() 可以獲取含正確類型的 DataFrame
        arrow_table = db_conn.execute(f"SELECT * FROM {MANIFEST_TABLE} WHERE file_hash = ?", (file_hash,)).fetch_arrow_table()
        if arrow_table.num_rows == 0:
            logger.debug(f"在 '{MANIFEST_TABLE}' 中未找到記錄 (Hash: {file_hash[:10]}...)。")
            return None

        record_df = arrow_table.to_pandas()
        # DuckDB Arrow interface 通常返回帶時區的 datetime64[ns, UTC] 或無時區的 datetime64[ns]
        # 轉換為 naive datetime 物件以便序列化或通用處理
        for col in record_df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
            if record_df[col].dt.tz is not None:
                record_df[col] = record_df[col].dt.tz_convert(None) # 轉換為 naive (本地時間)
            # record_df[col] = record_df[col].dt.to_pydatetime() # 轉為 Python datetime object list，但 to_dict() 通常能處理

        # 將 NaN/NaT 轉換為 None 以便得到更乾淨的字典
        record_dict = record_df.iloc[0].where(pd.notnull(record_df.iloc[0]), None).to_dict()
        logger.debug(f"從 '{MANIFEST_TABLE}' 獲取到記錄 (Hash: {file_hash[:10]}...)。")
        return record_dict
    except Exception as e:
        logger.error(f"從 '{MANIFEST_TABLE}' 獲取記錄 (Hash: {file_hash[:10]}...) 失敗: {e}", exc_info=True)
        return None

def get_files_by_status(status: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> List[str]:
    """
    根據指定的處理狀態從 `file_processing_log` (manifest) 表中檢索所有符合條件的檔案雜湊值列表。

    Args:
        status (str): 要查詢的檔案處理狀態 (例如, "RAW_INGESTED", "QUARANTINED")。
        conn (Optional[duckdb.DuckDBPyConnection]): 可選的資料庫連接。

    Returns:
        List[str]: 符合指定狀態的所有檔案的 SHA256 雜湊值列表。
                   如果查詢失敗或沒有符合條件的檔案，則返回空列表。
    """
    db_conn = conn or get_raw_lake_connection()
    try:
        results = db_conn.execute(f"SELECT file_hash FROM {MANIFEST_TABLE} WHERE status = ?", (status,)).fetchall()
        # fetchall() 返回 List[Tuple[Any, ...]]
        file_hashes = [str(row[0]) for row in results if row[0] is not None]
        logger.info(f"從 '{MANIFEST_TABLE}' 查詢到 {len(file_hashes)} 個狀態為 '{status}' 的檔案。")
        return file_hashes
    except Exception as e:
        logger.error(f"從 '{MANIFEST_TABLE}' 查詢狀態為 '{status}' 的檔案失敗: {e}", exc_info=True)
        return []

def check_file_hash_exists_in_manifest(file_hash: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """
    檢查指定的檔案 SHA256 雜湊值是否存在於 `file_processing_log` (manifest) 表中。

    Args:
        file_hash (str): 要檢查的檔案 SHA256 雜湊值。
        conn (Optional[duckdb.DuckDBPyConnection]): 可選的資料庫連接。

    Returns:
        bool: 如果記錄存在，返回 `True`；否則返回 `False`。
    """
    record = get_manifest_record(file_hash, conn)
    return record is not None

# --- Processed Data 操作 ---
def load_dataframe_to_processed_db(
    df: pd.DataFrame,
    table_name: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    if_exists: str = "append"  # "fail", "replace", "append"
) -> bool:
    """
    將 pandas DataFrame 的內容載入到 `processed_data.duckdb` 資料庫中的指定表格。

    此函式處理表格是否已存在的幾種情況：
    - `fail`: 如果表格已存在，則操作失敗並返回 `False`。
    - `replace`: 如果表格已存在，則先刪除舊表，然後創建新表並載入數據。
    - `append`: 如果表格已存在，則將數據追加到現有表格中。
                 如果表格不存在，則創建新表並載入數據。 (DuckDB CREATE TABLE AS 會隱含創建)

    Args:
        df (pd.DataFrame): 要載入的 pandas DataFrame。
        table_name (str): 目標資料庫中的資料表名稱。
        conn (Optional[duckdb.DuckDBPyConnection]): 可選的資料庫連接。
        if_exists (str): 當目標表格已存在時應執行的操作。
                         可選值為 "fail", "replace", "append"。預設為 "append"。

    Returns:
        bool: 如果數據載入操作成功，返回 `True`；否則記錄錯誤並返回 `False`。
    """
    if df.empty:
        logger.info(f"輸入的 DataFrame 為空，無需載入到目標表 '{table_name}'。")
        return True

    db_conn = conn or get_processed_data_connection()
    # 臨時註冊 DataFrame 為 DuckDB 中的一個可查詢的關聯 (relation)
    # 這比直接使用 df_to_table() 等方法在處理 schema 和 if_exists 時更靈活
    temp_relation_name = f"df_temp_{uuid.uuid4().hex[:8]}" # 產生一個唯一的臨時關聯名稱

    try:
        db_conn.register(temp_relation_name, df)

        # 檢查表格是否存在
        table_exists_result = db_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
            (table_name,)
        ).fetchone()
        table_exists = table_exists_result is not None

        if table_exists:
            if if_exists == "fail":
                logger.error(f"目標表 '{table_name}' 已存在於 '{PROCESSED_DB_NAME}'，且 if_exists='fail'。操作終止。")
                return False
            elif if_exists == "replace":
                db_conn.execute(f"DROP TABLE IF EXISTS \"{table_name}\"") # 使用引號以處理特殊表名
                logger.info(f"目標表 '{table_name}' 已存在，已被移除以進行替換。")
                # 接下來會執行創建新表的邏輯
                db_conn.execute(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM {temp_relation_name}")
                logger.info(f"成功替換並創建表格 '{table_name}'，並載入 {len(df)} 行數據到 '{PROCESSED_DB_NAME}'。")
            elif if_exists == "append":
                db_conn.execute(f"INSERT INTO \"{table_name}\" SELECT * FROM {temp_relation_name}")
                logger.info(f"{len(df)} 行數據已追加到現有表格 '{table_name}'。")
            else:
                logger.error(f"未知的 if_exists 選項: '{if_exists}'。無法處理表格 '{table_name}'。")
                return False
        else: # 表格不存在
            db_conn.execute(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM {temp_relation_name}")
            logger.info(f"成功創建新表格 '{table_name}' 並載入 {len(df)} 行數據到 '{PROCESSED_DB_NAME}'。")

        return True

    except Exception as e:
        logger.error(f"載入 DataFrame 到表格 '{table_name}' (在 '{PROCESSED_DB_NAME}') 時發生失敗: {e}", exc_info=True)
        return False
    finally:
        # 總是嘗試取消註冊臨時關聯
        try:
            if db_conn: # 確保連接物件存在
                 db_conn.unregister(temp_relation_name)
                 logger.debug(f"已取消註冊臨時關聯 '{temp_relation_name}'。")
        except Exception as unreg_e:
            logger.warning(f"取消註冊臨時關聯 '{temp_relation_name}' 時發生錯誤: {unreg_e}", exc_info=False)


# --- 測試與範例 ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # setup_global_logger(log_level_console=logging.DEBUG) # 方便查看調試訊息

    logger.info("開始執行 db_manager.py 範例...")

    try:
        # 0. 清理舊的DB檔案 (如果存在)，以便重複測試
        project_r = _get_project_root()
        db_path_raw_manifest = project_r / DEFAULT_DATA_DIR / RAW_LAKE_SUBDIR / RAW_LAKE_DB_NAME
        db_path_processed = project_r / DEFAULT_DATA_DIR / PROCESSED_SUBDIR / PROCESSED_DB_NAME

        # 確保在嘗試刪除前關閉所有可能的連接
        close_all_connections()

        if db_path_raw_manifest.exists():
            db_path_raw_manifest.unlink()
            logger.info(f"已刪除舊的 '{RAW_LAKE_DB_NAME}' 進行測試。")
        if db_path_processed.exists():
            db_path_processed.unlink()
            logger.info(f"已刪除舊的 '{PROCESSED_DB_NAME}' 進行測試。")

        # 1. 初始化資料庫 (建表)
        initialize_databases()

        # 2. Raw Lake 操作範例
        sample_hash_1 = "hash_abc123_ohlcv_test"
        sample_content_1 = b"trade_date,product_id,open\n20230101,TXF,14000"
        sample_hash_2 = "hash_def456_inst_test"
        sample_content_2 = b"date,investor_type,buy_lots\n20230101,FI,1000"

        assert store_raw_file(sample_hash_1, sample_content_1), "儲存檔案1到Raw Lake失敗"
        assert store_raw_file(sample_hash_2, sample_content_2), "儲存檔案2到Raw Lake失敗"

        retrieved_content_1 = get_raw_file_content(sample_hash_1)
        assert retrieved_content_1 == sample_content_1, "取回的檔案1內容與原始不符"
        logger.info(f"成功取回檔案1內容: {retrieved_content_1!r}")

        retrieved_content_non_exist = get_raw_file_content("non_existent_hash_value_sample")
        assert retrieved_content_non_exist is None, "對於不存在的雜湊，應返回None"

        # 3. Manifest 操作範例
        exec_id_sample = "test_exec_id_007"
        current_epoch_sample = time.time()

        assert update_manifest_record(
            file_hash=sample_hash_1,
            original_file_path="/test/path/file1.csv",
            status="RAW_INGESTED",
            ingestion_timestamp_epoch=current_epoch_sample,
            pipeline_execution_id=exec_id_sample
        ), "Manifest記錄1 (RAW_INGESTED) 更新失敗"

        record1 = get_manifest_record(sample_hash_1)
        logger.info(f"Manifest 記錄1 (RAW_INGESTED): {record1}")
        assert record1 is not None and record1["status"] == "RAW_INGESTED"

        transformation_epoch_1_sample = time.time() + 5 # 模擬5秒後轉換
        assert update_manifest_record(
            file_hash=sample_hash_1,
            status="TRANSFORMATION_SUCCESS",
            fingerprint_hash="fp_ohlcv_daily_v1",
            transformation_timestamp_epoch=transformation_epoch_1_sample,
            target_table_name="fact_ohlcv_daily",
            processed_row_count=500,
            pipeline_execution_id=exec_id_sample
        ), "Manifest記錄1 (TRANSFORMATION_SUCCESS) 更新失敗"

        record1_updated = get_manifest_record(sample_hash_1)
        logger.info(f"更新後的 Manifest 記錄1: {record1_updated}")
        assert record1_updated is not None and record1_updated["status"] == "TRANSFORMATION_SUCCESS"
        assert record1_updated["processed_row_count"] == 500

        # 4. Processed Data 操作範例
        sample_df_ohlcv = pd.DataFrame({
            'trade_date': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'product_id': ['TXF', 'MXF'],
            'open_price': [14000.0, 14010.5],
            'close_price': [14050.0, 14000.0]
        })
        ohlcv_table_name = "fact_ohlcv_daily_test"

        assert load_dataframe_to_processed_db(sample_df_ohlcv, ohlcv_table_name, if_exists="replace"), \
            f"首次載入DataFrame到 {ohlcv_table_name} 失敗"

        conn_proc_sample = get_processed_data_connection()
        loaded_df_sample = conn_proc_sample.table(f'"{ohlcv_table_name}"').df() # 使用 table() 更安全
        assert len(loaded_df_sample) == 2, f"載入後 {ohlcv_table_name} 行數不符"
        logger.info(f"從 '{ohlcv_table_name}' 讀取的數據:\n{loaded_df_sample}")

        logger.info("db_manager.py 範例執行成功！")

    except Exception as e_main:
        logger.error(f"db_manager.py 範例執行過程中發生嚴重錯誤: {e_main}", exc_info=True)
    finally:
        close_all_connections()
        logger.info("db_manager.py 範例執行完畢（含 finally 清理）。")

[end of MyTaifexDataProject/src/taifex_pipeline/database/db_manager.py]
