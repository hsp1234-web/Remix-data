from enum import Enum

class FileStatus(Enum):
    """表示 file_manifest 中檔案處理狀態的列舉。"""
    RAW_DISCOVERED = "RAW_DISCOVERED"          # 檔案被探勘器發現
    RAW_INGESTED = "RAW_INGESTED"            # 原始檔案已儲存到 raw_files 表
    RAW_INGESTION_FAILED = "RAW_INGESTION_FAILED" # 原始檔案儲存失敗

    TRANSFORMATION_PENDING = "TRANSFORMATION_PENDING" # 等待轉換處理
    TRANSFORMING = "TRANSFORMING"              # 正在轉換中
    TRANSFORMED_SUCCESS = "TRANSFORMED_SUCCESS"    # 轉換成功
    TRANSFORMATION_FAILED = "TRANSFORMATION_FAILED"  # 轉換失敗

    LOAD_PENDING = "LOAD_PENDING"              # (如果還有載入到最終目標表的階段) 等待載入
    LOADING = "LOADING"                        # 正在載入
    LOAD_SUCCESS = "LOAD_SUCCESS"              # 載入成功
    LOAD_FAILED = "LOAD_FAILED"                # 載入失敗

    ARCHIVED = "ARCHIVED"                      # 檔案已歸檔 (例如不再活躍處理)
    SKIPPED = "SKIPPED"                        # 檔案被明確跳過處理

    def __str__(self):
        return self.value

# file_manifest 表的欄位名稱常數
# 使用這些常數可以避免在程式碼中硬編碼字串，方便未來修改和維護
COLUMN_FILE_HASH = "file_hash"
COLUMN_ORIGINAL_PATH = "original_path"
COLUMN_STATUS = "status"
COLUMN_DISCOVERY_TIMESTAMP = "discovery_timestamp"
COLUMN_INGESTION_TIMESTAMP = "ingestion_timestamp"
COLUMN_TRANSFORMATION_START_TIMESTAMP = "transformation_start_timestamp"
COLUMN_TRANSFORMATION_END_TIMESTAMP = "transformation_end_timestamp"
COLUMN_ERROR_MESSAGE = "error_message"
COLUMN_SOURCE_SYSTEM = "source_system"
COLUMN_FILE_SIZE_BYTES = "file_size_bytes"
COLUMN_LAST_MODIFIED_AT_SOURCE = "last_modified_at_source"
COLUMN_NOTES = "notes"
# 如果未來有更多欄位，可以在此處添加

# 表名稱常數
TABLE_RAW_FILES = "raw_files"
TABLE_FILE_MANIFEST = "file_manifest"
