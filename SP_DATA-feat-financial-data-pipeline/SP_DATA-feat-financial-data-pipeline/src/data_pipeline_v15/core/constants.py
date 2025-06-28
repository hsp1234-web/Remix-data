# -*- coding: utf-8 -*-

"""
核心常數模組

集中管理整個數據管線中使用的所有固定設定值，
例如資料夾名稱、特定檔案名稱等，以提高可維護性。
"""

# --- 檔案處理相關 ---

MANIFEST_FILE = "manifest.json"
"""用來記錄每個檔案處理狀態的 JSON 檔案名稱。"""

# 用於結果字典的鍵 (Keys for result dictionaries)
KEY_STATUS = "status"
KEY_MESSAGE = "message"
KEY_FILE = "file"
KEY_REASON = "reason"
KEY_TABLE = "table"
KEY_COUNT = "count"
KEY_PATH = "path"
KEY_RESULTS = "results"
KEY_DATAFRAME = "dataframe" # For passing DataFrame object
KEY_MATCHED_SCHEMA_NAME = "matched_schema_name" # For schema name used by FileParser

# 狀態值 (Status values)
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"
STATUS_GROUP_RESULT = "group_result"