import logging
import logging.handlers # <--- 加入此行
import sys
import uuid
from datetime import datetime

import pytz
from pythonjsonlogger import jsonlogger

# 全域變數，用於存儲 execution_id，確保單次執行中所有日誌使用相同 ID
_EXECUTION_ID = None

class TaipeiFormatter(logging.Formatter):
    """
    自訂 Formatter 類別，用於將日誌時間轉換為台北時區。
    """
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=pytz.utc)
        return dt.astimezone(pytz.timezone("Asia/Taipei")).strftime(datefmt if datefmt else "%Y-%m-%d %H:%M:%S.%f")[:-3] + " (Asia/Taipei)"

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    自訂 JSON Formatter，加入 execution_id 並確保時間戳為台北時區。
    """
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            # 自訂時間戳格式並轉換為台北時區
            now = datetime.fromtimestamp(record.created, tz=pytz.utc).astimezone(pytz.timezone("Asia/Taipei"))
            log_record['timestamp'] = now.isoformat()
        log_record['level'] = record.levelname
        log_record['module_name'] = record.module
        # 確保 execution_id 存在
        log_record['execution_id'] = _EXECUTION_ID if _EXECUTION_ID else "NOT_INITIALIZED"
        # 將 message 從 message_dict 移到 log_record 的頂層
        if 'message' in message_dict:
            log_record['message'] = message_dict['message']
            del message_dict['message'] # 避免重複


def setup_logger(
    logger_name="taifex_pipeline",
    log_level=logging.INFO,
    log_file_path="logs/pipeline.log",
    max_bytes=10*1024*1024, # 10 MB
    backup_count=5
):
    """
    設定並返回一個 logger 物件，實現雙軌制日誌記錄：
    - Console Handler: 易讀的格式，輸出到主控台。
    - File Handler (JSON): 結構化的 JSON 格式，寫入到指定檔案。

    JSON 記錄包含: timestamp, execution_id, level, module_name, message。
    所有時間戳均使用「台北時區」。

    Args:
        logger_name (str): Logger 的名稱。
        log_level (int): 日誌級別 (例如 logging.INFO, logging.DEBUG)。
        log_file_path (str): JSON 日誌檔案的路徑。
        max_bytes (int): 日誌檔案輪替前的最大大小 (bytes)。
        backup_count (int): 保留的備份日誌檔案數量。

    Returns:
        logging.Logger: 設定好的 logger 物件。
    """
    global _EXECUTION_ID
    if _EXECUTION_ID is None:
        _EXECUTION_ID = str(uuid.uuid4())

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.propagate = False # 防止日誌向 root logger 傳播，避免重複輸出

    # 移除已存在的 handlers，避免重複添加 (尤其在 notebook 環境中)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    # 1. Console Handler (易讀格式)
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = TaipeiFormatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s (%(filename)s:%(lineno)d)",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    # 2. File Handler (JSON 格式)
    # 確保日誌目錄存在
    import os
    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            # 無法建立目錄時，輸出錯誤到 console (因為 logger 可能還沒完全設定好)
            print(f"錯誤：無法建立日誌目錄 {log_dir}: {e}", file=sys.stderr)
            # 可以選擇在此處引發異常或返回未設定檔案 handler 的 logger
            # 此處我們選擇繼續，但檔案日誌可能不會工作

    if not log_dir or os.path.exists(log_dir): # 只有在目錄存在或成功建立時才加入 FileHandler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        # 自訂 JSON formatter
        # 欄位順序可以透過 format 字串指定，但 python-json-logger 主要透過 add_fields 控制
        # format='%(timestamp) %(execution_id) %(level) %(module_name) %(message)'
        json_formatter = CustomJsonFormatter(
             '%(timestamp)s %(execution_id)s %(level)s %(module_name)s %(message)s'
        )
        file_handler.setFormatter(json_formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)
    else:
        logger.warning(f"日誌目錄 {log_dir} 不存在且無法建立，檔案日誌功能將停用。")


    logger.info(f"Logger '{logger_name}' 設定完成。Execution ID: {_EXECUTION_ID}")
    logger.info(f"主控台日誌級別: {logging.getLevelName(log_level)}")
    if any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        logger.info(f"檔案日誌將寫入至: {os.path.abspath(log_file_path)}")
    else:
        logger.warning("檔案日誌處理器未成功設定。")

    return logger

if __name__ == '__main__':
    # 測試 logger
    # 第一次設定
    logger1 = setup_logger("my_app_logger", log_level=logging.DEBUG, log_file_path="logs/app_test.log")
    logger1.debug("這是來自 logger1 的 DEBUG 訊息。")
    logger1.info("這是來自 logger1 的 INFO 訊息。")
    logger1.warning("這是來自 logger1 的 WARNING 訊息。")
    logger1.error("這是來自 logger1 的 ERROR 訊息。", extra={"custom_field": "some_value"}) # 測試 extra
    logger1.critical("這是來自 logger1 的 CRITICAL 訊息。")

    # 模擬在不同模組中取得 logger (應該是同一個 execution_id)
    logger2 = logging.getLogger("my_app_logger") # 不再次呼叫 setup_logger
    logger2.info("這是來自 logger2 (與 logger1 相同實例) 的 INFO 訊息，確認 execution_id 一致。")

    # 測試不同名稱的 logger，應該有不同的 execution_id (如果 setup_logger 被呼叫)
    # 但通常建議在應用程式啟動時設定一次 logger
    # logger_another = setup_logger("another_logger", log_file_path="logs/another_test.log")
    # logger_another.info("這是來自 another_logger 的 INFO 訊息。")

    # 測試設定檔路徑不存在的情況 (手動建立 logs 資料夾後再執行，或檢查錯誤處理)
    # logger_no_dir = setup_logger("no_dir_logger", log_file_path="non_existent_dir/test.log")
    # logger_no_dir.info("此訊息可能只會出現在 console。")

    print(f"\n請檢查主控台輸出以及 'logs/app_test.log' 和 'logs/another_test.log' (如果啟用了)。")
    print(f"Logger1 execution ID: {logger1.handlers[0].formatter.converter if hasattr(logger1.handlers[0].formatter, 'converter') else 'N/A'}") # 這裡的取得方式不對
    # 正確取得 execution_id 的方式應該是檢查日誌內容，或者讓 setup_logger 返回它

    # 為了能從外部取得 execution_id，我們可以稍微修改 setup_logger 或提供一個 get_execution_id 函式
    # 或者，在 CustomJsonFormatter 中直接使用全域的 _EXECUTION_ID

    # 假設我們想驗證 _EXECUTION_ID
    print(f"全域 Execution ID: {_EXECUTION_ID}")
    # 驗證日誌內容
    # 可以手動開啟日誌檔案查看，或寫更複雜的測試來讀取日誌內容並驗證
    # 例如:
    # with open("logs/app_test.log", "r", encoding="utf-8") as f:
    #     for line in f:
    #         print(f"日誌行: {line.strip()}")
    #         # 這裡可以加入 json.loads(line) 並檢查欄位
    #         break # 只看第一行範例
    print("\n測試完畢。")
