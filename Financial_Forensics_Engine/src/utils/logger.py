import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file_path_str, console_level_str="INFO", file_level_str="DEBUG"):
    """
    設定並返回一個 logger 物件。

    Args:
        name (str): Logger 的名稱。
        log_file_path_str (str): 日誌檔案的完整路徑。
        console_level_str (str): 控制台輸出的日誌級別 (字串形式, e.g., "INFO", "DEBUG").
        file_level_str (str): 檔案輸出的日誌級別 (字串形式, e.g., "INFO", "DEBUG").

    Returns:
        logging.Logger: 設定好的 Logger 物件。
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # 設定 Logger 的最低處理級別為 DEBUG

    # 避免重複添加 handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # 格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 控制台 Handler
    console_level = getattr(logging, console_level_str.upper(), logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 檔案 Handler
    # 確保日誌檔案目錄存在
    log_dir = os.path.dirname(log_file_path_str)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            # 在某些情況下 (例如 Colab 的虛擬路徑)，makedirs 可能會失敗
            # 但檔案仍可能可以創建，所以先打印一個警告
            print(f"警告：無法創建日誌目錄 {log_dir}：{e}。將嘗試直接創建日誌檔案。")


    file_level = getattr(logging, file_level_str.upper(), logging.DEBUG)
    # 使用 RotatingFileHandler 來避免日誌檔案無限增大
    # 保留最多5個檔案，每個檔案最大10MB
    fh = RotatingFileHandler(log_file_path_str, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    fh.setLevel(file_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

if __name__ == '__main__':
    # 測試 logger
    # 假設 project_root 在 logger.py 的上一層的上一層
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    log_file = os.path.join(project_root, "data_workspace", "logs", "test_engine.log")

    print(f"測試日誌將寫入到: {log_file}")

    logger_instance = setup_logger("TestLogger", log_file, console_level_str="DEBUG")

    logger_instance.debug("這是一條 DEBUG 訊息。")
    logger_instance.info("這是一條 INFO 訊息。")
    logger_instance.warning("這是一條 WARNING 訊息。")
    logger_instance.error("這是一條 ERROR 訊息。")
    logger_instance.critical("這是一條 CRITICAL 訊息。")

    print(f"請檢查 {log_file} 中的日誌內容以及控制台輸出。")
    print(f"預期：控制台應有 DEBUG 以上所有訊息，檔案中也應有 DEBUG 以上所有訊息。")

    # 測試從 project_config.yaml 中獲取配置的情況 (模擬)
    mock_project_config = {
        "logging": {
            "console_level": "INFO",
            "file_level": "WARNING",
            "log_file_path": "data_workspace/logs/app_test.log"
        }
    }

    app_log_file_path = os.path.join(project_root, mock_project_config["logging"]["log_file_path"])
    print(f"\n模擬應用日誌將寫入到: {app_log_file_path}")

    app_logger = setup_logger(
        "AppLogger",
        app_log_file_path,
        console_level_str=mock_project_config["logging"]["console_level"],
        file_level_str=mock_project_config["logging"]["file_level"]
    )
    app_logger.debug("應用 DEBUG (預期僅在檔案中，如果 file_level 是 DEBUG)")
    app_logger.info("應用 INFO (預期在控制台和檔案中)")
    app_logger.warning("應用 WARNING (預期在控制台和檔案中)")

    print(f"請檢查 {app_log_file_path} 中的日誌內容以及控制台輸出。")
    print(f"預期：控制台應有 INFO 和 WARNING，檔案中應有 WARNING。")

    # 測試日誌目錄不存在的情況
    non_existent_log_path = os.path.join(project_root, "data_workspace", "non_existent_dir", "test.log")
    print(f"\n測試不存在的日誌路徑: {non_existent_log_path}")
    try:
        ne_logger = setup_logger("NonExistentPathLogger", non_existent_log_path)
        ne_logger.info("測試寫入到不存在的路徑的日誌。")
        if os.path.exists(non_existent_log_path):
             print(f"成功創建並寫入日誌到: {non_existent_log_path}")
        else:
             print(f"警告: 未能成功寫入日誌到: {non_existent_log_path}")
    except Exception as e:
        print(f"寫入到不存在路徑的日誌時發生錯誤: {e}")

    # 測試 logger 是否重複添加 handler
    logger_instance_again = setup_logger("TestLogger", log_file, console_level_str="INFO")
    if len(logger_instance_again.handlers) == 2: # 假設一個 console 一個 file handler
        print("\nLogger Handler 數量正確 (沒有重複添加)。")
    else:
        print(f"\n警告: Logger Handler 數量異常: {len(logger_instance_again.handlers)}")
    logger_instance_again.info("再次測試 TestLogger。")

    print("\nLogger 測試完畢。")
