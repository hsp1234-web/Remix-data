import logging
import sys
import datetime

# 專案的根日誌記錄器名稱
PROJECT_LOGGER_NAME = "AI_Backtester"

def setup_logger(logger_name=PROJECT_LOGGER_NAME, level=logging.INFO, log_to_console=True):
    """
    設置和配置一個標準化的日誌記錄器。

    Args:
        logger_name (str): 日誌記錄器的名稱。
        level (int): 日誌記錄級別 (例如 logging.INFO, logging.DEBUG)。
        log_to_console (bool): 是否將日誌輸出到控制台。

    Returns:
        logging.Logger: 配置好的日誌記錄器實例。
    """
    logger = logging.getLogger(logger_name)

    # 防止重複添加處理器，如果日誌記錄器已經有處理器了
    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    # 格式化器
    # [時間戳] [日誌級別] [模組名稱] [函數名]: 訊息內容
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(module)s] [%(funcName)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    if log_to_console:
        # 控制台處理器
        ch = logging.StreamHandler(sys.stdout) # 強制輸出到 stdout
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # 強制無緩衝輸出 (Python 3.7+)
        # 對於 StreamHandler，其 stream 屬性就是 sys.stdout 或 sys.stderr
        # 我們在 viability_logic.py 中已經驗證了 sys.stdout.reconfigure 的有效性
        # logging 模塊內部可能會對 stream 進行自己的管理，
        # 但由於我們直接使用 sys.stdout，理論上外部的 reconfigure 應該持續有效。
        # 為了確保，我們可以在腳本最開始的地方全局設置。
        # 此處的 logger 更多是為了格式化和級別控制。

    # 如果未來需要日誌到文件，可以在此處添加 FileHandler
    # fh = logging.FileHandler(f"{logger_name}.log")
    # fh.setLevel(level)
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)

    return logger

def get_logger(module_name):
    """
    獲取一個以模組名為後綴的日誌記錄器實例。
    例如，如果 PROJECT_LOGGER_NAME 是 "AI_Backtester"，模組名是 "connectors.fred"，
    則記錄器名為 "AI_Backtester.connectors.fred"。

    Args:
        module_name (str): 調用日誌記錄器的模組的 __name__。

    Returns:
        logging.Logger: 日誌記錄器實例。
    """
    return logging.getLogger(f"{PROJECT_LOGGER_NAME}.{module_name}")

# 示例用法 (通常在每個模塊的開頭獲取 logger)
# from .logger import get_logger
# logger = get_logger(__name__)
# logger.info("這是一條來自模塊的 info 訊息。")
# logger.debug("這是一條 debug 訊息。")
# logger.error("這是一條 error 訊息。")

# 在應用程序的主入口點，可以調用一次 setup_logger() 來配置根記錄器
# setup_logger(level=logging.DEBUG) # 例如設置為 DEBUG 級別

if __name__ == '__main__':
    # 為了在直接執行此文件時，讓相對導入 (.logger) 工作，
    # 我們需要將父目錄 (src) 添加到 sys.path
    import os
    if os.path.basename(os.getcwd()) == "utils": # 如果是從 utils 目錄執行的
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        # 這種情況下，可以直接 from logger import ...
    elif "AI_Assisted_Historical_Backtesting" in os.getcwd(): # 如果是從項目根目錄或其子目錄執行
        # 假設項目根目錄是 AI_Assisted_Historical_Backtesting
        # 需要找到 src 目錄並添加到 path
        # 這部分邏輯可能需要更通用，或者假設執行時 PYTHONPATH 已正確設置
        # 為了簡單起見，假設直接執行此文件時，要麼 utils 是當前目錄，要麼 src 在 PYTHONPATH
        # 如果直接執行 python AI_Assisted_Historical_Backtesting/src/utils/logger.py
        # 則當前目錄是 AI_Assisted_Historical_Backtesting 的父目錄
        # 為了讓 from .logger (如果 error_handler.py 也這樣用) 或 from utils.logger (如果其他地方這樣用) 工作
        # 最好的方式是項目本身作為一個包被安裝或 PYTHONPATH 正確設置

        # 這裡的測試主要目的是驗證 logger.py 本身的功能，
        # 假設其作為包的一部分被導入時是正常的。
        # 直接執行的導入問題更多是執行環境配置問題。
        pass # 暫時不處理複雜的 sys.path 修改，依賴外部 PYTHONPATH 或執行方式

    # 這裡的配置是為了直接運行此文件時進行測試
    # 全局配置無緩衝輸出
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    # 基礎配置，應用於根日誌記錄器，這樣 getLogger 獲取到的子記錄器會繼承這個配置
    # 但我們使用自定義的 setup_logger 來進行更精細的控制

    print(f"[{datetime.datetime.now()}] Testing logger setup...")

    # 測試 setup_logger
    main_logger = setup_logger(level=logging.DEBUG)
    main_logger.info("主日誌記錄器 (AI_Backtester) 已設置 (INFO)。")
    main_logger.debug("主日誌記錄器 (AI_Backtester) 已設置 (DEBUG)。")

    # 模擬在其他模塊中使用 get_logger
    module_logger = get_logger("test_module")
    module_logger.info("來自 test_module 的 INFO 級別日誌。")
    module_logger.warning("來自 test_module 的 WARNING 級別日誌。")

    another_module_logger = get_logger("another.module")
    another_module_logger.error("來自 another.module 的 ERROR 級別日誌。")

    # 測試不同級別
    main_logger.setLevel(logging.WARNING)
    main_logger.info("這條 INFO 訊息不應該顯示，因為 logger 級別已設為 WARNING。")
    main_logger.warning("這條 WARNING 訊息應該顯示。")

    # 檢查子記錄器是否也受到影響 (通常子記錄器會繼承根記錄器的級別，除非單獨設置)
    # logging 模塊的行為是，如果子 logger 沒有顯式設置 level，它會傳遞消息給父 logger，
    # 直到找到一個設置了 level 的 logger 或者根 logger。處理器也是類似的繼承/傳播機制。
    # 我們的 get_logger 並沒有單獨為子 logger 設置 level，所以它會使用 PROJECT_LOGGER_NAME 的 level。
    module_logger.info("這條來自 test_module 的 INFO 也不應顯示。")
    module_logger.warning("這條來自 test_module 的 WARNING 應該顯示。")

    print(f"[{datetime.datetime.now()}] Logger setup test finished.")
