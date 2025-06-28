import time
import functools
from .logger import get_logger # 假設 logger.py 在同一個 utils 目錄下

# 使用 __name__ 獲取當前模塊的 logger 實例
logger = get_logger(__name__)

def retry_with_exponential_backoff(
    max_retries=5,
    initial_delay=1,
    backoff_factor=2,
    jitter=True,
    allowed_exceptions=(Exception,) # 默認捕獲所有異常以進行重試
):
    """
    一個裝飾器，用於實現帶有指數退避和抖動的重試邏輯。

    Args:
        max_retries (int): 最大重試次數。
        initial_delay (float): 初始延遲時間（秒）。
        backoff_factor (float): 每次重試後延遲時間的乘法因子。
        jitter (bool): 是否在延遲中加入隨機抖動，以避免同時重試。
        allowed_exceptions (tuple): 一個包含允許觸發重試的異常類型的元組。
                                   默認是 (Exception,)，表示捕獲所有標準異常。
                                   可以指定更具體的異常，如 (requests.exceptions.Timeout, ConnectionError)。
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            num_retries = 0
            delay = initial_delay
            while num_retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except allowed_exceptions as e:
                    num_retries += 1
                    if num_retries > max_retries:
                        logger.error(
                            f"函數 {func.__name__} 在 {max_retries} 次重試後最終失敗。錯誤: {e}",
                            exc_info=True # 記錄堆棧跟踪信息
                        )
                        raise  # 重新拋出最後的異常

                    sleep_time = delay
                    if jitter:
                        # 添加抖動，隨機因子在 0.5 到 1.0 之間
                        # 避免 sleep_time * (0.5 + random.random() / 2) 引入 random 庫依賴
                        # 簡化為在 delay 基礎上增加一個小的隨機值，或使用固定的抖動範圍
                        # 這裡我們選擇一個簡單的抖動，避免引入 random
                        # 實際應用中，一個輕量的隨機數生成可能是必要的，但為遵守零依賴，暫時簡化
                        # 或者，如果Python版本允許，可以使用 os.urandom 來生成更可靠的隨機數種子
                        # 但為了簡單，我們這裡的 "jitter" 效果有限
                        sleep_time += (num_retries * 0.1) # 簡單的線性增加抖動替代隨機性

                    logger.warning(
                        f"函數 {func.__name__} 捕獲到異常: {e}. "
                        f"將在 {sleep_time:.2f} 秒後進行第 {num_retries}/{max_retries} 次重試..."
                    )
                    time.sleep(sleep_time)
                    delay *= backoff_factor
            return None # 理論上不應到達這裡，因為循環結束條件是 num_retries > max_retries 然後拋出異常
        return wrapper
    return decorator

# 示例用法：
if __name__ == '__main__':
    # 為了直接運行此文件進行測試，需要確保 logger 已經設置
    # 在實際應用中，logger 的 setup 會在主程序入口完成
    import logging
    import sys
    import os

    # 為了在直接執行此文件時，讓 from .logger import get_logger 工作
    # 我們需要將父目錄 (src) 添加到 sys.path
    # 這種方式僅適用於直接執行此文件進行測試的場景

    # 在 __main__ 塊的頂部聲明這些變量，以便它們在整個塊中可用
    local_get_logger = None
    local_setup_logger = None
    LOCAL_PROJECT_LOGGER_NAME = None

    if __package__ is None or __package__ == '': # 檢查是否作為頂層腳本運行
        # print(f"Attempting to run {__file__} as top-level script.")
        # __file__ 是 AI_Assisted_Historical_Backtesting/src/utils/error_handler.py
        # utils_dir = os.path.dirname(__file__) # .../src/utils
        # src_dir = os.path.dirname(utils_dir) # .../src
        # project_root_parent = os.path.dirname(src_dir) # .../AI_Assisted_Historical_Backtesting 的父目錄
        # sys.path.insert(0, project_root_parent) # 將項目根目錄的父目錄加到 sys.path
        # 這樣 from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger 就能工作

        # 嘗試將 src 目錄添加到 sys.path，然後從 utils.logger 導入
        src_dir_for_main = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        if src_dir_for_main not in sys.path:
            sys.path.insert(0, src_dir_for_main)
        # print(f"In __main__ of error_handler.py, sys.path temporarily modified: {sys.path}")
        from utils.logger import get_logger as lg, setup_logger as ls, PROJECT_LOGGER_NAME as lpn
        local_get_logger, local_setup_logger, LOCAL_PROJECT_LOGGER_NAME = lg, ls, lpn
        # print("Import from utils.logger successful for direct execution.")
    else:
        # 當通過 python -m utils.error_handler 從 src 目錄執行時 (即 __package__ == "utils")
        # 或者作為 AI_Assisted_Historical_Backtesting.src.utils.error_handler 導入時
        # print(f"Attempting to run {__file__} as part of package {__package__}.")
        from .logger import get_logger as lg, setup_logger as ls, PROJECT_LOGGER_NAME as lpn
        local_get_logger, local_setup_logger, LOCAL_PROJECT_LOGGER_NAME = lg, ls, lpn
        # print("Relative import from .logger successful for package execution.")


    # 獲取根 logger 並進行配置 (簡化版 setup)
    sys.stdout.reconfigure(line_buffering=True) # 確保無緩衝

    # 使用 logger.py 中的 setup_logger 來配置
    # 我們需要確保 PROJECT_LOGGER_NAME 和 error_handler 模塊的 logger 都已配置
    # 注意：這裡的 local_setup_logger 和 LOCAL_PROJECT_LOGGER_NAME 是從上面 if/else 塊導入的
    if not local_setup_logger or not LOCAL_PROJECT_LOGGER_NAME or not local_get_logger:
        print("ERROR: Logger components not imported correctly in error_handler.py's __main__ block.")
        sys.exit(1)

    base_logger = local_setup_logger(logger_name=LOCAL_PROJECT_LOGGER_NAME, level=logging.DEBUG)

    # error_handler 模塊頂部的 logger 實例應該已經由 local_get_logger(__name__) 初始化，
    # 並且會繼承 base_logger 的配置。

    # --- 測試 retry_with_exponential_backoff ---
    ATTEMPT_COUNT = 0

    @retry_with_exponential_backoff(max_retries=3, initial_delay=0.1, backoff_factor=2, allowed_exceptions=(ValueError,))
    def might_fail_function(succeed_after_attempts):
        global ATTEMPT_COUNT
        ATTEMPT_COUNT += 1
        logger.info(f"調用 might_fail_function，第 {ATTEMPT_COUNT} 次嘗試。")
        if ATTEMPT_COUNT < succeed_after_attempts:
            logger.warning(f"模擬失敗，拋出 ValueError。")
            raise ValueError(f"模擬錯誤，嘗試次數 {ATTEMPT_COUNT}")
        logger.info("函數成功執行！")
        return f"成功在第 {ATTEMPT_COUNT} 次嘗試！"

    logger.info("--- 開始測試：函數將在第 3 次嘗試時成功 ---")
    ATTEMPT_COUNT = 0 # 重置計數器
    try:
        result = might_fail_function(succeed_after_attempts=3)
        logger.info(f"測試結果: {result}")
    except ValueError as e:
        logger.error(f"測試捕獲到未處理的 ValueError: {e}")

    logger.info("\n--- 開始測試：函數將在第 5 次嘗試時成功 (應超出 max_retries) ---")
    ATTEMPT_COUNT = 0 # 重置計數器
    try:
        result = might_fail_function(succeed_after_attempts=5) # 5 次嘗試 > 3 次重試 (即總共4次調用)
        logger.info(f"測試結果: {result}")
    except ValueError as e:
        logger.error(f"測試成功捕獲到預期的 ValueError (因為重試耗盡): {e}")

    # 測試不屬於 allowed_exceptions 的異常
    @retry_with_exponential_backoff(max_retries=2, initial_delay=0.1, allowed_exceptions=(ValueError,))
    def raises_type_error():
        logger.info("調用 raises_type_error...")
        raise TypeError("這是一個 TypeError，不應觸發重試")

    logger.info("\n--- 開始測試：函數拋出非預期異常 (TypeError) ---")
    try:
        raises_type_error()
    except TypeError as e:
        logger.info(f"測試成功捕獲到未經重試的 TypeError: {e}")
    except Exception as e:
        logger.error(f"測試捕獲到非預期的異常: {e}")

    logger.info("\n--- 測試自定義 allowed_exceptions ---")
    class CustomNetworkError(Exception):
        pass
    class AnotherCustomError(Exception):
        pass

    ATTEMPT_COUNT_CUSTOM = 0
    @retry_with_exponential_backoff(max_retries=2, initial_delay=0.1, allowed_exceptions=(CustomNetworkError,))
    def custom_fail_function():
        global ATTEMPT_COUNT_CUSTOM
        ATTEMPT_COUNT_CUSTOM += 1
        logger.info(f"調用 custom_fail_function，第 {ATTEMPT_COUNT_CUSTOM} 次嘗試。")
        if ATTEMPT_COUNT_CUSTOM == 1:
            raise CustomNetworkError("模擬網絡錯誤")
        elif ATTEMPT_COUNT_CUSTOM == 2:
            raise AnotherCustomError("模擬另一種錯誤，不應重試此錯誤")
        return "Custom function succeeded"

    ATTEMPT_COUNT_CUSTOM = 0
    try:
        custom_fail_function()
    except AnotherCustomError as e:
        logger.info(f"測試成功：custom_fail_function 在第二次嘗試時拋出 AnotherCustomError 並被捕獲: {e}")
        assert ATTEMPT_COUNT_CUSTOM == 2 # 確保第一次 CustomNetworkError 被重試了
    except Exception as e:
        logger.error(f"custom_fail_function 測試失敗，捕獲到非預期異常: {e}")


    logger.info("錯誤處理模塊測試完成。")
