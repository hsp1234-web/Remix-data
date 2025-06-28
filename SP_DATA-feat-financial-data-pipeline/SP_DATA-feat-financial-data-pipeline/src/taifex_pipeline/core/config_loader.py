import json
import logging
import os
from typing import Dict, Any

# 取得由 logger_setup 設定的 logger
# 這裡假設 logger 已經在應用程式的某個早期點被初始化
# 如果沒有，它會使用 logging 的預設設定
logger = logging.getLogger("taifex_pipeline.core.config_loader")

# 模組級快取變數
_cached_format_catalog: Dict[str, Any] | None = None # 仍然可以是 None (未快取時)

# 設定檔的相對路徑
# __file__ 是目前檔案 (config_loader.py) 的路徑
# os.path.abspath(__file__) 取得絕對路徑
# os.path.dirname(...) 取得目錄
# 我們需要從 src/taifex_pipeline/core 往上兩層到專案根目錄，然後到 config/
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_CATALOG_PATH = os.path.join(_PROJECT_ROOT, "config", "format_catalog.json")

def load_format_catalog(catalog_path: str = DEFAULT_CATALOG_PATH) -> Dict[str, Any]:
    """
    讀取並解析位於指定路徑的 format_catalog.json 檔案。
    包含錯誤處理機制 (FileNotFoundError, JSONDecodeError) 和記憶體快取功能。
    檔案不存在或解析錯誤時會拋出相應異常。

    Args:
        catalog_path (str): format_catalog.json 檔案的路徑。
                            預設為專案根目錄下的 'config/format_catalog.json'。

    Returns:
        Dict[str, Any]: 解析後的 JSON 內容 (字典)。
    """
    global _cached_format_catalog

    # 檢查快取
    if _cached_format_catalog is not None:
        logger.debug(f"從快取返回 format_catalog (路徑: {catalog_path})。")
        return _cached_format_catalog

    logger.info(f"嘗試從路徑讀取 format_catalog: {catalog_path}")
    try:
        # 確認檔案是否存在
        if not os.path.exists(catalog_path):
            logger.error(f"設定檔 '{catalog_path}' 不存在。")
            raise FileNotFoundError(f"設定檔 '{catalog_path}' 不存在。") # 直接拋出

        with open(catalog_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        _cached_format_catalog = data
        logger.info(f"成功讀取並快取 format_catalog (來自: {catalog_path})。")
        return data
    except FileNotFoundError: # 由 os.path.exists 檢查後拋出
        logger.warning(f"load_format_catalog 捕獲到 FileNotFoundError: {catalog_path}")
        raise # 重新拋出，讓呼叫者處理
    except json.JSONDecodeError as e:
        logger.error(f"解析 JSON 設定檔 '{catalog_path}' 時發生錯誤: {e}")
        # 考慮是否也應該拋出此錯誤，或者返回 None/空字典
        # 為了與 FileNotFoundError 的行為一致，且讓呼叫者能明確知道錯誤，選擇拋出
        raise # 或者 raise CustomConfigError("JSON 解析失敗") from e
    except Exception as e:
        logger.error(f"讀取設定檔 '{catalog_path}' 時發生未預期的錯誤: {e}")
        # 同樣，考慮拋出
        raise # 或者 raise CustomConfigError("未知設定檔讀取錯誤") from e

def clear_config_cache() -> None:
    """
    清除 format_catalog 的記憶體快取。
    主要用於測試或需要重新載入設定的場景。
    """
    global _cached_format_catalog
    _cached_format_catalog = None
    logger.info("format_catalog 的記憶體快取已清除。")


if __name__ == '__main__':
    # 為了測試這個模組，我們需要一個 logger。
    # 我們可以臨時設定一個，或者依賴於 logger_setup.py 中的設定。
    # 這裡我們假設 logger_setup.py 中的 logger 已經被某處呼叫並設定。
    # 如果直接執行此檔案，可能需要手動設定 logger。

    # 臨時設定基礎 logger 以便執行 __main__ 中的測試
    if not logging.getLogger("taifex_pipeline").hasHandlers():
        print("警告: taifex_pipeline logger 尚未設定。為此測試設定一個基礎 console logger。")
        # 你可以在這裡呼叫 logger_setup.setup_logger()，或者一個簡化的版本
        # from logger_setup import setup_logger # 假設在同一個 core 目錄下
        # test_logger = setup_logger("config_loader_test", log_level=logging.DEBUG, log_file_path="logs/config_loader_test.log")
        # logger = test_logger # 讓此模組的 logger 指向測試 logger
        # 或者更簡單的：
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger = logging.getLogger("taifex_pipeline.core.config_loader_test")


    print(f"預設設定檔路徑: {DEFAULT_CATALOG_PATH}")

    # 測試前，確保 config/format_catalog.json 存在且內容有效或為空 {}
    # 建立一個假的 config/format_catalog.json 用於測試
    test_config_dir = os.path.join(_PROJECT_ROOT, "config")
    test_config_file = DEFAULT_CATALOG_PATH

    if not os.path.exists(test_config_dir):
        os.makedirs(test_config_dir)

    # 測試案例 1: 正常的 JSON 檔案
    print("\n--- 測試案例 1: 正常讀取 ---")
    with open(test_config_file, 'w', encoding='utf-8') as f:
        json.dump({"key1": "value1", "numbers": [1, 2, 3]}, f, indent=2)

    config_data = load_format_catalog()
    if config_data:
        print(f"成功讀取設定: {config_data}")
    else:
        print("讀取設定失敗。")

    # 測試案例 2: 使用快取
    print("\n--- 測試案例 2: 讀取快取 ---")
    config_data_cached = load_format_catalog()
    if config_data_cached:
        print(f"從快取成功讀取設定: {config_data_cached}")
        assert config_data is config_data_cached, "快取應返回相同物件"
    else:
        print("讀取快取設定失敗。")

    clear_config_cache()
    print("快取已清除。")

    # 測試案例 3: 檔案不存在
    print("\n--- 測試案例 3: 檔案不存在 ---")
    non_existent_file = os.path.join(test_config_dir, "non_existent.json")
    config_data_non_existent = load_format_catalog(non_existent_file)
    if config_data_non_existent is None:
        print(f"檔案 '{non_existent_file}' 不存在，函式正確返回 None。")
    else:
        print(f"錯誤：對於不存在的檔案，函式未返回 None。得到: {config_data_non_existent}")

    # 測試案例 4: JSON 格式錯誤
    print("\n--- 測試案例 4: JSON 格式錯誤 ---")
    invalid_json_file = os.path.join(test_config_dir, "invalid.json")
    with open(invalid_json_file, 'w', encoding='utf-8') as f:
        f.write("{'key1': 'value1',,}") # 錯誤的 JSON 格式

    config_data_invalid = load_format_catalog(invalid_json_file)
    if config_data_invalid is None:
        print(f"檔案 '{invalid_json_file}' JSON 格式錯誤，函式正確返回 None。")
    else:
        print(f"錯誤：對於格式錯誤的 JSON，函式未返回 None。得到: {config_data_invalid}")

    # 清理測試建立的檔案
    if os.path.exists(test_config_file):
        # 保留 format_catalog.json 但使其為空，符合初始狀態
        with open(test_config_file, 'w', encoding='utf-8') as f:
            json.dump({}, f)
        print(f"\n已將 '{test_config_file}' 重設為空 JSON 物件。")
    if os.path.exists(invalid_json_file):
        os.remove(invalid_json_file)
        print(f"已刪除測試檔案 '{invalid_json_file}'。")

    print("\n設定檔讀取器測試完畢。")
