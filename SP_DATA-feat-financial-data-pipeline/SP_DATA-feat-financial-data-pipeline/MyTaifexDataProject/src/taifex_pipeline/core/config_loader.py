# -*- coding: utf-8 -*-
"""
核心共用模組：設定檔讀取 (Config Loader)

本模組提供功能以讀取專案的設定檔，主要是 `format_catalog.json`。
`format_catalog.json` 包含了檔案格式指紋及其對應的處理配方。

主要功能：
- `get_format_catalog()`: 讀取並返回格式指紋目錄的內容。
  為了提升效能，此函式會快取首次讀取的結果。
- `clear_config_cache()`: 清除已快取的設定檔內容，以便下次讀取時能獲取最新版本。
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional

from .logger_setup import get_logger # 使用相對導入

logger = get_logger(__name__)

_config_cache: Optional[Dict[str, Any]] = None
"""模組級變數，用於快取已讀取的設定檔內容。"""

def get_format_catalog(
    config_file_name: str = "format_catalog.json",
    config_dir_name: str = "config"
) -> Dict[str, Any]:
    """
    讀取並返回格式指紋目錄 (`format_catalog.json`) 的內容。

    此函式會快取首次成功讀取的設定檔內容。後續的調用將直接從快取返回，
    以避免重複的檔案 I/O 操作。如果需要強制重新載入設定檔（例如，
    當設定檔在應用程式運行期間被外部修改時），可以先調用 `clear_config_cache()`
    來清除快取。

    Args:
        config_file_name (str): 設定檔的名稱。預設為 "format_catalog.json"。
        config_dir_name (str): 存放設定檔的目錄名稱，相對於專案根目錄。
                               預設為 "config"。

    Returns:
        Dict[str, Any]: 解析後的 JSON 物件 (以字典形式表示)。
                        字典的鍵是格式指紋 (字串)，值是該格式的處理配方 (字典)。

    Raises:
        FileNotFoundError: 如果指定的設定檔路徑不存在。
        json.JSONDecodeError: 如果設定檔內容不是有效的 JSON 格式。
        IOError: 如果在讀取檔案時發生其他 I/O 相關錯誤。
        Exception: 其他未預期的讀取或解析錯誤。
    """
    global _config_cache
    if _config_cache is not None:
        logger.debug("從快取返回 format_catalog 設定。")
        return _config_cache

    try:
        # 確定專案根目錄的路徑
        # 此檔案位於 src/taifex_pipeline/core/config_loader.py
        project_root = Path(__file__).resolve().parents[3]
        config_path = project_root / config_dir_name / config_file_name

        if not config_path.is_file(): # 更精確的檢查，確保是檔案
            logger.error(f"設定檔未找到或不是一個有效檔案: {config_path}")
            raise FileNotFoundError(f"設定檔未找到或不是一個有效檔案: {config_path}")

        logger.info(f"正在從 {config_path.relative_to(project_root)} 讀取 format_catalog 設定...")
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data: Dict[str, Any] = json.load(f)

        _config_cache = config_data
        logger.info(f"成功讀取並快取 format_catalog，共 {len(config_data)} 個格式定義。")
        return config_data

    except FileNotFoundError: # 由 raise FileNotFoundError 捕獲
        # logger 已在前面記錄，此處直接重新拋出
        raise
    except json.JSONDecodeError as e:
        # config_path 可能未在 FileNotFoundError 時定義，所以用 args 中的路徑
        logger.error(f"解析設定檔 '{config_dir_name}/{config_file_name}' 失敗: JSON 格式錯誤 - {e}", exc_info=True)
        raise
    except IOError as e: # 更通用的 IO 錯誤
        logger.error(f"讀取設定檔 '{config_dir_name}/{config_file_name}' 時發生 IO 錯誤: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"讀取設定檔 '{config_dir_name}/{config_file_name}' 時發生未預期錯誤: {e}", exc_info=True)
        # 實際應用中，根據錯誤的嚴重性，可能需要更細緻的錯誤處理，
        # 例如返回一個空的預設配置或讓應用程式終止。
        raise

def clear_config_cache() -> None:
    """
    清除已快取的 `format_catalog.json` 設定檔內容。

    下次調用 `get_format_catalog()` 時將會強制重新從檔案系統讀取設定檔。
    這在設定檔可能在運行時被外部修改的情況下非常有用。
    """
    global _config_cache
    _config_cache = None
    logger.info("format_catalog 設定快取已清除。")

# --- 範例使用 (通常在其他需要格式配方的模組中導入並使用 get_format_catalog) ---
if __name__ == "__main__":
    # from .logger_setup import setup_global_logger # 使用相對導入
    # import logging
    # setup_global_logger(log_level_console=logging.DEBUG) # 方便查看調試訊息

    logger.info("開始執行 config_loader.py 範例...")

    # 為了測試，我們先手動在 MyTaifexDataProject/config/ 目錄下創建一個假的 format_catalog.json
    # 實際運行時，此檔案應由使用者或格式註冊腳本 (scripts/register_format.py) 維護
    try:
        # 確定專案根目錄
        current_project_root = Path(__file__).resolve().parents[3]
        demo_config_dir = current_project_root / "config"
        demo_config_dir.mkdir(parents=True, exist_ok=True) # 確保 config 目錄存在
        demo_file_path = demo_config_dir / "format_catalog.json"

        # 創建一個範例設定檔 (如果它還不存在)
        if not demo_file_path.exists():
            sample_catalog = {
                "fingerprint_example_alpha": {
                    "description": "測試用格式範例 Alpha (來自 config_loader.py 範例)",
                    "target_table": "alpha_table",
                    "parser_config": {"sep": ",", "encoding": "utf-8", "skiprows": 1},
                    "cleaner_function": "cleaners.clean_alpha_data",
                    "required_columns": ["date", "value1", "value2"]
                }
            }
            with open(demo_file_path, 'w', encoding='utf-8') as f_demo:
                json.dump(sample_catalog, f_demo, indent=2, ensure_ascii=False)
            logger.info(f"已創建範例設定檔: {demo_file_path.relative_to(current_project_root)}")

        # 1. 首次讀取 (應從檔案讀取並快取)
        catalog1 = get_format_catalog()
        logger.info(f"\n首次讀取的 Format Catalog (共 {len(catalog1)} 個條目):")
        logger.info(json.dumps(catalog1, indent=2, ensure_ascii=False))

        # 2. 再次讀取 (應從快取讀取)
        logger.info("\n再次讀取 (應從快取):")
        catalog2 = get_format_catalog()
        if id(catalog1) == id(catalog2):
            logger.info("成功從快取讀取 (物件 ID 相同)。")
        else:
            logger.error("錯誤：未能從快取讀取 (物件 ID 不同)!")

        # 3. 清除快取
        clear_config_cache()

        # 4. 清除快取後再次讀取 (應重新從檔案讀取)
        logger.info("\n清除快取後重新讀取:")
        catalog3 = get_format_catalog()
        if id(catalog2) != id(catalog3): # 比較與上一個快取物件的ID
            logger.info("成功重新從檔案讀取 (物件 ID 不同於前一個快取)。")
        else:
            logger.error("錯誤：清除快取後未能重新從檔案讀取 (物件 ID 仍相同)!")
        logger.info(f"重新讀取的 Format Catalog (共 {len(catalog3)} 個條目):")
        logger.info(json.dumps(catalog3, indent=2, ensure_ascii=False))

        # 5. 測試讀取不存在的設定檔
        logger.info("\n嘗試讀取不存在的設定檔:")
        try:
            get_format_catalog(config_file_name="non_existent_catalog.json")
        except FileNotFoundError as e:
            logger.info(f"成功捕獲 FileNotFoundError: {e}")

        logger.info("\nconfig_loader.py 範例執行完畢。")

    except Exception as e:
        logger.error(f"config_loader.py 範例執行過程中發生錯誤: {e}", exc_info=True)
    finally:
        # 測試完畢後可選擇是否刪除範例檔案
        # if demo_file_path.exists():
        #     demo_file_path.unlink()
        #     logger.info(f"\n已刪除範例設定檔: {demo_file_path.relative_to(current_project_root)}")
        pass

[end of MyTaifexDataProject/src/taifex_pipeline/core/config_loader.py]
