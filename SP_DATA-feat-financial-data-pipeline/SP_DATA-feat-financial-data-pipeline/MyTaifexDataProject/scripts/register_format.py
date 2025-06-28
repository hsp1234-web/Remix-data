# -*- coding: utf-8 -*-
"""
格式註冊輔助腳本 (Format Registration Helper Script)

命令行工具，用於協助使用者為新的檔案格式生成指紋，
並在 `config/format_catalog.json` 中添加或更新其處理配方。
"""
import argparse
import json
import io
from pathlib import Path
import sys

# 為了讓此腳本可以從專案根目錄執行 `python scripts/register_format.py ...`
# 需要能夠導入 src 下的模組。
# 一種方法是將 src 目錄添加到 sys.path。
# 假設此腳本位於 MyTaifexDataProject/scripts/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from taifex_pipeline.transformation.format_detector import calculate_format_fingerprint
    from taifex_pipeline.core.config_loader import get_format_catalog, _config_cache, clear_config_cache # 使用內部變數以更新
    from taifex_pipeline.core.logger_setup import setup_global_logger, get_logger
except ImportError as e:
    print(f"錯誤：無法導入必要的 taifex_pipeline 模組。請確保您從專案根目錄執行此腳本，"
          f"或者 PYTHONPATH 已正確設定。詳細錯誤: {e}")
    sys.exit(1)

# 初始化一個簡易的日誌記錄器，因為此腳本是工具性質
# setup_global_logger(log_level_console=logging.INFO) # 如果需要詳細日誌
logger = get_logger(__name__) # 使用 setup_global_logger 預設的 INFO 等級

CONFIG_DIR_NAME = "config"
CATALOG_FILE_NAME = "format_catalog.json"
CONFIG_PATH = PROJECT_ROOT / CONFIG_DIR_NAME / CATALOG_FILE_NAME

def prompt_for_value(prompt_message: str, default_value: Optional[str] = None) -> str:
    """通用提示使用者輸入的函式，支持預設值。"""
    if default_value is not None:
        prompt_message += f" (預設: {default_value})"
    prompt_message += ": "

    while True:
        value = input(prompt_message).strip()
        if value:
            return value
        if default_value is not None:
            return default_value
        logger.warning("輸入不能為空，請重新輸入。")

def prompt_for_list(prompt_message: str, default_value: Optional[List[str]] = None) -> List[str]:
    """提示使用者輸入一個列表 (逗號分隔)。"""
    default_str = ", ".join(default_value) if default_value else ""
    raw_input = prompt_for_value(f"{prompt_message} (逗號分隔)", default_str)
    if not raw_input: # 如果用戶接受了空的預設值
        return []
    return [item.strip() for item in raw_input.split(',') if item.strip()]

def prompt_for_parser_config(default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """引導使用者輸入 parser_config 的詳細內容。"""
    logger.info("\n--- 設定 Parser Config ---")
    logger.info("請提供 pandas reader (如 read_csv) 所需的參數。範例：")
    logger.info("  分隔符 (sep): ',' (預設) 或 '\\t'")
    logger.info("  跳過行數 (skiprows): 0 (預設) 或 1")
    logger.info("  檔案編碼 (encoding): 'utf-8' (預設) 或 'ms950'")
    logger.info("  標頭行號 (header): 'infer' (預設讓pandas推斷) 或 0 (第一行是標頭) 或 None (無標頭，需提供names)")
    logger.info("  欄位名稱 (names): (如果 header=None, 例如 'col1,col2,col3')")
    logger.info("  ...")

    config: Dict[str, Any] = default_config if default_config else {}

    # 常用參數
    config['sep'] = prompt_for_value("分隔符 (sep)", config.get('sep', ','))

    skiprows_str = prompt_for_value("跳過行數 (skiprows)", str(config.get('skiprows', 0)))
    try:
        config['skiprows'] = int(skiprows_str)
    except ValueError:
        logger.warning(f"無效的 skiprows 值 '{skiprows_str}'，將使用預設值 0。")
        config['skiprows'] = 0

    config['encoding'] = prompt_for_value("檔案編碼 (encoding)", config.get('encoding', 'utf-8'))

    header_val = prompt_for_value("標頭行號 (header, 'infer' 或數字或 'None')", str(config.get('header', 'infer')))
    if header_val.lower() == 'none':
        config['header'] = None
        default_names = config.get('names', [])
        config['names'] = prompt_for_list("欄位名稱 (names, 逗號分隔, 僅當 header=None 時需要)", default_names)
    elif header_val.lower() == 'infer':
        config['header'] = 'infer'
        if 'names' in config: del config['names'] # infer 時不需要 names
    else:
        try:
            config['header'] = int(header_val)
            if 'names' in config: del config['names'] # 指定 header 行號時不需要 names
        except ValueError:
            logger.warning(f"無效的 header 值 '{header_val}'，將使用預設值 'infer'。")
            config['header'] = 'infer'

    logger.info("您可以繼續添加其他 pandas reader 參數 (例如 dtype, usecols)。")
    while True:
        add_more = prompt_for_value("是否要添加/修改其他 parser_config 參數? (y/n)", "n").lower()
        if add_more != 'y':
            break
        param_name = prompt_for_value("參數名稱 (例如 dtype)")
        param_value_str = prompt_for_value(f"參數 '{param_name}' 的值 (如果是JSON物件/列表，請使用JSON格式)")
        try:
            # 嘗試解析為 JSON，這樣可以輸入數字、布林、列表、字典
            param_value = json.loads(param_value_str)
        except json.JSONDecodeError:
            param_value = param_value_str # 如果不是有效的JSON，則視為字串
        config[param_name] = param_value
        logger.info(f"已設定 {param_name} = {config[param_name]}")

    logger.info(f"最終 Parser Config: {json.dumps(config, ensure_ascii=False, indent=2)}")
    return config

def main():
    parser = argparse.ArgumentParser(description="格式註冊與更新工具")
    parser.add_argument("sample_file_path", type=str, help="用於計算指紋的範例檔案路徑。")
    parser.add_argument("--force-update", action="store_true", help="如果格式指紋已存在，強制更新其配方而不提示。")

    args = parser.parse_args()
    sample_file = Path(args.sample_file_path)

    if not sample_file.is_file():
        logger.error(f"錯誤：提供的範例檔案路徑無效或不是一個檔案: {sample_file}")
        sys.exit(1)

    logger.info(f"正在為檔案 '{sample_file.name}' 計算格式指紋...")
    try:
        with open(sample_file, "rb") as f:
            file_content_stream = io.BytesIO(f.read())
    except IOError as e:
        logger.error(f"讀取範例檔案 '{sample_file.name}' 失敗: {e}", exc_info=True)
        sys.exit(1)

    fingerprint = calculate_format_fingerprint(file_content_stream, sample_file.name)

    if not fingerprint:
        logger.error(f"未能為檔案 '{sample_file.name}' 計算出有效的格式指紋。請檢查檔案內容和標頭。")
        sys.exit(1)

    logger.info(f"檔案 '{sample_file.name}' 的格式指紋為: {fingerprint}")

    # 讀取現有的 format_catalog.json
    # 使用 get_format_catalog 首次讀取，然後直接操作快取的 _config_cache
    # 確保目錄存在
    (PROJECT_ROOT / CONFIG_DIR_NAME).mkdir(exist_ok=True)

    # 清除快取以確保讀取最新檔案，或在 get_format_catalog 內部處理 FileNotFoundError
    clear_config_cache()
    try:
        catalog_data = get_format_catalog(config_file_name=CATALOG_FILE_NAME, config_dir_name=CONFIG_DIR_NAME)
    except FileNotFoundError:
        logger.info(f"設定檔 '{CONFIG_PATH}' 不存在，將創建一個新的。")
        catalog_data = {}
    except json.JSONDecodeError:
        logger.error(f"設定檔 '{CONFIG_PATH}' 內容損毀，無法解析。請檢查或備份後刪除該檔案重試。")
        sys.exit(1)


    existing_recipe = catalog_data.get(fingerprint)
    update_mode = False

    if existing_recipe:
        logger.info(f"\n--- 指紋 '{fingerprint}' 已存在於目錄中 ---")
        logger.info("現有配方:")
        logger.info(json.dumps(existing_recipe, indent=2, ensure_ascii=False))
        if not args.force_update:
            choice = prompt_for_value("是否要更新此配方? (y/n)", "n").lower()
            if choice != 'y':
                logger.info("操作取消，未修改任何配方。")
                sys.exit(0)
        update_mode = True
        logger.info("將更新現有配方...")
    else:
        logger.info(f"\n--- 為新指紋 '{fingerprint}' 創建配方 ---")
        existing_recipe = {} # 為新配方提供空的預設值容器

    # 引導使用者輸入配方資訊
    recipe: Dict[str, Any] = {}
    recipe['description'] = prompt_for_value("格式描述 (例如 '期交所每日行情CSV v1')", existing_recipe.get('description', f"Format for {sample_file.name}"))
    recipe['target_table'] = prompt_for_value("目標資料庫表格名稱", existing_recipe.get('target_table', 'your_target_table_name'))

    # Parser Config 的輸入較複雜
    recipe['parser_config'] = prompt_for_parser_config(existing_recipe.get('parser_config'))

    recipe['cleaner_function'] = prompt_for_value("對應的清洗函式名稱 (例如 'clean_ohlc_data_v1')", existing_recipe.get('cleaner_function', 'default_cleaner'))
    recipe['required_columns'] = prompt_for_list("必要欄位列表 (用於驗證)", existing_recipe.get('required_columns'))

    # 更新 catalog_data
    catalog_data[fingerprint] = recipe

    # 將更新後的 catalog_data 寫回檔案
    try:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f_out:
            json.dump(catalog_data, f_out, indent=2, ensure_ascii=False) # ensure_ascii=False 以正確顯示中文
        logger.info(f"\n成功將 {'更新後的' if update_mode else '新的'} 配方寫入到 '{CONFIG_PATH}'。")
        # 更新 config_loader 中的快取 (如果直接修改 _config_cache)
        # 或者，下次 get_format_catalog 時它會重新讀取（如果我們沒直接修改快取）
        # 由於我們讀取時用了 get_format_catalog，它會建立快取。
        # 如果我們想讓同一個腳本執行緒內的後續 get_format_catalog 拿到更新，需要更新快取或清除。
        # 此處 _config_cache 是全域的，get_format_catalog 返回的是它的副本還是引用取決於實現。
        # 在 config_loader.py 中，返回的是 _config_cache 本身。
        # 所以，理論上 catalog_data 就是 _config_cache。
        # 但為了保險和清晰，可以再次調用 clear_config_cache()。
        clear_config_cache()
        logger.debug("Config loader cache cleared after update.")

    except IOError as e:
        logger.error(f"將配方寫回到 '{CONFIG_PATH}' 失敗: {e}", exc_info=True)
        sys.exit(1)

    logger.info("格式註冊/更新操作完成。")

if __name__ == "__main__":
    # 為了讓此腳本在執行時，logger_setup 能找到 logs 目錄
    # 這裡假設 logs 目錄在 PROJECT_ROOT 下
    logs_dir_for_script = PROJECT_ROOT / "logs"
    logs_dir_for_script.mkdir(parents=True, exist_ok=True)

    # 使用一個更適合腳本的日誌級別
    setup_global_logger(log_level_console=logging.INFO, log_dir=logs_dir_for_script.relative_to(PROJECT_ROOT))
    main()
