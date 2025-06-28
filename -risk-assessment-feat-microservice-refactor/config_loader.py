# config_loader.py

import yaml
import os
import logging
from typing import Dict, Any

# 配置日誌記錄器
logger = logging.getLogger(__name__)

# 預期的頂層配置鍵，用於基本結構驗證
EXPECTED_TOP_LEVEL_KEYS = [
    "project_name",
    "version",
    "log_level",
    "timezone",
    "api_keys_env_vars",
    "data_fetching",
    "stress_index_v2",
    "prompt_engineering"
]

def load_project_config(config_path: str = "config/project_config.yaml") -> Dict[str, Any]:
    """
    載入、解析並驗證項目設定檔 (YAML格式)。
    同時將環境變數中定義的 API Key 填充到設定字典中。

    Args:
        config_path (str): 設定檔的路徑。
                           預設為 "config/project_config.yaml"。

    Returns:
        Dict[str, Any]: 包含所有項目設定的字典。

    Raises:
        FileNotFoundError: 如果設定檔未找到。
        yaml.YAMLError: 如果設定檔解析失敗。
        ValueError: 如果設定檔基本結構驗證失敗或 API Key 環境變數未設定。
    """
    logger.info(f"開始載入項目設定檔從: {config_path}")

    if not os.path.exists(config_path):
        err_msg = f"設定檔未找到: {config_path}"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info("設定檔成功解析。")
    except yaml.YAMLError as e:
        err_msg = f"解析設定檔 {config_path} 失敗: {e}"
        logger.error(err_msg, exc_info=True)
        raise

    # 基本結構驗證
    if not isinstance(config, dict):
        err_msg = f"設定檔 {config_path} 頂層應為字典結構。"
        logger.error(err_msg)
        raise ValueError(err_msg)

    missing_keys = [key for key in EXPECTED_TOP_LEVEL_KEYS if key not in config]
    if missing_keys:
        warn_msg = (f"警告：設定檔 {config_path} 缺少以下預期頂層鍵: {', '.join(missing_keys)}。"
                    f"某些功能可能無法正常工作。")
        logger.warning(warn_msg)
        # 根據需求，這裡可以選擇是拋出 ValueError 還是僅記錄警告
        # 目前選擇記錄警告，允許部分配置缺失

    # 填充 API Key
    logger.info("開始填充 API Key จาก環境變數...")
    api_keys_config = config.get("api_keys_env_vars", {})
    if not isinstance(api_keys_config, dict):
        logger.warning("'api_keys_env_vars' 配置不是有效的字典，跳過 API Key 填充。")
    else:
        # 創建一個新的字典來儲存實際的 API keys，避免直接修改原始 config 中的 env var 名稱
        loaded_api_keys = {}
        for key_name, env_var_name in api_keys_config.items():
            api_key_value = os.getenv(env_var_name)
            if api_key_value:
                loaded_api_keys[key_name] = api_key_value
                logger.info(f"成功從環境變數 '{env_var_name}' 載入 API Key '{key_name}'。")
            else:
                # 對於某些 API (例如 FRED 如果我們決定用金鑰版)，金鑰可能是必需的
                # 其他 API (例如某些付費 API) 如果未使用，金鑰缺失可能是正常的
                # 目前僅記錄警告，具體是否拋出錯誤可以在調用方根據需要判斷
                warn_msg = (f"警告：未在環境變數中找到名為 '{env_var_name}' 的 API Key (用於 '{key_name}')。"
                            f"如果此 API 是必需的，相關功能將無法使用。")
                logger.warning(warn_msg)
                loaded_api_keys[key_name] = None # 標記為 None

        # 將載入的 API Keys 添加到配置字典的一個特定鍵下，例如 'runtime_api_keys'
        config['runtime_api_keys'] = loaded_api_keys
        logger.info("API Key 填充完成（或嘗試完成）。")

    logger.info(f"項目設定檔 {config_path} 載入完畢。")
    return config

if __name__ == '__main__':
    # 基本的測試和演示
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 為了測試 API Key 填充，可以模擬設定一些環境變數
    # 為了測試 API Key 填充，可以模擬設定一些環境變數
    # 在實際運行測試前，您可能需要在您的環境中設定這些變數，或者在這裡取消註解並賦值來模擬
    # 例如:
    # os.environ['FRED_API_KEY'] = 'YOUR_ACTUAL_FRED_KEY_FOR_TESTING_ONLY'
    # os.environ['ALPHA_VANTAGE_API_KEY'] = 'YOUR_ACTUAL_AV_KEY_FOR_TESTING_ONLY'

    try:
        project_settings = load_project_config()
        logger.info("--- 成功載入的項目設定 (部分預覽) ---")

        print(f"Project Name: {project_settings.get('project_name')}")
        print(f"Version: {project_settings.get('version')}")
        print(f"Log Level: {project_settings.get('log_level')}")
        print(f"Timezone: {project_settings.get('timezone')}")

        print("\n--- API Key 環境變數名稱 (來自設定檔) ---")
        if project_settings.get("api_keys_env_vars"):
            for key_name, env_var in project_settings["api_keys_env_vars"].items():
                print(f"  '{key_name}': 會從環境變數 '{env_var}' 讀取")
        else:
            print("  未配置 'api_keys_env_vars'")

        print("\n--- 實際載入的 API Keys (runtime_api_keys) ---")
        if project_settings.get('runtime_api_keys'):
            for key_name, key_value in project_settings['runtime_api_keys'].items():
                display_value = '********' if key_value else 'Not Set / Not Found in Env'
                print(f"  '{key_name}': {display_value}")
        else:
            print("  'runtime_api_keys' 未生成或為空 (可能 'api_keys_env_vars' 未配置或所有環境變數均未設定)")

        print("\n--- 數據獲取設定 (部分) ---")
        if project_settings.get("data_fetching", {}).get("unified_api_fetcher"):
            print(f"  Unified API Fetcher Cache Dir: {project_settings['data_fetching']['unified_api_fetcher'].get('cache_dir')}")
            print(f"  Unified API Fetcher Cache Expire (hours): {project_settings['data_fetching']['unified_api_fetcher'].get('cache_expire_after_hours')}")
        else:
            print("  未配置 'data_fetching.unified_api_fetcher'")

        print("\n--- 壓力指數 V2 設定 (部分) ---")
        if project_settings.get("stress_index_v2"):
            print(f"  Rolling Window Days: {project_settings['stress_index_v2'].get('rolling_window_days')}")
            print(f"  PCA Weights Enabled: {project_settings['stress_index_v2'].get('pca_weights', {}).get('enabled')}")
        else:
            print("  未配置 'stress_index_v2'")

        # 驗證 EXPECTED_TOP_LEVEL_KEYS 是否都存在
        print("\n--- 頂層配置鍵驗證 ---")
        all_expected_keys_present = True
        for key in EXPECTED_TOP_LEVEL_KEYS:
            if key not in project_settings:
                logger.warning(f"預期頂層鍵 '{key}' 在設定檔中缺失。")
                all_expected_keys_present = False
        if all_expected_keys_present:
            logger.info("所有預期的頂層配置鍵均存在。")
        else:
            logger.warning("部分預期的頂層配置鍵缺失，請檢查日誌。")


    except FileNotFoundError:
        logger.error("測試失敗：請確保 config/project_config.yaml 文件存在。")
    except ValueError as ve:
        logger.error(f"測試失敗：配置驗證錯誤 - {ve}")
    except Exception as e:
        logger.error(f"測試過程中發生未預期錯誤: {e}", exc_info=True)

    # 清理模擬的環境變數 (如果設定了)
    # if 'YOUR_FRED_API_KEY_ENV_NAME' in os.environ: del os.environ['YOUR_FRED_API_KEY_ENV_NAME']
    # if 'YOUR_AV_API_KEY_ENV_NAME' in os.environ: del os.environ['YOUR_AV_API_KEY_ENV_NAME']
