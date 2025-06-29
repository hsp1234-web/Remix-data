import yaml
import json
import os
from dotenv import load_dotenv

# 嘗試從 Colab Secrets 導入 userdata，如果失敗則忽略 (表示不在 Colab 環境)
try:
    from google.colab import userdata
    IS_COLAB = True
except ImportError:
    IS_COLAB = False

def load_config(config_path):
    """
    根據檔案擴展名加載單個 YAML 或 JSON 配置文件。

    Args:
        config_path (str): 配置檔案的路徑。

    Returns:
        dict: 加載後的配置內容。
        None: 如果檔案不存在或無法解析。
    """
    if not os.path.exists(config_path):
        print(f"警告：配置文件 {config_path} 不存在。")
        return None

    _, file_extension = os.path.splitext(config_path)

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            if file_extension == '.yaml' or file_extension == '.yml':
                return yaml.safe_load(f)
            elif file_extension == '.json':
                return json.load(f)
            else:
                print(f"警告：不支援的配置文件擴展名 {file_extension} (檔案: {config_path})。")
                return None
    except Exception as e:
        print(f"錯誤：加載配置文件 {config_path} 失敗：{e}")
        return None

def load_all_configs(config_dir):
    """
    加載指定目錄下所有 .yaml, .yml, .json 配置文件。
    並處理 API 金鑰的加載。

    Args:
        config_dir (str): 包含配置檔案的目錄路徑。

    Returns:
        dict: 一個字典，其中鍵是配置檔案的名稱 (不含擴展名)，值是其內容。
              例如 {"project_config": {...}, "data_sources": {...}}
    """
    configs = {}
    if not os.path.isdir(config_dir):
        print(f"錯誤：配置目錄 {config_dir} 不存在或不是一個目錄。")
        return configs

    for filename in os.listdir(config_dir):
        file_path = os.path.join(config_dir, filename)
        if os.path.isfile(file_path):
            config_name, file_extension = os.path.splitext(filename)
            if file_extension in ['.yaml', '.yml', '.json']:
                content = load_config(file_path)
                if content is not None:
                    configs[config_name] = content

    # 處理 API 金鑰加載
    # 首先加載 .env 檔案 (如果存在)，這主要用於本地開發
    load_dotenv() # 如果沒有 .env 檔案，此函數不會報錯

    if 'project_config' in configs and 'api_keys' in configs['project_config']:
        api_key_map = configs['project_config']['api_keys']
        loaded_api_keys = {}
        for key_alias, env_var_name in api_key_map.items():
            key_value = None
            if IS_COLAB:
                try:
                    key_value = userdata.get(env_var_name)
                    if key_value:
                         print(f"成功從 Colab Secrets 加載 API 金鑰：{key_alias} (對應變數 {env_var_name})")
                except Exception as e:
                    print(f"警告：嘗試從 Colab Secrets 加載 {env_var_name} 失敗：{e}。將嘗試從環境變數獲取。")

            if not key_value: # 如果不是 Colab 環境，或者 Colab Secrets 中沒有
                key_value = os.getenv(env_var_name)
                if key_value:
                    print(f"成功從環境變數加載 API 金鑰：{key_alias} (對應變數 {env_var_name})")

            if key_value:
                # 將加載到的金鑰值直接存儲在配置的 api_keys 映射中
                configs['project_config']['api_keys'][key_alias] = key_value
                print(f"成功將 API 金鑰 '{key_alias}' 的值存入配置。")
                # 同時也將其設置為環境變數，方便後續直接使用 os.getenv() 或向下兼容
                os.environ[env_var_name] = key_value
            else:
                # 如果未能加載到金鑰，將配置中的對應值設為 None，並打印警告
                configs['project_config']['api_keys'][key_alias] = None
                print(f"警告：未能加載 API 金鑰 '{key_alias}' (環境變數名: {env_var_name})。其在配置中的值將是 None。請確保已在 Colab Secrets 或 .env/.bashrc 中設定。")

        # 由於金鑰值已直接存入 api_keys 映射中，不再需要 loaded_api_key_aliases
        # 如果需要檢查哪些金鑰已加載，可以遍歷 api_keys 映射並檢查值是否為 None
        if 'loaded_api_key_aliases' in configs['project_config']:
            del configs['project_config']['loaded_api_key_aliases']

    return configs

if __name__ == '__main__':
    # 為了測試，假設我們在 Financial_Forensics_Engine 的根目錄下執行
    # 或者 config_loader.py 在 src/utils/ 下
    # current_dir = os.getcwd() # 如果從根目錄執行
    # script_dir = os.path.dirname(os.path.abspath(__file__)) # src/utils
    # project_root = os.path.dirname(os.path.dirname(script_dir)) # Financial_Forensics_Engine

    # 假設執行此腳本時，我們位於 Financial_Forensics_Engine 目錄中
    # 為了讓這個 if __name__ == '__main__': 塊能獨立運行進行基本測試，
    # 我們需要能夠找到 config 目錄。
    # 這裡我們假設 config 目錄與 src 在同一級別，即在專案根目錄下。

    # 獲取此腳本 (config_loader.py) 所在的目錄 (src/utils)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # 從 src/utils 推斷出專案根目錄 (Financial_Forensics_Engine)
    project_root_dir = os.path.dirname(os.path.dirname(current_script_dir))
    # 構造 config 目錄的路徑
    test_config_dir = os.path.join(project_root_dir, "config")

    print(f"測試: 專案根目錄推斷為: {project_root_dir}")
    print(f"測試: 將從以下目錄加載配置檔案: {test_config_dir}")

    if not os.path.isdir(test_config_dir):
        print(f"錯誤：測試用的配置目錄 {test_config_dir} 不存在。請確保在專案根目錄下有 config 資料夾並且裡面有測試用的設定檔。")
        print("將創建臨時的測試配置文件...")
        os.makedirs(test_config_dir, exist_ok=True)

        temp_project_config_content = {
            "project_name": "Test Engine",
            "version": "0.0.1",
            "api_keys": {
                "test_key_1": "MY_TEST_API_KEY_1",
                "test_key_2": "MY_TEST_API_KEY_2"
            }
        }
        with open(os.path.join(test_config_dir, "project_config.yaml"), "w", encoding="utf-8") as f_yaml:
            yaml.dump(temp_project_config_content, f_yaml)

        temp_data_sources_content = {"sources": [{"name": "Test API", "url": "http://example.com/api"}]}
        with open(os.path.join(test_config_dir, "data_sources.json"), "w", encoding="utf-8") as f_json:
            json.dump(temp_data_sources_content, f_json)

        print("臨時測試配置文件已創建。")
        # 為了測試 API 金鑰加載，我們需要模擬設置環境變數
        os.environ["MY_TEST_API_KEY_1"] = "dummy_api_key_value_12345"
        print("模擬設置環境變數 MY_TEST_API_KEY_1 = 'dummy_api_key_value_12345'")
        # MY_TEST_API_KEY_2 將保持未設置狀態以測試警告

    all_loaded_configs = load_all_configs(test_config_dir)

    if all_loaded_configs:
        print("\n成功加載所有配置:")
        for name, content in all_loaded_configs.items():
            print(f"\n--- {name} ---")
            # 為了簡潔，只打印一部分內容
            if isinstance(content, dict):
                for k, v in list(content.items())[:3]: # 最多打印3個頂層鍵值對
                    print(f"  {k}: {str(v)[:100]}{'...' if len(str(v)) > 100 else ''}")
            else:
                print(str(content)[:200])

        # 檢查 API 金鑰加載情況
        if 'project_config' in all_loaded_configs and 'api_keys' in all_loaded_configs['project_config']:
            print("\nAPI 金鑰加載狀態 (直接從配置中讀取):")
            for key_alias, key_value in all_loaded_configs['project_config']['api_keys'].items():
                if key_value:
                    # 為了安全，不直接打印金鑰值，只顯示是否已加載
                    print(f"  - {key_alias}: 已加載 (值已存入配置)")
                    # 驗證是否也設定到環境變數 (根據我們模擬的 test_key_1)
                    if key_alias == "test_key_1":
                         # 測試時，project_config.yaml 中的 test_key_1 對應的環境變數名是 MY_TEST_API_KEY_1
                         # 我們需要從原始的 api_key_map (即修改前的 project_config.yaml 結構) 或模擬的環境變數名來驗證
                         # 在這個測試例子中，我們模擬了 MY_TEST_API_KEY_1
                         env_var_for_test_key_1 = "MY_TEST_API_KEY_1" # 這是我們在測試中 os.environ 設定的
                         retrieved_from_env = os.getenv(env_var_for_test_key_1)
                         print(f"    └─ 對應環境變數 '{env_var_for_test_key_1}' 的值: {retrieved_from_env}")
                else:
                    print(f"  - {key_alias}: 未加載 (配置中的值為 None)")


        # 清理臨時創建的檔案 (如果適用)
        if "temp_project_config_content" in locals(): # 檢查變數是否存在於局部作用域
             print("\n清理臨時測試配置文件...")
             try:
                os.remove(os.path.join(test_config_dir, "project_config.yaml"))
                os.remove(os.path.join(test_config_dir, "data_sources.json"))
                # 如果 test_config_dir 是臨時創建的且為空，則可以刪除
                if not os.listdir(test_config_dir):
                    os.rmdir(test_config_dir)
                print("臨時文件已清理。")
             except OSError as e:
                print(f"清理臨時文件時出錯: {e}")

    else:
        print("未能加載任何配置。請檢查錯誤訊息。")

    print("\nConfig Loader 測試完畢。")
