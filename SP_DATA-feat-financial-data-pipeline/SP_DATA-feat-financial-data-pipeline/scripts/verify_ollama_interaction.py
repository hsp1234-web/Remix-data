# verify_ollama_interaction.py
# 此腳本用於驗證與本地運行的 Ollama 服務的互動，
# 透過向 /api/generate 端點發送請求來測試 Llama 3 模型的回應。

import requests
import json
import sys # 用於退出腳本
import os # 用於從環境變數讀取模型名稱

# --- 設定 ---
OLLAMA_API_URL = "http://localhost:11434/api/generate"
# 優先從環境變數 OLLAMA_MODEL_TO_TEST 讀取模型名稱，
# 如果未設定，則使用 deploy_ollama_llama3.sh 中的預設模型名稱。
DEFAULT_MODEL_NAME = "llama3:8b"
MODEL_TO_TEST = os.getenv("OLLAMA_MODEL_TO_TEST", DEFAULT_MODEL_NAME)

TEST_PROMPT = "Why is the sky blue?"
# 增加超時時間，因為模型首次加載和生成可能需要較長時間
REQUEST_TIMEOUT = 180  # 秒 (3 分鐘)

def main():
    """主執行函數"""
    print(f"開始驗證與 Ollama Llama 3 (模型: {MODEL_TO_TEST}) 的互動...")
    print(f"目標 API 端點: {OLLAMA_API_URL}")
    print(f"測試提示: \"{TEST_PROMPT}\"")

    payload = {
        "model": MODEL_TO_TEST,
        "prompt": TEST_PROMPT,
        "stream": False  # 獲取一次性完整回應
    }

    try:
        print(f"\n正在向 Ollama 發送請求 (超時設定: {REQUEST_TIMEOUT} 秒)...")
        # 使用 requests 套件發送 POST 請求
        response = requests.post(OLLAMA_API_URL, json=payload, timeout=REQUEST_TIMEOUT)

        # 檢查 HTTP 狀態碼
        response.raise_for_status()  # 如果狀態碼是 4xx 或 5xx，則拋出 HTTPError

        print("\n請求成功。正在解析回應...")
        response_data = response.json()

        print("\n--- Ollama API 完整 JSON 回應 ---")
        # 使用 ensure_ascii=False 以正確顯示可能的非 ASCII 字符 (例如中文)
        print(json.dumps(response_data, indent=2, ensure_ascii=False))

        if "response" in response_data:
            model_response_text = response_data["response"]
            print("\n--- Llama 3 模型的回應內容 ---")
            print(model_response_text)

            # 檢查回應是否為空或僅包含空白字符
            if model_response_text and model_response_text.strip():
                print("\n驗證成功！Llama 3 模型已透過 Ollama API 正確回應。")
            else:
                print("\n警告：Llama 3 模型返回了空的回應或僅包含空白字符。")
                print("這可能表示模型可以被調用，但對於此特定提示未生成有效內容。")
                # 這種情況不一定視為完全失敗，但需要注意。
        else:
            print("\n錯誤：Ollama API 的回應中未找到 'response' 欄位。")
            print("這可能表示模型未正確生成回應，或者 API 回應格式非預期。")
            sys.exit(1) # 將此視為錯誤並退出

    except requests.exceptions.ConnectionError as e:
        print(f"\n錯誤：無法連接到 Ollama 服務 ({OLLAMA_API_URL})。")
        print(f"請確保 Ollama 服務正在運行中，並且監聽在正確的地址和端口。")
        print(f"您可以使用 'ollama serve' 命令啟動服務，並透過 'curl http://localhost:11434' 檢查其狀態。")
        print(f"詳細錯誤: {e}")
        sys.exit(1)
    except requests.exceptions.Timeout as e:
        print(f"\n錯誤：請求 Ollama API 超時 (超過 {REQUEST_TIMEOUT} 秒)。")
        print(f"模型可能正在加載或推論時間過長。您可以嘗試在腳本中增加 REQUEST_TIMEOUT 值。")
        print(f"同時，請檢查 Ollama 服務的日誌 (例如 ollama_server.log) 是否有錯誤或進度指示。")
        print(f"詳細錯誤: {e}")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"\n錯誤：Ollama API 請求返回 HTTP 錯誤 {e.response.status_code}。")
        print(f"回應內容: {e.response.text}")
        if e.response.status_code == 404 and "model" in e.response.text.lower() and "not found" in e.response.text.lower():
            print(f"\n提示：HTTP 404 錯誤可能表示模型 '{MODEL_TO_TEST}' 未被正確拉取或名稱不正確。")
            print(f"請使用 'ollama list' 命令檢查已下載的模型，並使用 'ollama pull {MODEL_TO_TEST}' 拉取。")
        sys.exit(1)
    except requests.exceptions.JSONDecodeError as e:
        response_text_snippet = "無法獲取回應文本"
        if 'response' in locals() and hasattr(response, 'text'):
            response_text_snippet = response.text[:500] if response.text else "回應文本為空"

        print(f"\n錯誤：無法解析來自 Ollama API 的 JSON 回應。")
        print(f"可能是 API 未返回有效的 JSON，或者回應內容為空。")
        print(f"狀態碼: {response.status_code if 'response' in locals() and hasattr(response, 'status_code') else '未知'}")
        print(f"原始回應文本 (前500字符): {response_text_snippet}")
        print(f"詳細錯誤: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n執行 verify_ollama_interaction.py 時發生未預期的錯誤：{e}")
        print("詳細錯誤追蹤信息請見上方。")
        sys.exit(1)

if __name__ == "__main__":
    main()
