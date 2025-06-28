import urllib.request
import sys
import os
import json
import datetime

# 強制無緩衝輸出
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print(f"[{datetime.datetime.now()}] Python script viability_logic.py starting...")
print(f"[{datetime.datetime.now()}] Python version: {sys.version}")
print(f"[{datetime.datetime.now()}] Current working directory: {os.getcwd()}")

# 測試標準庫功能 - urllib.request 和 json
TARGET_URL_JSONPLACEHOLDER = "https://jsonplaceholder.typicode.com/todos/1"
print(f"[{datetime.datetime.now()}] Attempting to fetch data from: {TARGET_URL_JSONPLACEHOLDER}")

try:
    with urllib.request.urlopen(TARGET_URL_JSONPLACEHOLDER, timeout=10) as response:
        status_code = response.getcode()
        print(f"[{datetime.datetime.now()}] HTTP Status Code: {status_code}")
        if status_code == 200:
            data = response.read()
            print(f"[{datetime.datetime.now()}] Raw data received (first 100 chars): {data[:100].decode('utf-8', 'ignore')}...")
            try:
                json_data = json.loads(data)
                print(f"[{datetime.datetime.now()}] JSON data parsed successfully. Title: {json_data.get('title')}")
            except json.JSONDecodeError as e:
                print(f"[{datetime.datetime.now()}] ERROR: Failed to parse JSON data. Error: {e}")
        else:
            print(f"[{datetime.datetime.now()}] WARNING: Received non-200 status code: {status_code}")
except urllib.error.URLError as e:
    print(f"[{datetime.datetime.now()}] ERROR: urllib.error.URLError occurred. Reason: {e.reason}")
except TimeoutError:
    print(f"[{datetime.datetime.now()}] ERROR: Request timed out after 10 seconds.")
except Exception as e:
    print(f"[{datetime.datetime.now()}] ERROR: An unexpected error occurred during HTTP request: {e}")
finally:
    print(f"[{datetime.datetime.now()}] HTTP request test finished.")

# 測試另一個 URL，檢查不同的錯誤處理或防火牆行為
TARGET_URL_NYFED = "https://www.newyorkfed.org/markets/desk-operations/ambs" # 這個網址先前在原型中返回非預期 HTML
print(f"[{datetime.datetime.now()}] Attempting to fetch data from: {TARGET_URL_NYFED}")
try:
    with urllib.request.urlopen(TARGET_URL_NYFED, timeout=10) as response:
        status_code = response.getcode()
        print(f"[{datetime.datetime.now()}] HTTP Status Code for NYFed: {status_code}")
        data_nyfed = response.read()
        content_type_nyfed = response.info().get_content_type()
        print(f"[{datetime.datetime.now()}] NYFed Raw data received (first 100 chars): {data_nyfed[:100].decode('utf-8', 'ignore')}...")
        print(f"[{datetime.datetime.now()}] NYFed Content-Type: {content_type_nyfed}")
        if "text/html" in content_type_nyfed:
            print(f"[{datetime.datetime.now()}] INFO: NYFed returned HTML content as expected from past observations.")
        elif "application/json" in content_type_nyfed:
             print(f"[{datetime.datetime.now()}] INFO: NYFed returned JSON, which is different from past observations.")
        else:
            print(f"[{datetime.datetime.now()}] INFO: NYFed returned Content-Type: {content_type_nyfed}")

except urllib.error.URLError as e:
    print(f"[{datetime.datetime.now()}] ERROR: urllib.error.URLError occurred for NYFed. Reason: {e.reason}")
except TimeoutError:
    print(f"[{datetime.datetime.now()}] ERROR: NYFed Request timed out after 10 seconds.")
except Exception as e:
    print(f"[{datetime.datetime.now()}] ERROR: An unexpected error occurred during NYFed HTTP request: {e}")
finally:
    print(f"[{datetime.datetime.now()}] NYFed HTTP request test finished.")

print(f"[{datetime.datetime.now()}] Python script viability_logic.py finished.")
