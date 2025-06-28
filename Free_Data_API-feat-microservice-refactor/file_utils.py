import json
import hashlib
import os

MANIFEST_FILENAME = "processed_files_manifest.json"
OUTPUT_DIR = "OUT_Processed_Prompts"
MANIFEST_FILEPATH = os.path.join(OUTPUT_DIR, MANIFEST_FILENAME)

def load_manifest() -> dict:
    """
    載入處理過的檔案清單。
    如果清單檔案不存在，則回傳一個空字典。
    """
    if not os.path.exists(MANIFEST_FILEPATH):
        return {}
    try:
        with open(MANIFEST_FILEPATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        # 如果檔案內容不是有效的 JSON，回傳空字典並在後續操作中覆寫它
        print(f"警告：{MANIFEST_FILEPATH} 格式錯誤，將建立新的清單。")
        return {}

def save_manifest(manifest_data: dict) -> None:
    """
    將處理過的檔案清單儲存到 JSON 檔案。
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True) # 確保輸出資料夾存在
    with open(MANIFEST_FILEPATH, 'w', encoding='utf-8') as f:
        json.dump(manifest_data, f, indent=4, ensure_ascii=False)

def update_manifest(filepath: str, file_hash: str, manifest_data: dict) -> None:
    """
    更新清單中指定檔案的雜湊值。
    """
    manifest_data[filepath] = file_hash

def get_file_hash_from_manifest(filepath: str, manifest_data: dict) -> str | None:
    """
    從清單中取得指定檔案的已儲存雜湊值。
    """
    return manifest_data.get(filepath)

def calculate_sha256_hash(file_content: bytes) -> str:
    """
    計算檔案內容的 SHA-256 雜湊值。
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(file_content)
    return sha256_hash.hexdigest()

def file_should_be_processed(filepath: str, current_file_hash: str, manifest_data: dict) -> bool:
    """
    根據清單中的雜湊值判斷檔案是否需要處理。
    如果檔案是新的或內容已變更，則回傳 True。
    """
    stored_hash = get_file_hash_from_manifest(filepath, manifest_data)
    if stored_hash is None or stored_hash != current_file_hash:
        return True
    return False

if __name__ == '__main__':
    # 簡單測試
    print(f"清單檔案路徑: {MANIFEST_FILEPATH}")

    # 建立一個模擬的輸入檔案
    test_input_dir = "IN_Source_Reports"
    os.makedirs(test_input_dir, exist_ok=True)
    test_filepath = os.path.join(test_input_dir, "測試報告_2023年第01週_內容.txt")
    
    # 第一次寫入檔案
    with open(test_filepath, "w", encoding="utf-8") as f:
        f.write("這是第一版測試內容。")

    with open(test_filepath, "rb") as f:
        content_v1 = f.read()
    
    hash_v1 = calculate_sha256_hash(content_v1)
    
    manifest = load_manifest()
    print(f"初始清單: {manifest}")

    if file_should_be_processed(test_filepath, hash_v1, manifest):
        print(f"檔案 {test_filepath} 需要處理 (雜湊值: {hash_v1})。")
        update_manifest(test_filepath, hash_v1, manifest)
        save_manifest(manifest)
        print(f"更新後清單: {manifest}")
    else:
        print(f"檔案 {test_filepath} 無需處理。")

    # 第二次，檔案內容不變
    manifest = load_manifest() # 重新載入
    if file_should_be_processed(test_filepath, hash_v1, manifest):
        print(f"檔案 {test_filepath} 需要處理 (雜湊值: {hash_v1})。")
    else:
        print(f"檔案 {test_filepath} 無需處理 (雜湊值: {hash_v1}，與清單一致)。")

    # 第三次，檔案內容改變
    with open(test_filepath, "w", encoding="utf-8") as f:
        f.write("這是修改後的測試內容。")
    
    with open(test_filepath, "rb") as f:
        content_v2 = f.read()
    
    hash_v2 = calculate_sha256_hash(content_v2)

    manifest = load_manifest() # 重新載入
    if file_should_be_processed(test_filepath, hash_v2, manifest):
        print(f"檔案 {test_filepath} 需要處理 (雜湊值: {hash_v2})。")
        update_manifest(test_filepath, hash_v2, manifest)
        save_manifest(manifest)
        print(f"再次更新後清單: {manifest}")
    else:
        print(f"檔案 {test_filepath} 無需處理。")
    
    # 清理測試檔案
    os.remove(test_filepath)
    # 注意：測試後 processed_files_manifest.json 可能會留下，這是預期的
    print(f"測試完成。請檢查 {MANIFEST_FILEPATH} 的內容。")
