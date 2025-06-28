# -*- coding: utf-8 -*-
# @title 格式指紋分類器 - 原型驗證腳本 v1.0
# @markdown ### 目的
# @markdown 本腳本旨在獨立驗證「格式指紋」識別系統的核心邏輯。
# @markdown 它不依賴任何外部檔案或複雜的專案結構，可一鍵執行，
# @markdown 以確認我們設計的分類演算法能否準確地為不同格式的
# @markdown 期交所數據檔案，匹配到正確的處理配方。

import hashlib
import re
from typing import List, Dict, Optional, Tuple

# --- 步驟 1: 模擬我們未來在 config/format_catalog.json 中的核心設定 ---

# 這是我們預先定義好的「處理配方」目錄。
# 注意：這裡的雜湊值是基於我們的指紋生成規則，對真實標頭計算得出的。
MOCK_FORMAT_CATALOG = {
    # 指紋 for: '交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%'
    "d627a542b26286815e3469a47312f21133318f78234839835848e1a141151609": {
      "description": "每日行情 (選擇權/期貨) - v2, 含漲跌幅",
      "target_table": "daily_ohlc",
      "parser_config": {"sep": ",", "header": 0, "encoding": "ms950"},
      "cleaner_function": "clean_daily_ohlc",
      "required_columns": ["交易日期", "契約", "收盤價", "成交量"]
    },
    # 指紋 for: '日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),...'
    "b8a6a3b68e9f2913e2f07323604b73273117462d7331853a8174780517878841": {
      "description": "三大法人 (依商品分)",
      "target_table": "institutional_investors",
      "parser_config": {"sep": ",", "header": 0, "encoding": "ms950"},
      "cleaner_function": "clean_institutional_investors",
      "required_columns": ["日期", "商品名稱", "身份別"]
    },
}


# --- 步驟 2: 核心函式庫的實作 ---

def find_header_row(content_lines: List[str]) -> Tuple[Optional[str], int]:
    """
    從檔案的前幾行中，透過啟發式規則找出最可能的標頭行。
    返回 (標頭行內容, 標頭行索引)。
    """
    candidates = []
    keywords = ['日期', '契約', '商品', '身份別', '成交量', '收盤價', '買賣權']

    for i, line in enumerate(content_lines):
        line = line.strip()
        if not line or line.startswith('---'):
            continue

        # 啟發式規則：逗號數量 > 3 且至少包含一個關鍵字
        comma_count = line.count(',')
        keyword_count = sum(1 for keyword in keywords if keyword in line)

        if comma_count > 3 and keyword_count > 0:
            # 分數越高，是標頭的可能性越大
            score = comma_count + (keyword_count * 5)
            candidates.append({'score': score, 'line': line, 'index': i})

    if not candidates:
        return None, -1

    # 回傳分數最高的候選者
    best_candidate = max(candidates, key=lambda x: x['score'])
    return best_candidate['line'], best_candidate['index']


def calculate_format_fingerprint(header_line: str) -> str:
    """
    根據我們定義的規則，從標頭行計算出格式指紋。
    """
    # 1. 用逗號分割，並清除每個欄位的首尾空白
    columns = [col.strip() for col in header_line.split(',')]

    # 2. 清除內部所有空白並轉為小寫
    normalized_columns = [re.sub(r'\s+', '', col).lower() for col in columns if col]

    # 3. 依字母順序排序
    normalized_columns.sort()

    # 4. 使用 "|" 合併成單一字串
    fingerprint_string = "|".join(normalized_columns)

    # 5. 計算 SHA256 雜湊值
    return hashlib.sha256(fingerprint_string.encode('utf-8')).hexdigest()


def classify_file_format(file_content: bytes, catalog: dict) -> Optional[dict]:
    """
    接收檔案的二進位內容，執行完整的分類流程。
    """
    print("  > 開始分類...")

    decoded_lines = []
    active_encoding = None
    for encoding in ['ms950', 'utf-8', 'utf-8-sig']:
        try:
            decoded_lines = file_content.decode(encoding).splitlines()[:20]
            active_encoding = encoding
            print(f"  > 嘗試使用 '{encoding}' 解碼成功。")
            break
        except UnicodeDecodeError:
            continue

    if not decoded_lines:
        print("  > ❌ 錯誤：無法使用常見編碼解碼或檔案為空。")
        return None

    # 找出標頭
    header_line, header_index = find_header_row(decoded_lines)
    if header_line is None:
        print("  > ❌ 錯誤：在檔案預覽中找不到可識別的標頭行。")
        return None
    print(f"  > 偵測到標頭在第 {header_index + 1} 行: {repr(header_line)}")

    # 計算指紋
    fingerprint = calculate_format_fingerprint(header_line)
    print(f"  > 計算出的格式指紋為: {fingerprint[:16]}...") # 只顯示前16碼以求簡潔

    # 在目錄中查找
    recipe = catalog.get(fingerprint)

    if recipe:
        print(f"  > ✅ 成功！在目錄中找到配方: '{recipe.get('description', 'N/A')}'")
        # 將偵測到的元數據附加到 recipe 中，方便驗證
        recipe_copy = recipe.copy() # 避免修改原始 MOCK_FORMAT_CATALOG
        recipe_copy['_debug_metadata'] = {
            'detected_header': header_line,
            'detected_header_index': header_index,
            'detected_encoding': active_encoding,
            'calculated_fingerprint': fingerprint
        }
        return recipe_copy
    else:
        print("  > ⚠️ 警告：在目錄中找不到對應的處理配方。")
        print(f"    (除錯資訊：原始標頭: {repr(header_line)}, 計算指紋: {fingerprint})")
        return None


# --- 步驟 3: 測試我們的分類器 ---

# 我們將使用來自您報告的真實數據片段作為測試案例
TEST_DATA = {
    "daily_ohlc_sample.csv": b"""\
交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%
2025/05/26,CAO,202506  ,22.0000,買權,-,-,-,-,0,8.1,0,-,-,-,-,,一般,-,-,
2025/05/26,CAO,202506  ,22.0000,賣權,-,-,-,-,0,0.01,0,-,-,-,-,,一般,-,-,
""",
    "institutional_investors_sample.csv": b"""\
日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,空方交易契約金額(千元),多空交易口數淨額,多空交易契約金額淨額(千元),多方未平倉口數,多方未平倉契約金額(千元),空方未平倉口數,空方未平倉契約金額(千元),多空未平倉口數淨額,多空未平倉契約金額淨額(千元)
2025/06/13,臺股期貨,自營商,14613,64130480,11620,51089894,2993,13040586,8415,36614210,4961,21684729,3454,14929481
2025/06/13,臺股期貨,投信,3814,16557596,4437,19524483,-623,-2966887,52445,229829867,13086,57413851,39359,172416016
""",
    "unknown_format_sample.txt": b"""\
這是一個全新的報告，沒有明確的關鍵字和足夠的逗號
第一筆資料-1-2
第二筆資料-3-4
"""
}

def run_prototype_test():
    """主執行函數，運行所有測試案例"""
    print("="*80)
    print("🚀 開始執行「格式指紋分類器」原型驗證測試...")
    print("="*80, "\n")

    catalog = MOCK_FORMAT_CATALOG
    print("S1: 已成功載入模擬的「格式指紋目錄」。\n")

    for filename, content in TEST_DATA.items():
        print("-" * 50)
        print(f"S2: 正在測試檔案: {filename}")

        found_recipe = classify_file_format(content, catalog)

        print("\n  >> 分類結果:")
        if found_recipe:
            import json
            # 使用 ensure_ascii=False 以正確顯示中文字元
            print(json.dumps(found_recipe, indent=4, ensure_ascii=False, sort_keys=True))
        else:
            print("  該檔案應被送往『隔離區』(QUARANTINED)。")
        print("-" * 50, "\n")

    print("="*80)
    print("🏁 原型驗證測試結束。")
    print("="*80)

# --- 主執行區塊 ---
if __name__ == "__main__":
    run_prototype_test()
