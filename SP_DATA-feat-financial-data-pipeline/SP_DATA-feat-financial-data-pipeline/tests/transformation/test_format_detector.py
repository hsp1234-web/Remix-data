import pytest
import hashlib
import re # 確保 re 已導入
from typing import Dict, Any, Optional, List

# 被測模듈
from taifex_pipeline.transformation.format_detector import FormatDetector

# --- Helper Functions ---
def calculate_expected_fingerprint_new(header_str: str) -> str:
    """
    輔助函數，用於手動計算預期的指紋，與 FormatDetector._calculate_fingerprint 的新邏輯一致。
    (清除內部所有空白並轉為小寫，然後排序，用'|'合併)
    """
    if not header_str:
        return ""
    columns = [col.strip() for col in header_str.split(',')]
    normalized_columns = [re.sub(r'\s+', '', col).lower() for col in columns if col.strip()]
    if not normalized_columns:
        return ""
    normalized_columns.sort()
    fingerprint_string = "|".join(normalized_columns)
    return hashlib.sha256(fingerprint_string.encode('utf-8')).hexdigest()

# --- Test Fixtures ---
@pytest.fixture
def sample_catalog_data() -> Dict[str, Any]:
    """提供一個範例格式目錄，使用新的指紋計算邏輯。"""
    # 指紋 for: '交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%'
    # 正規化後: '交易日期' -> '交易日期', '契約' -> '契約', ... (假設中文不變，但re.sub會移除空白)
    # '到期月份(週別)' -> '到期月份(週別)'
    # 新的指紋計算方式：欄位內空白會被移除
    header1_raw = "交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%"
    fingerprint1 = calculate_expected_fingerprint_new(header1_raw)

    # 指紋 for: '日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),...'
    header2_raw = "日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,空方交易契約金額(千元),多空交易口數淨額,多空交易契約金額淨額(千元),多方未平倉口數,多方未平倉契約金額(千元),空方未平倉口數,空方未平倉契約金額(千元),多空未平倉口數淨額,多空未平倉契約金額淨額(千元)"
    fingerprint2 = calculate_expected_fingerprint_new(header2_raw)

    # 額外測試案例 from prototype
    header3_raw = "  FIELD B, field a  , Field C  "
    fingerprint3 = calculate_expected_fingerprint_new(header3_raw)

    # 來自舊測試的 "契約,到期月份(W),買賣權,履約價,開盤價,最高價,最低價,最新價"
    header4_raw = "契約,到期月份(W),買賣權,履約價,開盤價,最高價,最低價,最新價"
    fingerprint4 = calculate_expected_fingerprint_new(header4_raw)


    return {
        fingerprint1: {"description": "每日行情 (選擇權/期貨) - v2, 含漲跌幅", "id": "daily_ohlc_v2"},
        fingerprint2: {"description": "三大法人 (依商品分)", "id": "institutional_investors_by_product"},
        fingerprint3: {"description": "Sample Fields Data", "id": "sample_fields"},
        fingerprint4: {"description": "Daily Options MTX (old test)", "id": "daily_options_mtx_old"}
    }

@pytest.fixture
def default_detector(sample_catalog_data: Dict[str, Any]) -> FormatDetector:
    """提供一個使用 sample_catalog_data 初始化的 FormatDetector 實例。"""
    return FormatDetector(format_catalog=sample_catalog_data)


# --- Test Cases for _calculate_fingerprint (new logic) ---
class TestFormatDetectorNewFingerprint:

    # 輔助方法，直接調用私有方法進行測試 (注意：通常不建議，但對於核心工具函數可以接受)
    def calculate_fp(self, detector_instance: FormatDetector, header_line: str) -> str:
        return detector_instance._calculate_fingerprint(header_line)

    def test_calculate_fingerprint_simple_csv(self, default_detector: FormatDetector):
        header = "col_A,col_B,col_C"
        # re.sub(r'\s+', '', col).lower() -> cola, colb, colc
        # sorted: cola, colb, colc
        # joined: cola|colb|colc
        expected_fp = hashlib.sha256("cola|colb|colc".encode('utf-8')).hexdigest()
        assert self.calculate_fp(default_detector, header) == expected_fp

    def test_calculate_fingerprint_with_spaces_and_case(self, default_detector: FormatDetector):
        header = "  Col_B , col_A  , COL_C  "
        # col_b -> colb, col_a -> cola, col_c -> colc
        # sorted: cola, colb, colc
        # joined: cola|colb|colc
        expected_fp = hashlib.sha256("cola|colb|colc".encode('utf-8')).hexdigest()
        assert self.calculate_fp(default_detector, header) == expected_fp

    def test_calculate_fingerprint_with_internal_spaces(self, default_detector: FormatDetector):
        header = "Column With Space A, Column B"
        # column with space a -> columnwithspacea, column b -> columnb
        # sorted: columnb, columnwithspacea
        # joined: columnb|columnwithspacea
        expected_fp = hashlib.sha256("columnb|columnwithspacea".encode('utf-8')).hexdigest()
        assert self.calculate_fp(default_detector, header) == expected_fp

    def test_calculate_fingerprint_chinese_fields(self, default_detector: FormatDetector):
        header = "日期, 商品代號 ,到期月份(週別)"
        # 日期 -> 日期, 商品代號 -> 商品代號, 到期月份(週別) -> 到期月份(週別)
        # (假設中文的空白也被移除，但re.sub(r'\s+')主要針對ASCII空白，對全形空白可能行為不同，需確認)
        # 假設 re.sub 對中文間的空白也有效，或欄位本身無內部空白
        # normalized: ['日期', '商品代號', '到期月份(週別)']
        # sorted: ['到期月份(週別)', '日期', '商品代號'] (基於Unicode排序)
        # joined: 到期月份(週別)|日期|商品代號
        # 實際上，原型腳本的 re.sub(r'\s+', '', col) 會移除所有空白，包括中文間的（如果存在）。
        # 所以 "商 品 代 號" -> "商品代號"
        # 此處假設標頭是 "日期", "商品代號", "到期月份(週別)" (無內部空白)
        # sort: ['到期月份(週別)', '商品代號', '日期'] (這是錯誤的，中文排序應該是'到期月份(週別)', '日期', '商品代號')
        # Unicode: 到 U+5230, 日 U+65E5, 商 U+5546.  So: 到, 商, 日
        # 所以排序應為: ['到期月份(週別)', '商品代號', '日期'] -> 不對，'日期' (ri) vs '商品代號' (shang) vs '到期月份(週別)' (dao)
        # Python sort() for strings: '到期月份(週別)' (U+5230), '日期' (U+65E5), '商品代號' (U+5546)
        # Sorted: ['到期月份(週別)', '商品代號', '日期'] (This is based on Unicode code points)
        expected_normalized_str = "到期月份(週別)|商品代號|日期" # 與 validate_classifier.py 的 debug 輸出一致
        expected_fp = hashlib.sha256(expected_normalized_str.encode('utf-8')).hexdigest()
        assert self.calculate_fp(default_detector, header) == expected_fp

    def test_calculate_fingerprint_empty_header(self, default_detector: FormatDetector):
        assert self.calculate_fp(default_detector, "") == ""

    def test_calculate_fingerprint_header_normalizes_to_empty(self, default_detector: FormatDetector):
        assert self.calculate_fp(default_detector, ",,, , ") == ""
        assert self.calculate_fp(default_detector, "   ") == "" # 只有空白

    def test_calculate_fingerprint_from_prototype1(self, default_detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header_raw = "交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%"
        expected_fp = calculate_expected_fingerprint_new(header_raw) # 用輔助函數確保一致
        assert self.calculate_fp(default_detector, header_raw) == expected_fp
        assert expected_fp in sample_catalog_data # 確保 fixture 中的 key 是正確的

    def test_calculate_fingerprint_from_prototype2(self, default_detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header_raw = "日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,空方交易契約金額(千元),多空交易口數淨額,多空交易契約金額淨額(千元),多方未平倉口數,多方未平倉契約金額(千元),空方未平倉口數,空方未平倉契約金額(千元),多空未平倉口數淨額,多空未平倉契約金額淨額(千元)"
        expected_fp = calculate_expected_fingerprint_new(header_raw)
        assert self.calculate_fp(default_detector, header_raw) == expected_fp
        assert expected_fp in sample_catalog_data


# --- Test Cases for get_recipe (adapted for new FormatDetector) ---
class TestFormatDetectorGetRecipe:

    def test_get_recipe_known_format_daily_ohlc_ms950(self, default_detector: FormatDetector):
        header = "交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%"
        file_content_str = header + "\r\n2025/05/26,CAO,202506,22.0000,買權,-,-,-,-,0,8.1,0,-,-,-,-,,一般,-,-,\r\n"
        file_content_bytes = file_content_str.encode('ms950')

        recipe = default_detector.get_recipe(file_content_bytes)
        assert recipe is not None
        assert recipe["description"] == "每日行情 (選擇權/期貨) - v2, 含漲跌幅"
        assert recipe["id"] == "daily_ohlc_v2"
        assert recipe["_debug_metadata"]["detected_encoding"] == "ms950"
        assert recipe["_debug_metadata"]["detected_header_content"] == header
        assert recipe["_debug_metadata"]["calculated_fingerprint"] == calculate_expected_fingerprint_new(header)

    def test_get_recipe_known_format_institutional_investors_ms950(self, default_detector: FormatDetector):
        header = "日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,空方交易契約金額(千元),多空交易口數淨額,多空交易契約金額淨額(千元),多方未平倉口數,多方未平倉契約金額(千元),空方未平倉口數,空方未平倉契約金額(千元),多空未平倉口數淨額,多空未平倉契約金額淨額(千元)"
        file_content_str = header + "\r\n2025/06/13,臺股期貨,自營商,14613,64130480,11620,51089894,2993,13040586,8415,36614210,4961,21684729,3454,14929481\r\n"
        file_content_bytes = file_content_str.encode('ms950')

        recipe = default_detector.get_recipe(file_content_bytes)
        assert recipe is not None
        assert recipe["description"] == "三大法人 (依商品分)"
        assert recipe["id"] == "institutional_investors_by_product"
        assert recipe["_debug_metadata"]["detected_encoding"] == "ms950"

    def test_get_recipe_known_format_sample_fields_utf8(self, default_detector: FormatDetector):
        header = "  FIELD B, field a  , Field C  "
        file_content_str = header + "\r\nvalB,valA,valC\r\n"
        file_content_bytes = file_content_str.encode('utf-8')

        recipe = default_detector.get_recipe(file_content_bytes)
        assert recipe is not None
        assert recipe["description"] == "Sample Fields Data"
        assert recipe["id"] == "sample_fields"
        assert recipe["_debug_metadata"]["detected_encoding"] == "utf-8"

    def test_get_recipe_known_format_daily_options_mtx_big5(self, sample_catalog_data: Dict[str, Any]):
        header = "契約,到期月份(W),買賣權,履約價,開盤價,最高價,最低價,最新價"
        file_content_str = header + "\r\nTXO,202301W1,Call,18000,100,120,80,110\r\n"
        # 確保使用 big5 編碼，因為 sample_catalog_data 中的配方可能是基於此
        # 但 FormatDetector 的預設編碼列表是 ['ms950', 'utf-8', 'utf-8-sig']
        # ms950 是 big5 的擴展，通常可以解碼 big5
        file_content_bytes_big5 = file_content_str.encode('big5')

        # 測試使用預設編碼能否找到 (ms950 應能解碼 big5)
        detector_ms950_can_decode_big5 = FormatDetector(format_catalog=sample_catalog_data)
        recipe = detector_ms950_can_decode_big5.get_recipe(file_content_bytes_big5)
        assert recipe is not None
        assert recipe["description"] == "Daily Options MTX (old test)"
        assert recipe["id"] == "daily_options_mtx_old"
        assert recipe["_debug_metadata"]["detected_encoding"] == "ms950" # 因為ms950先被嘗試且成功

        # 測試明確將 big5 放在前面
        detector_big5_first = FormatDetector(format_catalog=sample_catalog_data, encodings=['big5', 'ms950', 'utf-8'])
        recipe_big5 = detector_big5_first.get_recipe(file_content_bytes_big5)
        assert recipe_big5 is not None
        assert recipe_big5["description"] == "Daily Options MTX (old test)"
        assert recipe_big5["_debug_metadata"]["detected_encoding"] == "big5"


    def test_get_recipe_unknown_format(self, default_detector: FormatDetector):
        header = "This,Is,An,Unknown,Header,With,Enough,Commas,And,Keywords,日期" # 確保能被_find_header_row識別
        file_content_bytes = (header + "\r\ndata,data,data,data,data\r\n").encode('utf-8')
        recipe = default_detector.get_recipe(file_content_bytes)
        assert recipe is None

    def test_get_recipe_decoding_failure_all_encodings(self, default_detector: FormatDetector):
        gbk_bytes = "你好世界".encode('gbk') # GBK 與預設編碼不兼容
        recipe = default_detector.get_recipe(gbk_bytes)
        assert recipe is None

    def test_get_recipe_header_on_second_line(self, default_detector: FormatDetector):
        header = "日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元)" # 使用部分標頭以簡化
        file_content_str = "Some garbage first line that might have,commas,and,日期\r\n" + \
                           header + "\r\n2025/06/13,臺股期貨,自營商,14613,64130480\r\n"
        file_content_bytes = file_content_str.encode('ms950')

        # 為了確保測試穩定，我們用一個只包含 header 指紋的 catalog
        fp = calculate_expected_fingerprint_new(header)
        simple_catalog = {fp: {"description": "Partial Institutional Investors", "id": "partial_insti"}}
        detector = FormatDetector(format_catalog=simple_catalog)

        recipe = detector.get_recipe(file_content_bytes)
        assert recipe is not None
        assert recipe["description"] == "Partial Institutional Investors"
        assert recipe["_debug_metadata"]["detected_header_row_index"] == 1 # 標頭在第二行 (索引1)

    def test_get_recipe_file_content_empty(self, default_detector: FormatDetector):
        recipe = default_detector.get_recipe(b"")
        assert recipe is None

    def test_get_recipe_catalog_empty(self): # 不需要 default_detector，因為我們要傳入空 catalog
        header = "日期,商品代號,到期月份(週別)"
        file_content_bytes = (header + "\r\n20230101,TXF,202301\r\n").encode('ms950')
        detector_empty_catalog = FormatDetector(format_catalog={}) # 傳入空 catalog
        recipe = detector_empty_catalog.get_recipe(file_content_bytes)
        assert recipe is None

    def test_get_recipe_no_identifiable_header(self, default_detector: FormatDetector):
        content = b"This is a line\nAnd another line\nNo commas, no keywords of substance"
        recipe = default_detector.get_recipe(content)
        assert recipe is None

    def test_get_recipe_uses_custom_init_params(self, sample_catalog_data: Dict[str, Any]):
        header = "Test,Header,Custom"
        fp = calculate_expected_fingerprint_new(header)
        # 使用 sample_catalog_data 的一個子集或修改版，確保key存在
        custom_catalog_subset = {fp: {"description": "Custom Recipe For Test", "id": "custom_test"}}

        file_content_bytes_latin1 = header.encode('latin1')

        # 1. 測試自訂編碼
        detector_custom_encoding = FormatDetector(format_catalog=custom_catalog_subset, encodings=['latin1'])
        recipe = detector_custom_encoding.get_recipe(file_content_bytes_latin1)
        assert recipe is not None
        assert recipe["description"] == "Custom Recipe For Test"
        assert recipe["_debug_metadata"]["detected_encoding"] == 'latin1'

        # 預設 detector (使用 default_detector fixture 的 catalog) 應該找不到 'latin1' 編碼的此特定標頭
        # 因為 custom_catalog_subset 與 default_detector 的 catalog 不同。
        # 這裡要測試的是編碼本身，所以用一個能被 latin1 解碼但不能被預設解碼的標頭，
        # 且其指紋存在於 default_detector.format_catalog 中
        # 但這比較複雜，目前測試已能驗證 encodings 參數的作用。

        # 2. 測試自訂 header_read_bytes
        short_read_bytes = len(header.encode('latin1')) - 3 # 確保讀取不完整
        detector_short_read = FormatDetector(format_catalog=custom_catalog_subset,
                                             encodings=['latin1'],
                                             header_read_bytes=short_read_bytes)
        recipe_short = detector_short_read.get_recipe(file_content_bytes_latin1)
        assert recipe_short is None

        # 3. 測試自訂 max_header_lines
        file_content_multiline_str = "Junk line with 日期 keyword, and commas\r\n" + header # header 在第二行
        file_content_multiline_bytes_latin1 = file_content_multiline_str.encode('latin1')

        detector_max_lines_1 = FormatDetector(format_catalog=custom_catalog_subset,
                                              encodings=['latin1'],
                                              max_header_lines=1)
        recipe_max_lines = detector_max_lines_1.get_recipe(file_content_multiline_bytes_latin1)
        assert recipe_max_lines is None # 因為只嗅探第一行 (Junk line)

        detector_max_lines_2 = FormatDetector(format_catalog=custom_catalog_subset,
                                              encodings=['latin1'],
                                              max_header_lines=2)
        recipe_enough_lines = detector_max_lines_2.get_recipe(file_content_multiline_bytes_latin1)
        assert recipe_enough_lines is not None
        assert recipe_enough_lines["description"] == "Custom Recipe For Test"
        assert recipe_enough_lines["_debug_metadata"]["detected_header_row_index"] == 1

    def test_normalize_header_with_problematic_chars_for_split(self, default_detector: FormatDetector):
        # 新的正規化 re.sub(r'\s+', '', col) 會移除所有空白
        # "Product, Name" -> "product,name" (逗號保留)
        # "Category" -> "category"
        # "Price" -> "price"
        # split by ',': ['"Product', ' Name"', '"Category"', '"Price"']
        # re.sub & lower: ['"product', 'name"', '"category"', '"price"']
        # sorted: ['"category"', '"price"', '"product', 'name"']
        # fingerprint_string: '"category"|"price"|"product|name"'
        # This is different from simple split. The key is consistency.
        header_with_quoted_comma = '"Product, Name","Category","Price"'
        fp_problematic = default_detector._calculate_fingerprint(header_with_quoted_comma)

        problematic_catalog = {fp_problematic: {"description": "CSV with Quoted Commas"}}
        detector = FormatDetector(format_catalog=problematic_catalog)
        file_content_bytes = header_with_quoted_comma.encode('utf-8')

        recipe = detector.get_recipe(file_content_bytes)
        assert recipe is not None
        assert recipe["description"] == "CSV with Quoted Commas"
        assert recipe["_debug_metadata"]["detected_header_content"] == header_with_quoted_comma
        assert recipe["_debug_metadata"]["calculated_fingerprint"] == fp_problematic

# --- Placeholder for direct execution (Phase 2 adjustment) ---
# (Will be filled in the next step of the plan)
# if __name__ == "__main__":
#     print("Running tests directly...")
#     # Manually run test functions or methods here
#     # Example:
#     # test_instance = TestFormatDetectorGetRecipe()
#     # catalog_data = sample_catalog_data() # Need to call fixture functions if used
#     # detector_instance = default_detector(catalog_data) # Need to call fixture functions
#     # test_instance.test_get_recipe_known_format_daily_ohlc_ms950(detector_instance)
#     # This manual setup can be complex due to pytest fixture dependencies.
#     # A simpler approach for direct execution might be to write standalone test functions
#     # that set up their own data and detector instances.
#     print("Test execution placeholder. Implement direct calls or simplify tests for direct run.")

if __name__ == "__main__":
    print("="*80)
    print("🚀 開始直接執行 test_format_detector.py 中的選定測試...")
    print("="*80, "\n")

    # --- 手動準備依賴 ---
    # 1. 模擬 sample_catalog_data() fixture
    print("🔧 準備模擬的 sample_catalog_data...")
    header1_raw_main = "交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%"
    fingerprint1_main = calculate_expected_fingerprint_new(header1_raw_main)
    header2_raw_main = "日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,空方交易契約金額(千元),多空交易口數淨額,多空交易契約金額淨額(千元),多方未平倉口數,多方未平倉契約金額(千元),空方未平倉口數,空方未平倉契約金額(千元),多空未平倉口數淨額,多空未平倉契約金額淨額(千元)"
    fingerprint2_main = calculate_expected_fingerprint_new(header2_raw_main)
    header3_raw_main = "  FIELD B, field a  , Field C  "
    fingerprint3_main = calculate_expected_fingerprint_new(header3_raw_main)
    header4_raw_main = "契約,到期月份(W),買賣權,履約價,開盤價,最高價,最低價,最新價"
    fingerprint4_main = calculate_expected_fingerprint_new(header4_raw_main)

    sample_catalog_main: Dict[str, Any] = {
        fingerprint1_main: {"description": "每日行情 (選擇權/期貨) - v2, 含漲跌幅", "id": "daily_ohlc_v2"},
        fingerprint2_main: {"description": "三大法人 (依商品分)", "id": "institutional_investors_by_product"},
        fingerprint3_main: {"description": "Sample Fields Data", "id": "sample_fields"},
        fingerprint4_main: {"description": "Daily Options MTX (old test)", "id": "daily_options_mtx_old"}
    }
    print(f"✔️ 模擬 catalog 準備完成，包含 {len(sample_catalog_main)} 個條目。\n")

    # 2. 創建 FormatDetector 實例 (類似 default_detector fixture)
    print("🔧 創建 FormatDetector 實例...")
    detector_main = FormatDetector(format_catalog=sample_catalog_main)
    print("✔️ FormatDetector 實例創建完成。\n")

    # --- 手動執行 TestFormatDetectorNewFingerprint 中的測試 ---
    print("-" * 50)
    print("🧪 執行 TestFormatDetectorNewFingerprint 測試類...")
    fingerprint_tests = TestFormatDetectorNewFingerprint()
    tests_passed_fp = 0
    tests_failed_fp = 0

    # 執行 test_calculate_fingerprint_simple_csv
    try:
        fingerprint_tests.test_calculate_fingerprint_simple_csv(detector_main)
        print("  ✅ test_calculate_fingerprint_simple_csv PASSED")
        tests_passed_fp += 1
    except AssertionError as e:
        print(f"  ❌ test_calculate_fingerprint_simple_csv FAILED: {e}")
        tests_failed_fp +=1

    # 執行 test_calculate_fingerprint_with_spaces_and_case
    try:
        fingerprint_tests.test_calculate_fingerprint_with_spaces_and_case(detector_main)
        print("  ✅ test_calculate_fingerprint_with_spaces_and_case PASSED")
        tests_passed_fp += 1
    except AssertionError as e:
        print(f"  ❌ test_calculate_fingerprint_with_spaces_and_case FAILED: {e}")
        tests_failed_fp += 1

    # 執行 test_calculate_fingerprint_chinese_fields
    try:
        fingerprint_tests.test_calculate_fingerprint_chinese_fields(detector_main)
        print("  ✅ test_calculate_fingerprint_chinese_fields PASSED")
        tests_passed_fp += 1
    except AssertionError as e:
        print(f"  ❌ test_calculate_fingerprint_chinese_fields FAILED: {e}")
        tests_failed_fp += 1

    # 執行 test_calculate_fingerprint_from_prototype1 (需要 sample_catalog_main)
    try:
        fingerprint_tests.test_calculate_fingerprint_from_prototype1(detector_main, sample_catalog_main)
        print("  ✅ test_calculate_fingerprint_from_prototype1 PASSED")
        tests_passed_fp += 1
    except AssertionError as e:
        print(f"  ❌ test_calculate_fingerprint_from_prototype1 FAILED: {e}")
        tests_failed_fp += 1

    print(f"🏁 TestFormatDetectorNewFingerprint 完成: {tests_passed_fp} PASSED, {tests_failed_fp} FAILED.\n")

    # --- 手動執行 TestFormatDetectorGetRecipe 中的部分選定測試 ---
    print("-" * 50)
    print("🧪 執行 TestFormatDetectorGetRecipe 測試類 (選定測試)...")
    get_recipe_tests = TestFormatDetectorGetRecipe()
    tests_passed_gr = 0
    tests_failed_gr = 0

    # 執行 test_get_recipe_file_content_empty
    try:
        get_recipe_tests.test_get_recipe_file_content_empty(detector_main)
        print("  ✅ test_get_recipe_file_content_empty PASSED")
        tests_passed_gr +=1
    except AssertionError as e:
        print(f"  ❌ test_get_recipe_file_content_empty FAILED: {e}")
        tests_failed_gr +=1

    # 執行 test_get_recipe_catalog_empty (這個測試自己創建 detector)
    try:
        get_recipe_tests.test_get_recipe_catalog_empty()
        print("  ✅ test_get_recipe_catalog_empty PASSED")
        tests_passed_gr +=1
    except AssertionError as e:
        print(f"  ❌ test_get_recipe_catalog_empty FAILED: {e}")
        tests_failed_gr +=1

    # 執行 test_get_recipe_known_format_daily_ohlc_ms950
    try:
        get_recipe_tests.test_get_recipe_known_format_daily_ohlc_ms950(detector_main)
        print("  ✅ test_get_recipe_known_format_daily_ohlc_ms950 PASSED")
        tests_passed_gr +=1
    except AssertionError as e:
        print(f"  ❌ test_get_recipe_known_format_daily_ohlc_ms950 FAILED: {e}")
        tests_failed_gr +=1

    # 執行 test_get_recipe_unknown_format
    try:
        get_recipe_tests.test_get_recipe_unknown_format(detector_main)
        print("  ✅ test_get_recipe_unknown_format PASSED")
        tests_passed_gr +=1
    except AssertionError as e:
        print(f"  ❌ test_get_recipe_unknown_format FAILED: {e}")
        tests_failed_gr +=1

    print(f"🏁 TestFormatDetectorGetRecipe (選定測試) 完成: {tests_passed_gr} PASSED, {tests_failed_gr} FAILED.\n")

    print("="*80)
    print("📢 注意：以上僅為部分選定測試的直接執行結果。")
    print("為了全面的測試覆蓋，請在環境允許時使用 pytest 執行完整的測試套件。")
    print("某些測試（特別是依賴複雜 fixture 或 pytest 特定功能的）未在此直接執行。")
    print("="*80)

    total_passed = tests_passed_fp + tests_passed_gr
    total_failed = tests_failed_fp + tests_failed_gr
    print(f"\nGrand Total: {total_passed} PASSED, {total_failed} FAILED (from direct run).")
    if total_failed > 0:
        print("\n🔥🔥🔥 有測試失敗，請檢查輸出！ 🔥🔥🔥")
    else:
        print("\n🎉🎉🎉 所有直接執行的測試均通過！ 🎉🎉🎉")
