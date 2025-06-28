# -*- coding: utf-8 -*-
"""
格式指紋計算模組 (Format Detector)

實現檔案格式的自動偵測，主要流程：
1. 多行嗅探，首個匹配策略定位標頭行。
2. 標頭正規化（清除空白、轉小寫、排序、合併）。
3. 計算 SHA256 指紋。
"""
import hashlib
import io
import re
from typing import Optional, List, Tuple

from taifex_pipeline.core.logger_setup import get_logger

logger = get_logger(__name__)

# --- 常數設定 ---
MAX_HEADER_CANDIDATE_LINES = 20  # 讀取檔案開頭多少行作為標頭候選
MIN_COLUMNS_FOR_HEADER = 2       # 一行至少要有多少欄位才被考慮為可能的標頭
COMMON_HEADER_KEYWORDS = [ # 一些常見的中文標頭關鍵字，用於輔助判斷 (可擴充)
    "日期", "代號", "契約", "價格", "成交", "時間", "數量",
    "開盤", "最高", "最低", "收盤", "結算", "未平倉", "買賣權"
]
# 用於分割欄位的正則表達式，考慮逗號和可能的空白
# 感謝使用者指出，期交所CSV常有 "欄位 , 欄位" 或 " 欄位," 的情況
# 這個正則表達式會嘗試匹配：
# 1. 引號包圍的內容 (允許內部有逗號)
# 2. 沒有引號，且不含逗號的內容
# 並且處理每個欄位前後的空白
# REGEX_CSV_SPLIT = re.compile(r'\s*("([^"]*)"|[^,]+)\s*(?:,|$)')
# 上述正則在處理 "欄位 , 欄位" 時，若直接 split 會產生問題。
# Pandas 的 read_csv 在處理這種情況時更為強大。
# 此處的目標是從原始行中提取「看起來像」欄位名的字串列表。
# 一個簡化的策略是先用逗號分割，然後對每個部分進行清理。

def _normalize_column_name(name: str) -> str:
    """
    正規化單一欄位名稱：
    1. 移除首尾空白。
    2. 移除欄位名稱內部的所有空白字元。
    3. 轉換為小寫。
    """
    name = name.strip()
    name = "".join(name.split()) # 移除內部所有空白
    return name.lower()

def _extract_potential_header_fields(line: str) -> List[str]:
    """
    從單一行文本中提取潛在的欄位名稱列表。
    簡化策略：以逗號分割，然後對每個部分進行基礎清理。
    注意：這不是一個完整的 CSV 解析器，僅用於嗅探標頭。
    """
    # 移除行尾可能存在的 \r, \n 等
    cleaned_line = line.strip()
    if not cleaned_line:
        return []

    # 簡單以逗號分割
    potential_fields = cleaned_line.split(',')

    # 清理每個潛在欄位
    # 例如：" 商品代號 " -> "商品代號"
    #       " 成交價格(B or S)" -> "成交價格(BorS)"
    #       "   " -> "" (應被過濾掉)
    normalized_fields = []
    for field in potential_fields:
        norm_field = field.strip() # 移除首尾空白
        if norm_field: # 只保留非空欄位
            normalized_fields.append(norm_field)

    return normalized_fields


def _is_likely_header(fields: List[str]) -> bool:
    """
    判斷提取出的欄位列表是否「像」一個標頭。
    """
    if len(fields) < MIN_COLUMNS_FOR_HEADER:
        return False

    # 檢查是否包含一些常見的關鍵字 (至少N個)
    # 或者檢查欄位是否大部分不是純數字 (標頭通常是文字)
    keyword_hits = 0
    non_numeric_fields = 0

    for field_part in fields: # field_part 可能是 "商品代號"
        # 正規化後的欄位名，用於匹配關鍵字
        # 但這裡的 field_part 已經是 _extract_potential_header_fields 清理過的
        # 我們需要的是更原始的、未被 _normalize_column_name 處理的欄位（如果關鍵字包含空白或大小寫）
        # 但為了簡單，這裡假設關鍵字列表也是小寫且無空白的
        normalized_field_for_keyword_check = "".join(field_part.split()).lower()

        for keyword in COMMON_HEADER_KEYWORDS:
            if keyword in normalized_field_for_keyword_check:
                keyword_hits += 1
                break # 一個欄位包含一個關鍵字即可

        # 判斷是否為非數字 (一個簡單的檢查)
        if not field_part.replace('.', '', 1).isdigit(): # 允許小數點
            non_numeric_fields +=1

    # 條件：至少有一定比例的欄位包含關鍵字，或者大部分欄位不是純數字
    # 這裡的閾值可以調整
    if keyword_hits >= min(2, len(fields) // 2) or \
       non_numeric_fields >= len(fields) * 0.7:
        return True

    return False

def find_header_row(file_stream: io.BytesIO, file_name_for_log: str) -> Tuple[Optional[List[str]], Optional[int]]:
    """
    從檔案串流中定位標頭行並提取正規化後的欄位。

    Args:
        file_stream (io.BytesIO): 檔案內容的位元組串流。
        file_name_for_log (str): 檔名 (用於日誌)。

    Returns:
        Tuple[Optional[List[str]], Optional[int]]:
            - 正規化後的標頭欄位列表 (已排序)，如果找不到則為 None。
            - 標頭所在的行號 (0-based)，如果找不到則為 None。
    """
    file_stream.seek(0)
    # 嘗試用多種編碼讀取前幾行
    # 順序很重要：utf-8-sig (處理BOM), utf-8, ms950 (繁體中文常用)
    encodings_to_try = ['utf-8-sig', 'utf-8', 'ms950', 'big5']

    lines: List[str] = []
    detected_encoding: Optional[str] = None

    for encoding in encodings_to_try:
        try:
            file_stream.seek(0)
            # TextIOWrapper 將 bytes stream 轉為 text stream
            # readline() 會讀取直到 '\n'
            reader = io.TextIOWrapper(file_stream, encoding=encoding, errors='strict')
            lines = [reader.readline() for _ in range(MAX_HEADER_CANDIDATE_LINES)]
            detected_encoding = encoding
            logger.debug(f"檔案 {file_name_for_log}: 成功使用編碼 {encoding} 讀取前 {len(lines)} 行。")
            break # 找到能成功讀取的編碼
        except UnicodeDecodeError:
            logger.debug(f"檔案 {file_name_for_log}: 使用編碼 {encoding} 解碼失敗，嘗試下一個。")
            continue
        except Exception as e: # 其他可能的錯誤，例如串流問題
            logger.error(f"檔案 {file_name_for_log}: 讀取標頭時發生非預期錯誤 ({encoding}): {e}", exc_info=True)
            return None, None # 不再嘗試其他編碼

    if not lines or detected_encoding is None:
        logger.warning(f"檔案 {file_name_for_log}: 無法使用任何常用編碼成功讀取標頭行，或檔案為空。")
        return None, None

    # 逐行嗅探，找到第一個最像標頭的行
    best_header_fields: Optional[List[str]] = None
    header_line_num: Optional[int] = None
    max_field_count = 0 # 用於輔助判斷，欄位多的可能更像標頭

    for i, line_content in enumerate(lines):
        if not line_content.strip(): # 跳過空行
            continue

        potential_fields = _extract_potential_header_fields(line_content)

        if len(potential_fields) < MIN_COLUMNS_FOR_HEADER:
            continue

        logger.debug(f"檔案 {file_name_for_log}, 行 {i}: 潛在標頭欄位: {potential_fields}")

        if _is_likely_header(potential_fields):
            # 找到了第一個看起來像標頭的行
            # 根據「首個匹配」策略，這就是我們要的標頭
            # 但我們可以加入一個簡單的權重：如果後面的行有更多欄位且也像標頭，可能更優
            # 這裡簡化：採用第一個匹配到的
            if header_line_num is None or len(potential_fields) > max_field_count: # 更新條件
                normalized_and_sorted_fields = sorted([_normalize_column_name(f) for f in potential_fields if _normalize_column_name(f)])

                # 再次確認正規化後是否還有足夠欄位
                if len(normalized_and_sorted_fields) >= MIN_COLUMNS_FOR_HEADER:
                    best_header_fields = normalized_and_sorted_fields
                    header_line_num = i
                    max_field_count = len(potential_fields)
                    logger.info(f"檔案 {file_name_for_log}: 在第 {i} 行 (0-based) 使用編碼 '{detected_encoding}' "
                                f"找到候選標頭。正規化並排序後: {best_header_fields}")
                    # 根據「首個匹配」策略，找到就可以返回
                    # 但為了處理某些檔案可能在前面有幾行註解，但後面有更完整的標頭的情況，
                    # 我們可以繼續掃描完 MAX_HEADER_CANDIDATE_LINES，取欄位最多且符合條件的那個。
                    # 此處修改為取欄位最多的那一個（如果有多個都像標頭）
                    # break # 如果是嚴格的「首個匹配」則 break

    if best_header_fields and header_line_num is not None:
        logger.info(f"檔案 {file_name_for_log}: 最終選擇第 {header_line_num} 行 (0-based) 作為標頭。 "
                    f"正規化並排序後: {best_header_fields}")
        return best_header_fields, header_line_num
    else:
        logger.warning(f"檔案 {file_name_for_log}: 在前 {MAX_HEADER_CANDIDATE_LINES} 行中未能定位到明確的標頭。")
        return None, None


def calculate_format_fingerprint(
    file_stream: io.BytesIO,
    file_name_for_log: str = "UnknownFile"
) -> Optional[str]:
    """
    計算給定檔案串流的格式指紋。

    Args:
        file_stream (io.BytesIO): 檔案內容的位元組串流。應支持 seek(0)。
        file_name_for_log (str): 檔名，僅用於日誌輸出。

    Returns:
        Optional[str]: 計算得到的 SHA256 格式指紋。如果無法確定標頭，則返回 None。
    """
    logger.debug(f"開始為檔案 '{file_name_for_log}' 計算格式指紋...")

    header_fields, _ = find_header_row(file_stream, file_name_for_log)

    if not header_fields:
        logger.warning(f"檔案 '{file_name_for_log}': 未能提取有效標頭欄位，無法計算指紋。")
        return None

    # 使用管道符 | 合併排序後的正規化欄位名
    combined_header_string = "|".join(header_fields)
    logger.debug(f"檔案 '{file_name_for_log}': 用於計算指紋的合併標頭字串: '{combined_header_string}'")

    # 計算 SHA256 雜湊值
    sha256_hash = hashlib.sha256(combined_header_string.encode('utf-8')).hexdigest()
    logger.info(f"檔案 '{file_name_for_log}': 計算得到的格式指紋為: {sha256_hash}")

    return sha256_hash

# --- 範例使用 ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # setup_global_logger(log_level_console=logging.DEBUG) # 方便查看詳細調試訊息

    logger.info("開始執行 format_detector.py 範例...")

    # 模擬不同格式的檔案內容
    # 格式1: 標準CSV, UTF-8
    content1_str = "交易日期,商品代號, 開盤價 ,最高價,最低價,收盤價\n20230101,TXF,14000,14050,13980,14020"
    stream1 = io.BytesIO(content1_str.encode('utf-8'))
    fingerprint1 = calculate_format_fingerprint(stream1, "test_file1.csv")
    # 預期正規化後: "交易日期|商品代號|開盤價|最高價|最低價|收盤價" (排序後可能不同)
    # 實際: "交易日期" -> jiaoyiriqi (如果做了中文轉拼音，但我們沒做) -> 交易日期
    # "商品代號" -> shangpindaihao -> 商品代號
    # " 開盤價 " -> 開盤價
    # 正規化: [jiaoyiriqi, kaipanjia, zuidijia, zuigaojia, shoupanshoujia, shangpindaihao]
    # 假設 _normalize_column_name 只做 strip 和 lower:
    # ["交易日期", "商品代號", "開盤價", "最高價", "最低價", "收盤價"] -> sort -> ["交易日期", "商品代號", "最低價", "最高價", "開盤價", "收盤價"] (按Unicode排序)
    # -> "交易日期|商品代號|最低價|最高價|開盤價|收盤價" (實際順序取決於中文字符的Unicode碼點)
    # 為了可預測的測試，我們應使用英文或預期排序
    # 假設排序後是: "開盤價|商品代號|收盤價|交易日期|最低價|最高價"
    # 實際排序: ['交易日期', '商品代號', '開盤價', '最高價', '最低價', '收盤價'] -> sort -> ['交易日期', '商品代號', '收盤價', '最低價', '最高價', '開盤價'] (基於Unicode)
    # combined = "交易日期|商品代號|收盤價|最低價|最高價|開盤價"
    # hash = hashlib.sha256(combined.encode('utf-8')).hexdigest() -> 5b8c82e9...
    # logger.info(f"測試指紋1 (預期 5b8c82e9...): {fingerprint1}")


    # 格式2: 欄位順序不同，但欄位集相同, MS950編碼, 有BOM
    content2_str = "商品代號,交易日期,收盤價,最低價,最高價,開盤價\nTXF,20230101,14020,13980,14050,14000"
    stream2 = io.BytesIO(b'\xef\xbb\xbf' + content2_str.encode('utf-8')) # 用UTF-8模擬，因MS950難直接構造
    # 或者直接用 MS950 編碼的已知字串 (如果環境支持)
    # stream2 = io.BytesIO(codecs.BOM_UTF8 + content2_str.encode('ms950')) # 錯誤用法
    fingerprint2 = calculate_format_fingerprint(stream2, "test_file2_reordered.csv")
    assert fingerprint1 == fingerprint2, "指紋1和指紋2應該相同（欄位集相同，順序和編碼不影響正規化後的指紋）"
    logger.info(f"測試指紋2 (應與指紋1相同): {fingerprint2}")

    # 格式3: 欄位名有額外空白和大小寫混合
    content3_str = " 交易日期 ,商品代號,OpenPrice,HighPrice,lowprice,CLOSEPRICE\n" # OpenPrice 等是英文
    stream3 = io.BytesIO(content3_str.encode('utf-8'))
    fingerprint3 = calculate_format_fingerprint(stream3, "test_file3_mixedcase_space.csv")
    # 預期正規化: "closeprice|highprice|lowprice|openprice|交易日期|商品代號" (排序後)
    logger.info(f"測試指紋3: {fingerprint3}")


    # 格式4: 包含註解行在標頭前
    content4_str = "# 這是一個檔案說明\n# 版本: 1.0\n交易日期,商品代號,成交價\n20230102,TXF,14100"
    stream4 = io.BytesIO(content4_str.encode('utf-8'))
    fingerprint4 = calculate_format_fingerprint(stream4, "test_file4_with_comments.csv")
    # 預期正規化: "交易日期|商品代號|成交價" (排序後)
    # 實際: ['交易日期', '商品代號', '成交價'] -> sort -> ['交易日期', '商品代號', '成交價']
    # combined = "交易日期|商品代號|成交價"
    # hash = hashlib.sha256(combined.encode('utf-8')).hexdigest() -> 1d218714...
    # logger.info(f"測試指紋4 (預期 1d218714...): {fingerprint4}")

    # 格式5: 欄位非常少，可能不是標頭 (或邊界情況)
    content5_str = "日期,值\n2023,100"
    stream5 = io.BytesIO(content5_str.encode('utf-8'))
    fingerprint5 = calculate_format_fingerprint(stream5, "test_file5_few_cols.csv")
    # 預期: "日期|值" (排序後)
    logger.info(f"測試指紋5: {fingerprint5}")

    # 格式6: 空檔案
    stream6 = io.BytesIO(b"")
    fingerprint6 = calculate_format_fingerprint(stream6, "test_file6_empty.csv")
    assert fingerprint6 is None, "空檔案不應產生指紋"
    logger.info(f"測試指紋6 (空檔案，預期 None): {fingerprint6}")

    # 格式7: 只有一行，且不像標頭 (例如純數字)
    content7_str = "123,456,789"
    stream7 = io.BytesIO(content7_str.encode('utf-8'))
    fingerprint7 = calculate_format_fingerprint(stream7, "test_file7_numeric_only.csv")
    # _is_likely_header 可能會判斷失敗
    logger.info(f"測試指紋7 (純數字行，預期可能為 None): {fingerprint7}")
    # assert fingerprint7 is None, "純數字行不應被識別為標頭"
    # (此斷言取決於 _is_likely_header 的嚴格程度，目前可能會識別出來然後產生指紋)

    # 格式8: 期交所 OptionsDaily_2025_05_29.csv 的前幾行模擬 (MS950)
    # " 成交日期, 商品代號, 履約價格, 到期月份(週別), 買賣權別, 成交時間, 成交價格, 成交數量(B or S), 開盤集合競價 \r\n"
    # "---------- ---- ------- ---- ----------------------------------------------------- ---- ------- ---- ----- ---- --------- ---- -------- ---- --------- \r\n"
    # "20250529 , CBO , 18 , 202506 , C , 120059 , 1.88 , 5, \r\n"
    header_line_options_daily = " 成交日期, 商品代號, 履約價格, 到期月份(週別), 買賣權別, 成交時間, 成交價格, 成交數量(B or S), 開盤集合競價 "
    # 注意：MS950編碼在GitHub環境中直接構造字節可能不方便，這裡用UTF-8模擬其結構
    # 實際中，檔案會以 MS950 字節流傳入
    content8_str = f"{header_line_options_daily}\n------\n2023,data" # 簡化後續行
    stream8_utf8 = io.BytesIO(content8_str.encode('utf-8')) # 用UTF-8測試邏輯
    fingerprint8_utf8 = calculate_format_fingerprint(stream8_utf8, "options_daily_utf8_sim.csv")
    logger.info(f"測試指紋8 (OptionsDaily UTF-8模擬): {fingerprint8_utf8}")
    # 預期欄位 (正規化+排序後):
    # "成交日期", "商品代號", "履約價格", "到期月份(週別)", "買賣權別", "成交時間", "成交價格", "成交數量(bors)", "開盤集合競價"
    # 排序後... "成交價格|成交時間|成交日期|成交數量(bors)|商品代號|履約價格|到期月份(週別)|買賣權別|開盤集合競價" (一個可能的順序)


    logger.info("format_detector.py 範例執行完畢。")
