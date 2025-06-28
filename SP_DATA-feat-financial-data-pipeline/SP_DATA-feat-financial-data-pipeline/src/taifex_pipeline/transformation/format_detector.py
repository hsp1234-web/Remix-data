import hashlib
import logging
import re # 新增 re 模組導入
from typing import Optional, List, Dict, Any, Tuple # 新增 Tuple 模組導入

# 取得 logger
logger = logging.getLogger("taifex_pipeline.transformation.format_detector")

class FormatDetector:
    """
    根據檔案標頭識別其格式，並從目錄中查找對應處理配方的類別。
    """

    DEFAULT_ENCODINGS = ['ms950', 'utf-8', 'utf-8-sig'] # 更新預設編碼列表
    DEFAULT_HEADER_READ_BYTES = 2048
    DEFAULT_MAX_HEADER_LINES = 20 # 增加預覽行數以提高標頭檢測機會

    def __init__(self,
                 format_catalog: Dict[str, Any], # 修改：直接接收 format_catalog
                 encodings: Optional[List[str]] = None,
                 header_read_bytes: Optional[int] = None,
                 max_header_lines: Optional[int] = None):
        """
        初始化 FormatDetector。

        Args:
            format_catalog (Dict[str, Any]): 已載入的「格式指紋目錄」。
            encodings (Optional[List[str]]): 解碼時嘗試的編碼列表。
                                             若為 None，則使用 DEFAULT_ENCODINGS。
            header_read_bytes (Optional[int]): 讀取檔案開頭多少位元組以尋找標頭。
                                               若為 None，則使用 DEFAULT_HEADER_READ_BYTES。
            max_header_lines (Optional[int]): 在讀取的 header_read_bytes 中最多嗅探多少行作為標頭。
                                             若為 None，則使用 DEFAULT_MAX_HEADER_LINES。
        """
        if not isinstance(format_catalog, dict):
            raise ValueError("format_catalog 必須是一個字典。")
        self.format_catalog = format_catalog # 保存 format_catalog

        self.try_encodings = encodings if encodings is not None else self.DEFAULT_ENCODINGS
        self.header_read_bytes = header_read_bytes if header_read_bytes is not None else self.DEFAULT_HEADER_READ_BYTES
        self.max_header_lines = max_header_lines if max_header_lines is not None else self.DEFAULT_MAX_HEADER_LINES

        logger.info(f"FormatDetector 初始化。嘗試編碼: {self.try_encodings}, "
                    f"標頭讀取位元組: {self.header_read_bytes}, 最大標頭行數: {self.max_header_lines}, "
                    f"目錄中配方數量: {len(self.format_catalog)}")

    def _find_header_row(self, content_lines: List[str]) -> Tuple[Optional[str], int]:
        """
        從檔案的前幾行中，透過啟發式規則找出最可能的標頭行。
        返回 (標頭行內容, 標頭行索引)。
        (源自原型驗證腳本)
        """
        candidates = []
        # 關鍵字列表可以考慮作為配置或常量
        keywords = ['日期', '契約', '商品', '身份別', '成交量', '收盤價', '買賣權',
                    '期貨', '選擇權', '總計', '序號', '代號', '名稱', '價格']

        for i, line in enumerate(content_lines):
            line = line.strip()
            if not line or line.startswith('---') or len(line) < 5: # 增加最短長度限制
                continue

            comma_count = line.count(',')
            # 考慮其他潛在分隔符，例如多個空格 (但需小心處理固定寬度檔案)
            # tab_count = line.count('\t')

            keyword_count = sum(1 for keyword in keywords if keyword in line)

            # 調整啟發式規則：
            # 1. 必須包含逗號 (CSV主要特徵) 或 包含多個關鍵字 (某些純文字列表型報告)
            # 2. 避免過多數字的行被誤認為標頭
            # 3. 欄位不應過長或過短 (啟發式)

            is_likely_header = False
            if comma_count > 2 and keyword_count > 0 : # 調整逗號數量和關鍵字要求
                 is_likely_header = True
            elif keyword_count > 3 and comma_count == 0: # 對於無逗號但多關鍵字的特殊情況
                 # 檢查是否大部分由非數字組成
                 if sum(c.isdigit() for c in line) / len(line) < 0.5:
                     is_likely_header = True

            if is_likely_header:
                # 分數可以更細緻，例如考慮欄位分佈的均勻性等
                score = (comma_count * 2) + (keyword_count * 5)
                # 懲罰包含過多連續數字的行，除非它們是日期格式的一部分
                if re.search(r'\d{5,}', line) and not re.search(r'\d{4}[/-]\d{1,2}[/-]\d{1,2}', line):
                    score -= 5
                candidates.append({'score': score, 'line': line, 'index': i})

        if not candidates:
            logger.debug("在預覽行中未找到符合啟發式規則的標頭候選。")
            return None, -1

        best_candidate = max(candidates, key=lambda x: x['score'])
        logger.debug(f"找到最佳標頭候選: 行 {best_candidate['index']+1}, 分數 {best_candidate['score']}, 內容: {repr(best_candidate['line'])}")
        return best_candidate['line'], best_candidate['index']

    def _calculate_fingerprint(self, header_line: str) -> str:
        """
        根據定義的規則，從標頭行計算出格式指紋。
        (源自原型驗證腳本，正規化規則調整以符合原型)
        """
        if not header_line: # 增加空行檢查
            logger.warning("傳入的 header_line 為空，無法計算指紋。")
            return ""

        # 1. 用逗號分割，並清除每個欄位的首尾空白
        columns = [col.strip() for col in header_line.split(',')]

        # 2. 清除內部所有空白並轉為小寫 (原型中的 re.sub(r'\s+', '', col))
        #    並過濾掉空字串欄位
        normalized_columns = [re.sub(r'\s+', '', col).lower() for col in columns if col.strip()]

        if not normalized_columns: # 如果正規化後沒有任何欄位
            logger.warning(f"標頭 '{header_line}' 正規化後無有效欄位，無法計算指紋。")
            return ""

        # 3. 依字母順序排序
        normalized_columns.sort()

        # 4. 使用 "|" 合併成單一字串
        fingerprint_string = "|".join(normalized_columns)

        # 5. 計算 SHA256 雜湊值
        sha256_hash = hashlib.sha256(fingerprint_string.encode('utf-8')).hexdigest()
        logger.debug(f"原始標頭: {repr(header_line)}, 正規化字串: '{fingerprint_string}', 計算指紋: {sha256_hash[:16]}...")
        return sha256_hash

    def get_recipe(self, file_content: bytes) -> Optional[Dict[str, Any]]:
        """
        接收檔案的原始二進位內容，嘗試找到並返回處理配方。
        整合了原型驗證腳本的核心邏輯。
        """
        logger.debug(f"開始使用 {len(self.format_catalog)} 個配方的目錄偵測檔案格式...")

        if not file_content:
            logger.warning("傳入的 file_content 為空，無法偵測格式。")
            return None
        if not self.format_catalog: # 檢查實例的 format_catalog
            logger.warning("FormatDetector 的 format_catalog 為空，無法查找配方。")
            return None

        header_data_blob = file_content[:self.header_read_bytes]
        decoded_lines: List[str] = []
        active_encoding: Optional[str] = None

        for encoding_attempt in self.try_encodings:
            try:
                decoded_header_text = header_data_blob.decode(encoding_attempt)
                decoded_lines = decoded_header_text.splitlines()[:self.max_header_lines]
                active_encoding = encoding_attempt
                logger.debug(f"成功使用編碼 '{active_encoding}' 解碼檔案頭部 {len(decoded_lines)} 行。")
                break
            except UnicodeDecodeError:
                logger.debug(f"使用編碼 '{encoding_attempt}' 解碼檔案頭部失敗。")
            except Exception as e: # 捕捉其他潛在解碼錯誤
                logger.warning(f"使用編碼 '{encoding_attempt}' 解碼時發生非預期錯誤: {e}")

        if not decoded_lines or active_encoding is None:
            logger.warning("無法使用任何指定編碼成功解碼檔案頭部，或解碼後無內容。")
            return None

        header_line, header_index = self._find_header_row(decoded_lines)

        if header_line is None:
            logger.info("在檔案預覽中找不到可識別的標頭行。")
            # 可以在此處增加更詳細的日誌，例如打印預覽的前幾行（如果未成功解碼，則打印 repr(header_data_blob[:100])）
            # logger.debug(f"預覽的前 {self.max_header_lines} 行 (或原始位元組):")
            # for i, line_content in enumerate(decoded_lines):
            #    logger.debug(f"  Line {i+1}: {repr(line_content)}")
            # if not decoded_lines:
            #    logger.debug(f"  Raw bytes preview: {repr(header_data_blob[:200])}")
            return None

        logger.info(f"偵測到最可能的標頭在第 {header_index + 1} 行 (基於 {active_encoding} 編碼): {repr(header_line)}")

        fingerprint = self._calculate_fingerprint(header_line)
        if not fingerprint: # 如果指紋計算失敗 (例如標頭正規化後為空)
            logger.warning(f"無法為偵測到的標頭 '{repr(header_line)}' 計算指紋。")
            return None

        logger.info(f"計算出的格式指紋為: {fingerprint}")

        recipe = self.format_catalog.get(fingerprint)

        if recipe:
            logger.info(f"成功！在目錄中找到配方: '{recipe.get('description', 'N/A')}' (指紋: {fingerprint[:16]}...)")
            # 創建配方副本並附加除錯元數據，與原型腳本行為一致
            # 這有助於下游追蹤和測試
            recipe_copy = recipe.copy()
            recipe_copy['_debug_metadata'] = {
                'detected_header_content': header_line,
                'detected_header_row_index': header_index,
                'detected_encoding': active_encoding,
                'calculated_fingerprint': fingerprint
            }
            return recipe_copy
        else:
            logger.info(f"警告：在目錄中找不到與指紋 '{fingerprint}' 對應的處理配方。")
            logger.debug(f"  (除錯資訊：原始標頭: {repr(header_line)}, 使用編碼: {active_encoding})")
            return None

# 移除舊的 if __name__ == '__main__': 區塊，因為現在這是個模組。
# 相關的簡易測試應轉移到單元測試檔案中。
