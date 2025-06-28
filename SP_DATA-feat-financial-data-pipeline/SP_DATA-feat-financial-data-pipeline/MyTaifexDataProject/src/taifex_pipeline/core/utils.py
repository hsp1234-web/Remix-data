# -*- coding: utf-8 -*-
"""
核心共用模듈：通用工具函式 (Utilities)

本模組包含專案中可能被多個不同部分共用的輔助函式。
例如：計算檔案或位元組內容的雜湊值、產生唯一的執行ID等。
"""
import hashlib
import uuid
from pathlib import Path
from typing import Union, Optional # 從 typing 導入 Optional

from .logger_setup import get_logger # 使用相對導入

logger = get_logger(__name__)

def generate_execution_id() -> str:
    """
    產生一個符合 UUID v4 標準的全域唯一執行 ID (Execution ID)。

    此 ID 可用於追蹤單次管線運行的所有相關日誌和操作。
    注意：`logger_setup.EXECUTION_ID` 在模組加載時已生成一個用於日誌的全局ID。
    此函式主要供其他需要獨立唯一ID的場景使用，或在需要時重新生成ID。

    Returns:
        str: 一個 UUID v4 格式的字串。
    """
    return str(uuid.uuid4())

def calculate_file_sha256(file_path: Union[str, Path]) -> Optional[str]:
    """
    計算指定檔案內容的 SHA256 雜湊值。

    此函式會以二進位模式讀取檔案，並分塊處理以有效處理大型檔案，
    避免一次性將整個檔案載入記憶體。

    Args:
        file_path (Union[str, Path]): 要計算雜湊值的檔案的完整路徑。
                                      可以是字串或 `pathlib.Path` 物件。

    Returns:
        Optional[str]: 檔案內容的 SHA256 雜湊值 (以十六進位字串表示)。
                       如果檔案不存在、無法讀取，或在計算過程中發生其他錯誤，
                       則記錄錯誤並返回 `None`。
    """
    try:
        path_obj = Path(file_path)
        if not path_obj.is_file():
            logger.error(f"計算檔案 SHA256 失敗：路徑 '{file_path}' 不存在或不是一個檔案。")
            return None

        sha256_hash = hashlib.sha256()
        with open(path_obj, "rb") as f:
            # 分塊讀取 (4KB per chunk) 以處理潛在的大型檔案
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        hex_digest = sha256_hash.hexdigest()
        logger.debug(f"檔案 '{path_obj.name}' (路徑: {file_path}) 的 SHA256 雜湊值: {hex_digest}")
        return hex_digest

    except IOError as e:
        logger.error(f"計算檔案 '{file_path}' SHA256 時發生 IO 錯誤: {e}", exc_info=True)
        return None
    except Exception as e: # 捕獲其他可能的非預期錯誤
        logger.error(f"計算檔案 '{file_path}' SHA256 時發生未預期錯誤: {e}", exc_info=True)
        return None

def calculate_bytes_sha256(data_bytes: bytes) -> str:
    """
    計算給定位元組串 (bytes string) 內容的 SHA256 雜湊值。

    Args:
        data_bytes (bytes): 要計算雜湊值的原始位元組內容。

    Returns:
        str: 位元組內容的 SHA256 雜湊值 (以十六進位字串表示)。
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data_bytes)
    hex_digest = sha256_hash.hexdigest()
    # 為避免日誌過長，只記錄 bytes 的摘要資訊
    logger.debug(f"位元組內容 (長度: {len(data_bytes)}) 的 SHA256 雜湊值: {hex_digest}")
    return hex_digest


# --- 範例使用 ---
if __name__ == "__main__":
    # from .logger_setup import setup_global_logger # 使用相對導入
    # import logging
    # setup_global_logger(log_level_console=logging.DEBUG)

    logger.info("開始執行 utils.py 範例...")

    # 1. 測試 generate_execution_id
    exec_id1 = generate_execution_id()
    exec_id2 = generate_execution_id()
    logger.info(f"產生的 Execution ID 1: {exec_id1}")
    logger.info(f"產生的 Execution ID 2: {exec_id2}")
    assert exec_id1 != exec_id2, "連續產生的 Execution ID 應該不同"

    # 2. 測試 calculate_file_sha256
    # 創建一個臨時檔案用於測試
    # 假設此 utils.py 位於 MyTaifexDataProject/src/taifex_pipeline/core/
    # 則專案根目錄是 Path(__file__).parent.parent.parent.parent
    project_root_for_test = Path(__file__).resolve().parents[3]
    temp_file_dir_for_test = project_root_for_test / "temp_test_data_utils" # 放在專案根目錄下的臨時資料夾
    temp_file_dir_for_test.mkdir(exist_ok=True)
    temp_file_path_for_test = temp_file_dir_for_test / "temp_file_for_sha256_test.txt"

    file_content_for_test = "這是Jules用於測試SHA256計算的檔案內容。\nHello, Taifex Pipeline!\n行尾不應有空白。"

    try:
        # 使用 'wb' 寫入 bytes，並明確使用 utf-8 編碼，以確保跨平台雜湊一致性
        with open(temp_file_path_for_test, "wb") as tf:
            tf.write(file_content_for_test.encode('utf-8'))

        file_hash_calculated = calculate_file_sha256(temp_file_path_for_test)
        logger.info(f"檔案 '{temp_file_path_for_test.name}' 的計算雜湊值: {file_hash_calculated}")

        # 手動計算一次以驗證 (使用相同的編碼和內容)
        expected_hash = hashlib.sha256(file_content_for_test.encode('utf-8')).hexdigest()
        logger.info(f"檔案 '{temp_file_path_for_test.name}' 的預期雜湊值: {expected_hash}")
        assert file_hash_calculated == expected_hash, "檔案雜湊值計算結果與預期不符"

        # 測試不存在的檔案
        non_existent_file_hash = calculate_file_sha256(project_root_for_test / "non_existent_file.txt")
        assert non_existent_file_hash is None, "對不存在的檔案計算雜湊應返回 None"
        logger.info(f"對不存在檔案計算雜湊的結果 (預期 None): {non_existent_file_hash}")

    except Exception as e:
        logger.error(f"utils.py 檔案雜湊測試過程中發生錯誤: {e}", exc_info=True)
    finally:
        # 清理臨時檔案和目錄
        if temp_file_path_for_test.exists():
            temp_file_path_for_test.unlink()
        if temp_file_dir_for_test.exists():
            try:
                temp_file_dir_for_test.rmdir() # 只有當目錄為空時才能成功
                logger.info(f"已清理臨時測試目錄: {temp_file_dir_for_test}")
            except OSError: # 目錄可能不為空 (例如日誌或其他工具產生檔案)
                logger.warning(f"臨時測試目錄 {temp_file_dir_for_test} 不為空，未刪除。")


    # 3. 測試 calculate_bytes_sha256
    byte_content_for_test = b"Test bytes for SHA256 calculation by Jules."
    bytes_hash_calculated = calculate_bytes_sha256(byte_content_for_test)
    expected_bytes_hash = hashlib.sha256(byte_content_for_test).hexdigest()
    logger.info(f"位元組內容 '{byte_content_for_test!r}' 的計算雜湊值: {bytes_hash_calculated}")
    logger.info(f"位元組內容 '{byte_content_for_test!r}' 的預期雜湊值: {expected_bytes_hash}")
    assert bytes_hash_calculated == expected_bytes_hash, "位元組內容雜湊值計算結果與預期不符"

    logger.info("\n核心工具函式模組 (utils.py) 範例執行完畢。")

[end of MyTaifexDataProject/src/taifex_pipeline/core/utils.py]
