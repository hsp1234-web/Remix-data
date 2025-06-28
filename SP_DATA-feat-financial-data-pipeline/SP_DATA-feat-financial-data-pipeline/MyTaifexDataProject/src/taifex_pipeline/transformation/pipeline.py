# -*- coding: utf-8 -*-
"""
轉換管線 (Transformation Pipeline) 實現

負責從 raw_lake.db 讀取原始檔案，進行格式識別、解析、清洗，
並將結果載入到 processed_data.duckdb。
"""
import io
import time
import os
import importlib
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple, Optional, Callable

from taifex_pipeline.core.logger_setup import get_logger, EXECUTION_ID
from taifex_pipeline.core.config_loader import get_format_catalog
from taifex_pipeline.core.utils import calculate_bytes_sha256 # 雖然原始hash已有，但可備用
from taifex_pipeline.database import db_manager
from taifex_pipeline.transformation.format_detector import calculate_format_fingerprint
from taifex_pipeline.transformation.parsers import parse_file_stream_to_dataframe
# 清洗函式將被動態導入

logger = get_logger(__name__)

# --- 清洗函式動態導入輔助 ---
CLEANER_MODULE_BASE_PATH = "taifex_pipeline.transformation.cleaners"

def get_cleaner_function(function_name_str: str) -> Optional[Callable[[pd.DataFrame], pd.DataFrame]]:
    """
    根據函式名稱字串，動態導入並返回清洗函式。
    假設清洗函式都位於 CLEANER_MODULE_BASE_PATH 下的某個模組中。
    例如，如果 function_name_str 是 'example_cleaners.clean_daily_ohlcv_example_v1',
    則會嘗試從 taifex_pipeline.transformation.cleaners.example_cleaners 導入。
    如果 function_name_str 不包含 '.', 則假設它在 CLEANER_MODULE_BASE_PATH.default_cleaners (如果存在)
    或者需要一個更明確的模組與函式分離機制。

    為了簡化，我們這裡假設 format_catalog.json 中的 cleaner_function
    格式為 'module_name.function_name'，例如 'example_cleaners.clean_daily_ohlcv_example_v1'
    """
    if '.' not in function_name_str:
        logger.error(f"清洗函式名稱 '{function_name_str}' 格式不正確，應為 'module_name.function_name'。")
        return None

    module_name, func_name = function_name_str.rsplit('.', 1)
    full_module_path = f"{CLEANER_MODULE_BASE_PATH}.{module_name}"

    try:
        module = importlib.import_module(full_module_path)
        cleaner_func = getattr(module, func_name)
        if not callable(cleaner_func):
            logger.error(f"在模組 '{full_module_path}' 中找到的 '{func_name}' 不是一個可呼叫的函式。")
            return None
        logger.debug(f"成功動態導入清洗函式: {full_module_path}.{func_name}")
        return cleaner_func # type: ignore
    except ImportError:
        logger.error(f"無法導入清洗函式所在的模組: {full_module_path}", exc_info=True)
        return None
    except AttributeError:
        logger.error(f"在模組 '{full_module_path}' 中未找到清洗函式: {func_name}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"動態導入清洗函式 '{function_name_str}' 時發生未預期錯誤: {e}", exc_info=True)
        return None


# --- 單一檔案處理函式 (在 ProcessPoolExecutor 的 worker 中執行) ---
def process_single_file_worker(
    file_hash: str,
    # raw_content: bytes, # 直接傳遞內容以減少DB查詢或進程間序列化大型bytes的開銷
    # 或者讓 worker 自己去讀取，這樣可以避免主進程讀取所有檔案到記憶體
    # 此處選擇讓 worker 自己讀取
    file_name_for_log_hint: str = "UnknownFileFromWorker" # 提示檔名
) -> Dict[str, Any]:
    """
    處理單個檔案的轉換邏輯。此函式將在 ProcessPoolExecutor 的一個獨立進程中執行。

    Args:
        file_hash (str): 要處理的檔案的 SHA256 雜湊值。
        file_name_for_log_hint (str): 檔案的原始名稱提示，用於日誌。

    Returns:
        Dict[str, Any]: 處理結果字典，包含：
            'file_hash': str
            'status': str ('TRANSFORMATION_SUCCESS', 'TRANSFORMATION_FAILED', 'QUARANTINED')
            'fingerprint_hash': Optional[str]
            'target_table_name': Optional[str]
            'processed_row_count': Optional[int]
            'error_message': Optional[str]
            'transformation_timestamp_epoch': float
    """
    worker_logger = get_logger(f"worker.{os.getpid()}") # 為每個 worker 使用帶 PID 的 logger
    worker_logger.info(f"Worker (PID:{os.getpid()}) 開始處理檔案 (Hash: {file_hash[:10]}..., Hint: {file_name_for_log_hint})")

    result: Dict[str, Any] = {
        "file_hash": file_hash,
        "status": "TRANSFORMATION_FAILED", # 預設失敗
        "fingerprint_hash": None,
        "target_table_name": None,
        "processed_row_count": 0,
        "error_message": "Worker process initiated but did not complete.",
        "transformation_timestamp_epoch": time.time()
    }

    try:
        # 1. 從 raw_lake.db 讀取原始檔案內容
        raw_content = db_manager.get_raw_file_content(file_hash)
        if raw_content is None:
            result["error_message"] = f"無法從 Raw Lake 讀取檔案內容 (Hash: {file_hash[:10]}...)"
            worker_logger.error(result["error_message"])
            return result

        file_stream = io.BytesIO(raw_content)

        # 2. 計算格式指紋
        fingerprint = calculate_format_fingerprint(file_stream, file_name_for_log_hint)
        if fingerprint is None:
            result["status"] = "QUARANTINED"
            result["error_message"] = f"無法計算格式指紋 (Hash: {file_hash[:10]}...). 檔案被隔離。"
            worker_logger.warning(result["error_message"])
            return result
        result["fingerprint_hash"] = fingerprint

        # 3. 查詢格式指紋目錄獲取處理配方
        format_catalog = get_format_catalog() # 每個進程會有自己的 config_loader 快取
        recipe = format_catalog.get(fingerprint)

        if recipe is None:
            result["status"] = "QUARANTINED"
            result["error_message"] = (f"找不到指紋 '{fingerprint}' (Hash: {file_hash[:10]}...) "
                                       f"對應的處理配方。檔案被隔離。")
            worker_logger.warning(result["error_message"])
            return result

        worker_logger.info(f"檔案 (Hash: {file_hash[:10]}...) 匹配到指紋 '{fingerprint}'. "
                           f"配方: {recipe.get('description', 'N/A')}")
        result["target_table_name"] = recipe.get("target_table")

        # 4. 解析數據 (Parser)
        parser_config = recipe.get("parser_config", {})
        df_or_iterator = parse_file_stream_to_dataframe(file_stream, parser_config, file_name_for_log_hint)

        if df_or_iterator is None: # 解析失敗
            result["error_message"] = f"檔案解析失敗 (Hash: {file_hash[:10]}...)."
            worker_logger.error(result["error_message"])
            return result

        # 5. 驗證必要欄位 & 清洗 (Cleaner) & 載入 (Loader)
        cleaner_function_name = recipe.get("cleaner_function")
        if not cleaner_function_name:
            result["error_message"] = f"配方中未指定有效的 cleaner_function (Hash: {file_hash[:10]}...)."
            worker_logger.error(result["error_message"])
            return result

        cleaner_func = get_cleaner_function(cleaner_function_name)
        if cleaner_func is None: # 動態導入失敗
            result["error_message"] = f"無法動態導入清洗函式 '{cleaner_function_name}' (Hash: {file_hash[:10]}...)."
            worker_logger.error(result["error_message"])
            return result

        required_columns = recipe.get("required_columns", [])
        target_table = result["target_table_name"]
        if not target_table:
            result["error_message"] = f"配方中未指定 target_table (Hash: {file_hash[:10]}...)."
            worker_logger.error(result["error_message"])
            return result

        total_rows_processed_for_file = 0

        # 處理分塊或單一 DataFrame
        data_iterator = [df_or_iterator] if isinstance(df_or_iterator, pd.DataFrame) else df_or_iterator

        for i, chunk_df in enumerate(data_iterator):
            worker_logger.debug(f"處理檔案 (Hash: {file_hash[:10]}...) 的第 {i+1} 個數據塊...")
            if not isinstance(chunk_df, pd.DataFrame):
                 result["error_message"] = f"解析器返回了非 DataFrame 的數據塊 (類型: {type(chunk_df)})。"
                 worker_logger.error(result["error_message"])
                 return result # 中斷此檔案處理

            # 5a. 驗證必要欄位
            if required_columns:
                missing_cols = [col for col in required_columns if col not in chunk_df.columns]
                if missing_cols:
                    result["error_message"] = (f"數據塊中缺失必要欄位: {missing_cols} "
                                               f"(Hash: {file_hash[:10]}...).")
                    worker_logger.error(result["error_message"])
                    return result # 中斷此檔案處理

            # 5b. 執行清洗函式
            worker_logger.debug(f"對數據塊執行清洗函式 '{cleaner_function_name}'...")
            cleaned_chunk_df = cleaner_func(chunk_df)
            if not isinstance(cleaned_chunk_df, pd.DataFrame):
                result["error_message"] = (f"清洗函式 '{cleaner_function_name}' 未返回 DataFrame "
                                           f"(Hash: {file_hash[:10]}...).")
                worker_logger.error(result["error_message"])
                return result # 中斷此檔案處理

            if cleaned_chunk_df.empty:
                worker_logger.info(f"數據塊在清洗後為空 (Hash: {file_hash[:10]}...)，不載入此塊。")
                continue

            # 5c. 載入到 processed_data.duckdb
            worker_logger.debug(f"將清洗後的數據塊載入到目標表 '{target_table}'...")
            # 注意：db_manager.load_dataframe_to_processed_db 在多進程中使用時，
            # 每個進程會創建自己的 DuckDB 連接。
            # if_exists 策略應適用於多塊追加。第一塊可能是 'replace' 或 'append' (如果表已存在)，
            # 後續塊應總是 'append'。這裡簡化，假設 load_dataframe_to_processed_db 內部能處理。
            # 更穩健的做法是，如果分塊，主流程在所有塊處理完後，一次性寫入，或第一塊 'replace' 後續 'append'。
            # 此處暫時讓每一塊都嘗試 'append' (假設表已由第一塊創建或已存在)。
            # 如果是第一個 chunk，且配方建議 replace，則用 replace，否則用 append。
            # 這需要 worker 知道自己是不是第一個 chunk，或 load_dataframe_to_processed_db 更智能。
            # 簡化：總是使用 append，建表邏輯在 load_dataframe_to_processed_db 中處理。
            if not db_manager.load_dataframe_to_processed_db(cleaned_chunk_df, target_table, if_exists="append"):
                result["error_message"] = (f"將清洗後的數據塊載入到表 '{target_table}' 失敗 "
                                           f"(Hash: {file_hash[:10]}...).")
                worker_logger.error(result["error_message"])
                return result # 中斷此檔案處理

            total_rows_processed_for_file += len(cleaned_chunk_df)

        # 如果所有塊都成功處理
        result["status"] = "TRANSFORMATION_SUCCESS"
        result["processed_row_count"] = total_rows_processed_for_file
        result["error_message"] = None # 清除預設的錯誤訊息
        worker_logger.info(f"檔案 (Hash: {file_hash[:10]}...) 成功轉換並載入 {total_rows_processed_for_file} 行到 '{target_table}'。")

    except Exception as e:
        # 捕獲所有其他未預期錯誤
        error_msg = f"處理檔案 (Hash: {file_hash[:10]}...) 時發生未預期錯誤: {e}"
        worker_logger.error(error_msg, exc_info=True)
        result["status"] = "TRANSFORMATION_FAILED" # 確保狀態是失敗
        result["error_message"] = error_msg

    result["transformation_timestamp_epoch"] = time.time() # 更新為實際完成時間
    return result


class TransformationPipeline:
    """
    轉換管線類，負責協調整個轉換過程。
    """
    def __init__(self,
                 reprocess_quarantined: bool = False,
                 max_workers: Optional[int] = None):
        """
        初始化轉換管線。

        Args:
            reprocess_quarantined (bool): 是否重新處理狀態為 'QUARANTINED' 的檔案。
                                          預設為 False (處理 'RAW_INGESTED' 的檔案)。
            max_workers (Optional[int]): ProcessPoolExecutor 的最大工作進程數。
                                         如果為 None，則使用 os.cpu_count()。
        """
        self.reprocess_quarantined = reprocess_quarantined
        self.max_workers = max_workers if max_workers is not None else os.cpu_count()
        logger.info(f"轉換管線初始化。模式: {'重新處理隔離檔案' if reprocess_quarantined else '處理新汲取檔案'}。"
                    f"最大工作進程數: {self.max_workers}")
        db_manager.initialize_databases() # 確保資料庫和表存在

    def run(self) -> Tuple[int, int, int, int]:
        """
        執行完整的轉換管線流程。

        Returns:
            Tuple[int, int, int, int]:
                (待處理檔案總數, 成功轉換檔案數, 轉換失敗檔案數, 被隔離檔案數)
        """
        logger.info(f"轉換管線啟動 (Execution ID: {EXECUTION_ID})。")
        start_time = time.time()

        status_to_query = "QUARANTINED" if self.reprocess_quarantined else "RAW_INGESTED"
        files_to_process_hashes = db_manager.get_files_by_status(status_to_query)

        if not files_to_process_hashes:
            logger.info(f"在 Manifest 中未找到狀態為 '{status_to_query}' 的檔案進行轉換。")
            duration_empty = time.time() - start_time
            logger.info(f"轉換管線執行完畢 (耗時: {duration_empty:.2f} 秒)。無檔案處理。")
            return 0, 0, 0, 0

        total_files = len(files_to_process_hashes)
        logger.info(f"共找到 {total_files} 個狀態為 '{status_to_query}' 的檔案待轉換。")

        success_count = 0
        failed_count = 0
        quarantined_count = 0

        # 使用 ProcessPoolExecutor 進行平行處理
        # 注意：DuckDB 連接在多進程中的處理。每個進程內的 db_manager 函式會創建自己的連接。
        # 這對於讀取是安全的。對於寫入 processed_data.duckdb，如果多個進程同時寫入同一個表，
        # DuckDB 本身支持並行寫入，但要注意事務和鎖的機制。
        # 另一種策略是 worker 只返回處理好的 DataFrame，由主進程統一寫入，但這會增加主進程負擔和記憶體。
        # 目前的 db_manager.load_dataframe_to_processed_db 每次都獲取連接，適合多進程。
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # 為每個 file_hash 提交一個任務，同時傳遞原始檔名作為日誌提示
            # 我們需要從 manifest 獲取原始檔名提示
            futures_map: Dict[Any, Tuple[str, str]] = {} # Future -> (file_hash, original_path_hint)

            for file_hash_to_proc in files_to_process_hashes:
                manifest_record = db_manager.get_manifest_record(file_hash_to_proc)
                original_path_hint = manifest_record.get("original_file_path", "N/A") if manifest_record else "N/A"

                future = executor.submit(process_single_file_worker, file_hash_to_proc, original_path_hint)
                futures_map[future] = (file_hash_to_proc, original_path_hint)

            for i, future_result in enumerate(as_completed(futures_map)):
                file_hash_completed, path_hint_completed = futures_map[future_result]
                logger.info(f"處理進度: {i+1}/{total_files} (檔案 Hash: {file_hash_completed[:10]}..., Path: {path_hint_completed})")
                try:
                    worker_output: Dict[str, Any] = future_result.result()

                    # 更新 Manifest
                    db_manager.update_manifest_record(
                        file_hash=worker_output["file_hash"],
                        status=worker_output["status"],
                        fingerprint_hash=worker_output.get("fingerprint_hash"), # worker 可能未設定
                        transformation_timestamp_epoch=worker_output["transformation_timestamp_epoch"],
                        target_table_name=worker_output.get("target_table_name"),
                        processed_row_count=worker_output.get("processed_row_count"),
                        error_message=worker_output.get("error_message"),
                        pipeline_execution_id=EXECUTION_ID # 主流程的 EXECUTION_ID
                    )

                    if worker_output["status"] == "TRANSFORMATION_SUCCESS":
                        success_count += 1
                    elif worker_output["status"] == "QUARANTINED":
                        quarantined_count += 1
                    else: # TRANSFORMATION_FAILED
                        failed_count += 1

                except Exception as exc:
                    failed_count += 1
                    logger.error(f"處理檔案 (Hash: {file_hash_completed[:10]}...) 的 worker 引發未捕獲的例外: {exc}", exc_info=True)
                    db_manager.update_manifest_record(
                        file_hash=file_hash_completed,
                        status="TRANSFORMATION_FAILED",
                        error_message=f"Worker process raised unhandled exception: {str(exc)[:500]}", # 限制錯誤訊息長度
                        transformation_timestamp_epoch=time.time(),
                        pipeline_execution_id=EXECUTION_ID
                    )

        duration = time.time() - start_time
        logger.info(f"轉換管線執行完畢 (耗時: {duration:.2f} 秒)。")
        logger.info(f"總共處理 {total_files} 個檔案。")
        logger.info(f"  成功轉換: {success_count} 個。")
        logger.info(f"  轉換失敗: {failed_count} 個。")
        logger.info(f"  被隔離 (新格式或無法處理): {quarantined_count} 個。")

        return total_files, success_count, failed_count, quarantined_count


# --- 範例使用 ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # setup_global_logger(log_level_console=logging.DEBUG)
    logger.info("開始執行 transformation_pipeline.py 範例...")

    # --- 準備測試數據 ---
    # 1. 清理並初始化資料庫
    project_r_tp = Path(__file__).resolve().parent.parent.parent.parent
    db_path_raw_m_tp = project_r_tp / db_manager.DEFAULT_DATA_DIR / db_manager.RAW_LAKE_SUBDIR / db_manager.RAW_LAKE_DB_NAME
    db_path_proc_tp = project_r_tp / db_manager.DEFAULT_DATA_DIR / db_manager.PROCESSED_SUBDIR / db_manager.PROCESSED_DB_NAME
    if db_path_raw_m_tp.exists(): db_path_raw_m_tp.unlink()
    if db_path_proc_tp.exists(): db_path_proc_tp.unlink()
    db_manager.initialize_databases()

    # 2. 準備 format_catalog.json
    catalog_path_tp = project_r_tp / "config" / "format_catalog.json"
    catalog_path_tp.parent.mkdir(exist_ok=True)

    # 假設的每日行情資料 (OHLCV)
    # 正規化後欄位: ['close', 'date', 'high', 'low', 'open', 'product_id', 'volume']
    # 合併字串 (假設排序後): "close|date|high|low|open|product_id|volume"
    # 此處的指紋需要與 format_detector.py 中的實際計算結果匹配
    # 為了簡化，我們手動指定一個指紋，並確保測試數據的標頭能產生此指紋
    # 標頭: "date,product_id,open,high,low,close,volume"
    # 正規化: "date", "product_id", "open", "high", "low", "close", "volume" (全小寫)
    # 排序: "close", "date", "high", "low", "open", "product_id", "volume"
    # 合併: "close|date|high|low|open|product_id|volume"
    ohlcv_header_str_sorted_joined = "close|date|high|low|open|product_id|volume"
    FINGERPRINT_OHLCV = hashlib.sha256(ohlcv_header_str_sorted_joined.encode('utf-8')).hexdigest()

    # 另一個格式，假設是三大法人資料
    # 標頭: "日期,機構類別,買方口數,賣方口數"
    # 正規化: "日期", "機構類別", "買方口數", "賣方口數"
    # 排序: "日期", "機構類別", "買方口數", "賣方口數" (假設Unicode排序結果)
    # 合併: "日期|機構類別|買方口數|賣方口數"
    inst_inv_header_str_sorted_joined = "日期|機構類別|買方口數|賣方口數"
    FINGERPRINT_INST_INV = hashlib.sha256(inst_inv_header_str_sorted_joined.encode('utf-8')).hexdigest()

    sample_catalog_data = {
        FINGERPRINT_OHLCV: {
            "description": "範例每日行情CSV (OHLCV)",
            "target_table": "fact_daily_ohlcv",
            "parser_config": {"sep": ",", "header": 0, "encoding": "utf-8",
                              "dtype": {"open":str, "high":str, "low":str, "close":str, "volume":str}}, # 讓cleaner處理轉型
            "cleaner_function": "example_cleaners.clean_daily_ohlcv_example_v1", # 指向我們創建的清洗函式
            "required_columns": ["date", "product_id", "open", "high", "low", "close", "volume"] # 清洗後的欄位名
        },
        FINGERPRINT_INST_INV: {
            "description": "範例三大法人買賣超CSV",
            "target_table": "fact_institutional_trades",
            "parser_config": {"sep": ",", "header": 0, "encoding": "utf-8"},
            "cleaner_function": "example_cleaners.another_cleaner_example", # 假設這個 cleaner 適用
            "required_columns": ["日期", "機構類別", "買方口數", "賣方口數"] # 假設清洗後還是這些中文名
        }
    }
    with open(catalog_path_tp, 'w', encoding='utf-8') as f:
        json.dump(sample_catalog_data, f, indent=2, ensure_ascii=False)
    logger.info(f"已創建範例 format_catalog.json 於 {catalog_path_tp}")
    clear_config_cache() # 確保 config_loader 能讀到最新的

    # 3. 準備 raw_lake.db 和 manifest.db 中的數據
    # 檔案1: 符合 FINGERPRINT_OHLCV
    ohlcv_content_str = "date,product_id,open,high,low,close,volume\n" \
                        "20230101,TXF,14000,14050,13980,14020,12345\n" \
                        "112/03/15,MXF,14010,14060,13990,14030,54321"
    ohlcv_content_bytes = ohlcv_content_str.encode('utf-8')
    ohlcv_hash = calculate_bytes_sha256(ohlcv_content_bytes)
    db_manager.store_raw_file(ohlcv_hash, ohlcv_content_bytes)
    db_manager.update_manifest_record(ohlcv_hash, "/test/ohlcv.csv", "RAW_INGESTED", ingestion_timestamp_epoch=time.time())

    # 檔案2: 符合 FINGERPRINT_INST_INV
    inst_inv_content_str = "日期,機構類別,買方口數,賣方口數\n" \
                           "2023/01/01,自營商,1000,500\n" \
                           "2023/01/01,投信,200,800"
    inst_inv_content_bytes = inst_inv_content_str.encode('utf-8')
    inst_inv_hash = calculate_bytes_sha256(inst_inv_content_bytes)
    db_manager.store_raw_file(inst_inv_hash, inst_inv_content_bytes)
    db_manager.update_manifest_record(inst_inv_hash, "/test/inst_inv.csv", "RAW_INGESTED", ingestion_timestamp_epoch=time.time())

    # 檔案3: 格式未知 (標頭不同)
    unknown_content_str = "col_x,col_y,col_z\ndata1,data2,data3"
    unknown_content_bytes = unknown_content_str.encode('utf-8')
    unknown_hash = calculate_bytes_sha256(unknown_content_bytes)
    db_manager.store_raw_file(unknown_hash, unknown_content_bytes)
    db_manager.update_manifest_record(unknown_hash, "/test/unknown.csv", "RAW_INGESTED", ingestion_timestamp_epoch=time.time())

    # 檔案4: 內容損毀或解析/清洗會失敗 (例如，cleaner期望的欄位不存在)
    bad_ohlcv_content_str = "date,product_id,open_price,high_price,low_price,close_price,vol\n" \
                            "20230102,ABC,10,11,9,10,100" # 欄位名與cleaner期望的不同
    bad_ohlcv_content_bytes = bad_ohlcv_content_str.encode('utf-8')
    bad_ohlcv_hash = calculate_bytes_sha256(bad_ohlcv_content_bytes)
    db_manager.store_raw_file(bad_ohlcv_hash, bad_ohlcv_content_bytes)
    db_manager.update_manifest_record(bad_ohlcv_hash, "/test/bad_ohlcv.csv", "RAW_INGESTED", ingestion_timestamp_epoch=time.time())


    # --- 執行轉換管線 ---
    logger.info("\n--- 開始執行轉換管線 (處理 RAW_INGESTED) ---")
    pipeline = TransformationPipeline(max_workers=2) # 限制 worker 數量以便觀察
    total, success, failed, quarantined = pipeline.run()

    logger.info(f"轉換結果: Total={total}, Success={success}, Failed={failed}, Quarantined={quarantined}")
    assert total == 4
    assert success == 2 # ohlcv.csv, inst_inv.csv
    assert quarantined == 1 # unknown.csv
    assert failed == 1 # bad_ohlcv.csv (假設 cleaner 因欄位名不匹配而失敗或 required_columns 驗證失敗)

    # --- 驗證結果 ---
    logger.info("\n--- 驗證 Manifest 和 Processed DB ---")
    # 驗證 ohlcv.csv
    ohlcv_record = db_manager.get_manifest_record(ohlcv_hash)
    assert ohlcv_record and ohlcv_record["status"] == "TRANSFORMATION_SUCCESS"
    assert ohlcv_record["fingerprint_hash"] == FINGERPRINT_OHLCV
    assert ohlcv_record["target_table_name"] == "fact_daily_ohlcv"
    assert ohlcv_record["processed_row_count"] == 2
    conn_proc = db_manager.get_processed_data_connection()
    ohlcv_data_db = conn_proc.execute("SELECT COUNT(*) FROM fact_daily_ohlcv").fetchone()
    assert ohlcv_data_db and ohlcv_data_db[0] == 2
    logger.info("ohlcv.csv 轉換成功並已驗證。")

    # 驗證 inst_inv.csv
    inst_inv_record = db_manager.get_manifest_record(inst_inv_hash)
    assert inst_inv_record and inst_inv_record["status"] == "TRANSFORMATION_SUCCESS"
    # ... 其他 inst_inv 的斷言 ...
    inst_data_db = conn_proc.execute("SELECT COUNT(*) FROM fact_institutional_trades").fetchone()
    assert inst_data_db and inst_data_db[0] == 2
    logger.info("inst_inv.csv 轉換成功並已驗證。")

    # 驗證 unknown.csv
    unknown_record = db_manager.get_manifest_record(unknown_hash)
    assert unknown_record and unknown_record["status"] == "QUARANTINED"
    assert unknown_record["error_message"] is not None and "找不到指紋" in unknown_record["error_message"]
    logger.info("unknown.csv 已被正確隔離。")

    # 驗證 bad_ohlcv.csv
    bad_ohlcv_record = db_manager.get_manifest_record(bad_ohlcv_hash)
    assert bad_ohlcv_record and bad_ohlcv_record["status"] == "TRANSFORMATION_FAILED"
    assert bad_ohlcv_record["error_message"] is not None
    logger.info(f"bad_ohlcv.csv 轉換失敗，錯誤訊息: {bad_ohlcv_record['error_message']}")


    # --- 測試重新處理隔離檔案 (假設我們手動更新了 catalog) ---
    logger.info("\n--- 測試重新處理隔離檔案 ---")
    # 假設 unknown.csv 的指紋現在被添加到了 catalog
    unknown_fingerprint = calculate_format_fingerprint(io.BytesIO(unknown_content_bytes), "unknown.csv")
    assert unknown_fingerprint is not None

    updated_catalog = get_format_catalog() # 獲取當前 (可能是快取的) catalog
    updated_catalog[unknown_fingerprint] = { # type: ignore
        "description": "未知格式現已註冊",
        "target_table": "fact_unknown_data",
        "parser_config": {"sep": ",", "header": 0, "encoding": "utf-8"},
        "cleaner_function": "example_cleaners.another_cleaner_example", # 復用一個
        "required_columns": ["col_x", "col_y", "col_z"]
    }
    with open(catalog_path_tp, 'w', encoding='utf-8') as f: # 覆寫 catalog 檔案
        json.dump(updated_catalog, f, indent=2, ensure_ascii=False)
    clear_config_cache() # 清除快取以便 TransformationPipeline 讀取更新後的

    pipeline_reprocess = TransformationPipeline(reprocess_quarantined=True, max_workers=1)
    total_re, success_re, failed_re, quarantined_re = pipeline_reprocess.run()

    logger.info(f"隔離檔案重處理結果: Total={total_re}, Success={success_re}, Failed={failed_re}, Quarantined={quarantined_re}")
    assert total_re == 1 # 只有一個 unknown.csv 是 QUARANTINED
    assert success_re == 1

    unknown_record_reprocessed = db_manager.get_manifest_record(unknown_hash)
    assert unknown_record_reprocessed and unknown_record_reprocessed["status"] == "TRANSFORMATION_SUCCESS"
    unknown_data_db = conn_proc.execute("SELECT COUNT(*) FROM fact_unknown_data").fetchone()
    assert unknown_data_db and unknown_data_db[0] == 1
    logger.info("隔離檔案 unknown.csv 已成功重新處理。")


    logger.info("transformation_pipeline.py 範例執行成功！")

    db_manager.close_all_connections()
