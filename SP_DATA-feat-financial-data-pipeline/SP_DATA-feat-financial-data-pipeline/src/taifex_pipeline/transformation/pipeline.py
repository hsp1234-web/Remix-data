import logging
import importlib
import pandas as pd
from io import BytesIO, StringIO
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING, Dict, Any, List, Optional
from datetime import datetime, timezone # 確保導入 timezone

if TYPE_CHECKING:
    from taifex_pipeline.transformation.format_detector import FormatDetector
    # from taifex_pipeline.database.constants import FileStatus

from taifex_pipeline.database.db_manager import DBManager

logger = logging.getLogger("taifex_pipeline.transformation.pipeline")

DEFAULT_STATUS_RAW_INGESTED = "RAW_INGESTED"
DEFAULT_STATUS_TRANSFORMED_SUCCESS = "TRANSFORMED_SUCCESS"
DEFAULT_STATUS_QUARANTINED = "QUARANTINED"
DEFAULT_STATUS_TRANSFORMATION_FAILED = "TRANSFORMATION_FAILED"

class TransformationPipeline:
    def __init__(self,
                 db_path: str,
                 format_detector: 'FormatDetector',
                 format_catalog: Dict[str, Any],
                 max_workers: Optional[int] = None):
        if not db_path: raise ValueError("資料庫路徑 (db_path) 不能為空。")
        if format_detector is None: raise ValueError("FormatDetector 實例不能為 None。")
        if format_catalog is None: raise ValueError("Format Catalog 不能為 None。")

        self.db_path = db_path
        self.format_detector = format_detector
        self.format_catalog = format_catalog
        self.max_workers = max_workers

        try:
            from taifex_pipeline.database.constants import FileStatus
            self.STATUS_RAW_INGESTED = FileStatus.RAW_INGESTED.value
            self.STATUS_TRANSFORMED_SUCCESS = FileStatus.TRANSFORMED_SUCCESS.value
            self.STATUS_QUARANTINED = FileStatus.QUARANTINED.value
            self.STATUS_TRANSFORMATION_FAILED = FileStatus.TRANSFORMATION_FAILED.value
        except ImportError:
            logger.warning("無法從 taifex_pipeline.database.constants 匯入 FileStatus Enum...")
            # 使用已定義的 DEFAULT_* 值

        logger.info(f"TransformationPipeline 初始化完成。DB Path: {self.db_path}, "
                    f"FormatDetector: {type(format_detector).__name__}, "
                    f"Catalog entries: {len(self.format_catalog)}, "
                    f"Max Workers: {self.max_workers or 'Default'}")

    def run(self):
        logger.info("開始執行轉換管線...")
        db_manager_main = None
        try:
            db_manager_main = DBManager(self.db_path)
            if not hasattr(db_manager_main, 'get_manifest_records_by_status'):
                logger.error("DBManager 缺少 'get_manifest_records_by_status' 方法。")
                raise NotImplementedError("DBManager 必須實作 get_manifest_records_by_status 方法。")

            files_to_process = db_manager_main.get_manifest_records_by_status(self.STATUS_RAW_INGESTED)

            if not files_to_process:
                logger.info("在 manifest 中沒有找到狀態為 RAW_INGESTED 的檔案，轉換管線提前結束。")
                return

            logger.info(f"找到 {len(files_to_process)} 個狀態為 RAW_INGESTED 的檔案待處理。")
            results = []
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                logger.info(f"使用 ProcessPoolExecutor (max_workers={self.max_workers or '預設'}) 提交 {len(files_to_process)} 個檔案。")
                future_to_file_info = { # 使用 file_info 而不是僅 file_hash，方便獲取更多信息
                    executor.submit(TransformationPipeline._process_file_worker,
                                    file_info, self.db_path, self.format_detector,
                                    self.format_catalog, # 傳遞整個 catalog
                                    {
                                        'STATUS_QUARANTINED': self.STATUS_QUARANTINED,
                                        'STATUS_TRANSFORMATION_FAILED': self.STATUS_TRANSFORMATION_FAILED,
                                        'STATUS_TRANSFORMED_SUCCESS': self.STATUS_TRANSFORMED_SUCCESS
                                    }
                                   ): file_info # 鍵是 future，值是完整的 file_info
                    for file_info in files_to_process
                }

                for future in as_completed(future_to_file_info):
                    original_file_info = future_to_file_info[future]
                    file_hash_completed = original_file_info.get('file_hash', 'UNKNOWN_HASH')
                    try:
                        result = future.result()
                        results.append(result)
                        logger.info(f"檔案 {file_hash_completed} (原始路徑: {original_file_info.get('original_path')}) 處理完成。狀態: {result.get('status', 'UNKNOWN')}")
                    except Exception as exc:
                        logger.error(f"檔案 {file_hash_completed} (原始路徑: {original_file_info.get('original_path')}) 在平行處理中產生未捕獲異常: {exc}", exc_info=True)
                        # 即使 worker 內部有 try-except，這裡的 except 也能捕獲 submit/result 本身的錯誤或 worker 中未被捕獲的嚴重錯誤
                        results.append({
                            'file_hash': file_hash_completed,
                            'status': self.STATUS_TRANSFORMATION_FAILED,
                            'error_message': f"Critical error in worker or task submission: {exc}",
                            'processed_rows': 0,
                            'transformation_start_timestamp': datetime.now(timezone.utc).isoformat(), # 記錄大致的失敗時間
                            'transformation_end_timestamp': datetime.now(timezone.utc).isoformat()
                        })

            logger.info(f"所有 {len(files_to_process)} 個檔案的平行處理階段已完成。共收到 {len(results)} 個結果。")

            if not hasattr(db_manager_main, 'update_manifest_transformation_status'):
                logger.error("DBManager 缺少 'update_manifest_transformation_status' 方法。無法更新 manifest。")
                raise NotImplementedError("DBManager 必須實作 update_manifest_transformation_status 方法。")

            self._update_manifest_with_results(db_manager_main, results)

        except NotImplementedError as nie: # 捕獲 DBManager 方法缺失的錯誤
            logger.error(f"TransformationPipeline run 方法因 DBManager 功能未實現而中止: {nie}", exc_info=True)
            # 這裡不應再呼叫 db_manager_main.close() 因為它可能未成功初始化
            # 但 finally 區塊會處理 db_manager_main (如果它被賦值過)
        except Exception as e:
            logger.error(f"TransformationPipeline run 方法執行期間發生未預期錯誤: {e}", exc_info=True)
        finally:
            if db_manager_main: # 只有在 db_manager_main 成功初始化後才嘗試關閉
                db_manager_main.close()
                logger.info("主進程 DBManager 連線已關閉。")
        logger.info("轉換管線執行完畢。")

    def _update_manifest_with_results(self, db_manager_instance: DBManager, results: List[Dict[str, Any]]):
        if not results:
            logger.info("沒有處理結果需要更新到 manifest。")
            return
        logger.info(f"開始將 {len(results)} 個處理結果更新到 manifest...")
        s_count, q_count, f_count = 0, 0, 0
        for result in results:
            file_hash = result.get('file_hash')
            status_val = result.get('status')
            if not file_hash or not status_val:
                logger.error(f"結果缺少 file_hash 或 status，無法更新 manifest: {result}")
                f_count +=1 # 將此視為一種失敗
                continue

            # 從 result 中提取 DBManager.update_manifest_transformation_status 所需的參數
            params_for_update = {
                'file_hash': file_hash,
                'status': status_val,
                'error_message': result.get('error_message'),
                'processed_rows': result.get('processed_rows'),
                'transformation_start_timestamp': result.get('transformation_start_timestamp'),
                'transformation_end_timestamp': result.get('transformation_end_timestamp'),
                'target_table': result.get('target_table'), # 額外信息，DBM 可能會用到
                'recipe_id': result.get('recipe_id')      # 額外信息
            }
            # 過濾掉值為 None 的參數，除非 DBManager 的方法明確可以處理它們
            params_for_update = {k: v for k, v in params_for_update.items() if v is not None}

            try:
                db_manager_instance.update_manifest_transformation_status(**params_for_update)
                if status_val == self.STATUS_TRANSFORMED_SUCCESS: s_count += 1
                elif status_val == self.STATUS_QUARANTINED: q_count += 1
                elif status_val == self.STATUS_TRANSFORMATION_FAILED: f_count += 1
                logger.debug(f"Manifest 更新：檔案 {file_hash} 狀態更新為 {status_val}。")
            except Exception as e:
                logger.error(f"更新檔案 {file_hash} 在 manifest 中的狀態為 {status_val} 時發生錯誤: {e}", exc_info=True)
                f_count +=1
        logger.info(f"--- 轉換結果摘要 ---\n成功: {s_count}, 隔離: {q_count}, 失敗(含更新失敗): {f_count}\n----------------------")

    @staticmethod
    def _static_parse_raw_content(raw_content: bytes, recipe: Dict[str, Any], default_encodings: List[str]) -> pd.DataFrame:
        parser_type = recipe.get('parser_type', 'csv').lower()
        parser_config = recipe.get('parser_config', {}).copy()
        data_io = BytesIO(raw_content)
        worker_logger_prefix = f"[ParserWorker FT:{parser_type}]" # 簡化前綴
        logger.debug(f"{worker_logger_prefix} Config: {parser_config}")

        detected_encoding_in_recipe = recipe.get('_debug_metadata', {}).get('detected_encoding')
        encoding_to_try = parser_config.pop('encoding', detected_encoding_in_recipe or (default_encodings[0] if default_encodings else 'utf-8'))

        try:
            if parser_type == 'csv':
                decoded_content = raw_content.decode(encoding_to_try)
                data_io = StringIO(decoded_content)
                df = pd.read_csv(data_io, **parser_config)
            elif parser_type == 'excel':
                df = pd.read_excel(data_io, **parser_config)
            elif parser_type == 'fixed_width':
                decoded_content = raw_content.decode(encoding_to_try)
                data_io = StringIO(decoded_content)
                df = pd.read_fwf(data_io, **parser_config)
            else:
                raise ValueError(f"不支援的 parser_type: {parser_type}")
            logger.info(f"{worker_logger_prefix} 解析完成，DataFrame shape: {df.shape}, 使用編碼: {encoding_to_try if parser_type != 'excel' else 'N/A for Excel'}")
            return df
        except UnicodeDecodeError as ude:
            logger.error(f"{worker_logger_prefix} 使用編碼 '{encoding_to_try}' 解碼失敗: {ude}")
            raise
        except Exception as e:
            logger.error(f"{worker_logger_prefix} 解析時 (編碼: {encoding_to_try if parser_type != 'excel' else 'N/A for Excel'}, config: {parser_config}) 發生錯誤: {e}")
            raise

    @staticmethod
    def _process_file_worker(file_info: Dict[str, Any],
                             db_path: str,
                             format_detector_instance: 'FormatDetector',
                             format_catalog_instance: Dict[str, Any], # 雖然 detector 內部有，但依賴 run 的傳遞
                             statuses: Dict[str,str]
                            ) -> Dict[str, Any]:
        file_hash = file_info.get('file_hash', 'UNKNOWN_HASH') # 提供預設值
        original_path = file_info.get('original_path', 'N/A')
        worker_logger_prefix = f"[Worker H:{file_hash[:8]} P:{original_path}]"

        start_time_iso = datetime.now(timezone.utc).isoformat()
        end_time_iso = start_time_iso # 預設結束時間
        processed_rows = 0

        status_quarantined = statuses['STATUS_QUARANTINED']
        status_failed = statuses['STATUS_TRANSFORMATION_FAILED']
        status_success = statuses['STATUS_TRANSFORMED_SUCCESS']

        result_payload = {
            'file_hash': file_hash,
            'status': status_failed, # 預設失敗
            'error_message': None,
            'processed_rows': 0,
            'transformation_start_timestamp': start_time_iso,
            'transformation_end_timestamp': start_time_iso, # 初始化
            'target_table': None,
            'recipe_id': None
        }

        local_db_manager = None
        try:
            logger.info(f"{worker_logger_prefix} 開始處理。")
            local_db_manager = DBManager(db_path)

            if not hasattr(local_db_manager, 'get_raw_file_content'):
                raise NotImplementedError("DBManager 必須實作 get_raw_file_content 方法。")
            raw_content = local_db_manager.get_raw_file_content(file_hash)

            if raw_content is None:
                result_payload['error_message'] = "Raw content not found in database"
                logger.error(f"{worker_logger_prefix} {result_payload['error_message']}")
                return result_payload # 結束時間已是 start_time_iso

            logger.debug(f"{worker_logger_prefix} 開始偵測格式...")
            # 使用傳入的 format_detector_instance，它應該是用主進程的 format_catalog 初始化的
            recipe = format_detector_instance.get_recipe(raw_content)

            if recipe is None:
                result_payload['status'] = status_quarantined
                result_payload['error_message'] = "No matching recipe found by FormatDetector."
                logger.info(f"{worker_logger_prefix} {result_payload['error_message']}")
                end_time_iso = datetime.now(timezone.utc).isoformat()
                result_payload['transformation_end_timestamp'] = end_time_iso
                return result_payload

            result_payload['recipe_id'] = recipe.get('id', recipe.get('description', 'N/A'))
            logger.info(f"{worker_logger_prefix} 匹配到配方: '{result_payload['recipe_id']}'")

            df = TransformationPipeline._static_parse_raw_content(
                raw_content, recipe, format_detector_instance.try_encodings
            )

            cleaner_function_name = recipe.get('cleaner_function')
            if not cleaner_function_name: raise ValueError("配方中未指定 cleaner_function。")

            logger.debug(f"{worker_logger_prefix} 嘗試載入清洗函式: {cleaner_function_name}")
            try:
                cleaners_module = importlib.import_module('taifex_pipeline.transformation.cleaners')
                cleaner_function = getattr(cleaners_module, cleaner_function_name)
            except (ImportError, AttributeError) as import_err:
                raise ValueError(f"無法載入清洗函式: {cleaner_function_name}") from import_err

            logger.info(f"{worker_logger_prefix} 開始使用 '{cleaner_function_name}' 清洗數據...")
            cleaned_df = cleaner_function(df)
            processed_rows = len(cleaned_df)
            result_payload['processed_rows'] = processed_rows
            logger.info(f"{worker_logger_prefix} 清洗完成，共 {processed_rows} 行有效數據。")

            target_table = recipe.get('target_table')
            if not target_table: raise ValueError("配方中未指定 target_table。")
            result_payload['target_table'] = target_table

            if not hasattr(local_db_manager, 'load_dataframe_to_table'):
                raise NotImplementedError("DBManager 必須實作 load_dataframe_to_table 方法。")

            load_options = recipe.get('load_options', {'if_exists': 'append'})
            logger.info(f"{worker_logger_prefix} 準備將數據載入到表格 '{target_table}' 使用選項: {load_options}")
            local_db_manager.load_dataframe_to_table(cleaned_df, target_table, load_options)
            logger.info(f"{worker_logger_prefix} 數據已成功載入到表格 '{target_table}'。")

            result_payload['status'] = status_success

        except NotImplementedError as nie:
            logger.error(f"{worker_logger_prefix} 處理過程中發生錯誤 (功能未實現): {nie}", exc_info=True)
            result_payload['error_message'] = f"NotImplementedError: {str(nie)}"
        except ValueError as ve:
            logger.error(f"{worker_logger_prefix} 處理過程中發生配置錯誤: {ve}", exc_info=True)
            result_payload['error_message'] = f"ConfigurationError: {str(ve)}"
        except Exception as e:
            logger.error(f"{worker_logger_prefix} 處理過程中發生未預期錯誤: {e}", exc_info=True)
            result_payload['error_message'] = f"UnexpectedError: {str(e)}"
        finally:
            end_time_iso = datetime.now(timezone.utc).isoformat()
            result_payload['transformation_end_timestamp'] = end_time_iso
            if local_db_manager:
                local_db_manager.close()
                logger.info(f"{worker_logger_prefix} DBManager 連線已關閉。")
            logger.info(f"{worker_logger_prefix} 處理結束，最終狀態: {result_payload['status']}")

        return result_payload
