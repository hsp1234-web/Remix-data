
import os
import shutil
import time
import json
import yaml
from datetime import datetime
from pathlib import Path
import pytz # Added for timezone in report
import hashlib # Added for temp parquet naming for validator

import duckdb

# Removed direct imports of directory constants
from .core.constants import (
    KEY_STATUS,
    KEY_PATH,
    KEY_TABLE,
    KEY_REASON,
    KEY_RESULTS,
    KEY_FILE,
    KEY_COUNT,
    STATUS_SUCCESS,
    STATUS_ERROR,
    STATUS_GROUP_RESULT,
    STATUS_SKIPPED,
)
from .core import constants  # Added to allow access to constants like constants.KEY_STATUS
# from .data_transformer import DataTransformer # Added for new transformation stage
from .database_loader import DatabaseLoader
from .file_parser import FileParser
from .manifest_manager import ManifestManager
from .data_validator import Validator # Import Validator
from .utils.logger import setup_logger
from .utils.monitor import get_hardware_usage


class PipelineOrchestrator:
    """
    協調整個數據管線的執行流程。
    """

    def __init__(
        self,
        config_file_path: str, # New parameter for config file
        base_path: str,
        project_folder_name_override: str = None, # Allow override from main.py args
        database_name_override: str = None,       # Allow override from main.py args
        log_name_override: str = None,             # Allow override from main.py args
        target_zip_files: str = "", # Default to empty string
        debug_mode: bool = False,
        schemas_file_path: str = None, # Optional path for schemas
    ):
        """
        初始化協調器。

        Args:
            config_file_path (str): 設定檔 (config.yaml) 的路徑。
            base_path (str): 專案的基礎路徑 (例如 /content 或 /content/drive/MyDrive)。
            project_folder_name_override (str, optional): 覆寫設定檔中的專案資料夾名稱。
            database_name_override (str, optional): 覆寫設定檔中的資料庫檔案名稱。
            log_name_override (str, optional): 覆寫設定檔中的日誌檔案名稱。
            target_zip_files (str): 指定要處理的 ZIP 檔案列表（以逗號分隔），或為空。
            debug_mode (bool, optional): 是否啟用除錯模式。預設為 False。
            schemas_file_path (str, optional): schemas.json 的可選路徑。如果為 None，則使用預設路徑。
        """
        # --- 載入設定檔 ---
        self.config = self._load_config(config_file_path)

        # --- 參數設定 (優先使用覆蓋值，其次是設定檔，最後是預設值) ---
        self.project_folder_name = project_folder_name_override or self.config.get("project_folder", "MyTaifexDataProject")
        self.database_name = database_name_override or self.config.get("database_name", "processed_data.duckdb")
        self.log_name = log_name_override or self.config.get("log_name", "pipeline.log")
        self.directories_config = self.config.get("directories", {})
        self.local_workspace_root = Path(self.config.get("local_workspace", "/tmp/taifex_data_workspace"))
        # remote_base_path is the equivalent of the old base_path for GDrive, not read from main.py args anymore directly for this var
        self.remote_base_path = Path(self.config.get("remote_base_path", "/content/drive/MyDrive"))


        # --- 本地路徑設定 ---
        # base_path from main.py is now considered the root for the *remote* project folder if not using GDrive,
        # or the parent of MyDrive if using GDrive.
        # For local-first, the primary operations happen in local_workspace_root.
        self.local_project_path = self.local_workspace_root / self.project_folder_name
        self.local_input_path = self.local_project_path / self.directories_config.get("input", "00_input")
        self.local_processed_path = self.local_project_path / self.directories_config.get("processed", "01_processed")
        self.local_archive_path = self.local_project_path / self.directories_config.get("archive", "02_archive")
        self.local_quarantine_path = self.local_project_path / self.directories_config.get("quarantine", "03_quarantine")
        self.local_db_path = self.local_project_path / self.directories_config.get("db", "98_database")
        self.local_log_path = self.local_project_path / self.directories_config.get("log", "99_logs")
        self.local_database_file = self.local_db_path / self.database_name
        self.local_manifest_file = self.local_archive_path / constants.MANIFEST_FILE

        # --- 遠端路徑設定 ---
        # The 'base_path' argument passed to __init__ is used here to construct the root of the remote project.
        # This allows for flexibility (e.g. if --no-gdrive is used, base_path is /content)
        self.remote_project_path = Path(base_path) / self.project_folder_name # base_path is /content/drive/MyDrive or /content
        self.remote_input_path = self.remote_project_path / self.directories_config.get("input", "00_input")
        self.remote_processed_path = self.remote_project_path / self.directories_config.get("processed", "01_processed")
        self.remote_archive_path = self.remote_project_path / self.directories_config.get("archive", "02_archive")
        self.remote_quarantine_path = self.remote_project_path / self.directories_config.get("quarantine", "03_quarantine")
        self.remote_db_path = self.remote_project_path / self.directories_config.get("db", "98_database")
        self.remote_log_path = self.remote_project_path / self.directories_config.get("log", "99_logs")
        self.remote_database_file = self.remote_db_path / self.database_name
        self.remote_manifest_file = self.remote_archive_path / constants.MANIFEST_FILE

        self.debug_mode = debug_mode

        # Ensure local log path exists before setting up logger
        Path(self.local_log_path).mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger(self.local_log_path, self.log_name, debug_mode) # Use self.log_name

        # Call _setup_local_directories early, before other components are initialized
        # This ensures paths for DB, manifest, etc., are ready.
        self._setup_local_directories() # Moved this call earlier

        # Determine max_workers for parallel processing
        config_max_workers = self.config.get("max_workers")
        if config_max_workers is None:
            cpu_cores = os.cpu_count()
            calculated_workers = cpu_cores // 2 if cpu_cores and cpu_cores > 0 else 4
            self.max_workers = max(4, min(calculated_workers, 32))
            self.logger.info(f"max_workers 未在設定檔中指定或為 null，已自動設定為: {self.max_workers} (CPU核心數: {cpu_cores})")
        else:
            try:
                self.max_workers = int(config_max_workers)
                if self.max_workers <= 0:
                    self.logger.warning(f"設定檔中的 max_workers ({config_max_workers}) 不是正整數，將預設為 4。")
                    self.max_workers = 4
                self.logger.info(f"max_workers 從設定檔中讀取為: {self.max_workers}")
            except ValueError:
                self.logger.warning(f"設定檔中的 max_workers ('{config_max_workers}') 無法轉換為整數，將預設為 4。")
                self.max_workers = 4

        # _setup_directories will now primarily ensure local structure. Remote structure is assumed or handled by sync.
        # os.makedirs(self.local_db_path, exist_ok=True) # Done in _setup_local_directories

        # --- 載入 Schemas 設定 ---
        # Use provided schemas_file_path if available, otherwise default
        effective_schemas_file_path_str = None
        if schemas_file_path:
            effective_schemas_file_path_str = str(schemas_file_path)
            self.logger.info(f"使用提供的 schemas 路徑: '{effective_schemas_file_path_str}'")
        else:
            # Default path logic for schemas.json, relative to project root (which is base_path/project_folder)
            # Assuming config.yaml is at the root of the project_path for consistency.
            # If schemas.json is meant to be outside project_path, this needs adjustment.
            # For now, let's assume it's within the project structure, e.g., in a 'config' subfolder of project_path.
            # However, the original code put it outside the data_pipeline_v15 package,
            # in a 'config' folder at the repository root.
            # Let's keep that logic for now. The project_root is base_path.
            # No, project_root should be where pyproject.toml or .git is.
            # Let's assume config/schemas.json is relative to the project_path for now.
            # If config.yaml is at project_path, then schemas.json could be project_path/config/schemas.json

            # Re-evaluating the original logic: Path(__file__).parent.parent.parent.parent
            # This means if pipeline_orchestrator.py is in src/data_pipeline_v15/pipeline_orchestrator.py
            # then parent is src/data_pipeline_v15
            # parent.parent is src
            # parent.parent.parent is the repo root where config/schemas.json was.
            # This implies base_path might not be the repo root.
            # Let's clarify: if config.yaml is at repo root, then schemas.json is also at repo root in a config folder.
            # The `base_path` is where the `project_folder_name` is created.
            # So, if `base_path` is `/content/drive/MyDrive`, and `project_folder_name` is `MyTaifexDataProject`,
            # then `project_path` is `/content/drive/MyDrive/MyTaifexDataProject`.
            # If `config.yaml` is expected to be at `project_path/config.yaml`,
            # then `schemas.json` could be at `project_path/config/schemas.json`.
            # Or, if `config.yaml` is at the repo root (where `main.py` is), then `schemas.json` is `repo_root/config/schemas.json`.
            # The current `main.py` implies `CONFIG_FILE = "config.yaml"` is at the same level as `main.py`.
            # Let's assume the repo root is where `main.py` and `config.yaml` are.
            # `base_path` is for Colab/Drive specifics. The actual project structure is created *within* `base_path/_project_folder`.

            # Let's assume schemas.json is located relative to where config.yaml is.
            # If config_file_path is "config.yaml", then schemas.json is "config/schemas.json"
            # This seems the most consistent approach.
            config_dir = Path(config_file_path).parent
            default_schemas_path = config_dir / "config" / "schemas.json" # This implies config.yaml is NOT in the config folder
                                                                    # but schemas.json is in a subfolder 'config' relative to config.yaml's location
            # This was the previous structure: project_root / "config" / "schemas.json"
            # If config.yaml is at the root, then schemas.json is at ./config/schemas.json
            # This seems correct.

            current_processing_dir = Path(os.getcwd()) # Where the script is run from
            # If main.py runs from repo root, and config.yaml is there, then config_file_path is "config.yaml"
            # default_schemas_path would be "config/schemas.json"
            # This matches the original structure.

            default_schemas_path = Path("config") / "schemas.json" # Relative to where main.py is run
            effective_schemas_file_path_str = str(default_schemas_path)
            self.logger.info(f"使用預設的 schemas 路徑: '{effective_schemas_file_path_str}'")


        self.schemas_config = {}
        if os.path.exists(effective_schemas_file_path_str):
            try:
                with open(effective_schemas_file_path_str, "r", encoding="utf-8") as f:
                    self.schemas_config = json.load(f)
                self.logger.info(f"成功從 '{effective_schemas_file_path_str}' 載入 schemas 設定。")
            except json.JSONDecodeError as e:
                self.logger.error(f"解析 schemas 設定檔 '{effective_schemas_file_path_str}' 失敗: {e}")
                self.schemas_config = {} # Keep it as an empty dict on error
            except Exception as e:
                self.logger.error(f"讀取 schemas 設定檔 '{effective_schemas_file_path_str}' 時發生其他錯誤: {e}")
                self.schemas_config = {} # Keep it as an empty dict on error
        else:
            self.logger.warning(f"Schemas 設定檔 '{effective_schemas_file_path_str}' 不存在。FileParser 將使用空設定。") # Changed to warning
            self.schemas_config = {}

        # --- 模組初始化 ---
        # ManifestManager and DatabaseLoader now operate on local paths
        # Local directories including DB path are now created by _setup_local_directories() above.
        self.manifest_manager = ManifestManager(manifest_path=self.local_manifest_file, logger=self.logger)
        self.file_parser = FileParser(self.manifest_manager, self.logger, self.schemas_config)
        self.db_loader = DatabaseLoader(self.local_database_file, self.logger) # Operates on local DB file

        # Initialize Validator
        validation_rules = self.config.get("validation_rules", {})
        self.validator = Validator(validation_rules, self.logger)

        # Initialize DataTransformer
        # self.data_transformer = DataTransformer(db_path=str(self.local_database_file)) # Commented out as DataTransformer is removed
        # self.logger.info(f"DataTransformer initialized with DB path: {self.local_database_file}") # Commented out

        # Initialize report_stats
        self.report_stats = {
            "execution_id": f"exec_{datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y%m%d%H%M%S%f')}_{os.getpid()}",
            "status": "UNKNOWN", # Will be SUCCESS, PARTIAL_SUCCESS, or FAILURE
            "start_time": None,
            "end_time": None,
            "total_duration_seconds": 0,
            "files_processed_total": 0,
            "files_successfully_parsed_and_validated_loaded": 0, # Renamed for clarity
            "files_with_quarantined_rows": 0, # Renamed
            "files_skipped_manifest": 0,
            "files_failed_parsing_or_other_error": 0,
            "rows_source_total_from_parsed_files": 0, # Renamed for clarity (pre-validation)
            "rows_added_to_main_tables": 0,
            "rows_added_to_quarantine_table": 0,
            "rows_skipped_on_load_due_to_conflict": 0
        }
        self.logger.info(f"管線執行 ID: {self.report_stats['execution_id']}")


        # --- 目標檔案處理 ---
        self.target_files = (
            [f.strip() for f in target_zip_files.split(",") if f.strip()]
            if target_zip_files
            else []
        )
        if self.target_files:
            self.logger.info(f"已指定處理特定檔案：{self.target_files}")
        else:
            self.logger.info("未指定特定檔案，將處理所有輸入檔案。")

    def _load_config(self, config_file_path: str) -> dict:
        """載入 YAML 設定檔"""
        try:
            with open(config_file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Logger might not be initialized yet if this is called early.
            # Consider logging this error after logger setup, or print.
            print(f"警告: 設定檔 {config_file_path} 未找到。將使用空的設定字典。")
            return {}
        except yaml.YAMLError as e:
            print(f"錯誤: 設定檔 {config_file_path} 解析失敗: {e}")
            return {}

    def _setup_local_directories(self):
        """建立所有必要的本地資料夾結構"""
        self.logger.info(f"正在設定本地工作區資料夾結構於: {self.local_project_path}")
        directories = [
            self.local_input_path,
            self.local_processed_path,
            self.local_archive_path,
            self.local_quarantine_path,
            self.local_db_path,
            self.local_log_path, # Already created for logger, but good to have in list
        ]
        # Create project root first
        self.local_project_path.mkdir(parents=True, exist_ok=True)
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"成功建立或確認本地目錄存在：{directory}")
            except OSError as e:
                self.logger.error(f"建立本地目錄失敗：{directory}，錯誤：{e}")
                raise # Re-raise to stop execution if essential dirs can't be made
        self.logger.info("✅ 本地工作區資料夾結構設定完成。")

    def _create_remote_directories_if_not_exist(self):
        """如果遠端目錄不存在，則建立它們"""
        self.logger.info(f"檢查/建立遠端專案資料夾結構於: {self.remote_project_path}")
        remote_dirs_to_check = [
            self.remote_project_path,
            self.remote_input_path,
            self.remote_processed_path,
            self.remote_archive_path,
            self.remote_quarantine_path,
            self.remote_db_path,
            self.remote_log_path,
        ]
        for remote_dir in remote_dirs_to_check:
            try:
                remote_dir.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"成功建立或確認遠端目錄存在：{remote_dir}")
            except Exception as e:
                # Log error but don't necessarily stop, as some might be non-critical (e.g. GDrive might auto-create)
                self.logger.error(f"嘗試建立遠端目錄 {remote_dir} 失敗: {e}. 可能需要手動建立。")


    def _sync_file(self, source_file: Path, dest_file: Path, direction: str):
        """同步單個檔案，如果來源存在。 direction 是 'to_local' 或 'to_remote'."""
        if source_file.exists():
            try:
                # Ensure destination directory exists
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_file, dest_file)
                self.logger.info(f"同步 ({direction}): '{source_file}' -> '{dest_file}'")
            except Exception as e:
                self.logger.error(f"同步 ({direction}) 檔案 '{source_file}' 至 '{dest_file}' 失敗: {e}")
        else:
            self.logger.info(f"同步 ({direction}): 來源檔案 '{source_file}' 不存在，跳過。")

    def _sync_directory_content(self, source_dir: Path, dest_dir: Path, direction: str):
        """同步目錄內容 (非遞迴，僅檔案)。 direction 是 'to_local' 或 'to_remote'."""
        if not source_dir.exists():
            self.logger.warning(f"同步 ({direction}): 來源目錄 '{source_dir}' 不存在，跳過。")
            return

        dest_dir.mkdir(parents=True, exist_ok=True) # Ensure destination directory exists

        for item in source_dir.iterdir():
            if item.is_file():
                self._sync_file(item, dest_dir / item.name, direction)
            # Optionally handle subdirectories if recursive sync is needed for some folders

    def _process_single_file(self, filename: str) -> dict:
        """
        處理單個檔案的解析階段（雜湊計算、manifest 檢查、檔案解析到 Parquet）。
        此方法設計為可在 ProcessPoolExecutor 中並行執行。
        """
        local_file_path = self.local_input_path / filename
        self.logger.info(f"並行處理中 - 開始檢查檔案: {filename} (路徑: {local_file_path})")

        file_hash = ManifestManager.get_file_hash(str(local_file_path))
        if not file_hash:
            self.logger.error(f"並行處理中 - 無法計算檔案雜湊值: {local_file_path}。將標記為錯誤。")
            # manifest_manager is not directly updatable from here in a process-safe way for the main manifest
            # This status will be recorded later by the main process.
            return {
                "filename": filename,
                "original_file_path": str(local_file_path),
                "file_hash": None,
                "status": constants.STATUS_ERROR,
                "message": f"無法計算檔案雜湊值 (路徑: {local_file_path})",
                "parquet_path": None,
                "table_name": None,
                "data_count": 0
            }

        # Note: Accessing self.manifest_manager.has_been_processed might be problematic if it relies on
        # an in-memory manifest that isn't shared/updated across processes.
        # For this reason, the check might be better done in the main thread before submitting,
        # or ensure ManifestManager is designed to be process-safe (e.g., re-reads manifest file).
        # For now, assume it's checked before submitting, or this check is okay for read-only.
        # If it's not okay, this check needs to move to main thread.
        # Let's assume for now that has_been_processed is based on the initially synced manifest and is safe.
        if self.manifest_manager.has_been_processed(file_hash):
            self.logger.warning(f"並行處理中 - 檔案 '{filename}' (雜湊: {file_hash}) 已被處理過，將跳過。")
            return {
                "filename": filename,
                "original_file_path": str(local_file_path),
                "file_hash": file_hash,
                "status": constants.STATUS_SKIPPED, # Mark as skipped
                "message": f"檔案 '{filename}' (雜湊: {file_hash}) 已被處理過。",
                "parquet_path": None,
                "table_name": None,
                "data_count": 0
            }

        self.logger.info(f"並行處理中 - 解析檔案: {filename}")
        try:
            # FileParser instance is part of 'self', its schema_config is loaded.
            # The processed_path argument to parse_file is where Parquet files are temporarily stored.
            # This should ideally be a unique temp location per process or use unique filenames if processes share it.
            # For now, assuming self.local_processed_path is okay if filenames are unique (which they are).
            # Or, parse_file itself handles unique temp file creation within that dir.
            parse_result = self.file_parser.parse_file(str(local_file_path), str(self.local_processed_path))
        except Exception as e:
            self.logger.error(f"並行處理中 - 檔案 '{filename}' 解析時發生未預期錯誤: {e}", exc_info=True)
            return {
                "filename": filename,
                "original_file_path": str(local_file_path),
                "file_hash": file_hash, # Hash was successful
                "status": constants.STATUS_ERROR,
                "message": f"檔案 '{filename}' 解析時發生未預期錯誤: {e}",
                "parquet_path": None, "table_name": None, "data_count": 0
            }

        # Adapt parse_result to the dictionary structure expected by the main loop
        return {
            "filename": filename,
            "original_file_path": str(local_file_path),
            "file_hash": file_hash,
            "status": parse_result.get(constants.KEY_STATUS, constants.STATUS_ERROR),
            "message": parse_result.get(constants.KEY_REASON, f"檔案 '{filename}' 處理時遇到未知狀況。"),
            constants.KEY_DATAFRAME: parse_result.get(constants.KEY_DATAFRAME), # Pass DataFrame
            constants.KEY_MATCHED_SCHEMA_NAME: parse_result.get(constants.KEY_MATCHED_SCHEMA_NAME), # Pass schema name
            # KEY_PATH is no longer provided by FileParser directly for the final valid parquet
            "table_name": parse_result.get(constants.KEY_TABLE), # DB target table name
            "data_count": parse_result.get(constants.KEY_COUNT, 0), # Original count from parser
            "sub_results": parse_result.get(constants.KEY_RESULTS)
        }

    def _dataframe_to_temp_parquet(self, df, temp_identifier: str, original_filename_for_hash: str) -> Path:
        """Helper to save a DataFrame to a uniquely named Parquet file in a temporary local staging area."""
        # Use a consistent staging area for these intermediate parquets
        temp_parquet_staging_dir = self.local_project_path / "temp_intermediate_parquets"
        temp_parquet_staging_dir.mkdir(parents=True, exist_ok=True)

        # Create a unique filename to avoid clashes if multiple DFs from same original file (e.g. valid/invalid splits)
        # Using time might lead to issues if called too quickly for the same file, hash of content might be better
        # but for simplicity, a hash of original filename + identifier + time should be reasonably unique.
        unique_hash_input = f"{original_filename_for_hash}_{temp_identifier}_{time.time_ns()}"
        file_hash = hashlib.sha256(unique_hash_input.encode()).hexdigest()
        parquet_path = temp_parquet_staging_dir / f"{file_hash}.parquet"

        df.to_parquet(parquet_path, engine="pyarrow", index=False)
        self.logger.info(f"DataFrame ({temp_identifier} for {original_filename_for_hash}) saved to temporary Parquet: {parquet_path}")
        return parquet_path

    def run(self):
        """執行完整數據管線，採用本地優先工作流程並行處理檔案解析。"""
        import concurrent.futures # Import here as it's specific to this method

        self.report_stats["start_time"] = datetime.now(pytz.timezone('Asia/Taipei')).isoformat()
        # Moved start_time here, as the original time.time() was for overall duration, not ISO start.

        if self.debug_mode:
            self.logger.debug(get_hardware_usage("管線啟動前 (本地優先, 並行)"))

        run_had_critical_failure = False # Flag to track if critical exception occurs
        start_time_perf = time.time() # For performance timing

        try:
            # self._setup_local_directories() # Moved to __init__
            self._create_remote_directories_if_not_exist() # Remote dirs can be checked/created at start of run

            self.logger.info("--- 開始啟動時同步 (遠端 -> 本地) ---")
            self._sync_directory_content(self.remote_input_path, self.local_input_path, "to_local")
            self._sync_file(self.remote_database_file, self.local_database_file, "to_local")
            self._sync_file(self.remote_manifest_file, self.local_manifest_file, "to_local")
            self.logger.info("--- 啟動時同步完成 ---")

            self.logger.info("正在載入處理紀錄 (manifest) 從本地...")
            self.manifest_manager.load_or_create_manifest()

            self.logger.info(f"正在掃描本地輸入資料夾：{self.local_input_path}")
            if not self.local_input_path.exists():
                self.logger.warning(f"本地輸入資料夾 {self.local_input_path} 不存在，無法處理。")
                return

            all_files_in_input = [f.name for f in self.local_input_path.iterdir() if f.is_file()]
            if self.target_files:
                files_to_process_names = [f for f in all_files_in_input if f in self.target_files]
            else:
                files_to_process_names = all_files_in_input

            self.logger.info(f"找到 {len(files_to_process_names)} 個檔案待處理。")
            self.report_stats["files_processed_total"] = len(files_to_process_names) # Set total files to process

            if not files_to_process_names:
                self.logger.warning("本地輸入資料夾中沒有找到任何要處理的檔案。")

            processed_file_results = []
            if files_to_process_names: # Only run executor if there are files
                self.logger.info(f"開始並行處理檔案解析，最大工作程序數: {self.max_workers}")

                # Close DB connection before ProcessPoolExecutor to avoid pickling issues
                if self.db_loader:
                    self.db_loader.close_connection()
                    self.logger.info("暫時關閉資料庫連線以進行並行處理。")

                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit tasks to the executor
                    future_to_filename = {executor.submit(self._process_single_file, filename): filename for filename in files_to_process_names}
                    for future in concurrent.futures.as_completed(future_to_filename):
                        filename = future_to_filename[future]
                        try:
                            result = future.result()
                            processed_file_results.append(result)
                        except Exception as exc:
                            self.logger.error(f"檔案 '{filename}' 在並行處理中產生例外: {exc}", exc_info=True)
                            processed_file_results.append({
                                "filename": filename,
                                "original_file_path": str(self.local_input_path / filename), # Store original path
                                "file_hash": None, # Or try to get if available
                                "status": constants.STATUS_ERROR,
                                "message": f"並行執行時發生錯誤: {exc}",
                                "parquet_path": None, "table_name": None, "data_count": 0
                            })
                self.logger.info("所有檔案解析任務已完成。")

                # Re-establish DB connection after ProcessPoolExecutor
                if self.db_loader:
                    self.db_loader._connect() # Assuming _connect can be called to re-establish
                    self.logger.info("重新建立資料庫連線以進行後續處理。")

            # 主程序中串列處理結果 (資料庫載入、檔案移動、Manifest 更新)
            self.logger.info("開始串列處理檔案解析結果...")
            for result_item in processed_file_results:
                filename = result_item["filename"]
                original_file_path = Path(result_item["original_file_path"])
                file_hash = result_item["file_hash"]
                current_status = result_item["status"]
                current_message = result_item["message"]
                parsed_df = result_item.get(constants.KEY_DATAFRAME)
                db_target_table = result_item.get("table_name")
                matched_schema_name_for_rules = result_item.get(constants.KEY_MATCHED_SCHEMA_NAME, "unknown_schema")

                self.logger.info(f"處理檔案 '{filename}' (雜湊: {file_hash}) 的解析結果。初始狀態: {current_status}")

                final_overall_status_for_file = current_status
                final_overall_message_for_file = current_message
                move_original_to_processed = False

                file_had_quarantined_rows = False

                if current_status == constants.STATUS_GROUP_RESULT:
                    self.logger.info(f"處理 ZIP 檔案 '{filename}' 的子項目結果。")
                    sub_results = result_item.get("sub_results", [])
                    all_sub_failed_or_skipped = True
                    successful_sub_files = 0
                    quarantined_sub_rows_count = 0
                    loaded_sub_rows_count = 0
                    sub_file_messages = []

                    if not sub_results:
                        final_overall_message_for_file = f"ZIP 檔案 '{filename}' 未包含任何可處理的子項目或解析時發生錯誤。"
                        final_overall_status_for_file = constants.STATUS_ERROR
                    else:
                        for sub_result in sub_results:
                            sub_filename = sub_result.get(constants.KEY_FILE, "unknown_subfile")
                            sub_status = sub_result.get(constants.KEY_STATUS)
                            sub_message = sub_result.get(constants.KEY_REASON, "無子項目訊息。")
                            sub_df = sub_result.get(constants.KEY_DATAFRAME)
                            sub_db_target_table = sub_result.get(constants.KEY_TABLE)
                            sub_matched_schema = sub_result.get(constants.KEY_MATCHED_SCHEMA_NAME, "unknown_schema")

                            sub_file_messages.append(f"  - 子檔案 '{sub_filename}': {sub_status} ({sub_message})")

                            if sub_status == constants.STATUS_SUCCESS and sub_df is not None and not sub_df.empty:
                                self.report_stats["rows_source_total_from_parsed_files"] += len(sub_df)
                                self.logger.info(f"  對子檔案 '{sub_filename}' (schema: {sub_matched_schema}) 執行資料驗證...")
                                valid_sub_df, invalid_sub_df = self.validator.validate(sub_df, sub_filename, sub_matched_schema)

                                if not invalid_sub_df.empty:
                                    self.report_stats["rows_added_to_quarantine_table"] += len(invalid_sub_df)
                                    quarantined_sub_rows_count += len(invalid_sub_df)
                                    file_had_quarantined_rows = True # Mark parent zip if any subfile has quarantined rows
                                    try:
                                        invalid_parquet_path = self._dataframe_to_temp_parquet(invalid_sub_df, f"quarantine_{sub_filename}", filename)
                                        self.db_loader.load_parquet("quarantine_data", str(invalid_parquet_path))
                                    except Exception as e_q_sub:
                                        self.logger.error(f"    將子檔案 '{sub_filename}' 的無效數據載入至隔離區時失敗: {e_q_sub}")
                                        # This sub-file's valid part might still load.

                                if not valid_sub_df.empty:
                                    try:
                                        valid_parquet_path = self._dataframe_to_temp_parquet(valid_sub_df, f"valid_{sub_filename}", filename)
                                        load_stats = self.db_loader.load_parquet(sub_db_target_table, str(valid_parquet_path))
                                        self.report_stats["rows_added_to_main_tables"] += load_stats["rows_inserted"]
                                        self.report_stats["rows_skipped_on_load_due_to_conflict"] += (load_stats["rows_in_source"] - load_stats["rows_inserted"])
                                        loaded_sub_rows_count += load_stats["rows_inserted"]
                                        successful_sub_files += 1
                                        all_sub_failed_or_skipped = False # At least one sub-file processed some data
                                    except Exception as e_l_sub:
                                        self.logger.error(f"    載入子檔案 '{sub_filename}' 的有效數據至主表 '{sub_db_target_table}' 時失敗: {e_l_sub}")
                                        # This sub-file counts as failed.
                                elif invalid_sub_df.empty : # valid_sub_df is empty AND invalid_sub_df is also empty (e.g. parse ok, but df was empty)
                                    self.logger.warning(f"  子檔案 '{sub_filename}' 解析後無數據可載入。")
                                    # This sub-file is not an error, but also not a success in terms of data loaded.
                                    # It doesn't change all_sub_failed_or_skipped if others succeed.

                            elif sub_status == constants.STATUS_SUCCESS and (sub_df is None or sub_df.empty): # Parsed OK, but no data
                                self.logger.warning(f"  子檔案 '{sub_filename}' 解析成功但無數據，將跳過載入。")
                                # This is not a failure that makes the whole ZIP fail if other files are fine.
                                # It doesn't change all_sub_failed_or_skipped if others succeed.
                                # If ALL sub-files are like this, then all_sub_failed_or_skipped remains true.
                            else: # sub_status is ERROR or SKIPPED
                                self.logger.warning(f"  子檔案 '{sub_filename}' 未成功處理或被跳過。狀態: {sub_status}")
                                # all_sub_failed_or_skipped remains true unless another file succeeds.

                        if successful_sub_files > 0:
                            final_overall_status_for_file = constants.STATUS_SUCCESS
                            final_overall_message_for_file = f"ZIP 檔案 '{filename}' 部分成功: {successful_sub_files} 個子檔案成功載入數據 ({loaded_sub_rows_count} 行)。"
                            if quarantined_sub_rows_count > 0:
                                final_overall_message_for_file += f" {quarantined_sub_rows_count} 行數據被隔離。"
                                self.report_stats["files_with_quarantined_rows"] += 1 # Count the ZIP as having quarantined rows
                            move_original_to_processed = True # Move ZIP to processed if any part was successful
                        elif file_had_quarantined_rows: # No successful loads, but some rows were quarantined
                            final_overall_status_for_file = constants.STATUS_ERROR # Or a specific partial status
                            final_overall_message_for_file = f"ZIP 檔案 '{filename}': 無子檔案數據成功載入主表，但有 {quarantined_sub_rows_count} 行數據被隔離。"
                            self.report_stats["files_with_quarantined_rows"] += 1
                        elif all_sub_failed_or_skipped:
                            final_overall_status_for_file = constants.STATUS_ERROR
                            final_overall_message_for_file = f"ZIP 檔案 '{filename}': 所有子項目處理失敗或無資料可載入。"
                        else: # Should not happen if logic is correct
                            final_overall_status_for_file = constants.STATUS_ERROR
                            final_overall_message_for_file = f"ZIP 檔案 '{filename}': 處理狀況不明確，請檢查日誌。詳細子項目訊息:\n" + "\n".join(sub_file_messages)

                        if successful_sub_files == 0 and not file_had_quarantined_rows : # If no data loaded and nothing quarantined, count as failed
                             self.report_stats["files_failed_parsing_or_other_error"] += 1

                elif current_status == constants.STATUS_SKIPPED: # Skipped by manifest check in _process_single_file
                    self.report_stats["files_skipped_manifest"] += 1
                    # No further processing, status and message already set
                elif current_status == constants.STATUS_ERROR: # Error during parsing in _process_single_file
                    self.report_stats["files_failed_parsing_or_other_error"] += 1
                    # No further processing, status and message already set
                elif parsed_df is not None and not parsed_df.empty: # This is for non-ZIP files or successfully parsed single files
                    self.report_stats["rows_source_total_from_parsed_files"] += len(parsed_df)
                    self.logger.info(f"對檔案 '{filename}' (schema: {matched_schema_name_for_rules}) 執行資料驗證...")
                    valid_df, invalid_df = self.validator.validate(parsed_df, filename, matched_schema_name_for_rules)

                    if not invalid_df.empty:
                        self.report_stats["rows_added_to_quarantine_table"] += len(invalid_df)
                        file_had_quarantined_rows = True
                        self.logger.warning(f"檔案 '{filename}' 中有 {len(invalid_df)} 行數據未通過驗證，將移至隔離資料庫表。")
                        try:
                            invalid_parquet_path = self._dataframe_to_temp_parquet(invalid_df, "quarantine", filename)
                            # For quarantine_data, we don't need to track skipped rows due to conflict, assume all load.
                            self.db_loader.load_parquet("quarantine_data", str(invalid_parquet_path))
                            self.logger.info(f"已將 {len(invalid_df)} 行無效數據從 '{filename}' 載入至 'quarantine_data' 表。")
                        except Exception as e_quarantine:
                            self.logger.error(f"將 '{filename}' 的無效數據載入至隔離區時失敗: {e_quarantine}")
                            final_overall_status_for_file = constants.STATUS_ERROR # Mark file as error if quarantine load fails
                            final_overall_message_for_file += f"; 隔離區載入失敗: {e_quarantine}"

                    if valid_df.empty and not file_had_quarantined_rows: # No valid data and nothing was quarantined (e.g. all rows failed validation but invalid_df was empty due to other reasons)
                        self.logger.warning(f"檔案 '{filename}' 經過解析和驗證後，沒有有效的數據可載入主表。")
                        if final_overall_status_for_file != constants.STATUS_ERROR: # Don't override if already error
                            final_overall_status_for_file = constants.STATUS_ERROR
                        final_overall_message_for_file = f"所有數據行均未通過驗證或解析後為空 (原始訊息: {current_message})"
                    elif not valid_df.empty : # There is valid data to load
                        self.logger.info(f"檔案 '{filename}' 有 {len(valid_df)} 行有效數據準備載入主表 '{db_target_table}'。")
                        try:
                            valid_parquet_path = self._dataframe_to_temp_parquet(valid_df, "valid", filename)
                            load_stats = self.db_loader.load_parquet(db_target_table, str(valid_parquet_path))

                            self.report_stats["rows_added_to_main_tables"] += load_stats["rows_inserted"]
                            self.report_stats["rows_skipped_on_load_due_to_conflict"] += (load_stats["rows_in_source"] - load_stats["rows_inserted"])

                            self.logger.info(f"成功將 {load_stats['rows_inserted']} 行有效數據從 '{filename}' 載入至主表 '{db_target_table}'。")
                            move_original_to_processed = True
                            final_overall_status_for_file = constants.STATUS_SUCCESS
                            final_overall_message_for_file = f"成功處理並載入 {load_stats['rows_inserted']} 行有效數據。"
                            if file_had_quarantined_rows:
                                final_overall_message_for_file += f" {len(invalid_df)} 行數據被隔離。"
                            if load_stats["rows_in_source"] - load_stats["rows_inserted"] > 0:
                                final_overall_message_for_file += f" {load_stats['rows_in_source'] - load_stats['rows_inserted']} 行因主鍵衝突被跳過。"
                        except Exception as e_load_valid:
                            self.logger.error(f"載入 '{filename}' 的有效數據至主表 '{db_target_table}' 時失敗: {e_load_valid}")
                            final_overall_status_for_file = constants.STATUS_ERROR
                            final_overall_message_for_file = f"有效數據載入失敗: {e_load_valid}"
                            move_original_to_processed = False
                    # If valid_df is empty but some rows were quarantined, the status might remain PARTIAL or ERROR depending on earlier stages.
                    # If final_overall_status_for_file is not explicitly set to SUCCESS here, it means either all data was invalid,
                    # or valid data failed to load.
                    if file_had_quarantined_rows and final_overall_status_for_file != constants.STATUS_SUCCESS:
                         self.report_stats["files_with_quarantined_rows"] +=1


                elif current_status == constants.STATUS_SUCCESS and (parsed_df is None or parsed_df.empty): # Parsed OK, but no data
                    self.logger.warning(f"檔案 '{filename}' 解析成功但無數據，將跳過載入。")
                    final_overall_status_for_file = constants.STATUS_SKIPPED
                    final_overall_message_for_file = "解析成功但無數據可載入。"
                    move_original_to_processed = True
                    self.report_stats["files_successfully_parsed_and_validated_loaded"] += 1 # Count as success as it's not an error

                # Update aggregate counters based on final_overall_status_for_file for this file
                if final_overall_status_for_file == constants.STATUS_SUCCESS and move_original_to_processed:
                    self.report_stats["files_successfully_parsed_and_validated_loaded"] += 1
                elif final_overall_status_for_file == constants.STATUS_ERROR : # Catch errors not caught by specific parsing error counter
                    if current_status != constants.STATUS_ERROR : # if it wasn't already counted as parsing error
                        self.report_stats["files_failed_parsing_or_other_error"] += 1

                # File movement logic
                if original_file_path.exists():
                    if move_original_to_processed and final_overall_status_for_file == constants.STATUS_SUCCESS:
                        try:
                            shutil.move(str(original_file_path), str(self.local_processed_path / filename))
                            self.logger.info(f"原始檔案 '{filename}' 已移至本地 processed 資料夾。")
                        except Exception as move_e:
                            self.logger.error(f"移動原始檔案 '{filename}' 至本地 processed 資料夾失敗: {move_e}")
                            final_overall_status_for_file = constants.STATUS_ERROR
                            final_overall_message_for_file += f" 但移至 processed 失敗: {move_e}"
                    else: # Not success or not designated for processed folder
                         if final_overall_status_for_file != constants.STATUS_SKIPPED: # Don't force error if it was just a skip
                            final_overall_status_for_file = constants.STATUS_ERROR
                         try:
                            shutil.move(str(original_file_path), str(self.local_quarantine_path / filename))
                            self.logger.warning(f"原始檔案 '{filename}' 已移至本地 quarantine 資料夾。原因: {final_overall_message_for_file}")
                         except Exception as move_e:
                            self.logger.error(f"移動原始檔案 '{filename}' 至本地 quarantine 資料夾失敗: {move_e}")

                # Update Manifest
                self.manifest_manager.update_manifest(
                    str(original_file_path), # Path for hashing or identification
                    final_overall_status_for_file,
                    final_overall_message_for_file,
                    original_filename=filename # This is display_name
                    # file_hash_override was removed as it's not an accepted argument
                )
                self.logger.info(f"--- 檔案 '{filename}' 處理完畢。最終狀態: {final_overall_status_for_file} ---")
            self.logger.info("所有檔案解析結果處理完成。")

            # --- Data Transformation Stage ---
            self.logger.info("--- 開始數據轉換階段 (已移除) ---")
            # try:
            #     with self.data_transformer as transformer: # Commented out as DataTransformer is removed
            #         transformer.transform_data() # Commented out
            #     self.logger.info("✅ 數據轉換階段成功完成。") # Commented out
            # except Exception as e_transform:
            #     self.logger.error(f"數據轉換階段發生錯誤: {e_transform}", exc_info=True) # Commented out
            #     # Decide if this error should be critical to the pipeline
            #     # For now, logging the error and continuing.
            self.logger.info("--- 數據轉換階段結束 (已移除) ---") # Updated log message

            # Cleanup temp intermediate parquets dir
            temp_intermediate_parquets_dir = self.local_project_path / "temp_intermediate_parquets"
            if temp_intermediate_parquets_dir.exists():
                try:
                    shutil.rmtree(temp_intermediate_parquets_dir)
                    self.logger.info(f"臨時 Parquet 資料夾 {temp_intermediate_parquets_dir} 已清理。")
                except Exception as e_clean_temp:
                    self.logger.warning(f"清理臨時 Parquet 資料夾 {temp_intermediate_parquets_dir} 失敗: {e_clean_temp}")

            # D. End-of-run Sync (Local to Remote)

            # Log the summary report before final cleanup and sync
            # Ensure final status is determined
            if self.report_stats["files_failed_parsing_or_other_error"] > 0 or self.report_stats["files_with_quarantined_rows"] > 0 : # Check if any file had issues
                if self.report_stats["files_successfully_parsed_and_validated_loaded"] > 0:
                    self.report_stats["status"] = "PARTIAL_SUCCESS"
                else:
                    self.report_stats["status"] = "FAILURE"
            elif self.report_stats["files_successfully_parsed_and_validated_loaded"] > 0: # All processed files were successful
                 self.report_stats["status"] = "SUCCESS"
            elif self.report_stats["files_processed_total"] == 0 : # No files to process
                 self.report_stats["status"] = "SUCCESS" # Or "NO_FILES_PROCESSED"
            else: # No files loaded, but some were processed (e.g. all skipped or empty after validation)
                 self.report_stats["status"] = "UNKNOWN" # Or a more specific status

            end_time_perf = time.time() # For performance timing
            self.report_stats["end_time"] = datetime.now(pytz.timezone('Asia/Taipei')).isoformat()
            self.report_stats["total_duration_seconds"] = round(end_time_perf - start_time_perf, 2)
            self.logger.info(self.report_stats, extra={'event_type': 'execution_summary_report'})


            # IMPORTANT: Close DB connection BEFORE syncing the DB file to ensure all data is flushed.
            if hasattr(self, 'db_loader') and self.db_loader:
                self.db_loader.close_connection()
                self.logger.info("本地資料庫連接已在同步回遠端前關閉，以確保資料完整性。")
            else:
                self.logger.warning("DB Loader 未定義或不存在，無法在同步前關閉連接。")

            self.logger.info("--- 開始結束時同步 (本地 DB/Manifest -> 遠端) ---")
            if hasattr(self, 'local_database_file') and self.local_database_file.exists():
                self._sync_file(self.local_database_file, self.remote_database_file, "to_remote")
            else:
                self.logger.warning(f"本地資料庫檔案 {getattr(self, 'local_database_file', 'N/A')} 不存在，無法同步至遠端。")

            if hasattr(self, 'local_manifest_file') and self.local_manifest_file.exists():
                self._sync_file(self.local_manifest_file, self.remote_manifest_file, "to_remote")
            else:
                self.logger.warning(f"本地 Manifest 檔案 {getattr(self, 'local_manifest_file', 'N/A')} 不存在，無法同步至遠端。")
            self.logger.info("--- 結束時同步完成 (DB/Manifest) ---")

            self.logger.info("--- 開始結束時同步 (本地 processed/quarantine -> 遠端) ---")
            if hasattr(self, 'local_processed_path'):
                 self._sync_directory_content(self.local_processed_path, self.remote_processed_path, "to_remote")
            if hasattr(self, 'local_quarantine_path'):
                 self._sync_directory_content(self.local_quarantine_path, self.remote_quarantine_path, "to_remote")
            self.logger.info("--- 結束時同步完成 (processed/quarantine) ---")

        except Exception as e:
            self.logger.critical(f"管線執行過程中發生無法恢復的錯誤: {e}", exc_info=True)
        finally:
            self.logger.info("--- 開始日誌同步 (本地 -> 遠端) ---")
            if hasattr(self, 'local_log_path') and hasattr(self, 'remote_log_path'): # Ensure paths are defined
                self._sync_directory_content(self.local_log_path, self.remote_log_path, "to_remote")
            self.logger.info("--- 日誌同步完成 ---")

            # Workspace cleanup (DB connection should already be closed if db_loader was initialized)
            if hasattr(self, 'local_project_path') and self.local_project_path.exists():
                # try:
                #     shutil.rmtree(self.local_project_path) # TEST MODIFICATION: Temporarily disable cleanup
                #     self.logger.info(f"✅ 本地工作區 {self.local_project_path} 已成功清理。")
                # except Exception as e:
                #     self.logger.error(f"清理本地工作區 {self.local_project_path} 失敗: {e}")
                self.logger.info(f"本地工作區清理已臨時禁用於測試: {self.local_project_path}") # TEST MODIFICATION
            elif hasattr(self, 'local_project_path'):
                self.logger.info(f"本地工作區 {self.local_project_path} 未找到，無需清理。")
            else:
                self.logger.info("本地工作區路徑未定義，跳過清理。")

            end_time_perf = time.time() # For performance timing
            self.logger.info(f"====== 數據管線 (本地優先, 並行) 執行完畢 @ {datetime.now():%Y-%m-%d %H:%M:%S} ======")
            self.logger.info(f"總耗時: {end_time_perf - start_time_perf:.2f} 秒")
            if self.debug_mode:
                self.logger.debug(get_hardware_usage("管線結束後 (本地優先, 並行)"))

            # Final check on DB connection status (it should be closed by now)
            if hasattr(self, 'db_loader') and self.db_loader and self.db_loader.connection is None:
                self.logger.info("本地資料庫連接已確認於管線末端關閉。")
            elif hasattr(self, 'db_loader') and self.db_loader and self.db_loader.connection is not None:
                 # This case should ideally not be reached if logic is correct.
                self.logger.error("本地資料庫連接在管線末端仍未關閉！嘗試再次關閉。")
                self.db_loader.close_connection() # Attempt to close again if it wasn't.
            else:
                self.logger.info("DB Loader 未初始化或不存在，在最終清理步驟無需關閉連接。")

