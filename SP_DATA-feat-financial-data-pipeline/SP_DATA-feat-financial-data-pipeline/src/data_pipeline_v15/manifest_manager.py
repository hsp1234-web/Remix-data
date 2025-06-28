import hashlib
import json
import os
from datetime import datetime # Moved here
from typing import TYPE_CHECKING, Set, Union, Iterable # 為類型提示而添加

if TYPE_CHECKING:
    from .utils.logger import Logger # 用於 Logger 的類型提示


class ManifestManager:
    """檔案處理清單管理器。

    這個類別用於追蹤哪些檔案已經被處理過。它透過維護一個包含
    已處理檔案雜湊值 (hashes) 的 JSON 清單檔案來實現此功能。
    這樣可以避免重複處理相同的檔案，除非明確指示要重新處理。

    主要功能包括：
    - 從 JSON 檔案載入已處理的檔案雜湊值集合。
    - 將新的已處理檔案雜湊值儲存回 JSON 檔案。
    - 計算檔案的 SHA256 雜湊值。
    - 檢查特定檔案（根據其雜湊值）是否已經被處理過。

    :ivar path: 清單檔案 (manifest file) 的路徑。
    :vartype path: str
    :ivar logger: 用於記錄日誌的 Logger 物件執行個體。
    :vartype logger: data_pipeline_v15.utils.logger.Logger
    :ivar processed_hashes: 一個包含所有已處理檔案雜湊值的集合。
    :vartype processed_hashes: set[str]
    """

    def __init__(self, manifest_path: str, logger: 'Logger'):
        """初始化 FileManifest 物件。

        在初始化時，會嘗試從指定的 `manifest_path` 載入已處理的雜湊值。

        :param manifest_path: 清單檔案的路徑。例如 'processed_files.json'。
        :type manifest_path: str
        :param logger: 用於記錄日誌訊息的 Logger 物件執行個體。
        :type logger: data_pipeline_v15.utils.logger.Logger
        """
        self.path = manifest_path
        self.logger = logger
        self.manifest_data = self._load() # Will store {"files": {identifier: {status, message, original_filename}}}

    def load_or_create_manifest(self) -> None:
        """
        Ensures the manifest file exists and is loaded.
        If the manifest file does not exist, it creates an empty one.
        """
        if not os.path.exists(self.path):
            self.logger.info(f"Manifest file '{self.path}' not found. Creating a new empty manifest.")
            self.manifest_data = {"files": {}}
            self._save()
        else:
            # File exists, load it. This is already done by __init__ if path existed then.
            self.manifest_data = self._load() # Ensure it's loaded if it existed.
            if not isinstance(self.manifest_data.get("files"), dict): # Ensure "files" key exists and is a dict
                self.logger.warning(f"Manifest file '{self.path}' is missing 'files' dictionary or it's malformed. Initializing with empty 'files'.")
                self.manifest_data = {"files": {}}
                self._save() # Save corrected structure
            self.logger.info(f"Manifest file '{self.path}' loaded.")

    def update_manifest(self, file_identifier_for_hash: str, status: str, message: str, original_filename: str = None) -> None:
        """
        Updates the manifest with the processing status of a file.
        Uses file hash as key for successful records, otherwise uses original_filename.

        Args:
            file_identifier_for_hash (str): The path to the file used for generating a hash if successful.
                                            If hashing fails or status is not success, original_filename is used as key.
            status (str): The status of the processing (e.g., "SUCCESS", "ERROR", "SKIPPED").
            message (str): A message describing the outcome.
            original_filename (str, optional): The original basename of the file. If None, derived from file_identifier_for_hash.
        """
        if original_filename is None:
            original_filename = os.path.basename(file_identifier_for_hash)

        key_to_use = None
        # PipelineOrchestrator passes filename (basename) as file_identifier_for_hash
        # We need the full path for hashing, which is available in PipelineOrchestrator.
        # For now, this method will use original_filename as key if hashing `file_identifier_for_hash` fails.
        # This means PipelineOrchestrator should pass the *full path* as `file_identifier_for_hash`
        # if it wants hashing to be attempted.

        if status == "SUCCESS": # Replace with constants.STATUS_SUCCESS
            # For successful files, the key should ideally be the hash of the file *content*.
            # PipelineOrchestrator calls update_manifest with the *original input file path* (or just filename).
            # We should use the hash of this input file path.
            file_hash = self.get_file_hash(file_identifier_for_hash) # file_identifier_for_hash should be full path
            if file_hash:
                key_to_use = file_hash
                self.logger.debug(f"File '{original_filename}' (hash: {file_hash}) will be updated in manifest.")
            else: # Should not happen if file_identifier_for_hash is a valid path to an existing file.
                self.logger.warning(f"Could not generate hash for successful file '{original_filename}' (path: {file_identifier_for_hash}). Using filename as key.")
                key_to_use = original_filename
        else: # For ERROR, SKIPPED, etc.
            key_to_use = original_filename # Use the original filename as the key

        self.manifest_data["files"][key_to_use] = {
            "status": status,
            "message": message,
            "original_filename": original_filename, # Store original filename for easier debugging
            "timestamp": datetime.now().isoformat() # Add timestamp
        }
        self.logger.info(f"Manifest update for '{original_filename}' (key: {key_to_use}): Status - {status}, Message - {message}")
        self._save()

    def _load(self) -> dict:
        """從指定的路徑載入 manifest data。

        如果檔案不存在，則返回 {"files": {}}。
        如果在讀取或解析現有檔案時發生任何錯誤，則會記錄錯誤並回傳 {"files": {}}。
        """
        if not os.path.exists(self.path):
            return {"files": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict) or not isinstance(data.get("files"), dict):
                    self.logger.warning(f"Manifest file '{self.path}' is malformed. Initializing with empty 'files'.")
                    return {"files": {}}
                return data
        except Exception as e:
            self.logger.error(f"載入 Manifest 檔案 '{self.path}' 時發生錯誤: {e}")
            return {"files": {}} # Return empty structure on error

    def _save(self) -> None:
        """將目前的 manifest_data 儲存到清單檔案中。
        """
        try:
            print(f"DEBUG_MANIFEST: Saving manifest to: {self.path}") # DEBUG line
            print(f"DEBUG_MANIFEST: Data being saved: {self.manifest_data}") # DEBUG line
            manifest_dir = os.path.dirname(self.path)
            if not manifest_dir:
                manifest_dir = "."
            os.makedirs(manifest_dir, exist_ok=True)

            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.manifest_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"儲存 Manifest 檔案 '{self.path}' 時發生錯誤: {e}")

    @staticmethod
    def get_file_hash(file_path: str) -> Union[str, None]:
        """計算給定檔案路徑的 SHA256 雜湊值。

        以二進位模式讀取檔案，分塊計算雜湊值以處理大檔案。

        :param file_path: 要計算雜湊值的檔案的路徑。
        :type file_path: str
        :return: 檔案的 SHA256 雜湊值（十六進位字串），如果檔案未找到或讀取錯誤則回傳 None。
        :rtype: str or None
        """
        # Ensure file_path is a valid string path and exists before hashing
        if not isinstance(file_path, (str, bytes, os.PathLike)) or not os.path.exists(file_path) or not os.path.isfile(file_path):
            # self.logger.error(f"File not found, not a file, or invalid path type for hashing: {file_path}") # Instance method needs logger
            # For static method, can't use self.logger. Consider logging if logger is passed or raise error.
            return None

        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192): # PEP 572: Assignment expressions
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except IOError: # Catches FileNotFoundError as well if path is invalid despite os.path.exists
            # Consider logging here if a logger was passed or accessible
            return None


    def has_been_processed(self, file_hash_or_name: str) -> bool:
        """
        Checks if a file (identified by hash for successes, or name for errors/skips)
        has a 'success' record in the manifest.
        This method is primarily for checking if a file was *successfully* processed before.
        Args:
            file_hash_or_name (str): The file hash (for successful files) or filename.
        Returns:
            bool: True if the file has a "SUCCESS" status in the manifest, False otherwise.
        """
        entry = self.manifest_data.get("files", {}).get(file_hash_or_name)
        if entry and entry.get("status") == "SUCCESS": # Replace with constants.STATUS_SUCCESS
            return True
        return False

    # add_processed_hashes is no longer directly used by orchestrator; update_manifest handles saving.
    # It could be kept for other uses or removed if manifest structure is now solely managed by update_manifest.
    # For now, let's comment it out to avoid confusion with the new manifest structure.
    # def add_processed_hashes(self, hashes_to_add: Union[str, Iterable[str]]) -> None:
    #     """將一組新的雜湊值添加到已處理清單中並儲存。
    #
    #     這個方法會更新 self.processed_hashes 集合，然後呼叫 _save() 方法
    #     將更新後的集合持久化到檔案中。
    #
    #     :param hashes_to_add: 一個包含要添加的檔案雜湊值的字串，或可迭代物件 (例如列表或集合)。
    #     :type hashes_to_add: str or collections.abc.Iterable[str]
    #     """
    #     # 確保 hashes_to_add 是可迭代的，即使它只是一個雜湊字串
    #     if isinstance(hashes_to_add, str):
    #         self.processed_hashes.add(hashes_to_add)
    #     else:  # 假設它是一個可迭代物件 (例如列表、集合)
    #         self.processed_hashes.update(hashes_to_add)
    #     self._save()
