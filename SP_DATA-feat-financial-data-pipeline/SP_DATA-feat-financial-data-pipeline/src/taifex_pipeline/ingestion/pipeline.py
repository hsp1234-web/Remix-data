import zipfile
import collections
from pathlib import Path

from ..core.logger_setup import get_logger

class IngestionPipeline:
    """
    Handles the ingestion of data from a source directory, processing
    zip files, and extracting their contents. This version supports
    nested zip files.
    """

    def __init__(self, config: dict):
        """
        Initializes the IngestionPipeline.

        Args:
            config: A dictionary containing pipeline configurations.
        """
        ingestion_config = config.get("ingestion", {})
        self.source_dir = Path(ingestion_config.get("source_dir", "data/00_source"))
        self.output_dir = Path(ingestion_config.get("output_dir", "data/01_raw"))
        self.logger = get_logger(self.__class__.__name__)

        # 1. 初始化動態工作佇列和已處理路徑集合
        self.work_queue = collections.deque()
        self.processed_paths = set()

    def _scan_initial_zips(self):
        """Scans the source directory for top-level zip files to seed the queue."""
        self.logger.info(f"Scanning for initial zip files in {self.source_dir}...")
        initial_files = [p for p in self.source_dir.glob("*.zip")]
        self.work_queue.extend(initial_files)
        self.logger.info(f"Found {len(initial_files)} initial zip files to process.")

    def _process_single_zip(self, zip_path: Path) -> list[Path]:
        """
        Processes a single zip file, extracts its contents, and identifies
        any nested zip files.

        Args:
            zip_path: The path to the zip file to process.

        Returns:
            A list of paths to any newly found nested zip files.
        """
        nested_zips_found = []
        self.logger.info(f"Processing archive: {zip_path.name}")

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for member_info in zf.infolist():
                    if member_info.is_dir():
                        continue

                    member_path = Path(member_info.filename) # Relative path within zip
                    # Ensure output_path is relative to self.output_dir for consistent structure
                    # If member_path can contain ".." or absolute paths, sanitization might be needed.
                    # For now, assume member_path.name is sufficient if flat structure in output is OK.
                    # If preserving internal zip structure: output_file_path = self.output_dir / member_info.filename
                    # Current implementation extracts all members to the root of self.output_dir using member_path.name
                    output_file_path = self.output_dir / member_path.name

                    if member_path.name.lower().endswith('.zip'):
                        # This is a nested zip file. Extract it and add to queue.
                        self.logger.info(f"  Found nested archive: {member_path.name}. Extracting to {output_file_path}...")
                        # Extract the nested zip into the raw output directory
                        # Ensure parent directory for output_file_path exists if preserving structure
                        output_file_path.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(member_info) as source, open(output_file_path, 'wb') as target:
                            target.write(source.read())
                        nested_zips_found.append(output_file_path)
                    else:
                        # This is a regular data file.
                        self.logger.debug(f"  Extracting data file: {member_path.name} to {output_file_path}")
                        # Ensure parent directory for output_file_path exists
                        output_file_path.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(member_info) as source, open(output_file_path, 'wb') as target:
                            target.write(source.read())

        except zipfile.BadZipFile:
            self.logger.error(f"Failed to process {zip_path.name}: Corrupted zip file.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while processing {zip_path.name}: {e}", exc_info=True)

        return nested_zips_found

    def run(self):
        """
        Executes the ingestion pipeline using a dynamic work queue to handle
        nested zip files.
        """
        # 確保輸出目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 2. 掃描頂層目錄，初始化佇列
        self._scan_initial_zips()

        # 3. 只要佇列不為空，就持續處理
        while self.work_queue:
            file_path = self.work_queue.popleft()

            # Resolve path to ensure consistency in processed_paths set
            resolved_file_path = file_path.resolve()

            # 4. 防範無限迴圈
            if resolved_file_path in self.processed_paths:
                self.logger.warning(f"Skipping already processed file to prevent infinite loop: {resolved_file_path.name} (Path: {resolved_file_path})")
                continue

            self.processed_paths.add(resolved_file_path)

            # 5. 處理單一壓縮檔，並獲取新發現的巢狀壓縮檔列表
            newly_found_zips = self._process_single_zip(resolved_file_path) # Pass resolved_file_path

            # 6. 將新任務加入佇列
            if newly_found_zips:
                # Resolve paths before adding to queue as well for consistency
                resolved_newly_found_zips = [p.resolve() for p in newly_found_zips]
                self.work_queue.extend(resolved_newly_found_zips)
                self.logger.info(f"Added {len(resolved_newly_found_zips)} new nested archives to the processing queue.")

        self.logger.info("Ingestion pipeline finished. All archives processed.")
