import os
import pandas as pd
from datetime import datetime
from pathlib import Path # Added
from taifex_pipeline.database.metadata_manager import MetadataManager
from taifex_pipeline.core.logger_setup import get_logger

class MetadataScanner:
    """
    Scans a directory of processed Parquet files and registers their
    metadata (symbol, data_date, file_path) into the metadata database.
    """

    def __init__(self, parquet_dir: str, db_path: str, batch_size: int = 100):
        """
        Initializes the MetadataScanner.

        Args:
            parquet_dir: The directory containing the processed Parquet files.
            db_path: The path to the SQLite metadata database.
            batch_size: The number of records to batch before committing to the DB.
        """
        self.parquet_dir = Path(parquet_dir).resolve()
        self.db_path = Path(db_path).resolve()
        self.batch_size = batch_size
        self.logger = get_logger(self.__class__.__name__)
        # Ensure db_path is passed as string to MetadataManager if it expects str
        self.metadata_manager = MetadataManager(str(self.db_path))
        self.metadata_manager.create_tables()

    def _extract_metadata_from_file(self, file_path: Path) -> list[dict]: # Changed file_path type
        """
        Extracts metadata from a single Parquet file.

        A single Parquet file might contain data for multiple symbols,
        but it is assumed to be for a single data_date.

        Args:
            file_path: The path to the Parquet file.

        Returns:
            A list of dictionaries, where each dictionary contains
            the metadata for one symbol found in the file.
            e.g., [{'symbol': 'TXO', 'data_date': '2023-10-17'}, ...]
        """
        try:
            df = pd.read_parquet(file_path, columns=['symbol', 'data_date'])
            if df.empty:
                self.logger.warning(f"Skipping empty parquet file: {file_path}")
                return []

            # Ensure data_date is treated as a date
            if 'data_date' in df.columns:
                df['data_date'] = pd.to_datetime(df['data_date']).dt.date

            # Get unique symbol-date combinations
            metadata = df[['symbol', 'data_date']].drop_duplicates().to_dict('records')

            # Convert date objects to string for consistency
            for record in metadata:
                if 'data_date' in record and hasattr(record['data_date'], 'isoformat'):
                    record['data_date'] = record['data_date'].isoformat()

            return metadata

        except FileNotFoundError:
            self.logger.error(f"File not found during metadata extraction: {file_path}")
            return []
        except pd.errors.EmptyDataError:
            self.logger.warning(f"Empty data error for file: {file_path}")
            return []
        except Exception as e:
            self.logger.error(f"Failed to process file {file_path}: {e}", exc_info=True)
            return []

    def _scan_parquet_files(self) -> list[Path]: # Return list of Path objects
        """Scans the directory for .parquet files."""
        parquet_files = []
        for root, _, files in os.walk(self.parquet_dir): # self.parquet_dir is already a Path
            for file in files:
                if file.endswith('.parquet'):
                    parquet_files.append(Path(root) / file) # Use Path object for appending
        return parquet_files

    def run_scan(self):
        """
        Executes the full scan and registration process.
        """
        self.logger.info(f"Starting scan of directory: {self.parquet_dir}")
        all_parquet_files = self._scan_parquet_files()
        self.logger.info(f"Found {len(all_parquet_files)} parquet files to process.")

        records_batch = []
        for file_path_obj in all_parquet_files: # file_path_obj is a Path object
            # os.stat and pd.read_parquet can handle Path objects directly
            file_stat = os.stat(file_path_obj)
            file_info = {
                'file_name': file_path_obj.name, # Use Path.name
                'gdrive_path': str(file_path_obj), # Store as string if DB expects string
                'last_modified': datetime.fromtimestamp(file_stat.st_mtime),
                'file_size_bytes': file_stat.st_size,
            }

            # _extract_metadata_from_file now expects a Path object
            metadata_list = self._extract_metadata_from_file(file_path_obj)

            if not metadata_list:
                self.logger.warning(f"No valid metadata extracted from {file_path}. Skipping file registration.")
                continue

            # Add file info and metadata to the batch for registration
            records_batch.append({
                'file_info': file_info,
                'metadata_list': metadata_list
            })

            if len(records_batch) >= self.batch_size:
                self.logger.info(f"Registering batch of {len(records_batch)} files.")
                self.metadata_manager.register_batch(records_batch)
                records_batch = []

        # Register any remaining records in the last batch
        if records_batch:
            self.logger.info(f"Registering final batch of {len(records_batch)} files.")
            self.metadata_manager.register_batch(records_batch)

        self.logger.info("Metadata scan and registration complete.")
