import pathlib
from src.sp_data_v16.core.config import load_config
from src.sp_data_v16.ingestion.manifest import ManifestManager
from src.sp_data_v16.ingestion.scanner import FileScanner
from .raw_loader import RawLakeLoader

class IngestionPipeline:
    """
    Controls the overall file ingestion process, integrating scanning, manifest checking,
    and registration of new files.
    """
    def __init__(self, config_path: str = "config_v16.yaml"):
        """
        Initializes the IngestionPipeline.

        Args:
            config_path: Path to the configuration file (e.g., "config_v16.yaml").
        """
        self.config = load_config(config_path)

        # Ensure necessary config keys exist
        if not self.config:
            raise ValueError("Configuration could not be loaded.")
        if "database" not in self.config or "manifest_db_path" not in self.config["database"]:
            raise ValueError("Manifest DB path not found in configuration.")
        if "paths" not in self.config or "input_directory" not in self.config["paths"]:
            raise ValueError("Input directory path not found in configuration.")
        if "raw_lake_db_path" not in self.config["database"]:
            raise ValueError("Raw Lake DB path (raw_lake_db_path) not found in configuration.")

        self.manifest_db_path = self.config["database"]["manifest_db_path"]
        self.raw_lake_db_path = self.config["database"]["raw_lake_db_path"]
        self.input_directory = self.config["paths"]["input_directory"]

        # Ensure the directory for the manifest DB exists
        db_parent_dir = pathlib.Path(self.manifest_db_path).parent
        db_parent_dir.mkdir(parents=True, exist_ok=True)

        # Ensure the directory for the raw lake DB exists
        raw_lake_db_parent_dir = pathlib.Path(self.raw_lake_db_path).parent
        raw_lake_db_parent_dir.mkdir(parents=True, exist_ok=True)

        self.manifest_manager = ManifestManager(db_path=self.manifest_db_path)
        self.raw_loader = RawLakeLoader(db_path=self.raw_lake_db_path)
        print(f"IngestionPipeline initialized. Manifest DB: '{self.manifest_db_path}', Raw Lake DB: '{self.raw_lake_db_path}', Input Dir: '{self.input_directory}'")

    def run(self):
        """
        Executes the full ingestion pipeline:
        1. Scans the input directory for files.
        2. For each file, checks if it's already registered in the manifest.
        3. If new, registers the file. If existing, skips it.
        4. Prints a summary report.
        """
        print(f"Starting ingestion process for directory: {self.input_directory}...")
        scanned_count = 0
        added_count = 0
        skipped_count = 0

        try:
            for file_hash, file_path in FileScanner.scan_directory(self.input_directory):
                scanned_count += 1
                file_name = file_path.name

                if self.manifest_manager.hash_exists(file_hash):
                    print(f"檔案已存在：{file_name} (Hash: {file_hash[:8]}...), 跳過處理。")
                    skipped_count += 1
                else:
                    self.manifest_manager.register_file(file_hash, str(file_path))
                    self.raw_loader.save_file(file_path, file_hash)
                    self.manifest_manager.update_status(file_hash, 'loaded_to_raw_lake')
                    print(f"新檔案發現：{file_name} (Hash: {file_hash[:8]}...), 已登錄 Manifest 並存入 Raw Lake。")
                    added_count += 1
        except FileNotFoundError as e:
            print(f"Error during scanning: {e}")
            print("Ingestion process aborted.")
            return
        except Exception as e:
            print(f"An unexpected error occurred during the ingestion run: {e}")
            print("Ingestion process aborted.")
            return
        finally:
            self.manifest_manager.close() # Ensure DB connection is closed
            if hasattr(self, 'raw_loader') and self.raw_loader:
                self.raw_loader.close()

        print("\n--- Ingestion Summary ---")
        print(f"流程結束。共掃描 {scanned_count} 個檔案，新增 {added_count} 個，跳過 {skipped_count} 個。")

if __name__ == '__main__':
    # This is an example of how to run the pipeline.
    # For actual execution, it's better to use the run_ingestion.py script.

    # Create a dummy config for the __main__ example
    # In a real scenario, config_v16.yaml would be in the project root.
    current_dir = pathlib.Path(__file__).parent
    example_config_path = current_dir / "temp_example_config.yaml"
    example_input_dir = current_dir / "example_pipeline_input"
    example_data_dir = current_dir / "example_pipeline_data/v16"

    # Clean up previous run's example files/dirs if they exist
    if example_input_dir.exists():
        for item in example_input_dir.rglob('*'):
            if item.is_file(): item.unlink()
        for item in sorted(list(example_input_dir.rglob('*')), key=lambda p: len(p.parts), reverse=True):
            if item.is_dir(): item.rmdir()
        example_input_dir.rmdir()

    if example_data_dir.exists():
        if (example_data_dir / "manifest.db").exists():
            (example_data_dir / "manifest.db").unlink()
        if (example_data_dir / "manifest.db.wal").exists(): # DuckDB WAL file
            (example_data_dir / "manifest.db.wal").unlink()
        if (example_data_dir / "raw_lake.db").exists():
            (example_data_dir / "raw_lake.db").unlink()
        if (example_data_dir / "raw_lake.db.wal").exists():
            (example_data_dir / "raw_lake.db.wal").unlink()
        # Remove data/v16 and then data if they become empty
        try:
            example_data_dir.rmdir()
            example_data_dir.parent.rmdir() # Try to remove 'example_pipeline_data'
        except OSError: # Directory not empty or does not exist
            pass


    example_input_dir.mkdir(parents=True, exist_ok=True)
    example_data_dir.mkdir(parents=True, exist_ok=True)

    (example_input_dir / "sample_doc1.txt").write_text("Hello world")
    (example_input_dir / "sample_doc2.txt").write_text("Another document")

    example_config_content = f"""
database:
  manifest_db_path: "{example_data_dir.as_posix()}/manifest.db"
  raw_lake_db_path: "{example_data_dir.as_posix()}/raw_lake.db"
  processed_db_path: "data/v16/processed_data.db" # Placeholder

logging:
  level: "INFO"
  format: "[%(asctime)s] [%(levelname)s] - %(message)s"

paths:
  input_directory: "{example_input_dir.as_posix()}"
"""
    with open(example_config_path, 'w', encoding='utf-8') as f:
        f.write(example_config_content)

    print("Running IngestionPipeline example...")
    try:
        pipeline = IngestionPipeline(config_path=str(example_config_path))
        pipeline.run()

        print("\n--- Second run (should skip files) ---")
        # Re-initialize to simulate a fresh start but with existing DB
        pipeline2 = IngestionPipeline(config_path=str(example_config_path))
        pipeline2.run()

    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Clean up example config and dirs
        if example_config_path.exists():
            example_config_path.unlink()
        # Further cleanup of example_input_dir and example_data_dir can be added here
        # For simplicity, we'll leave them for manual inspection or next run's cleanup.
        print("\nExample finished.")
