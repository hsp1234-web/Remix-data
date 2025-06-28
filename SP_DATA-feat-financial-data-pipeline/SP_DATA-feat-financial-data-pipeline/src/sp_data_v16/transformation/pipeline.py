import logging
import duckdb
import pathlib
import pandas as pd
from .schema_manager import SchemaManager
from .raw_lake_reader import RawLakeReader
from .parser import DataParser
from .validator import DataValidator
from .processed_loader import ProcessedDBLoader
from src.sp_data_v16.ingestion.manifest import ManifestManager # Assuming ManifestManager handles its own connection
from src.sp_data_v16.core.config import load_config

class TransformationPipeline:
    def __init__(self, config_path: str = "config_v16.yaml"):
        self.config = load_config(config_path)

        db_config = self.config.get("database", {})
        paths_config = self.config.get("paths", {})

        self.manifest_db_path = db_config.get("manifest_db_path")
        self.raw_lake_db_path = db_config.get("raw_lake_db_path")
        self.processed_db_path = db_config.get("processed_db_path")
        self.schema_config_path = paths_config.get("schema_config_path")

        if not all([self.manifest_db_path, self.raw_lake_db_path, self.processed_db_path, self.schema_config_path]):
            missing = [
                path_name for path_name, path_val in {
                    "manifest_db_path": self.manifest_db_path,
                    "raw_lake_db_path": self.raw_lake_db_path,
                    "processed_db_path": self.processed_db_path,
                    "schema_config_path": self.schema_config_path
                }.items() if not path_val
            ]
            raise ValueError(f"Missing required paths in configuration: {', '.join(missing)}")

        for db_path_str in [self.manifest_db_path, self.raw_lake_db_path, self.processed_db_path]:
            if db_path_str: # Should always be true due to the check above, but good practice
                db_parent_dir = pathlib.Path(db_path_str).parent
                db_parent_dir.mkdir(parents=True, exist_ok=True)

        # Ensure schema_config_path parent directory exists if it's specified and not just a filename
        if self.schema_config_path:
            schema_parent_dir = pathlib.Path(self.schema_config_path).parent
            if not schema_parent_dir.exists(): # only create if it's a real path, not current dir
                 schema_parent_dir.mkdir(parents=True, exist_ok=True)


        self.manifest_con = duckdb.connect(database=str(self.manifest_db_path), read_only=False)
        # self.raw_lake_con is effectively replaced by RawLakeReader instance
        # self.raw_lake_con = duckdb.connect(database=str(self.raw_lake_db_path), read_only=True) # Keep for now if other methods use it
        # self.processed_con = duckdb.connect(database=str(self.processed_db_path), read_only=False) # Replaced by ProcessedDBLoader

        self.schema_manager = SchemaManager(schema_path=str(self.schema_config_path))
        self.parser = DataParser()
        self.validator = DataValidator() # Initialize DataValidator
        try:
            # Initialize ProcessedDBLoader, ensuring db_path is a string
            self.processed_loader = ProcessedDBLoader(db_path=str(self.processed_db_path))
        except Exception as e:
            print(f"Error initializing ProcessedDBLoader: {e}")
            raise

        try:
            self.raw_lake_reader = RawLakeReader(db_path=str(self.raw_lake_db_path))
        except ConnectionError as e:
            print(f"Error initializing RawLakeReader: {e}")
            # Depending on desired behavior, either re-raise or set to None and handle in run()
            raise  # Re-raising for now

        try:
            # ManifestManager will create its own connection.
            # It's designed to be independent or take an existing connection.
            # For this integration, let it manage its own based on path.
            self.manifest_manager = ManifestManager(db_path=str(self.manifest_db_path))
        except ConnectionError as e: # Assuming ManifestManager might raise ConnectionError
            print(f"Error initializing ManifestManager: {e}")
            raise # Re-raising for now

        # Note: self.raw_lake_con is still present from original code.
        # If RawLakeReader completely replaces its functionality, self.raw_lake_con could be removed.
        # For now, let's assume it might be used by other parts of the class not being modified here.
        # If not, it should be cleaned up. The RawLakeReader does what raw_lake_con was doing for reading raw_files.
        # Let's remove the direct self.raw_lake_con initialization for now as RawLakeReader handles it.
        # self.raw_lake_con = duckdb.connect(database=str(self.raw_lake_db_path), read_only=True)

        print(f"TransformationPipeline initialized. Manifest: {self.manifest_db_path}, RawLake (via Reader): {self.raw_lake_db_path}, ProcessedDB: {self.processed_db_path}, Schemas: {self.schema_config_path}")

    def find_pending_files(self) -> list[dict]:
        """
        Queries the manifest.db for all records with the status 'loaded_to_raw_lake'.

        Returns:
            list[dict]: A list of dictionaries, where each dictionary represents a record.
                        Keys are column names.
        """
        try:
            cursor = self.manifest_con.execute(
                "SELECT file_hash, file_path, status, registration_timestamp FROM file_manifest WHERE status = 'loaded_to_raw_lake'"
            )
            results = cursor.fetchall()
            if not results:
                return []

            # Get column names from cursor description
            column_names = [desc[0] for desc in cursor.description]

            # Convert list of tuples to list of dictionaries
            return [dict(zip(column_names, row)) for row in results]
        except duckdb.Error as e:
            print(f"Database error in find_pending_files: {e}")
            return []

    def run(self):
        logging.info("Transformation pipeline run method started.")
        try:
            pending_files_data = self.find_pending_files()
            logging.info(f"Found {len(pending_files_data)} tasks to process.")

            if not pending_files_data:
                print("目前沒有待處理的檔案。")
                return

            for file_data in pending_files_data:
                file_hash = file_data['file_hash']
                file_path = file_data['file_path']
                print(f"\nProcessing file: {file_path} (Hash: {file_hash[:8]})")

                try:
                    # 1. Get Raw Content
                    raw_content = self.raw_lake_reader.get_raw_content(file_hash)
                    print(f"[進度] 成功從 Raw Lake 讀取檔案 {file_hash}")
                    if raw_content is None:
                        print(f"Error: Raw content not found for {file_hash} in Raw Lake. Skipping.")
                        self.manifest_manager.update_status(file_hash, 'parse_error_no_content')
                        continue

                    # 2. Identify Schema
                    schema_name = self.schema_manager.identify_schema_from_content(raw_content)
                    print(f"[進度] 成功識別綱要為 {schema_name}")
                    if schema_name is None:
                        print(f"Warning: Could not identify schema for {file_path} (Hash: {file_hash[:8]}). Skipping.")
                        self.manifest_manager.update_status(file_hash, 'parse_error_schema_not_identified')
                        continue

                    schema_definition = self.schema_manager.schemas.get(schema_name)
                    if schema_definition is None: # Should be rare if identify_schema_from_content works
                        print(f"Error: Schema definition not found for identified schema '{schema_name}'. Skipping.")
                        self.manifest_manager.update_status(file_hash, 'parse_error_schema_missing')
                        continue
                    print(f"Identified schema '{schema_name}' for {file_path}")

                    # 3. Parse Data
                    dataframe = self.parser.parse(raw_content, schema_definition)
                    # parser.py 內部會印出詳細錯誤並回傳 None
                    if dataframe is None:
                        print(f"[進度] 解析資料失敗 {file_path} (Hash: {file_hash[:8]})")
                        # parser.py 內部已經 print 詳細錯誤
                        # 根據 parser 的錯誤類型 (例如解碼、Pandas內部錯誤)來設定狀態
                        # 這裡假設 parser 失敗主要是 parse_error_parser_failed
                        # 如果 parser.py 能拋出更具體的錯誤，這裡可以捕捉
                        self.manifest_manager.update_status(file_hash, 'parse_error_parser_failed')
                        continue # 處理下一個檔案
                    print(f"[進度] 成功解析資料為 DataFrame for {file_path}")


                    # 4. Validate Data
                    validated_df = self.validator.validate(dataframe, schema_definition)
                    # validator.py 內部會印出詳細錯誤並回傳 None

                    # 核心 Bug 修復：檢查 validated_df 是否為 None 或 empty
                    if validated_df is None or validated_df.empty:
                        # 雖然原始程式碼多處使用 print，但此處關鍵日誌應使用 logging
                        logging.warning(f"檔案 {file_path} (Hash: {file_hash[:8]}) 因數據驗證失敗或無有效數據，已中止後續載入流程。")
                        # validator.py 內部已經 print 詳細錯誤，這裡的 print 主要是流程提示
                        print(f"[進度] 資料驗證失敗或無有效數據 {file_path} (Hash: {file_hash[:8]})")
                        self.manifest_manager.update_status(file_hash, 'validation_error')
                        continue # 處理下一個檔案

                    # 原有的檢查條件 (dataframe.shape[0] > 0 and validated_df.empty) 其實已經被 validated_df.empty 涵蓋
                    # 如果 validated_df.empty 為 True，那麼 validated_df 就是空的，無論原始 dataframe 是否有數據。
                    # 如果 validated_df 不是 None 且不是 empty，才代表驗證成功且有數據。
                    print(f"[進度] 成功驗證 DataFrame for {file_path}")

                    # print("Validated DataFrame head:")
                    # print(validated_df.head())

                    # 5. Load Data
                    # Use 'table_name' from schema if defined, otherwise fallback to schema_name
                    table_name = schema_definition.get('table_name', schema_name)

                    # 將 schema_definition 傳遞給 load_dataframe
                    self.processed_loader.load_dataframe(validated_df, table_name, schema_definition)
                    print(f"[進度] 成功載入資料至 Processed DB")
                    self.manifest_manager.update_status(file_hash, 'processed')
                    print(f"[成功] 檔案 {file_hash} 已成功處理完畢！")

                # Errors during parsing stage (structural or decoding issues)
                except (pd.errors.ParserError, UnicodeDecodeError, ValueError) as parse_related_err:
                    # Check if the ValueError originates from the parser or is a more general one.
                    # For now, grouping them as transformation_failed if they occur before validation.
                    # If ValueError needs to be split based on origin (parser vs validator), more context in DataParser/DataValidator would be needed.
                    # Based on user feedback, parser-related ValueErrors should lead to 'transformation_failed'.
                    error_msg = f"解析或轉換階段錯誤: {parse_related_err}"
                    logging.error(f"[錯誤] 處理檔案 {file_path} (Hash: {file_hash[:8]}) 時發生解析或轉換階段錯誤: {error_msg}")
                    print(f"[錯誤] 處理檔案 {file_hash} 時發生解析或轉換階段錯誤: {error_msg}")
                    if file_hash:
                        self.manifest_manager.update_status(file_hash, 'transformation_failed')

                # Errors related to data type/value issues after successful parsing, but typically caught by DataValidator.
                # This block might catch other KeyErrors or TypeErrors not from parsing.
                # If DataValidator catches and returns None/empty, it's handled before this.
                # This block is more for unexpected errors after parsing but before/during loading, if not IO or DB related.
                except (TypeError, KeyError) as data_err: # Data-related errors not caught by explicit parsing exceptions
                    error_msg = f"資料處理錯誤 (非解析): {data_err}"
                    logging.error(f"[錯誤] 處理檔案 {file_path} (Hash: {file_hash[:8]}) 時發生資料處理錯誤: {error_msg}")
                    print(f"[錯誤] 處理檔案 {file_hash} 時發生資料處理錯誤: {error_msg}")
                    if file_hash:
                        # Deciding status: 'validation_error' if it implies bad data content,
                        # or 'transformation_failed' if it's a more structural issue missed by parser exceptions.
                        # Given the context, 'validation_error' seems appropriate if it's about data values/types
                        # not fitting subsequent processing steps after parsing.
                        self.manifest_manager.update_status(file_hash, 'validation_error')

                except Exception as e: # Catch-all for other unexpected errors (e.g., IO, DB issues not caught by specific handlers)
                    error_msg = f"未預期錯誤: {e}"
                    logging.error(f"[錯誤] 處理檔案 {file_path} (Hash: {file_hash[:8]}) 時發生嚴重未預期錯誤: {error_msg}")
                    print(f"[錯誤] 處理檔案 {file_hash} 時發生嚴重未預期錯誤: {error_msg}")
                    if file_hash: # Ensure file_hash is available
                        self.manifest_manager.update_status(file_hash, 'transformation_failed')
        finally:
            self.close()

    def close(self):
        # Close connections in a controlled manner
        if hasattr(self, 'manifest_con') and self.manifest_con:
            try:
                self.manifest_con.close()
            except duckdb.Error as e:
                print(f"Error closing manifest_con: {e}")

        if hasattr(self, 'raw_lake_reader') and self.raw_lake_reader:
            self.raw_lake_reader.close() # Manages its own connection closing

        if hasattr(self, 'manifest_manager') and self.manifest_manager:
            self.manifest_manager.close() # Manages its own connection closing

        if hasattr(self, 'processed_loader') and self.processed_loader:
            self.processed_loader.close() # Manages its own connection closing

        # The direct self.processed_con is no longer managed here.
        # if hasattr(self, 'processed_con') and self.processed_con:
        #     try:
        #         self.processed_con.close()
        #     except duckdb.Error as e:
        #         print(f"Error closing processed_con: {e}")

        print("TransformationPipeline connections closed.")

if __name__ == '__main__':
    import json # Moved import here for clarity within __main__
    # Basic example for quick testing.
    # This requires setting up a dummy config, schema file, and dummy DBs.

    current_dir = pathlib.Path(__file__).parent
    example_config_path = current_dir / "temp_transform_config.yaml"
    example_data_dir = current_dir / "example_transform_data"
    example_schema_path = current_dir / "example_schemas.json"

    # Clean up from previous runs
    if example_config_path.exists(): example_config_path.unlink()
    if example_schema_path.exists(): example_schema_path.unlink()
    # Simplified cleanup for example_data_dir
    if example_data_dir.exists():
        for item in example_data_dir.rglob('*'):
            if item.is_file(): item.unlink()
        for item in sorted(list(example_data_dir.rglob('*')), key=lambda p: len(p.parts), reverse=True):
            if item.is_dir(): item.rmdir() # rmdir only if empty
        if example_data_dir.exists() and not list(example_data_dir.iterdir()): # Check if empty before removing
             example_data_dir.rmdir()


    example_data_dir.mkdir(parents=True, exist_ok=True)

    manifest_db = example_data_dir / "manifest.db"
    raw_lake_db = example_data_dir / "raw_lake.db"
    processed_db = example_data_dir / "processed.db"

    # Create dummy schema file
    # This schema should be identifiable by SchemaManager from the raw_content below
    dummy_schemas_content = {
        "schemaA_csv": {
            "table_name": "target_table_A", # Explicit table name for processed data
            "keywords": ["keyword_for_schemaA", "important_data"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 2, # Number of rows to skip at the beginning of the CSV
            "columns": { # Column definitions with types for validation
                "id": {"dtype": "integer", "nullable": False},
                "name": {"dtype": "string", "nullable": False},
                "value": {"dtype": "float", "nullable": True},
                "event_date": {"dtype": "datetime", "nullable": True}
            }
        },
        "typeB_json": { # Renamed for clarity
            "table_name": "target_table_B",
            "keywords": ["beta report", "type b data"],
            "file_type": "json",
            # Assuming JSON is a list of objects, or a single object per file.
            # Parser needs to handle this structure.
            "columns": {
                 "report_id": {"dtype": "string", "nullable": False},
                 "metric": {"dtype": "float", "nullable": True}
            }
        }
    }
    with open(example_schema_path, 'w', encoding='utf-8') as f:
        json.dump(dummy_schemas_content, f)

    # Create dummy config
    example_config_content = f"""
database:
  manifest_db_path: "{manifest_db.as_posix()}"
  raw_lake_db_path: "{raw_lake_db.as_posix()}"
  processed_db_path: "{processed_db.as_posix()}"
paths:
  schema_config_path: "{example_schema_path.as_posix()}"
  input_directory: "dummy_input" # Not used by TransformationPipeline directly
    """
    with open(example_config_path, 'w', encoding='utf-8') as f:
        f.write(example_config_content)

    # Initialize dummy databases and insert sample data
    # Manifest DB
    con_m = duckdb.connect(str(manifest_db))
    con_m.execute("""
        CREATE TABLE IF NOT EXISTS file_manifest (
            file_hash VARCHAR PRIMARY KEY,
            file_path VARCHAR,
            registration_timestamp TIMESTAMP DEFAULT current_timestamp,
            status VARCHAR DEFAULT 'registered'
        );
    """)
    con_m.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)",
                  ("hash123abc", "/path/to/file1.txt", "loaded_to_raw_lake"))
    con_m.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)",
                  ("hash456def", "/path/to/file2.csv", "loaded_to_raw_lake"))
    con_m.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)",
                  ("hash789ghi", "/path/to/file3.json", "registered")) # This one should be ignored
    con_m.close()

    # Raw Lake DB
    con_rl = duckdb.connect(str(raw_lake_db))
    con_rl.execute("CREATE TABLE IF NOT EXISTS raw_files (file_hash VARCHAR PRIMARY KEY, raw_content BLOB);")
    # Insert content for 'hash123abc' that matches 'schemaA_csv'
    # This data includes lines to be skipped, and values that test validation (good, bad float, bad date)
    sample_raw_data_for_hash123abc = (
        b"keyword_for_schemaA\n"
        b"important_data_header_ignored\n"
        b"1,Alice,100.5,2023-01-15\n"
        b"2,Bob,invalid_float,2023-02-20\n" # invalid_float should become NaN
        b"3,Charlie,300.0,not-a-date\n"     # not-a-date should become NaT
        b"4,Eve,,2023-04-10\n"              # Empty value for nullable float, valid date
        b"5,Mallory,500.7,\n"               # Valid float, empty for nullable date
        b"invalid_id,Grace,600.0,2023-05-01" # invalid_id for integer column
    )
    con_rl.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)",
                   ("hash123abc", sample_raw_data_for_hash123abc))

    # Insert content for 'hash456def' - designed to fail schema identification or parsing
    sample_raw_data_for_hash456def = b"completely_different_data,unrelated_to_any_schema"
    con_rl.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)",
                   ("hash456def", sample_raw_data_for_hash456def))
    con_rl.close()

    # Processed DB (ProcessedDBLoader will create it if it doesn't exist via its __init__,
    # including parent directories. No need to explicitly create con_p here for this purpose)
    con_p = duckdb.connect(str(processed_db))
    con_p.close()


    print("Running TransformationPipeline example...")
    try:
        pipeline = TransformationPipeline(config_path=str(example_config_path))
        pipeline.run()
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nTransformation example finished.")
        # Optional: Add more cleanup here if needed
        if example_config_path.exists(): example_config_path.unlink(missing_ok=True)
        if example_schema_path.exists(): example_schema_path.unlink(missing_ok=True)
        # db files will be cleaned on next run or manually
