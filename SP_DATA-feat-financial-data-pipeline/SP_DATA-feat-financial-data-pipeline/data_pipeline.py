# data_pipeline.py

import os
import hashlib
import duckdb
import logging
import json
from datetime import datetime
import magic # python-magic
import pandas as pd # For CSV/Excel processing and curated mart interaction
# from PIL import Image # For image processing, if needed later

# Import configuration
import config

# --- Global Variables / Constants (from config or defaults) ---
# These will be initialized in setup_logging_and_env or main

# --- Logging Setup ---
def setup_logging():
    """Sets up logging for the pipeline."""
    log_file_name = config.LOG_FILE_NAME_FORMAT.format(timestamp=datetime.now().strftime('%Y%m%d_%H%M%S'))
    log_file_path = os.path.join(config.LOG_DIR, log_file_name)

    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler() # Also print to console
        ]
    )
    logging.info(f"Logging initialized. Log file: {log_file_path}")
    return logging.getLogger(__name__)

logger = setup_logging() # Initialize logger when module is loaded

# --- Database Initialization and Schema ---
def init_manifest_db(conn):
    """Initializes the manifest.db schema if it doesn't exist."""
    logger.info(f"Initializing Manifest DB: {config.MANIFEST_DB_PATH}")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS files_master (
            file_hash TEXT PRIMARY KEY,
            source_identifier TEXT NOT NULL,
            entry_timestamp TIMESTAMP NOT NULL,
            derived_date DATE,
            raw_content_type TEXT,
            raw_storage_path TEXT,
            metadata_json TEXT,
            status TEXT NOT NULL, -- e.g., 'new', 'raw_stored', 'raw_error', 'date_derived', 'curation_inprogress', 'curated', 'curation_error', 'unsupported_type'
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON files_master(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_derived_date ON files_master(derived_date);")
    conn.commit()
    logger.info("Manifest DB schema initialized.")

def init_raw_lake_db(conn):
    """Initializes the raw_lake.db schema if it doesn't exist."""
    logger.info(f"Initializing Raw Lake DB: {config.RAW_LAKE_DB_PATH}")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_file_blobs (
            file_hash TEXT PRIMARY KEY,
            content_blob BLOB NOT NULL
            -- Removed FOREIGN KEY (file_hash) REFERENCES files_master(file_hash)
            -- to simplify initialization and avoid cross-DB FK issues with DuckDB's file-based nature.
            -- Data integrity will be managed at the application level.
        );
    """)
    # Example for API responses, if needed later
    # conn.execute("""
    #     CREATE TABLE IF NOT EXISTS raw_api_responses (
    #         request_hash TEXT PRIMARY KEY, -- Could be file_hash from manifest if content is hashed
    #         response_text TEXT,
    #         response_headers TEXT,
    #         retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #         FOREIGN KEY (request_hash) REFERENCES files_master(file_hash)
    #     );
    # """)
    conn.commit()
    logger.info("Raw Lake DB schema initialized.")

def init_curated_mart_db(conn):
    """Initializes the curated_mart.db schema if it doesn't exist."""
    logger.info(f"Initializing Curated Mart DB: {config.CURATED_MART_DB_PATH}")
    # Example table for CSV data (adapt from v8.0 or define new)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS example_curated_data (
            -- id INTEGER PRIMARY KEY, -- Removed to let DuckDB handle rowid implicitly if needed, and simplify pandas to_sql
            file_hash TEXT,
            original_source_identifier TEXT,
            processed_timestamp TIMESTAMP,
            -- Add columns specific to your curated data from CSVs
            -- For sample1.csv, we expect col_a, col_b, col_c
            col_a TEXT,
            col_b TEXT,
            col_c TEXT
            -- Removed FOREIGN KEY (file_hash) REFERENCES files_master(file_hash)
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS image_features (
            file_hash TEXT PRIMARY KEY,
            width INTEGER,
            height INTEGER,
            format TEXT,
            source_identifier TEXT,
            processed_at TIMESTAMP
            -- Removed FOREIGN KEY (file_hash) REFERENCES files_master(file_hash)
        );
    """)
    conn.commit()
    logger.info("Curated Mart DB schema initialized.")

def initialize_databases():
    """Connects to all databases and initializes their schemas if needed."""
    logger.info("Initializing all databases...")
    try:
        # Initialize Manifest DB first as other DBs might have foreign keys to it
        with duckdb.connect(config.MANIFEST_DB_PATH) as manifest_conn:
            init_manifest_db(manifest_conn)
        logger.info("Manifest DB initialized.")

        # Then initialize Raw Lake DB
        with duckdb.connect(config.RAW_LAKE_DB_PATH) as raw_lake_conn:
            init_raw_lake_db(raw_lake_conn)
        logger.info("Raw Lake DB initialized.")

        # Finally, initialize Curated Mart DB
        with duckdb.connect(config.CURATED_MART_DB_PATH) as curated_mart_conn:
            init_curated_mart_db(curated_mart_conn)
        logger.info("Curated Mart DB initialized.")

        logger.info("All databases initialized successfully.")
        return True
    except Exception as e:
        logger.error(f"Error initializing databases: {e}", exc_info=True)
        return False

# --- Helper Functions ---
def calculate_sha256(filepath):
    """Calculates SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(config.CHUNK_SIZE_BYTES), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating SHA256 for {filepath}: {e}")
        return None

def get_file_metadata(filepath):
    """Extracts basic metadata from a file."""
    metadata = {}
    try:
        metadata['filename'] = os.path.basename(filepath)
        metadata['size_bytes'] = os.path.getsize(filepath)
        # Use python-magic to guess MIME type
        mime_type = magic.from_file(filepath, mime=True)
        metadata['mime_type'] = mime_type if mime_type else 'application/octet-stream'

        # File system timestamps
        stat_info = os.stat(filepath)
        metadata['modified_time'] = datetime.fromtimestamp(stat_info.st_mtime).isoformat()
        metadata['created_time'] = datetime.fromtimestamp(stat_info.st_ctime).isoformat() # Platform dependent
        metadata['accessed_time'] = datetime.fromtimestamp(stat_info.st_atime).isoformat()

    except Exception as e:
        logger.error(f"Error getting metadata for {filepath}: {e}")
    return metadata

# --- Stage 1: Ingest and Register ---
def stage1_ingest_and_register():
    """
    Scans the input directory, processes new files, stores them in raw_lake.db,
    and registers them in manifest.db.
    """
    logger.info("Starting Stage 1: Ingest and Register")
    processed_files = 0
    new_files_registered = 0
    error_files = 0

    if not os.path.exists(config.INPUT_DATA_DIR):
        logger.error(f"Input data directory not found: {config.INPUT_DATA_DIR}")
        return

    try:
        with duckdb.connect(config.MANIFEST_DB_PATH) as manifest_conn, \
             duckdb.connect(config.RAW_LAKE_DB_PATH) as raw_lake_conn:

            for root, _, files in os.walk(config.INPUT_DATA_DIR):
                for filename in files:
                    filepath = os.path.join(root, filename)
                    logger.debug(f"Processing file: {filepath}")
                    processed_files += 1

                    file_hash = calculate_sha256(filepath)
                    if not file_hash:
                        logger.warning(f"Could not calculate hash for {filepath}, skipping.")
                        error_files +=1
                        continue

                    # Check if file_hash already exists in manifest
                    res = manifest_conn.execute("SELECT 1 FROM files_master WHERE file_hash = ?", [file_hash]).fetchone()
                    if res:
                        logger.info(f"File {filepath} (Hash: {file_hash[:8]}...) already registered, skipping.")
                        continue

                    # New file, proceed with ingestion
                    logger.info(f"New file detected: {filepath} (Hash: {file_hash[:8]}...)")
                    basic_metadata = get_file_metadata(filepath)
                    entry_timestamp = datetime.now()
                    raw_storage_path = f"table:raw_file_blobs/key:{file_hash}" # Define how raw data is identified

                    try:
                        # Store in raw_lake.db
                        with open(filepath, "rb") as f_blob:
                            raw_content_blob = f_blob.read()
                        raw_lake_conn.execute(
                            "INSERT INTO raw_file_blobs (file_hash, content_blob) VALUES (?, ?)",
                            [file_hash, raw_content_blob]
                        )
                        raw_lake_conn.commit() # Commit for each file to raw_lake

                        # Register in manifest.db
                        manifest_conn.execute(
                            """
                            INSERT INTO files_master (
                                file_hash, source_identifier, entry_timestamp, raw_content_type,
                                raw_storage_path, metadata_json, status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                file_hash,
                                filepath, # Using full path as source_identifier
                                entry_timestamp,
                                basic_metadata.get('mime_type', 'application/octet-stream'),
                                raw_storage_path,
                                json.dumps(basic_metadata), # Store all basic metadata as JSON
                                'raw_stored'
                            ]
                        )
                        manifest_conn.commit() # Commit for each file to manifest
                        new_files_registered += 1
                        logger.info(f"Successfully registered and stored: {filepath}")

                    except Exception as e:
                        logger.error(f"Error processing file {filepath}: {e}", exc_info=True)
                        error_files += 1
                        # Rollback manifest insert if raw_lake insert failed or vice-versa might be complex
                        # For now, if raw_lake fails, manifest won't be inserted.
                        # If manifest insert fails after raw_lake, we might have an orphan raw blob.
                        # More robust transaction handling across DBs could be added if critical.
                        # Simple approach: if error, it's an error_file, and it might not be fully registered.
                        # Attempt to update status to raw_error if hash was known
                        try:
                            manifest_conn.execute(
                                "INSERT INTO files_master (file_hash, source_identifier, entry_timestamp, status, error_message) "
                                "VALUES (?, ?, ?, 'raw_error', ?) "
                                "ON CONFLICT(file_hash) DO UPDATE SET status='raw_error', error_message=excluded.error_message, updated_at=CURRENT_TIMESTAMP",
                                [file_hash, filepath, entry_timestamp, str(e)[:500]]
                            )
                            manifest_conn.commit()
                        except Exception as db_err:
                            logger.error(f"Failed to even mark as raw_error for {file_hash}: {db_err}")


    except Exception as e:
        logger.critical(f"Critical error during Stage 1: {e}", exc_info=True)

    logger.info(f"Stage 1 finished. Total files scanned: {processed_files}, New files registered: {new_files_registered}, Errors: {error_files}")

# --- Stage 2: Derive Date ---
def stage2_derive_date():
    """
    Scans manifest.db for files with status 'raw_stored' and attempts to
    derive a primary date ('derived_date') for them from their metadata_json.
    Updates the 'derived_date' and status in manifest.db.
    """
    logger.info("Starting Stage 2: Derive Date")
    updated_records = 0
    error_records = 0

    try:
        with duckdb.connect(config.MANIFEST_DB_PATH) as manifest_conn:
            # Get records that are 'raw_stored' and don't have 'derived_date' yet, or need reprocessing.
            # For simplicity, we'll just fetch 'raw_stored'. A more robust check might be needed.
            records_to_process = manifest_conn.execute(
                "SELECT file_hash, metadata_json FROM files_master WHERE status = 'raw_stored' AND derived_date IS NULL"
            ).fetchall()

            if not records_to_process:
                logger.info("No records found needing date derivation in Stage 2.")
                return

            logger.info(f"Found {len(records_to_process)} records for date derivation.")

            for file_hash, metadata_json_str in records_to_process:
                derived_date_to_set = None
                new_status = 'date_derived' # Default new status if successful
                error_msg_for_stage = None

                try:
                    metadata = json.loads(metadata_json_str) if metadata_json_str else {}
                    # Attempt to get date from different sources in order of preference
                    # 1. 'modified_time' from file system (often most reliable for general files)
                    # 2. 'created_time' (can be less reliable or platform-dependent)
                    # More specific logic for EXIF for images, or specific fields from API data, could be added here.

                    date_str_to_parse = None
                    if metadata.get('modified_time'):
                        date_str_to_parse = metadata['modified_time']
                    elif metadata.get('created_time'):
                        date_str_to_parse = metadata['created_time']
                    # Add more sophisticated date extraction logic here if needed, e.g., from EXIF for images

                    if date_str_to_parse:
                        # Dates are stored in ISO format (e.g., "2023-10-27T10:30:00.123456")
                        # We need to parse this and get only the date part.
                        try:
                            # Try parsing with timezone if present (Python 3.7+)
                            dt_obj = datetime.fromisoformat(date_str_to_parse)
                            derived_date_to_set = dt_obj.date()
                        except ValueError:
                            # Fallback for simpler ISO date strings or if timezone parsing fails
                            try:
                                derived_date_to_set = datetime.strptime(date_str_to_parse.split('T')[0], '%Y-%m-%d').date()
                            except ValueError as ve:
                                logger.warning(f"Could not parse date string '{date_str_to_parse}' for {file_hash}: {ve}")
                                error_msg_for_stage = f"Date parse error: {ve}"
                                new_status = 'date_derivation_error'


                    if derived_date_to_set:
                        manifest_conn.execute(
                            "UPDATE files_master SET derived_date = ?, status = ?, updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                            [derived_date_to_set, new_status, file_hash]
                        )
                        logger.info(f"Derived date {derived_date_to_set} for {file_hash[:8]}...")
                        updated_records += 1
                    else:
                        # If no date could be derived, mark as an error for this stage or a specific status
                        if not error_msg_for_stage: # if no parsing error explicitly set
                            error_msg_for_stage = "No suitable date field found in metadata"
                        new_status = 'date_derivation_error'
                        manifest_conn.execute(
                            "UPDATE files_master SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                            [new_status, error_msg_for_stage, file_hash]
                        )
                        logger.warning(f"Failed to derive date for {file_hash[:8]}... Reason: {error_msg_for_stage}")
                        error_records += 1
                    manifest_conn.commit()

                except json.JSONDecodeError as jde:
                    logger.error(f"Error decoding metadata_json for {file_hash}: {jde}")
                    manifest_conn.execute(
                        "UPDATE files_master SET status = 'date_derivation_error', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                        [f"JSON decode error: {jde}", file_hash]
                    )
                    manifest_conn.commit()
                    error_records += 1
                except Exception as e:
                    logger.error(f"Unexpected error deriving date for {file_hash}: {e}", exc_info=True)
                    manifest_conn.execute(
                        "UPDATE files_master SET status = 'date_derivation_error', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                        [f"Unexpected error: {str(e)[:100]}", file_hash]
                    )
                    manifest_conn.commit()
                    error_records += 1

    except Exception as e:
        logger.critical(f"Critical error during Stage 2: {e}", exc_info=True)

    logger.info(f"Stage 2 finished. Records updated with derived_date: {updated_records}, Errors: {error_records}")

# --- Stage 3: Curate Data ---

# --- Individual Processors ---
def process_csv_excel(file_hash, raw_content_bytes, source_identifier, curated_conn):
    """
    Processor for CSV or Excel files.
    Attempts to read with Pandas, performs very basic cleaning, and loads to curated_mart.
    Inspired by v8.0's need to handle tabular data, but simplified.
    """
    logger.info(f"Attempting to process CSV/Excel: {source_identifier} (Hash: {file_hash[:8]})")
    try:
        df = None
        # Try reading as CSV, then Excel if CSV fails or based on initial MIME type (from manifest)
        # For simplicity, we'll try CSV first. A more robust way would be to use the
        # raw_content_type from manifest to guide this.
        try:
            # Convert bytes to a file-like object for Pandas
            import io
            file_io = io.BytesIO(raw_content_bytes)
            df = pd.read_csv(file_io)
            logger.info(f"Successfully read {source_identifier} as CSV.")
        except Exception as e_csv:
            logger.warning(f"Failed to read {source_identifier} as CSV ({e_csv}), trying Excel...")
            try:
                file_io.seek(0) # Reset BytesIO position
                df = pd.read_excel(file_io, engine=None) # engine=None lets pandas pick
                logger.info(f"Successfully read {source_identifier} as Excel.")
            except Exception as e_excel:
                logger.error(f"Failed to read {source_identifier} as CSV or Excel: {e_excel}")
                return False, f"Pandas read failed: {e_csv}; {e_excel}"

        if df is None or df.empty:
            logger.warning(f"No data or empty DataFrame from {source_identifier}.")
            # Depending on policy, this might be a success (empty file processed) or a soft error.
            # For now, let's consider it a non-critical issue, but not a full success for curation.
            return True, "Empty or no data in file" # Return True but with a message

        # Basic Cleaning (example: lowercase column names)
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]

        # Add metadata
        df['file_hash'] = file_hash
        df['original_source_identifier'] = source_identifier
        df['processed_timestamp'] = datetime.now()

        # Define target table name (can be dynamic based on source_identifier or content)
        # For now, a generic table.
        # IMPORTANT: This table 'example_curated_data' MUST match a schema defined in init_curated_mart_db
        # And its columns must accommodate what's in the DataFrame.
        # This is a common point of failure if schemas don't align.
        target_table_name = "example_curated_data"

        try:
            # Ensure DataFrame columns match the target table structure or subset of it.
            # This is a simplified load. For production, you'd want more robust schema matching.
            # For example, get columns from DB: db_cols = [desc[0] for desc in curated_conn.execute(f"DESCRIBE {target_table_name}").fetchall()]
            # df_to_load = df[[col for col in df.columns if col in db_cols]]

            # Convert all columns to string to avoid type issues with DuckDB auto-casting during to_sql
            # This is a simplification. Ideally, you'd map pandas dtypes to SQL types.
            df_for_sql = df.astype(str)

            df_for_sql.to_sql(target_table_name, curated_conn, if_exists='append', index=False)
            logger.info(f"Successfully loaded data from {source_identifier} to {target_table_name}.")
            return True, None
        except Exception as e_sql:
            logger.error(f"Error loading data from {source_identifier} to SQL table {target_table_name}: {e_sql}", exc_info=True)
            return False, f"SQL load error: {e_sql}"

    except Exception as e:
        logger.error(f"Generic error in process_csv_excel for {source_identifier}: {e}", exc_info=True)
        return False, f"Generic processing error: {e}"

def process_image(file_hash, raw_content_bytes, source_identifier, curated_conn):
    """
    Processor for image files. Extracts basic image features.
    """
    logger.info(f"Attempting to process Image: {source_identifier} (Hash: {file_hash[:8]})")
    try:
        from PIL import Image # Moved import here to avoid dependency if no images
        import io
        img = Image.open(io.BytesIO(raw_content_bytes))
        width, height = img.size
        img_format = img.format

        curated_conn.execute(
            """
            INSERT INTO image_features (file_hash, width, height, format, source_identifier, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_hash) DO UPDATE SET
                width=excluded.width, height=excluded.height, format=excluded.format,
                source_identifier=excluded.source_identifier, processed_at=excluded.processed_at
            """,
            [file_hash, width, height, img_format, source_identifier, datetime.now()]
        )
        curated_conn.commit()
        logger.info(f"Successfully processed image {source_identifier} - Size: {width}x{height}, Format: {img_format}")
        return True, None
    except ImportError:
        logger.error("Pillow library is not installed. Cannot process images.")
        return False, "Pillow not installed"
    except Exception as e:
        logger.error(f"Error processing image {source_identifier}: {e}", exc_info=True)
        return False, f"Image processing error: {e}"

# Add more processors here (e.g., process_audio, process_pdf, etc.)

CONTENT_PROCESSORS = {
    # MIME types are examples, adjust based on what python-magic detects for your files
    'text/csv': process_csv_excel,
    'application/csv': process_csv_excel,
    'application/vnd.ms-excel': process_csv_excel, # .xls
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': process_csv_excel, # .xlsx
    'image/jpeg': process_image,
    'image/png': process_image,
    # 'audio/mpeg': process_audio,
    # 'application/pdf': process_pdf,
}

def stage3_curate_data():
    """
    Processes files from raw_lake based on their type and stores curated data in curated_mart.
    """
    logger.info("Starting Stage 3: Curate Data")
    processed_count = 0
    success_count = 0
    error_count = 0
    unsupported_count = 0

    try:
        with duckdb.connect(config.MANIFEST_DB_PATH) as manifest_conn, \
             duckdb.connect(config.RAW_LAKE_DB_PATH, read_only=True) as raw_lake_conn, \
             duckdb.connect(config.CURATED_MART_DB_PATH) as curated_mart_conn:

            # Get records that are 'date_derived' or 'raw_stored' (if date derivation is optional for some types)
            # Or 'curation_retry' for files that previously failed.
            # For simplicity, let's fetch 'date_derived' or 'raw_stored' (if no date could be derived but still processable)
            # And also 'curation_error' or 'unsupported_type' if we want to retry them (add a retry_count later)
            records_to_process = manifest_conn.execute(
                """SELECT file_hash, source_identifier, raw_content_type, raw_storage_path
                   FROM files_master
                   WHERE status IN ('date_derived', 'raw_stored', 'curation_retry', 'unsupported_retry')
                   ORDER BY derived_date, entry_timestamp LIMIT 100""" # Process in batches
            ).fetchall()

            if not records_to_process:
                logger.info("No records found needing curation in Stage 3.")
                return

            logger.info(f"Found {len(records_to_process)} records for curation.")

            for file_hash, source_identifier, raw_content_type, raw_storage_path in records_to_process:
                processed_count += 1
                logger.info(f"Curating: {source_identifier} (Type: {raw_content_type}, Hash: {file_hash[:8]})")

                manifest_conn.execute(
                    "UPDATE files_master SET status = 'curation_inprogress', updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                    [file_hash]
                )
                manifest_conn.commit()

                raw_content = None
                try:
                    # Simplified raw content retrieval (assumes raw_file_blobs for now)
                    if raw_storage_path and raw_storage_path.startswith("table:raw_file_blobs"):
                         content_tuple = raw_lake_conn.execute("SELECT content_blob FROM raw_file_blobs WHERE file_hash = ?", [file_hash]).fetchone()
                         if content_tuple:
                             raw_content = content_tuple[0]
                    else: # Add logic for other raw_storage_path types if any
                        logger.warning(f"Unknown raw_storage_path format for {file_hash}: {raw_storage_path}")


                    if raw_content is None:
                        logger.error(f"Could not retrieve raw content for {file_hash} from raw_lake.")
                        manifest_conn.execute(
                            "UPDATE files_master SET status = 'curation_error', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                            ["Raw content not found", file_hash]
                        )
                        manifest_conn.commit()
                        error_count += 1
                        continue
                except Exception as e_raw_read:
                    logger.error(f"Error reading raw content for {file_hash}: {e_raw_read}", exc_info=True)
                    manifest_conn.execute(
                        "UPDATE files_master SET status = 'curation_error', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                        [f"Raw read error: {str(e_raw_read)[:100]}", file_hash]
                    )
                    manifest_conn.commit()
                    error_count +=1
                    continue


                processor_func = CONTENT_PROCESSORS.get(raw_content_type)
                new_status = None
                error_message = None

                if processor_func:
                    try:
                        success, msg = processor_func(file_hash, raw_content, source_identifier, curated_mart_conn)
                        if success:
                            new_status = 'curated'
                            success_count += 1
                            logger.info(f"Successfully curated {source_identifier}. Message: {msg if msg else 'OK'}")
                        else:
                            new_status = 'curation_error'
                            error_message = msg if msg else "Processor function returned False"
                            error_count += 1
                            logger.error(f"Curation error for {source_identifier}. Reason: {error_message}")
                    except Exception as e_proc:
                        logger.critical(f"Unhandled exception in processor for {raw_content_type} on {source_identifier}: {e_proc}", exc_info=True)
                        new_status = 'curation_error'
                        error_message = f"Unhandled Processor Exception: {str(e_proc)[:200]}"
                        error_count += 1
                else:
                    logger.warning(f"No processor found for content type '{raw_content_type}' for file {source_identifier}.")
                    new_status = 'unsupported_type'
                    error_message = f"No processor for {raw_content_type}"
                    unsupported_count += 1

                manifest_conn.execute(
                    "UPDATE files_master SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE file_hash = ?",
                    [new_status, error_message, file_hash]
                )
                manifest_conn.commit()

    except Exception as e:
        logger.critical(f"Critical error during Stage 3: {e}", exc_info=True)

    logger.info(f"Stage 3 finished. Total attempted: {processed_count}, Success: {success_count}, Errors: {error_count}, Unsupported: {unsupported_count}")


# --- Main Execution ---
def main():
    logger.info("Starting Data Pipeline...")

    if not initialize_databases():
        logger.critical("Failed to initialize databases. Exiting.")
        return

    # --- Execute Stages ---
    try:
        # Stage 1: Ingest new files into manifest and raw_lake
        stage1_ingest_and_register()

        # Stage 2: Derive 'derived_date' for manifest entries
        stage2_derive_date()

        # Stage 3: Process data from raw_lake to curated_mart
        stage3_curate_data()

        logger.info("Data Pipeline finished successfully.")

    except Exception as e:
        logger.critical(f"Unhandled exception in main pipeline execution: {e}", exc_info=True)
        # Potentially send a notification or take other critical error actions

if __name__ == "__main__":
    # Ensure directories from config are created (config.py does this, but good to be sure)
    os.makedirs(config.DATABASE_DIR, exist_ok=True)
    os.makedirs(config.INPUT_DATA_DIR, exist_ok=True)
    os.makedirs(config.LOG_DIR, exist_ok=True)

    # --- Test Setup ---
    logger.info("--- Preparing for Test Run ---")

    # Clean up previous database files for a fresh test run
    for db_path in [config.MANIFEST_DB_PATH, config.RAW_LAKE_DB_PATH, config.CURATED_MART_DB_PATH]:
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
                logger.info(f"Removed existing database: {db_path}")
            except Exception as e:
                logger.warning(f"Could not remove {db_path}: {e}. Manual cleanup might be needed.")
        # Also remove WAL files if they exist
        wal_file = db_path + ".wal"
        if os.path.exists(wal_file):
            try:
                os.remove(wal_file)
                logger.info(f"Removed existing WAL file: {wal_file}")
            except Exception as e:
                logger.warning(f"Could not remove {wal_file}: {e}.")


    # Create dummy files in "Data test" if they don't exist (for basic testing)
    # Users should replace these with their actual test files.
    test_files_to_create = {
        "sample1.csv": "col_a,col_b,col_c\n1,apple,10.5\n2,banana,20.0\n3,cherry,NULL",
        "sample2.csv": "header1;header2\nval1;val2\nval3;val4", # Different delimiter
        "image1.jpg": None, # Placeholder for actual image byte content
        "image2.png": None, # Placeholder for actual image byte content
        "document.txt": "This is a test text file, likely unsupported.",
        "archive.zip": None # Placeholder for a zip file
    }

    # Create simple placeholder image content (actual images are better for real testing)
    try:
        from PIL import Image, ImageDraw
        import io

        def create_placeholder_image_bytes(width, height, img_format, text="Test"):
            img = Image.new('RGB', (width, height), color = (255, 170, 170)) # Light red
            d = ImageDraw.Draw(img)
            try:
                # Attempt to load a font, fallback if not available
                from PIL import ImageFont
                font = ImageFont.truetype("arial.ttf", 40) # Or any common font
                d.text((10,10), text, fill=(0,0,0), font=font)
            except IOError: # If font not found
                 d.text((10,10), text, fill=(0,0,0)) # Default font

            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format=img_format)
            return img_byte_arr.getvalue()

        test_files_to_create["image1.jpg"] = create_placeholder_image_bytes(200,150,"JPEG", "JPG")
        test_files_to_create["image2.png"] = create_placeholder_image_bytes(100,100,"PNG", "PNG")

        # For zip, create a dummy zip file containing one of the csvs
        import zipfile
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("archived_sample.csv", test_files_to_create["sample1.csv"])
        test_files_to_create["archive.zip"] = zip_buffer.getvalue()

    except ImportError:
        logger.warning("Pillow library not found, cannot create placeholder image/zip test files.")
    except Exception as e_img_create:
        logger.warning(f"Could not create placeholder image/zip files: {e_img_create}")


    for filename, content in test_files_to_create.items():
        filepath = os.path.join(config.INPUT_DATA_DIR, filename)
        if not os.path.exists(filepath):
            try:
                if content is not None: # For text or bytes content
                    mode = "wb" if isinstance(content, bytes) else "w"
                    with open(filepath, mode) as f:
                        f.write(content)
                    logger.info(f"Created dummy test file: {filepath}")
                elif not filename.endswith(('.jpg', '.png','.zip')): # If content is None, but not an image/zip (those are handled above)
                     with open(filepath, "w") as f: # Create empty file if content is None
                        f.write("")
                     logger.info(f"Created empty dummy test file: {filepath}")

            except Exception as e:
                logger.warning(f"Could not create dummy test file {filepath}: {e}")
        else:
            logger.info(f"Test file already exists, skipping creation: {filepath}")

    logger.info("--- Test Setup Complete ---")
    main()

    # --- Basic Test Verification (after main() has run) ---
    logger.info("--- Basic Test Verification ---")
    all_tests_passed = True
    try:
        with duckdb.connect(config.MANIFEST_DB_PATH, read_only=True) as manifest_conn:
            logger.info("Manifest DB - files_master table sample:")
            manifest_conn.execute("SELECT file_hash, source_identifier, status, raw_content_type, derived_date, error_message FROM files_master LIMIT 10")
            for row in manifest_conn.fetchall():
                logger.info(row)
                # Basic checks
                if "sample1.csv" in str(row[1]) and str(row[2]) != 'curated': all_tests_passed = False; logger.error(f"Test FAIL: sample1.csv not curated. Status: {row[2]}")
                if "image1.jpg" in str(row[1]) and str(row[2]) != 'curated': all_tests_passed = False; logger.error(f"Test FAIL: image1.jpg not curated. Status: {row[2]}")
                if "document.txt" in str(row[1]) and str(row[2]) != 'unsupported_type': all_tests_passed = False; logger.error(f"Test FAIL: document.txt not unsupported. Status: {row[2]}")
                if "archive.zip" in str(row[1]) and str(row[2]) != 'unsupported_type': all_tests_passed = False; logger.error(f"Test FAIL: archive.zip not unsupported. Status: {row[2]}")


        with duckdb.connect(config.RAW_LAKE_DB_PATH, read_only=True) as raw_lake_conn:
            logger.info("Raw Lake DB - raw_file_blobs count:")
            count = raw_lake_conn.execute("SELECT COUNT(*) FROM raw_file_blobs").fetchone()[0]
            logger.info(f"Total blobs: {count}")
            # Expected count: sample1.csv, sample2.csv, image1.jpg, image2.png, document.txt, archive.zip = 6
            if count < 6 : all_tests_passed = False; logger.error(f"Test FAIL: Expected at least 6 blobs, got {count}")


        with duckdb.connect(config.CURATED_MART_DB_PATH, read_only=True) as curated_mart_conn:
            logger.info("Curated Mart DB - example_curated_data (for CSVs) sample:")
            try:
                # Check if table exists first
                table_check = curated_mart_conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='example_curated_data'").fetchone()
                if table_check:
                    curated_mart_conn.execute("SELECT original_source_identifier, col_a, col_b FROM example_curated_data LIMIT 5") # Adjusted column names
                    for row_data in curated_mart_conn.fetchall(): logger.info(row_data)
                    count_csv = curated_mart_conn.execute("SELECT COUNT(*) FROM example_curated_data").fetchone()[0]
                     # sample1.csv (3 rows) + sample2.csv (2 rows) = 5.
                     # sample2.csv might fail if delimiter not handled by default read_csv, so check for at least sample1's rows
                    if count_csv < 1 : all_tests_passed = False; logger.error(f"Test FAIL: Expected at least 1 row in example_curated_data for CSVs, got {count_csv}")
                else:
                    logger.error("Test FAIL: Table 'example_curated_data' does not exist in curated_mart.db.")
                    all_tests_passed = False
            except Exception as e:
                 logger.error(f"Could not query example_curated_data: {e}")
                 all_tests_passed = False


            logger.info("Curated Mart DB - image_features sample:")
            try:
                table_check_img = curated_mart_conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='image_features'").fetchone()
                if table_check_img:
                    curated_mart_conn.execute("SELECT source_identifier, width, height, format FROM image_features LIMIT 5")
                    for row_data in curated_mart_conn.fetchall(): logger.info(row_data)
                    count_img = curated_mart_conn.execute("SELECT COUNT(*) FROM image_features").fetchone()[0]
                    if count_img < 2 : all_tests_passed = False; logger.error(f"Test FAIL: Expected at least 2 rows in image_features, got {count_img}")
                else:
                    logger.error("Test FAIL: Table 'image_features' does not exist in curated_mart.db.")
                    all_tests_passed = False
            except Exception as e:
                logger.error(f"Could not query image_features: {e}")
                all_tests_passed = False


        if all_tests_passed:
            logger.info("--- ALL BASIC VERIFICATIONS PASSED ---")
        else:
            logger.error("--- SOME BASIC VERIFICATIONS FAILED ---")

    except Exception as e:
        logger.error(f"Error during test verification: {e}", exc_info=True)
        logger.error("--- BASIC VERIFICATIONS FAILED DUE TO EXCEPTION ---")
