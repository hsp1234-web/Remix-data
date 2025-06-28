# config.py

import os

# --- Project Root ---
# Assuming this config.py is in the project root or a subdirectory.
# For simplicity, let's assume it's in the root for now.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# --- Directories ---
DATABASE_DIR = os.path.join(PROJECT_ROOT, "database")
INPUT_DATA_DIR = os.path.join(PROJECT_ROOT, "Data test") # Corrected to match user's folder name
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# --- Database Paths ---
MANIFEST_DB_PATH = os.path.join(DATABASE_DIR, "manifest.db")
RAW_LAKE_DB_PATH = os.path.join(DATABASE_DIR, "raw_lake.db")
CURATED_MART_DB_PATH = os.path.join(DATABASE_DIR, "curated_mart.db")

# --- Logging Configuration ---
LOG_FILE_NAME_FORMAT = "pipeline_run_{timestamp}.log" # timestamp will be dynamically generated
LOG_LEVEL = "INFO" # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL

# --- File Processing ---
CHUNK_SIZE_BYTES = 8192 # For reading files in chunks for hashing

# --- API (Placeholder if needed in future) ---
# API_ENDPOINT_EXAMPLE = "https://api.example.com/data"
# API_KEY_EXAMPLE = "YOUR_API_KEY"

# --- Create directories if they don't exist ---
# This is a side effect, but useful for a self-contained config.
# Alternatively, this can be handled by the main application logic.
os.makedirs(DATABASE_DIR, exist_ok=True)
os.makedirs(INPUT_DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

if __name__ == '__main__':
    # Quick test to print out the configured paths
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Database Directory: {DATABASE_DIR}")
    print(f"Input Data Directory: {INPUT_DATA_DIR}")
    print(f"Log Directory: {LOG_DIR}")
    print(f"Manifest DB Path: {MANIFEST_DB_PATH}")
    print(f"Raw Lake DB Path: {RAW_LAKE_DB_PATH}")
    print(f"Curated Mart DB Path: {CURATED_MART_DB_PATH}")
