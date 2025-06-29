import os
import pandas as pd
import duckdb
import json
import argparse
import logging
import glob

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Default Configuration ---
DEFAULT_BRONZE_DIR = "data/bronze/taifex"
DEFAULT_CATALOG_FILE = "taifex_format_catalog.json"
DEFAULT_DB_SCHEMAS_FILE = "database_schemas.json"
DEFAULT_DUCKDB_FILE = "data/financial_data.duckdb"
SILVER_TABLE_NAME = "silver_fact_taifex_quotes"

def load_json_config(filepath):
    """Loads a JSON configuration file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {filepath}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {filepath}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading {filepath}: {e}")
        raise

def transform_data(df, column_mapping, data_types_config):
    """
    Transforms the DataFrame based on the catalog:
    - Renames columns.
    - Converts data types.
    - Handles potential errors during type conversion.
    """
    # Rename columns
    df = df.rename(columns=column_mapping)

    # Ensure all columns defined in mapping are present, add if missing
    for target_col in column_mapping.values():
        if target_col not in df.columns:
            df[target_col] = pd.NA # Or appropriate default like np.nan or None

    # Convert data types
    for col_name, col_type in data_types_config.items():
        if col_name in df.columns:
            try:
                if col_type == "datetime64[ns]":
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                elif col_type in ["float64", "float32", "double"]:
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                elif col_type in ["int64", "int32", "int16", "bigint"]:
                    # For integers, coercing to float first can help with non-standard missing values like '-'
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('float64').astype(pd.Int64Dtype()) # Use nullable Int
                else: # primarily 'object' or 'VARCHAR'
                    df[col_name] = df[col_name].astype(str)
            except Exception as e:
                logging.warning(f"Could not convert column '{col_name}' to type '{col_type}': {e}. Leaving as is or NaN.")
        else:
            logging.warning(f"Column '{col_name}' specified in data_types_config not found in DataFrame after renaming.")

    return df

def upsert_to_duckdb(con, table_name, df, schema_info):
    """
    Upserts data into a DuckDB table.
    Creates the table if it doesn't exist based on schema_info.
    """
    if df.empty:
        logging.info(f"DataFrame for table '{table_name}' is empty. Nothing to upsert.")
        return

    cols_def = ", ".join([f"\"{col['name']}\" {col['type']}" for col in schema_info['columns']])
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def})")

    primary_keys = schema_info.get('primary_keys')
    if not primary_keys:
        logging.warning(f"No primary keys defined for table {table_name}. Performing append-only.")
        con.append(table_name, df)
        return

    # For UPSERT, we need a temporary table
    temp_table_name = f"temp_{table_name}_{os.urandom(8).hex()}"
    con.register(temp_table_name, df)

    # Construct SET part of the UPDATE statement
    set_clause = ", ".join([f"\"{col['name']}\" = excluded.\"{col['name']}\"" for col in schema_info['columns'] if col['name'] not in primary_keys])

    # Construct WHERE part of the UPDATE statement (for matching rows)
    where_clause_update = " AND ".join([f"main.\"{pk}\" = excluded.\"{pk}\"" for pk in primary_keys])

    # Construct INSERT columns and EXCLUDED columns for values
    insert_columns = ", ".join([f"\"{col['name']}\"" for col in schema_info['columns']])
    excluded_columns = ", ".join([f"excluded.\"{col['name']}\"" for col in schema_info['columns']])

    # DuckDB's INSERT ... ON CONFLICT DO UPDATE (SQLite-like UPSERT)
    # The CONFLICT target must be a UNIQUE constraint or PRIMARY KEY.
    # Ensure primary keys have a unique constraint (DuckDB usually infers this for PRIMARY KEY)

    # Check if table has a primary key defined in DuckDB (not just in our JSON)
    # This is a bit complex to check directly, so we'll assume the CREATE TABLE works or rely on DuckDB errors.
    # For simplicity, we'll use a common way to define primary keys if they are not automatically unique constrained.
    # However, DuckDB's `PRIMARY KEY` definition in `CREATE TABLE` implies `UNIQUE` and `NOT NULL` for those columns.

    conflict_target = ", ".join([f"\"{pk}\"" for pk in primary_keys])

    upsert_sql = f"""
    INSERT INTO {table_name} ({insert_columns})
    SELECT {insert_columns} FROM {temp_table_name}
    ON CONFLICT ({conflict_target}) DO UPDATE SET {set_clause};
    """

    try:
        con.execute(upsert_sql)
        logging.info(f"Successfully upserted {len(df)} rows into '{table_name}'.")
    except Exception as e:
        logging.error(f"Error during UPSERT to '{table_name}': {e}")
        logging.info("Attempting append as fallback (if appropriate for your use case, or remove this).")
        # As a very basic fallback, you might append, but this can lead to duplicates if UPSERT was intended.
        # For a robust system, you'd handle this error more gracefully.
        # con.append(table_name, df)
    finally:
        con.unregister(temp_table_name)


def main():
    parser = argparse.ArgumentParser(description="Transform Taifex Bronze data and load to Silver in DuckDB.")
    parser.add_argument("--bronze-dir", type=str, default=DEFAULT_BRONZE_DIR,
                        help=f"Directory of Taifex Bronze Parquet files. Default: {DEFAULT_BRONZE_DIR}")
    parser.add_argument("--catalog-file", type=str, default=DEFAULT_CATALOG_FILE,
                        help=f"Path to the Taifex format catalog JSON. Default: {DEFAULT_CATALOG_FILE}")
    parser.add_argument("--db-schemas-file", type=str, default=DEFAULT_DB_SCHEMAS_FILE,
                        help=f"Path to the database schemas JSON. Default: {DEFAULT_DB_SCHEMAS_FILE}")
    parser.add_argument("--duckdb-file", type=str, default=DEFAULT_DUCKDB_FILE,
                        help=f"Path to the DuckDB database file. Default: {DEFAULT_DUCKDB_FILE}")

    args = parser.parse_args()

    logging.info("Starting Taifex Bronze to Silver transformation.")

    try:
        catalog = load_json_config(args.catalog_file)
        db_schemas = load_json_config(args.db_schemas_file)
    except Exception:
        logging.error("Failed to load critical configuration. Exiting.")
        return

    column_mapping = catalog.get("column_mapping_curated", {})
    data_types_config = catalog.get("data_types", {})
    silver_schema_info = db_schemas.get(SILVER_TABLE_NAME)

    if not silver_schema_info:
        logging.error(f"Schema for '{SILVER_TABLE_NAME}' not found in '{args.db_schemas_file}'. Exiting.")
        return

    if not os.path.isdir(args.bronze_dir):
        logging.error(f"Bronze directory '{args.bronze_dir}' does not exist. Exiting.")
        return

    parquet_files = glob.glob(os.path.join(args.bronze_dir, "*.parquet"))
    if not parquet_files:
        logging.warning(f"No Parquet files found in '{args.bronze_dir}'. Transformation complete.")
        return

    all_transformed_dfs = []
    for pq_file in parquet_files:
        try:
            logging.info(f"Processing Parquet file: {pq_file}")
            df = pd.read_parquet(pq_file)
            if df.empty:
                logging.info(f"File {pq_file} is empty. Skipping.")
                continue

            df_transformed = transform_data(df, column_mapping, data_types_config)

            # Select only columns defined in the silver schema to avoid issues with extra columns
            silver_columns = [col_def['name'] for col_def in silver_schema_info['columns']]
            # Ensure all necessary columns exist in df_transformed, fill with NA if not
            for sc in silver_columns:
                if sc not in df_transformed.columns:
                    df_transformed[sc] = pd.NA
            df_transformed = df_transformed[silver_columns]

            all_transformed_dfs.append(df_transformed)
        except Exception as e:
            logging.error(f"Failed to process file {pq_file}: {e}")
            continue # Skip to next file

    if not all_transformed_dfs:
        logging.info("No data to load into DuckDB after processing all files.")
        return

    combined_df = pd.concat(all_transformed_dfs, ignore_index=True)

    # Drop rows where all primary key columns are NaN, as they cannot be inserted/upserted.
    # This can happen if all original PK columns were bad and got coerced to NaT/NaN.
    pk_cols = silver_schema_info.get('primary_keys', [])
    if pk_cols:
        combined_df.dropna(subset=pk_cols, how='all', inplace=True)


    if combined_df.empty:
        logging.info("Final combined DataFrame is empty after processing and cleaning. Nothing to load.")
        return

    try:
        con = duckdb.connect(database=args.duckdb_file, read_only=False)

        # Create schema (silver) if not exists - DuckDB doesn't have explicit CREATE SCHEMA IF NOT EXISTS for default schema
        # Tables are created with schema implicitly if named like 'schema.table'
        # For simplicity, we are not using named schemas beyond 'main' here.

        upsert_to_duckdb(con, SILVER_TABLE_NAME, combined_df, silver_schema_info)

        # Create indexes if defined
        if 'indexes' in silver_schema_info:
            for index_def in silver_schema_info['indexes']:
                index_name = index_def['name']
                index_cols = ", ".join([f"\"{c}\"" for c in index_def['columns']])
                try:
                    # Check if index exists (DuckDB doesn't have a direct IF NOT EXISTS for CREATE INDEX on specific columns)
                    # A simple way is to try creating and catch exception if it exists, though not ideal.
                    # Or query system tables: PRAGMA show_indexes('silver_fact_taifex_quotes');
                    # For now, just attempt to create.
                    con.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {SILVER_TABLE_NAME} ({index_cols})")
                    logging.info(f"Ensured index '{index_name}' exists on '{SILVER_TABLE_NAME}'.")
                except Exception as e_idx:
                    logging.warning(f"Could not create index '{index_name}': {e_idx}. It might already exist or there's an issue.")

    except Exception as e:
        logging.error(f"An error occurred with DuckDB: {e}")
    finally:
        if 'con' in locals() and con:
            con.close()

    logging.info("Taifex Bronze to Silver transformation finished.")

if __name__ == "__main__":
    main()
