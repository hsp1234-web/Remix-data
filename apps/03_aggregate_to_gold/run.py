import os
import duckdb
import pandas as pd
import argparse
import logging
import json

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Default Configuration ---
DEFAULT_DUCKDB_FILE = "data/financial_data.duckdb"
DEFAULT_DB_SCHEMAS_FILE = "database_schemas.json" # To get primary keys for upsert
SILVER_TABLE_NAME = "silver_fact_taifex_quotes"
GOLD_TABLE_NAME = "gold_weekly_market_summary"

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

def aggregate_to_weekly(con):
    """
    Aggregates daily Taifex data from Silver to weekly Gold summaries.
    """
    # Heuristic to determine contract_group. Example: 'TXF202309' -> 'TX'
    # This might need refinement based on actual contract naming conventions.
    # For options, it might be based on the underlying, e.g., 'TXO' contracts.
    # For simplicity, let's assume 'contract' column has values like 'TX', 'MTX', 'TE', 'TF', etc.
    # Or, if it's full contract names like 'TXF202309', we extract the prefix.
    # For this example, let's assume a UDF or a CASE statement can define contract_group.
    # We'll simplify and group by the first two letters of the 'contract' field if it's specific,
    # or use a fixed value if we only care about a primary contract like 'TX'.

    # Let's assume for now we are primarily interested in 'TX' (Taiwan Stock Exchange Futures)
    # and want to aggregate all contracts that start with 'TX'.
    # A more robust solution would involve a mapping or more complex UDF.
    # For a generic approach, we can try to extract a common prefix.
    # For this iteration, we'll create a 'contract_group' based on the first 2 chars of 'contract' field.
    # This is a placeholder and likely needs adjustment based on real 'contract' field values.

    # Updated SQL to handle potential non-string 'contract' and ensure 'trade_date' is date type
    # Also, ensure we handle weeks correctly (e.g., ISO weeks)
    query = f"""
    WITH DailyDataWithWeek AS (
        SELECT
            CAST("trade_date" AS DATE) AS trade_day, -- Ensure trade_date is a DATE
            SUBSTRING(CAST("contract" AS VARCHAR), 1, 2) AS contract_group, -- Simplified: first 2 chars
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
            "pc_ratio_percentage" -- Assuming this column exists from your catalog
        FROM {SILVER_TABLE_NAME}
        WHERE "close" IS NOT NULL AND "trade_date" IS NOT NULL -- Basic filter for valid entries
    ),
    WeeklyAggregates AS (
        SELECT
            strftime(trade_day, '%Y-W%W') AS week_id, -- YYYY-Www (SQLite week, Sunday as first day)
                                                      -- For ISO week (Monday first day): strftime(trade_day, '%Y-W%V') - DuckDB might use %V for ISO
            MIN(trade_day) AS week_start_date, -- Actual first trading day of the week in data
            MAX(trade_day) AS week_end_date,   -- Actual last trading day of the week in data
            contract_group,
            FIRST("open" ORDER BY trade_day ASC) AS weekly_open,
            MAX("high") AS weekly_high,
            MIN("low") AS weekly_low,
            LAST("close" ORDER BY trade_day ASC) AS weekly_close,
            SUM("volume") AS total_weekly_volume,
            AVG("volume") AS avg_daily_volume,
            AVG("pc_ratio_percentage") AS avg_pc_ratio,
            AVG("open_interest") AS avg_open_interest
        FROM DailyDataWithWeek
        GROUP BY week_id, contract_group
    )
    SELECT
        week_start_date,
        week_end_date,
        week_id,
        contract_group,
        weekly_open,
        weekly_high,
        weekly_low,
        weekly_close,
        total_weekly_volume,
        avg_daily_volume,
        avg_pc_ratio,
        avg_open_interest
    FROM WeeklyAggregates
    ORDER BY contract_group, week_id;
    """
    try:
        logging.info(f"Executing weekly aggregation query from {SILVER_TABLE_NAME}...")
        weekly_df = con.execute(query).fetchdf()
        logging.info(f"Successfully fetched {len(weekly_df)} weekly aggregated rows.")
        return weekly_df
    except Exception as e:
        logging.error(f"Error during weekly aggregation SQL query: {e}")
        raise

def upsert_to_duckdb(con, table_name, df, schema_info):
    """
    Upserts data into a DuckDB table.
    Creates the table if it doesn't exist based on schema_info.
    """
    if df.empty:
        logging.info(f"DataFrame for table '{table_name}' is empty. Nothing to upsert.")
        return

    cols_def_list = []
    for col in schema_info['columns']:
        col_def = f"\"{col['name']}\" {col['type']}"
        if not col.get('nullable', True): # Default to nullable if not specified
            col_def += " NOT NULL"
        cols_def_list.append(col_def)

    # Add primary key constraint if defined
    primary_keys = schema_info.get('primary_keys')
    if primary_keys:
        pk_def = ", ".join([f"\"{pk}\"" for pk in primary_keys])
        cols_def_list.append(f"PRIMARY KEY ({pk_def})")

    cols_def_statement = ", ".join(cols_def_list)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def_statement})")

    if not primary_keys: # Should not happen for gold_weekly_market_summary as per schema
        logging.warning(f"No primary keys defined for table {table_name}. Performing append-only.")
        con.append(table_name, df)
        return

    temp_table_name = f"temp_{table_name}_{os.urandom(8).hex()}"
    con.register(temp_table_name, df)

    set_clause_parts = []
    for col in schema_info['columns']:
        if col['name'] not in primary_keys:
            set_clause_parts.append(f"\"{col['name']}\" = excluded.\"{col['name']}\"")
    set_clause = ", ".join(set_clause_parts)

    insert_columns = ", ".join([f"\"{col['name']}\"" for col in schema_info['columns']])
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
    finally:
        con.unregister(temp_table_name)


def main():
    parser = argparse.ArgumentParser(description="Aggregate Taifex Silver data to Gold weekly summaries.")
    parser.add_argument("--duckdb-file", type=str, default=DEFAULT_DUCKDB_FILE,
                        help=f"Path to the DuckDB database file. Default: {DEFAULT_DUCKDB_FILE}")
    parser.add_argument("--db-schemas-file", type=str, default=DEFAULT_DB_SCHEMAS_FILE,
                        help=f"Path to the database schemas JSON. Default: {DEFAULT_DB_SCHEMAS_FILE}")

    args = parser.parse_args()
    logging.info(f"Starting Taifex Silver to Gold aggregation to table '{GOLD_TABLE_NAME}'.")

    try:
        db_schemas = load_json_config(args.db_schemas_file)
        gold_schema_info = db_schemas.get(GOLD_TABLE_NAME)
        if not gold_schema_info:
            logging.error(f"Schema for '{GOLD_TABLE_NAME}' not found in '{args.db_schemas_file}'. Exiting.")
            return
    except Exception:
        logging.error("Failed to load database schemas configuration. Exiting.")
        return

    con = None
    try:
        con = duckdb.connect(database=args.duckdb_file, read_only=False)

        # Check if silver table exists and has data
        try:
            count_silver = con.execute(f"SELECT COUNT(*) FROM {SILVER_TABLE_NAME}").fetchone()[0]
            if count_silver == 0:
                logging.warning(f"Silver table '{SILVER_TABLE_NAME}' is empty. No data to aggregate. Exiting.")
                return
            logging.info(f"Silver table '{SILVER_TABLE_NAME}' contains {count_silver} rows.")
        except duckdb.CatalogException:
            logging.error(f"Silver table '{SILVER_TABLE_NAME}' does not exist. Run transformation first. Exiting.")
            return

        weekly_df = aggregate_to_weekly(con)

        if weekly_df.empty:
            logging.info("No weekly data generated from aggregation. Exiting.")
            return

        # Ensure DataFrame columns match the schema for Gold table (order and presence)
        gold_table_cols_ordered = [col_def['name'] for col_def in gold_schema_info['columns']]
        # Reindex and fill missing if any (though aggregation should produce them)
        weekly_df = weekly_df.reindex(columns=gold_table_cols_ordered)


        upsert_to_duckdb(con, GOLD_TABLE_NAME, weekly_df, gold_schema_info)

        # Create indexes if defined (similar to 02_transform_taifex)
        if 'indexes' in gold_schema_info:
            for index_def in gold_schema_info['indexes']:
                index_name = index_def['name']
                index_cols_str = ", ".join([f"\"{c}\"" for c in index_def['columns']])
                try:
                    con.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {GOLD_TABLE_NAME} ({index_cols_str})")
                    logging.info(f"Ensured index '{index_name}' exists on '{GOLD_TABLE_NAME}'.")
                except Exception as e_idx:
                    logging.warning(f"Could not create index '{index_name}' on {GOLD_TABLE_NAME}: {e_idx}.")

    except Exception as e:
        logging.error(f"An error occurred during the aggregation process: {e}", exc_info=True)
    finally:
        if con:
            con.close()

    logging.info("Taifex Silver to Gold aggregation finished.")

if __name__ == "__main__":
    main()
