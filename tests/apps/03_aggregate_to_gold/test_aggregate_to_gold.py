import unittest
import os
import pandas as pd
import shutil
import sys
import duckdb
import json
from datetime import date

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from apps.x03_aggregate_to_gold.run import main as aggregate_main
from apps.x03_aggregate_to_gold.run import GOLD_TABLE_NAME, SILVER_TABLE_NAME

class TestAggregateToGold(unittest.TestCase):

    def setUp(self):
        self.fixture_dir = os.path.join(PROJECT_ROOT, "tests", "fixtures")
        self.base_test_output_dir = os.path.join(PROJECT_ROOT, "tests", "test_outputs")
        # Ensure base_test_output_dir exists, as other test classes might also use it.
        os.makedirs(self.base_test_output_dir, exist_ok=True)

        self.test_db_schemas_file = os.path.join(self.fixture_dir, "test_database_schemas.json")
        # Use a unique DB file name for this test class to prevent interference
        self.test_duckdb_file = os.path.join(self.base_test_output_dir, "test_financial_data_03_agg.duckdb")

        con = None
        try:
            con = duckdb.connect(database=self.test_duckdb_file, read_only=False)
            db_schemas = json.load(open(self.test_db_schemas_file))
            silver_schema = db_schemas.get(SILVER_TABLE_NAME)
            if not silver_schema:
                self.fail(f"Could not find schema for {SILVER_TABLE_NAME} in test_database_schemas.json")

            cols_def_list = [f"\"{col['name']}\" {col['type']}" for col in silver_schema['columns']]
            pk_def = ""
            if silver_schema.get('primary_keys'):
                pk_cols = ", ".join([f"\"{pk}\"" for pk in silver_schema['primary_keys']])
                pk_def = f", PRIMARY KEY ({pk_cols})"

            con.execute(f"DROP TABLE IF EXISTS {SILVER_TABLE_NAME}") # Ensure clean state
            con.execute(f"CREATE TABLE {SILVER_TABLE_NAME} ({', '.join(cols_def_list)}{pk_def})")

            # Using columns from test_database_schemas.json for silver_fact_taifex_quotes:
            # trade_date, contract, expiry_month_week, open, close, volume
            silver_test_data_for_df = [
                (date(2023, 1, 2), 'TX', '202301', 15000.0, 15050.0, 50000),
                (date(2023, 1, 3), 'TX', '202301', 15060.0, 15100.0, 52000),
                (date(2023, 1, 4), 'TX', '202301', 15110.0, 15180.0, 55000), # Added for more data in W01 for TX
                (date(2023, 1, 9), 'TX', '202302', 15200.0, 15220.0, 48000),
                (date(2023, 1, 10), 'TX', '202302', 15230.0, 15280.0, 51000),# Added for more data in W02 for TX
                (date(2023, 1, 2), 'MX', '202301W1', 3000.0, 3010.0, 1000),
            ]
            df_silver_fixture = pd.DataFrame(silver_test_data_for_df,
                                             columns=["trade_date", "contract", "expiry_month_week", "open", "close", "volume"])
            con.append(SILVER_TABLE_NAME, df_silver_fixture)

        except Exception as e:
            self.fail(f"setUp failed to prepare test DuckDB: {e}")
        finally:
            if con:
                con.close()

    def tearDown(self):
        if os.path.exists(self.test_duckdb_file):
            try:
                os.remove(self.test_duckdb_file)
            except OSError:
                pass
        # Do not remove base_test_output_dir here as other tests might be using it or its subdirs.
        # Each test class should manage its own specific output files/dirs.

    def test_aggregate_main_execution(self):
        original_argv = sys.argv
        sys.argv = [
            "apps/03_aggregate_to_gold/run.py",
            "--duckdb-file", self.test_duckdb_file,
            "--db-schemas-file", self.test_db_schemas_file
        ]

        try:
            aggregate_main()
        except SystemExit as e:
            self.fail(f"Aggregate script exited prematurely: {e}")
        finally:
            sys.argv = original_argv

        self.assertTrue(os.path.exists(self.test_duckdb_file))
        con = None
        try:
            con = duckdb.connect(database=self.test_duckdb_file, read_only=True)
            tables_df = con.execute("SHOW TABLES").fetchdf()
            self.assertIn(GOLD_TABLE_NAME, tables_df['name'].tolist(),
                          f"Table '{GOLD_TABLE_NAME}' should exist in DuckDB.")

            result_df = con.execute(f"SELECT * FROM {GOLD_TABLE_NAME} ORDER BY contract_group, week_id").fetchdf()

            # Based on sample data:
            # MX 2023-W01: 1 row
            # TX 2023-W01: 1 row (Jan 2,3,4)
            # TX 2023-W02: 1 row (Jan 9,10)
            self.assertEqual(len(result_df), 3, f"Number of rows in Gold table is incorrect. Got: \n{result_df}")

            # Using DuckDB's week format '%Y-W%V' for ISO weeks (Monday as first day)
            # date(2023,1,2) is Monday, week 1.
            # date(2023,1,9) is Monday, week 2.
            # The script uses '%Y-W%W' which is Sunday as first day. This might cause discrepancies.
            # Let's adjust test expectations based on '%Y-W%W' or assume script uses ISO week logic
            # The current script uses strftime(trade_day, '%Y-W%W') which is Sunday-based week numbering.
            # Jan 2, 2023 (Mon) -> 2023-W01 (Sunday as first day, so it's week 1)
            # Jan 9, 2023 (Mon) -> 2023-W02

            # TX 2023-W01 (Jan 2, 3, 4)
            tx_w01 = result_df[(result_df['contract_group'] == 'TX') & (result_df['week_id'] == '2023-W01')]
            self.assertEqual(len(tx_w01), 1, "Should be one record for TX 2023-W01")
            self.assertEqual(tx_w01['weekly_open'].iloc[0], 15000.0)  # First open on Jan 2
            self.assertEqual(tx_w01['weekly_close'].iloc[0], 15180.0) # Last close on Jan 4
            self.assertEqual(tx_w01['total_weekly_volume'].iloc[0], 50000 + 52000 + 55000) # Sum of volumes

            # TX 2023-W02 (Jan 9, 10)
            tx_w02 = result_df[(result_df['contract_group'] == 'TX') & (result_df['week_id'] == '2023-W02')]
            self.assertEqual(len(tx_w02), 1, "Should be one record for TX 2023-W02")
            self.assertEqual(tx_w02['weekly_open'].iloc[0], 15200.0) # First open on Jan 9
            self.assertEqual(tx_w02['weekly_close'].iloc[0], 15280.0) # Last close on Jan 10
            self.assertEqual(tx_w02['total_weekly_volume'].iloc[0], 48000 + 51000)

            # MX 2023-W01 (Jan 2)
            mx_w01 = result_df[(result_df['contract_group'] == 'MX') & (result_df['week_id'] == '2023-W01')]
            self.assertEqual(len(mx_w01), 1, "Should be one record for MX 2023-W01")
            self.assertEqual(mx_w01['weekly_open'].iloc[0], 3000.0)
            self.assertEqual(mx_w01['weekly_close'].iloc[0], 3010.0)
            self.assertEqual(mx_w01['total_weekly_volume'].iloc[0], 1000)

        except Exception as e:
            self.fail(f"Error during DuckDB verification for Gold table: {e}\nDF Content:\n{result_df if 'result_df' in locals() else 'N/A'}")
        finally:
            if con:
                con.close()

if __name__ == '__main__':
    unittest.main()
