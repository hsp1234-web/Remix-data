import unittest
import os
import pandas as pd
import shutil
import sys
import duckdb
import json

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from apps.x01_ingest_taifex.run import process_csv_to_parquet as ingest_to_bronze
from apps.x02_transform_taifex.run import main as transform_main
from apps.x02_transform_taifex.run import SILVER_TABLE_NAME

class TestTransformTaifex(unittest.TestCase):

    def setUp(self):
        self.fixture_dir = os.path.join(PROJECT_ROOT, "tests", "fixtures")
        self.base_test_output_dir = os.path.join(PROJECT_ROOT, "tests", "test_outputs")

        self.bronze_dir = os.path.join(self.base_test_output_dir, "bronze", "taifex_02_input")
        os.makedirs(self.bronze_dir, exist_ok=True)

        self.test_catalog_file = os.path.join(self.fixture_dir, "test_taifex_format_catalog.json")
        self.test_db_schemas_file = os.path.join(self.fixture_dir, "test_database_schemas.json")

        self.test_duckdb_file = os.path.join(self.base_test_output_dir, "test_financial_data_02.duckdb")

        self.source_csv_path = os.path.join(self.fixture_dir, "sample_taifex_daily.csv")
        if not os.path.exists(self.source_csv_path):
            self.fail(f"Source CSV fixture not found at {self.source_csv_path}")

        ingest_success = ingest_to_bronze(self.source_csv_path, self.bronze_dir)
        if not ingest_success:
            self.fail("Failed to create Bronze Parquet fixture for transform test.")

        self.bronze_parquet_filename = os.path.splitext(os.path.basename(self.source_csv_path))[0] + ".parquet"
        self.bronze_parquet_path = os.path.join(self.bronze_dir, self.bronze_parquet_filename)
        if not os.path.exists(self.bronze_parquet_path):
             self.fail(f"Bronze Parquet fixture was not created at {self.bronze_parquet_path}")


    def tearDown(self):
        if os.path.exists(self.bronze_dir):
            shutil.rmtree(self.bronze_dir)
        if os.path.exists(self.test_duckdb_file):
            try:
                os.remove(self.test_duckdb_file)
            except OSError as e: # Handle case where DB might still be locked briefly
                print(f"Warning: Could not remove test DuckDB file immediately: {e}")

        if os.path.exists(self.base_test_output_dir):
            try:
                if not os.listdir(self.base_test_output_dir):
                     shutil.rmtree(self.base_test_output_dir)
                # If base_test_output_dir still has other subdirs (e.g. from other tests), don't remove it
            except FileNotFoundError:
                pass
            except OSError as e:
                print(f"Warning: Could not cleanup base_test_output_dir: {e}")


    def test_transform_main_execution(self):
        """Test the main execution flow of the transformation script."""
        original_argv = sys.argv
        sys.argv = [
            "apps/02_transform_taifex/run.py",
            "--bronze-dir", self.bronze_dir,
            "--catalog-file", self.test_catalog_file,
            "--db-schemas-file", self.test_db_schemas_file,
            "--duckdb-file", self.test_duckdb_file
        ]

        try:
            transform_main()
        except SystemExit as e:
            self.fail(f"Transform script exited prematurely: {e}")
        finally:
            sys.argv = original_argv

        self.assertTrue(os.path.exists(self.test_duckdb_file), "DuckDB file should be created.")

        con = None
        try:
            con = duckdb.connect(database=self.test_duckdb_file, read_only=True)
            tables_df = con.execute("SHOW TABLES").fetchdf()
            self.assertIn(SILVER_TABLE_NAME, tables_df['name'].tolist(),
                          f"Table '{SILVER_TABLE_NAME}' should exist in DuckDB.")

            result_df = con.execute(f"SELECT * FROM {SILVER_TABLE_NAME}").fetchdf()
            self.assertEqual(len(result_df), 3, "Number of rows in Silver table should match input.")

            db_schema_config = json.load(open(self.test_db_schemas_file))
            expected_cols = [col['name'] for col in db_schema_config[SILVER_TABLE_NAME]['columns']]
            self.assertListEqual(sorted(list(result_df.columns)), sorted(expected_cols), "Silver table columns mismatch.")

            # Check 'open' for first row (original CSV 15000)
            # Order by trade_date and contract to ensure consistent first row for testing
            first_row_open = con.execute(f"SELECT open FROM {SILVER_TABLE_NAME} ORDER BY trade_date, contract LIMIT 1").fetchone()[0]
            self.assertEqual(first_row_open, 15000.0, "Transformed 'open' value mismatch.")

            # Check 'trade_date' type
            date_val_obj = con.execute(f"SELECT trade_date FROM {SILVER_TABLE_NAME} LIMIT 1").fetchone()[0]
            self.assertIsInstance(date_val_obj, pd.Timestamp.date_type, # type: ignore
                                  "trade_date should be a date type (datetime.date).")
            # Original '2023/01/15' should become date(2023, 1, 15)
            self.assertEqual(date_val_obj.year, 2023)
            self.assertEqual(date_val_obj.month, 1)
            self.assertEqual(date_val_obj.day, 15)


        except Exception as e:
            self.fail(f"Error during DuckDB verification: {e}")
        finally:
            if con:
                con.close()

if __name__ == '__main__':
    unittest.main()
