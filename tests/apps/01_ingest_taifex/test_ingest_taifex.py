import unittest
import os
import pandas as pd
import shutil
import sys

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from apps.x01_ingest_taifex.run import process_csv_to_parquet # Corrected import path

class TestIngestTaifex(unittest.TestCase):

    def setUp(self):
        self.fixture_dir = os.path.join(PROJECT_ROOT, "tests", "fixtures")
        self.base_test_output_dir = os.path.join(PROJECT_ROOT, "tests", "test_outputs")
        self.test_output_dir = os.path.join(self.base_test_output_dir, "bronze", "taifex_01") # Unique dir for this test class
        os.makedirs(self.test_output_dir, exist_ok=True)

        self.sample_csv_path = os.path.join(self.fixture_dir, "sample_taifex_daily.csv")
        # The output parquet filename is derived from the input CSV filename in the SUT (System Under Test)
        self.output_parquet_filename = os.path.splitext(os.path.basename(self.sample_csv_path))[0] + ".parquet"
        self.output_parquet_path = os.path.join(self.test_output_dir, self.output_parquet_filename)

    def tearDown(self):
        # Clean up the specific test output directory
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        # Attempt to clean up base if it's empty
        if os.path.exists(self.base_test_output_dir) and not os.listdir(self.base_test_output_dir):
             shutil.rmtree(self.base_test_output_dir)


    def test_process_taifex_csv_to_parquet_success(self):
        """Test successful conversion of a sample Taifex CSV to Parquet."""
        success = process_csv_to_parquet(self.sample_csv_path, self.test_output_dir) # SUT takes output_dir

        self.assertTrue(success, "Processing should return True on success.")
        self.assertTrue(os.path.exists(self.output_parquet_path), f"Output Parquet file should exist at {self.output_parquet_path}")

        try:
            df_parquet = pd.read_parquet(self.output_parquet_path)
            df_csv = pd.read_csv(self.sample_csv_path, encoding='utf-8')

            self.assertEqual(len(df_parquet), len(df_csv), "DataFrame lengths should match.")
            self.assertListEqual(sorted(list(df_parquet.columns)), sorted(list(df_csv.columns)), "Column names should match.")
            self.assertEqual(df_parquet['契約'].iloc[0], df_csv['契約'].iloc[0], "Content mismatch in '契約' column.")
            self.assertEqual(int(df_parquet['成交量'].iloc[0]), int(df_csv['成交量'].iloc[0]), "Content mismatch in '成交量' column.")

        except Exception as e:
            self.fail(f"Error reading or comparing Parquet/CSV files: {e}")

    def test_process_taifex_csv_to_parquet_file_not_found(self):
        """Test handling of a non-existent input CSV file."""
        non_existent_csv = os.path.join(self.fixture_dir, "non_existent_taifex.csv")
        success = process_csv_to_parquet(non_existent_csv, self.test_output_dir)

        self.assertFalse(success, "Processing should return False if input file not found.")
        # Check that no files were created in the specific output directory for this test
        self.assertEqual(len(os.listdir(self.test_output_dir)), 0,
                         "No files should be created in output directory for non-existent input.")


    def test_process_taifex_csv_with_encoding_issue_handled(self):
        """Test if cp950 fallback encoding works."""
        tricky_csv_filename = "sample_taifex_big5.csv"
        tricky_csv_path = os.path.join(self.fixture_dir, tricky_csv_filename)
        content_big5 = "交易日期,契約\n2023/01/01,測試契約".encode('cp950')

        os.makedirs(self.fixture_dir, exist_ok=True)
        with open(tricky_csv_path, "wb") as f:
            f.write(content_big5)

        output_filename_expected = os.path.splitext(tricky_csv_filename)[0] + ".parquet"
        specific_output_path = os.path.join(self.test_output_dir, output_filename_expected)

        success = process_csv_to_parquet(tricky_csv_path, self.test_output_dir)

        self.assertTrue(success, "Processing should succeed with cp950 fallback.")
        self.assertTrue(os.path.exists(specific_output_path),
                        f"Output Parquet file should exist for cp950 encoded file at {specific_output_path}")

        try:
            df_parquet = pd.read_parquet(specific_output_path)
            self.assertEqual(df_parquet['契約'].iloc[0], "測試契約", "Content mismatch after cp950 decoding.")
        except Exception as e:
            self.fail(f"Error reading Parquet from cp950 test: {e}")
        finally:
            if os.path.exists(tricky_csv_path):
                os.remove(tricky_csv_path)

if __name__ == '__main__':
    unittest.main()
