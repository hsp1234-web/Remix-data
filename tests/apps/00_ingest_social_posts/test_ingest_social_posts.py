import unittest
import os
import pandas as pd
import shutil
import sys

# Add project root to sys.path to allow importing project modules
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

# Corrected import path
from apps.x00_ingest_social_posts.run import process_social_csv_to_parquet

class TestIngestSocialPosts(unittest.TestCase):

    def setUp(self):
        self.fixture_dir = os.path.join(PROJECT_ROOT, "tests", "fixtures")
        # Create a unique test output directory for this test class to avoid conflicts
        self.base_test_output_dir = os.path.join(PROJECT_ROOT, "tests", "test_outputs")
        self.test_output_dir = os.path.join(self.base_test_output_dir, "bronze", "social_posts_00") # Unique
        os.makedirs(self.test_output_dir, exist_ok=True)

        self.sample_csv_path = os.path.join(self.fixture_dir, "sample_social_posts.csv")
        self.output_parquet_path = os.path.join(self.test_output_dir, "test_social_posts.parquet")

    def tearDown(self):
        # Clean up the specific test output directory created in setUp
        if os.path.exists(self.test_output_dir): # Check specific dir
            shutil.rmtree(self.test_output_dir)
        # Attempt to clean up base if it's empty (optional, be careful)
        if os.path.exists(self.base_test_output_dir) and not os.listdir(self.base_test_output_dir):
            shutil.rmtree(self.base_test_output_dir)


    def test_process_social_csv_to_parquet_success(self):
        """Test successful conversion of a sample social posts CSV to Parquet."""
        success = process_social_csv_to_parquet(self.sample_csv_path, self.output_parquet_path)

        self.assertTrue(success, "Processing should return True on success.")
        self.assertTrue(os.path.exists(self.output_parquet_path), f"Output Parquet file should exist at {self.output_parquet_path}")

        try:
            df_parquet = pd.read_parquet(self.output_parquet_path)
            df_csv = pd.read_csv(self.sample_csv_path)

            self.assertEqual(len(df_parquet), len(df_csv), "DataFrame lengths should match.")
            self.assertListEqual(sorted(list(df_parquet.columns)), sorted(list(df_csv.columns)), "Column names should match.")
            self.assertEqual(df_parquet['author'].iloc[0], df_csv['author'].iloc[0], "Content mismatch in 'author' column.")
        except Exception as e:
            self.fail(f"Error reading or comparing Parquet/CSV files: {e}")

    def test_process_social_csv_to_parquet_file_not_found(self):
        """Test handling of a non-existent input CSV file."""
        non_existent_csv = os.path.join(self.fixture_dir, "non_existent.csv")
        success = process_social_csv_to_parquet(non_existent_csv, self.output_parquet_path)

        self.assertFalse(success, "Processing should return False if input file not found.")
        self.assertFalse(os.path.exists(self.output_parquet_path), "Output Parquet file should not be created.")

    def test_process_social_csv_to_parquet_bad_csv(self):
        """Test handling of a malformed CSV file."""
        bad_csv_path = os.path.join(self.fixture_dir, "bad_sample_social.csv")
        # Ensure fixture directory exists for writing this temp bad file
        os.makedirs(self.fixture_dir, exist_ok=True)
        with open(bad_csv_path, "w", encoding="utf-8") as f:
            f.write("header1,header2\n")
            f.write("value1,value2,extravalue\n") # Malformed line for typical CSV parsing

        success = process_social_csv_to_parquet(bad_csv_path, self.output_parquet_path)

        # The current script's process_social_csv_to_parquet catches general exceptions
        # from pd.read_csv and returns False.
        self.assertFalse(success, "Processing should return False for a malformed CSV if read_csv fails.")
        # It's also good to check that no incomplete/bad parquet file was created.
        self.assertFalse(os.path.exists(self.output_parquet_path),
                         "Output Parquet file should not be created on CSV read failure.")

        if os.path.exists(bad_csv_path): # Clean up the bad CSV
            os.remove(bad_csv_path)

if __name__ == '__main__':
    # This allows running the test file directly for convenience, e.g. python tests/apps/00_ingest_social_posts/test_ingest_social_posts.py
    # It's generally better to run tests using `python -m unittest discover tests` from the project root.
    unittest.main()
