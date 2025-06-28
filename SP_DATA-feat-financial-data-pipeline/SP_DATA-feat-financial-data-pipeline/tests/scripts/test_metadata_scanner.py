import unittest
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import os
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd

# Adjust the import path based on your project structure
from taifex_pipeline.scripts.metadata_scanner import MetadataScanner
from taifex_pipeline.database.metadata_manager import MetadataManager # For mocking

# Helper to create dummy parquet files
def create_dummy_parquet_file(filepath: Path, data: list[dict] = None):
    if data is None:
        data = [{'symbol': 'TXO', 'data_date': '2023-01-01', 'value': 1}]

    df = pd.DataFrame(data)
    if 'data_date' in df.columns: # Ensure data_date is datetime if present
        df['data_date'] = pd.to_datetime(df['data_date'])
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath)

class TestMetadataScanner(unittest.TestCase):

    def setUp(self):
        """Set up temporary directories and files for testing."""
        self.test_dir = Path(tempfile.mkdtemp(prefix="scanner_test_"))
        self.parquet_dir = self.test_dir / "parquet_data"
        self.parquet_dir.mkdir()

        # Create a temporary db file for MetadataManager that MetadataScanner will use
        self.temp_db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db", dir=self.test_dir)
        self.db_path = Path(self.temp_db_file.name)
        self.temp_db_file.close() # Close the file so it can be opened by other processes/MetadataManager

        # print(f"setUp: Test dir {self.test_dir}, Parquet dir {self.parquet_dir}, DB {self.db_path}")


    def tearDown(self):
        """Clean up temporary directories and files."""
        # print(f"tearDown: Removing test dir {self.test_dir}")
        # Ensure DB connection is closed if held by a mock or real manager instance
        # This might be tricky if the manager instance is within the scanner
        # For now, rely on scanner not holding it open, or manager closing it.
        shutil.rmtree(self.test_dir)
        # print(f"tearDown: Test dir {self.test_dir} removed.")


    @patch('taifex_pipeline.scripts.metadata_scanner.MetadataManager')
    def test_init_paths_and_manager_creation(self, MockMetadataManager):
        """Test MetadataScanner initialization."""
        mock_manager_instance = MockMetadataManager.return_value

        scanner = MetadataScanner(str(self.parquet_dir), str(self.db_path), batch_size=50)

        self.assertEqual(scanner.parquet_dir, self.parquet_dir.resolve())
        self.assertEqual(scanner.db_path, self.db_path.resolve())
        self.assertEqual(scanner.batch_size, 50)

        MockMetadataManager.assert_called_once_with(str(self.db_path.resolve()))
        mock_manager_instance.create_tables.assert_called_once()

    def test_scan_parquet_files_finds_correct_files(self):
        """Test _scan_parquet_files method for finding .parquet files."""
        # Create some dummy files
        file1 = self.parquet_dir / "file1.parquet"
        file2 = self.parquet_dir / "sub_dir" / "file2.parquet"
        not_parquet = self.parquet_dir / "file3.txt"
        file4 = self.parquet_dir / "another_sub" / "deep_dir" / "file4.parquet"

        create_dummy_parquet_file(file1)
        create_dummy_parquet_file(file2)
        not_parquet.parent.mkdir(parents=True, exist_ok=True)
        not_parquet.touch()
        create_dummy_parquet_file(file4)

        # Scanner instance (MetadataManager is not used in this specific method directly)
        # We can pass a dummy db_path as it won't be used for _scan_parquet_files
        scanner = MetadataScanner(str(self.parquet_dir), "dummy.db")

        found_files = scanner._scan_parquet_files()

        expected_files = sorted([file1.resolve(), file2.resolve(), file4.resolve()])
        self.assertEqual(len(found_files), 3)
        self.assertListEqual(sorted(found_files), expected_files)

    def test_scan_parquet_files_empty_directory(self):
        """Test _scan_parquet_files with an empty directory."""
        scanner = MetadataScanner(str(self.parquet_dir), "dummy.db")
        found_files = scanner._scan_parquet_files()
        self.assertEqual(len(found_files), 0)

    def test_scan_parquet_files_no_parquet_files(self):
        """Test _scan_parquet_files with a directory containing no .parquet files."""
        (self.parquet_dir / "file1.txt").touch()
        (self.parquet_dir / "sub_dir" / "file2.csv").parent.mkdir(parents=True, exist_ok=True)
        (self.parquet_dir / "sub_dir" / "file2.csv").touch()

        scanner = MetadataScanner(str(self.parquet_dir), "dummy.db")
        found_files = scanner._scan_parquet_files()
        self.assertEqual(len(found_files), 0)

    def test_extract_metadata_from_file_valid_file(self):
        """Test extracting metadata from a valid Parquet file."""
        file_path = self.parquet_dir / "valid_data.parquet"
        data = [
            {'symbol': 'TXO', 'data_date': '2023-01-01', 'value': 100},
            {'symbol': 'MXF', 'data_date': '2023-01-01', 'value': 200}, # Same date, different symbol
            {'symbol': 'TXO', 'data_date': '2023-01-01', 'value': 101}, # Duplicate symbol/date
        ]
        create_dummy_parquet_file(file_path, data)

        scanner = MetadataScanner(str(self.parquet_dir), "dummy.db")
        metadata = scanner._extract_metadata_from_file(file_path)

        expected_metadata = sorted([
            {'symbol': 'TXO', 'data_date': '2023-01-01'},
            {'symbol': 'MXF', 'data_date': '2023-01-01'}
        ], key=lambda x: (x['symbol'], x['data_date']))

        self.assertEqual(len(metadata), 2)
        self.assertListEqual(sorted(metadata, key=lambda x: (x['symbol'], x['data_date'])), expected_metadata)

    def test_extract_metadata_from_file_empty_file(self):
        """Test extracting metadata from an empty Parquet file."""
        file_path = self.parquet_dir / "empty.parquet"
        create_dummy_parquet_file(file_path, data=[]) # Create an empty parquet

        scanner = MetadataScanner(str(self.parquet_dir), "dummy.db")

        with patch.object(scanner.logger, 'warning') as mock_log_warning:
            metadata = scanner._extract_metadata_from_file(file_path)
            self.assertEqual(metadata, [])
            mock_log_warning.assert_any_call(f"Skipping empty parquet file: {file_path.resolve()}")

    def test_extract_metadata_from_file_missing_columns(self):
        """Test Parquet file missing 'symbol' or 'data_date' columns."""
        file_path = self.parquet_dir / "missing_cols.parquet"
        # File is created with only 'value' column
        create_dummy_parquet_file(file_path, data=[{'value': 1}])

        scanner = MetadataScanner(str(self.parquet_dir), "dummy.db")

        # pd.read_parquet with specific columns will raise KeyError if a column is missing
        with patch.object(scanner.logger, 'error') as mock_log_error:
            metadata = scanner._extract_metadata_from_file(file_path)
            self.assertEqual(metadata, [])
            # Check that an error was logged (the exact message depends on pandas version and error)
            self.assertTrue(mock_log_error.called)
            # Example: mock_log_error.assert_any_call(f"Failed to process file {file_path.resolve()}: ...", exc_info=True)
            # The actual error message for missing columns when `columns` kwarg is used is a KeyError.
            # "None of [Index(['symbol', 'data_date'], dtype='object')] are in the [columns]"
            args, kwargs = mock_log_error.call_args
            self.assertIn(f"Failed to process file {file_path.resolve()}", args[0])
            # self.assertIsInstance(kwargs.get('exc_info').__cause__, KeyError) # This check is too specific maybe

    def test_extract_metadata_from_file_file_not_found(self):
        """Test extracting metadata when file does not exist."""
        file_path = self.parquet_dir / "non_existent.parquet"
        scanner = MetadataScanner(str(self.parquet_dir), "dummy.db")

        with patch.object(scanner.logger, 'error') as mock_log_error:
            metadata = scanner._extract_metadata_from_file(file_path)
            self.assertEqual(metadata, [])
            mock_log_error.assert_any_call(f"File not found during metadata extraction: {file_path.resolve()}")

    def test_run_scan_end_to_end_logic(self):
        """Test the main run_scan method for end-to-end logic."""
        # Setup: Create some parquet files
        file1_path = self.parquet_dir / "file1.parquet"
        data1 = [{'symbol': 'TXO', 'data_date': '2023-01-01', 'value': 1}]
        create_dummy_parquet_file(file1_path, data1)
        os.utime(file1_path, (datetime(2023, 1, 1, 10, 0, 0).timestamp(), datetime(2023, 1, 1, 10, 0, 0).timestamp()))


        file2_path = self.parquet_dir / "subdir" / "file2.parquet"
        data2 = [
            {'symbol': 'MXF', 'data_date': '2023-01-01', 'value': 2},
            {'symbol': 'TXO', 'data_date': '2023-01-02', 'value': 3}
        ]
        create_dummy_parquet_file(file2_path, data2)
        os.utime(file2_path, (datetime(2023, 1, 2, 10, 0, 0).timestamp(), datetime(2023, 1, 2, 10, 0, 0).timestamp()))

        # File3 will have a conflicting (symbol, data_date) with file1 for TXO@2023-01-01
        file3_path = self.parquet_dir / "file3.parquet"
        data3 = [{'symbol': 'TXO', 'data_date': '2023-01-01', 'value': 4}] # Conflict with file1
        create_dummy_parquet_file(file3_path, data3)
        os.utime(file3_path, (datetime(2023, 1, 3, 10, 0, 0).timestamp(), datetime(2023, 1, 3, 10, 0, 0).timestamp()))

        # Initialize real MetadataManager and MetadataScanner
        # db_path is already set up in self.setUp()
        scanner = MetadataScanner(str(self.parquet_dir), str(self.db_path), batch_size=2) # Use smaller batch
        scanner.run_scan()

        # Verification: Check database content directly
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Check files table (should have 3 files)
            cursor.execute("SELECT file_id, file_name, gdrive_path, DATETIME(last_modified) as lm, file_size_bytes FROM files ORDER BY file_name")
            files_rows = cursor.fetchall()
            self.assertEqual(len(files_rows), 3)

            file1_db = next(r for r in files_rows if r['file_name'] == 'file1.parquet')
            file2_db = next(r for r in files_rows if r['file_name'] == 'file2.parquet')
            file3_db = next(r for r in files_rows if r['file_name'] == 'file3.parquet')

            self.assertEqual(Path(file1_db['gdrive_path']).resolve(), file1_path.resolve())
            self.assertEqual(datetime.fromisoformat(file1_db['lm']), datetime(2023,1,1,10,0,0))

            # Check data_map table
            cursor.execute("SELECT symbol, data_date, file_id FROM data_map ORDER BY symbol, data_date")
            map_rows = cursor.fetchall()

            # Expected mappings:
            # (TXO, 2023-01-01) -> file1_db['file_id'] (file1 wins due to "first wins" for data_map)
            # (MXF, 2023-01-01) -> file2_db['file_id']
            # (TXO, 2023-01-02) -> file2_db['file_id']

            self.assertEqual(len(map_rows), 3)

            map_txo_0101 = next(r for r in map_rows if r['symbol'] == 'TXO' and r['data_date'] == '2023-01-01')
            map_mxf_0101 = next(r for r in map_rows if r['symbol'] == 'MXF' and r['data_date'] == '2023-01-01')
            map_txo_0102 = next(r for r in map_rows if r['symbol'] == 'TXO' and r['data_date'] == '2023-01-02')

            self.assertEqual(map_txo_0101['file_id'], file1_db['file_id'])
            self.assertEqual(map_mxf_0101['file_id'], file2_db['file_id'])
            self.assertEqual(map_txo_0102['file_id'], file2_db['file_id'])

    def test_run_scan_with_file_update(self):
        """Test run_scan where a file is updated and rescanned."""
        file_path = self.parquet_dir / "update_me.parquet"
        initial_data = [{'symbol': 'UPD', 'data_date': '2023-02-01'}]
        create_dummy_parquet_file(file_path, initial_data)
        initial_mtime = datetime(2023, 2, 1, 10, 0, 0).timestamp()
        os.utime(file_path, (initial_mtime, initial_mtime))

        scanner = MetadataScanner(str(self.parquet_dir), str(self.db_path))
        scanner.run_scan() # First scan

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_id, DATETIME(last_modified) as lm, file_size_bytes FROM files WHERE file_name = ?", (file_path.name,))
            initial_file_row = cursor.fetchone()
            initial_file_id = initial_file_row[0]
            initial_db_mtime = datetime.fromisoformat(initial_file_row[1])
            initial_db_size = initial_file_row[2]

        self.assertEqual(initial_db_mtime, datetime(2023,2,1,10,0,0))

        # Update the file
        updated_data = [{'symbol': 'UPD', 'data_date': '2023-02-01'}, {'symbol': 'NEW', 'data_date': '2023-02-02'}]
        create_dummy_parquet_file(file_path, updated_data) # This will change size
        updated_mtime = datetime(2023, 2, 2, 11, 0, 0).timestamp()
        os.utime(file_path, (updated_mtime, updated_mtime)) # Change mtime

        scanner.run_scan() # Rescan

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT file_id, DATETIME(last_modified) as lm, file_size_bytes FROM files WHERE file_name = ?", (file_path.name,))
            updated_file_row = cursor.fetchone()
            self.assertIsNotNone(updated_file_row)
            self.assertEqual(updated_file_row['file_id'], initial_file_id, "File ID should be the same.")
            self.assertEqual(datetime.fromisoformat(updated_file_row['lm']), datetime(2023,2,2,11,0,0))
            self.assertNotEqual(updated_file_row['file_size_bytes'], initial_db_size, "File size should have changed.")

            # Check data_map: (UPD, 2023-02-01) should still point to initial_file_id (first wins)
            # (NEW, 2023-02-02) should be added and point to initial_file_id (which is the updated file's ID)
            cursor.execute("SELECT symbol, data_date, file_id FROM data_map ORDER BY symbol, data_date")
            map_rows = cursor.fetchall()

            map_upd_0201 = next(r for r in map_rows if r['symbol'] == 'UPD' and r['data_date'] == '2023-02-01')
            map_new_0202 = next(r for r in map_rows if r['symbol'] == 'NEW' and r['data_date'] == '2023-02-02')

            self.assertEqual(map_upd_0201['file_id'], initial_file_id)
            self.assertEqual(map_new_0202['file_id'], initial_file_id)


if __name__ == '__main__':
    unittest.main()
