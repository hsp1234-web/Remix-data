import unittest
import unittest
import sqlite3
import tempfile
import os
from pathlib import Path
from datetime import datetime # Correctly placed at the top

# Adjust the import path based on your project structure
from taifex_pipeline.database.metadata_manager import MetadataManager
from taifex_pipeline.database.constants import METADATA_TABLE_DEFINITIONS, METADATA_DB_SCHEMA

class TestMetadataManager(unittest.TestCase):

    def setUp(self):
        """Set up a temporary database for each test."""
        # Create a temporary file for the SQLite database
        self.temp_db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.db_path = self.temp_db_file.name
        self.temp_db_file.close() # Close the file so MetadataManager can open it

        self.manager = MetadataManager(self.db_path)
        # print(f"setUp: DB created at {self.db_path}")

    def tearDown(self):
        """Clean up the temporary database file."""
        # print(f"tearDown: Removing DB at {self.db_path}")
        if hasattr(self, '_conn') and self.manager._conn:
             self.manager._conn.close() # Ensure connection is closed if manager holds it

        # Attempt to remove the file, with retries or delay if needed on Windows
        # For simplicity here, direct removal.
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        # else:
            # print(f"tearDown: DB file {self.db_path} already removed or not found.")


    def assert_table_exists(self, table_name):
        """Helper to assert that a table exists in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            self.assertIsNotNone(cursor.fetchone(), f"Table '{table_name}' should exist.")

    def test_create_tables_successfully_creates_tables(self):
        """Test that create_tables creates all defined tables."""
        self.manager.create_tables()
        self.assert_table_exists("files")
        self.assert_table_exists("data_map")
        # Potentially add more checks for other tables if METADATA_TABLE_DEFINITIONS expands

    def test_create_tables_is_idempotent(self):
        """Test that calling create_tables multiple times is harmless."""
        self.manager.create_tables() # First call
        self.manager.create_tables() # Second call
        self.assert_table_exists("files")
        self.assert_table_exists("data_map")

    def test_get_connection_returns_connection(self):
        """Test the internal _get_connection method (optional, as it's internal)."""
        conn = None
        try:
            conn = self.manager._get_connection()
            self.assertIsNotNone(conn)
            self.assertIsInstance(conn, sqlite3.Connection)
            # Check if row_factory is set (though this is an internal detail)
            self.assertTrue(conn.row_factory == sqlite3.Row)
        finally:
            if conn:
                conn.close()

    def test_register_batch_insert_single_new_file(self):
        """Test registering a single new file."""
        self.manager.create_tables()

        file_info_1 = {
            'file_name': 'test_file_1.parquet',
            'gdrive_path': '/path/to/test_file_1.parquet',
            'last_modified': datetime(2023, 1, 1, 12, 0, 0),
            'file_size_bytes': 1024
        }
        metadata_list_1 = [{'symbol': 'TXO', 'data_date': '2023-01-01'}]
        records = [{'file_info': file_info_1, 'metadata_list': metadata_list_1}]

        self.manager.register_batch(records)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_name, gdrive_path, DATETIME(last_modified), file_size_bytes FROM files WHERE file_name = ?", (file_info_1['file_name'],))
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], file_info_1['file_name'])
            self.assertEqual(row[1], file_info_1['gdrive_path'])
            # SQLite stores datetime as text in ISO format by default from Python datetime
            self.assertEqual(datetime.fromisoformat(row[2]), file_info_1['last_modified'])
            self.assertEqual(row[3], file_info_1['file_size_bytes'])

    def test_register_batch_update_existing_file(self):
        """Test that re-registering a file with the same name updates its info."""
        self.manager.create_tables()

        file_info_initial = {
            'file_name': 'test_file_A.parquet',
            'gdrive_path': '/path/to/initial/test_file_A.parquet',
            'last_modified': datetime(2023, 1, 1, 10, 0, 0),
            'file_size_bytes': 2000
        }
        records_initial = [{'file_info': file_info_initial, 'metadata_list': [{'symbol': 'S1', 'data_date': '2023-01-01'}]}]
        self.manager.register_batch(records_initial)

        # Get initial file_id
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_id FROM files WHERE file_name = ?", (file_info_initial['file_name'],))
            initial_file_id = cursor.fetchone()[0]

        file_info_updated = {
            'file_name': 'test_file_A.parquet', # Same name
            'gdrive_path': '/path/to/updated/test_file_A.parquet', # New path
            'last_modified': datetime(2023, 1, 2, 11, 0, 0), # New timestamp
            'file_size_bytes': 2500 # New size
        }
        records_updated = [{'file_info': file_info_updated, 'metadata_list': [{'symbol': 'S1', 'data_date': '2023-01-02'}]}] # Metadata can be different
        self.manager.register_batch(records_updated)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_id, gdrive_path, DATETIME(last_modified), file_size_bytes FROM files WHERE file_name = ?", (file_info_updated['file_name'],))
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], initial_file_id, "File ID should remain the same on update.")
            self.assertEqual(row[1], file_info_updated['gdrive_path'])
            self.assertEqual(datetime.fromisoformat(row[2]), file_info_updated['last_modified'])
            self.assertEqual(row[3], file_info_updated['file_size_bytes'])

            cursor.execute("SELECT COUNT(*) FROM files WHERE file_name = ?", (file_info_initial['file_name'],))
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, "There should still be only one record for this file name.")

    def test_register_batch_data_map_first_wins_on_conflict(self):
        """Test data_map 'first wins' (DO NOTHING) on (symbol, data_date) conflict."""
        self.manager.create_tables()

        # File 1: Establishes the initial mapping for (S1, D1)
        file_info_1 = {
            'file_name': 'file1.parquet', 'gdrive_path': '/path/file1.parquet',
            'last_modified': datetime(2023, 1, 1, 10, 0, 0), 'file_size_bytes': 100
        }
        metadata_list_1 = [{'symbol': 'S1', 'data_date': '2023-01-01'}, {'symbol': 'S2', 'data_date': '2023-01-01'}]
        records_1 = [{'file_info': file_info_1, 'metadata_list': metadata_list_1}]
        self.manager.register_batch(records_1)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_id FROM files WHERE file_name = ?", (file_info_1['file_name'],))
            file_1_id = cursor.fetchone()[0]

        # File 2: Different file, same (S1, D1) mapping, also (S3, D1)
        file_info_2 = {
            'file_name': 'file2.parquet', 'gdrive_path': '/path/file2.parquet',
            'last_modified': datetime(2023, 1, 2, 10, 0, 0), 'file_size_bytes': 200
        }
        # (S1, 2023-01-01) is a conflict. (S3, 2023-01-01) is new for data_map.
        metadata_list_2 = [{'symbol': 'S1', 'data_date': '2023-01-01'}, {'symbol': 'S3', 'data_date': '2023-01-01'}]
        records_2 = [{'file_info': file_info_2, 'metadata_list': metadata_list_2}]
        self.manager.register_batch(records_2)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_id FROM files WHERE file_name = ?", (file_info_2['file_name'],))
            file_2_id = cursor.fetchone()[0]
            self.assertIsNotNone(file_2_id)
            self.assertNotEqual(file_1_id, file_2_id) # Ensure file2 was added to files table

            # Check (S1, 2023-01-01) - should still map to file_1_id
            cursor.execute("SELECT file_id FROM data_map WHERE symbol = ? AND data_date = ?", ('S1', '2023-01-01'))
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], file_1_id, "Mapping for (S1, 2023-01-01) should not change (first wins).")

            # Check (S2, 2023-01-01) - should map to file_1_id
            cursor.execute("SELECT file_id FROM data_map WHERE symbol = ? AND data_date = ?", ('S2', '2023-01-01'))
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], file_1_id)

            # Check (S3, 2023-01-01) - should map to file_2_id (new mapping from file2)
            cursor.execute("SELECT file_id FROM data_map WHERE symbol = ? AND data_date = ?", ('S3', '2023-01-01'))
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], file_2_id)

    def test_register_batch_transaction_rollback_on_error(self):
        """Test that the batch is rolled back if an error occurs."""
        self.manager.create_tables()

        # Valid record
        file_info_valid = {
            'file_name': 'valid.parquet', 'gdrive_path': '/path/valid.parquet',
            'last_modified': datetime(2023, 1, 1, 10, 0, 0), 'file_size_bytes': 100
        }
        metadata_list_valid = [{'symbol': 'VLD', 'data_date': '2023-01-01'}]

        # Invalid record that will cause an error (e.g., symbol is None, violating NOT NULL if schema has it)
        # For this test, we'll make metadata_list itself invalid to break processing loop
        # or mock internal call to raise an error.
        # Here, let's assume metadata_list being None for a file_info could be an issue
        # or more directly, mock a part of the execution.

        # To reliably test rollback, we mock cursor.execute to raise an error during the batch
        # This is a bit more involved and might require patching sqlite3.Cursor.execute
        # For a simpler approach, we can try to insert data that violates a constraint
        # if we know one that is not handled by ON CONFLICT.
        # However, the current ON CONFLICT clauses are quite comprehensive.

        # Let's try a different approach: insert a record that would normally work,
        # then simulate an error by directly manipulating or checking state.
        # The current structure of register_batch makes it hard to inject an error
        # mid-batch without mocking.

        # Given the difficulty of reliably injecting a specific mid-batch error
        # without heavy mocking of sqlite3 internals or changing MetadataManager code
        # to be more testable for this specific scenario, we'll focus on what can be tested.
        # The existing try/except/rollback in register_batch is standard.
        # We can test that if one record in a batch is structurally bad (e.g. missing keys
        # that the code *expects* before DB interaction, though current code is robust to this),
        # and causes an exception *before* DB commit, then other records are not committed.

        # Simpler test: If the whole batch fails due to a connection error (hard to simulate here)
        # or an early programmatic error.

        # Let's assume a programmatic error occurs *within* the loop in `register_batch`
        # after some records might have been processed by cursor.execute but before commit.
        # If an unhandled Python exception occurs, __exit__ or the method's own finally
        # block should rollback.

        # For this test, we will assume the rollback mechanism in MetadataManager's
        # `with self._get_connection() as conn:` and the `try/except` within `register_batch`
        # function correctly for sqlite3.Error.
        # A more direct test would require deeper mocking.

        # Test with a batch where one item is malformed in a way that python code might fail.
        records_mixed_validity = [
            {'file_info': file_info_valid, 'metadata_list': metadata_list_valid},
            {'file_info': "this_is_not_a_dict"} # This will cause TypeError
        ]

        with self.assertRaises(TypeError): # Expecting an error due to bad record structure
            self.manager.register_batch(records_mixed_validity)

        # Check that the valid record was not committed
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM files WHERE file_name = ?", (file_info_valid['file_name'],))
            count = cursor.fetchone()[0]
            self.assertEqual(count, 0, "No records should be committed if batch fails.")

            cursor.execute("SELECT COUNT(*) FROM data_map WHERE symbol = ?", (metadata_list_valid[0]['symbol'],))
            count = cursor.fetchone()[0]
            self.assertEqual(count, 0, "No data_map records should be committed if batch fails.")


from datetime import datetime # Moved to top

if __name__ == '__main__':
    unittest.main()
