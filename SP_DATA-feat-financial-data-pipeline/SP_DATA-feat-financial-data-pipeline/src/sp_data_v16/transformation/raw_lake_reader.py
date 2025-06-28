import duckdb
import pathlib

class RawLakeReader:
    """
    A class to read raw file content from a DuckDB-based Raw Lake.
    """
    def __init__(self, db_path: str):
        """
        Initializes the RawLakeReader and connects to the database.

        Args:
            db_path (str): The path to the Raw Lake DuckDB database file.

        Raises:
            ConnectionError: If the connection to the database fails.
        """
        self.db_path = pathlib.Path(db_path)
        try:
            self.con = duckdb.connect(database=str(self.db_path), read_only=True)
            print(f"Successfully connected to Raw Lake database: {self.db_path}")
        except duckdb.Error as e:
            print(f"Failed to connect to Raw Lake database at {self.db_path}: {e}")
            raise ConnectionError(f"Could not connect to Raw Lake database: {e}") from e

    def get_raw_content(self, file_hash: str) -> bytes | None:
        """
        Retrieves the raw content of a file based on its hash.

        Args:
            file_hash (str): The hash of the file to retrieve.

        Returns:
            bytes | None: The raw content as bytes if found, otherwise None.
        """
        if not hasattr(self, 'con') or not self.con: # Check if con exists and is not None
            print("Error: Database connection is not open or available.")
            return None

        try:
            result = self.con.execute(
                "SELECT raw_content FROM raw_files WHERE file_hash = ?", (file_hash,)
            ).fetchone()

            if result:
                return result[0]
            else:
                print(f"No content found for file_hash: {file_hash}")
                return None
        except duckdb.Error as e:
            print(f"Database error while fetching content for file_hash {file_hash}: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while fetching content for file_hash {file_hash}: {e}")
            return None


    def close(self):
        """
        Closes the connection to the database.
        """
        if hasattr(self, 'con') and self.con: # Check if con exists and is not None
            try:
                self.con.close()
                # Setting self.con to None after closing is a common pattern to indicate it's closed
                # self.con = None # Optional: uncomment if you want to explicitly mark as closed
                print(f"Successfully disconnected from Raw Lake database: {self.db_path}")
            except duckdb.Error as e:
                print(f"Error while closing connection to Raw Lake database {self.db_path}: {e}")
        else:
            print("Raw Lake database connection already closed or not initialized.")


if __name__ == '__main__':
    dummy_db_dir = pathlib.Path(__file__).parent / "temp_dbs"
    dummy_db_dir.mkdir(parents=True, exist_ok=True)
    dummy_db_path = dummy_db_dir / "temp_raw_lake.db"

    # Setup: Create dummy DB and table
    conn = None
    try:
        conn = duckdb.connect(str(dummy_db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_files (
                file_hash VARCHAR PRIMARY KEY,
                raw_content BLOB
            );
        """)
        conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)", ('test_hash123', b'Hello World from Raw Lake!'))
        conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)", ('test_hash456', b'Another file content.'))
        conn.close()
        print(f"Dummy Raw Lake DB created at {dummy_db_path} with test data.")

        # Test RawLakeReader
        reader = None
        try:
            print("\n--- Testing RawLakeReader ---")
            reader = RawLakeReader(db_path=str(dummy_db_path))

            print("\nFetching existing content (test_hash123):")
            content1 = reader.get_raw_content('test_hash123')
            if content1:
                print(f"  Retrieved: {content1.decode()} (type: {type(content1)})")
            else:
                print("  Content not found (unexpected).")

            print("\nFetching another existing content (test_hash456):")
            content2 = reader.get_raw_content('test_hash456')
            if content2:
                print(f"  Retrieved: {content2.decode()} (type: {type(content2)})")
            else:
                print("  Content not found (unexpected).")

            print("\nFetching non-existent content (non_existent_hash):")
            content_none = reader.get_raw_content('non_existent_hash')
            if content_none is None:
                print("  Content not found (as expected).")
            else:
                print(f"  Retrieved: {content_none} (unexpected).")

            print("\nAttempting to fetch with a closed connection (for testing robustness):")
            reader.close() # Close connection explicitly
            content_after_close = reader.get_raw_content('test_hash123')
            if content_after_close is None:
                print("  Could not fetch content after closing (as expected).")
            else:
                print(f"  Retrieved: {content_after_close} (unexpected).")


        except ConnectionError as ce:
            print(f"ConnectionError during RawLakeReader test: {ce}")
        except Exception as e:
            print(f"An error occurred during RawLakeReader testing: {e}")
        finally:
            if reader: # reader might not be initialized if RawLakeReader(..) fails
                # The connection is already closed in the test above,
                # calling close() again should be handled gracefully.
                print("\nCalling reader.close() again (should be handled gracefully):")
                reader.close()
            print("--- RawLakeReader Test Finished ---")

    except duckdb.Error as db_e:
        print(f"A DuckDB error occurred during __main__ setup: {db_e}")
    except Exception as e:
        print(f"An unexpected error occurred in __main__: {e}")
    finally:
        # Cleanup: Remove the dummy DB file
        if dummy_db_path.exists():
            try:
                dummy_db_path.unlink()
                print(f"\nDummy Raw Lake DB {dummy_db_path} removed.")
            except OSError as e:
                print(f"Error removing dummy DB {dummy_db_path}: {e}")
        if dummy_db_dir.exists() and not any(dummy_db_dir.iterdir()): # Remove dir if empty
            try:
                dummy_db_dir.rmdir()
                print(f"Dummy DB directory {dummy_db_dir} removed.")
            except OSError as e:
                print(f"Error removing dummy DB directory {dummy_db_dir}: {e}")
