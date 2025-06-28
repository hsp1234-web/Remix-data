import duckdb
import datetime

class ManifestManager:
    def __init__(self, db_path: str):
        """
        Initializes the ManifestManager with a connection to a DuckDB database.

        Args:
            db_path: Path to the DuckDB database file.
        """
        self.db_path = db_path
        # Connect to the database. If the file doesn't exist, it will be created.
        self.con = duckdb.connect(database=self.db_path, read_only=False)
        self._initialize_schema()

    def _initialize_schema(self):
        """
        Initializes the 'file_manifest' table in the database if it doesn't exist.
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS file_manifest (
            file_hash VARCHAR PRIMARY KEY,
            source_path VARCHAR,
            registration_timestamp TIMESTAMP DEFAULT current_timestamp,
            status VARCHAR DEFAULT 'registered'
        );
        """
        self.con.execute(create_table_sql)
        self.con.commit() # Commit schema changes

    def hash_exists(self, file_hash: str) -> bool:
        """
        Checks if a given file_hash already exists in the 'file_manifest' table.

        Args:
            file_hash: The SHA256 hash of the file.

        Returns:
            True if the hash exists, False otherwise.
        """
        query = "SELECT COUNT(*) FROM file_manifest WHERE file_hash = ?;"
        result = self.con.execute(query, [file_hash]).fetchone()
        return result[0] > 0 if result else False

    def register_file(self, file_hash: str, source_path: str):
        """
        Registers a new file in the 'file_manifest' table.

        Args:
            file_hash: The SHA256 hash of the file.
            source_path: The original path of the file.

        Raises:
            duckdb.ConstraintException: If the file_hash (primary key) already exists.
        """
        current_timestamp = datetime.datetime.now() # Explicit timestamp for clarity if needed elsewhere
        insert_sql = """
        INSERT INTO file_manifest (file_hash, source_path, registration_timestamp, status)
        VALUES (?, ?, ?, ?);
        """
        # Note: 'status' and 'registration_timestamp' will use DEFAULT if not provided or if NULL is inserted
        # Depending on exact DB behavior, explicitly providing 'registered' and current_timestamp is safer.
        try:
            self.con.execute(insert_sql, [file_hash, source_path, current_timestamp, 'registered'])
            self.con.commit() # Commit the transaction
        except duckdb.Error as e: # Catch DuckDB specific errors
            raise e # Re-raise the exception to be handled by the caller

    def update_status(self, file_hash: str, new_status: str):
        """
        Updates the status of an existing file in the manifest.

        Args:
            file_hash: The SHA256 hash of the file to update.
            new_status: The new status (e.g., 'processed', 'error').
        """
        try:
            self.con.execute(
                "UPDATE file_manifest SET status = ? WHERE file_hash = ?",
                (new_status, file_hash)
            )
            self.con.commit()
            # print(f"Status updated to '{new_status}' for file_hash: {file_hash}") # Optional log
        except Exception as e:
            print(f"Error updating status for {file_hash} to {new_status}: {e}")
            # Consider re-raising or specific error handling

    def get_file_status(self, file_hash: str) -> str | None:
        """
        Retrieves the status of a file by its hash.

        Args:
            file_hash: The SHA256 hash of the file.

        Returns:
            The status of the file, or None if the file is not found.
        """
        query = "SELECT status FROM file_manifest WHERE file_hash = ?;"
        result = self.con.execute(query, [file_hash]).fetchone()
        return result[0] if result else None

    def close(self):
        """
        Closes the database connection.
        """
        if self.con:
            self.con.close()

if __name__ == '__main__':
    # Example Usage (for direct testing of this script)
    # Using an in-memory database for this example
    manager = ManifestManager(db_path=':memory:')

    # Test case 1: Register a new file
    file_hash_1 = "hash123"
    source_path_1 = "/path/to/file1.txt"
    print(f"Registering file: {file_hash_1}")
    manager.register_file(file_hash_1, source_path_1)
    print(f"Hash exists ({file_hash_1}): {manager.hash_exists(file_hash_1)}")
    print(f"Status of {file_hash_1}: {manager.get_file_status(file_hash_1)}")

    # Test case 2: Update status
    print(f"Updating status of {file_hash_1} to 'processed'")
    manager.update_status(file_hash_1, 'processed') # Changed from update_file_status
    print(f"Status of {file_hash_1}: {manager.get_file_status(file_hash_1)}")

    # Test case 3: Try to register the same file (should raise ConstraintException)
    try:
        print(f"Attempting to re-register file: {file_hash_1}")
        manager.register_file(file_hash_1, source_path_1)
    except duckdb.ConstraintException as e:
        print(f"Caught expected error for re-registering: {e}")

    # Test case 4: Check non-existent hash
    non_existent_hash = "hash_unknown"
    print(f"Hash exists ({non_existent_hash}): {manager.hash_exists(non_existent_hash)}")
    print(f"Status of {non_existent_hash}: {manager.get_file_status(non_existent_hash)}")

    # Test case 5: Update status for non-existent hash
    print(f"Attempting to update status for {non_existent_hash}:")
    manager.update_status(non_existent_hash, 'processed') # Changed from update_file_status, no return value to print

    manager.close()
    print("ManifestManager example usage complete.")
