import pathlib
import duckdb

class RawLakeLoader:
    def __init__(self, db_path: str):
        self.db_path = pathlib.Path(db_path)
        try:
            self.con = duckdb.connect(database=str(self.db_path))
            self._initialize_schema()
        except Exception as e:
            print(f"Error initializing database: {e}")
            raise

    def _initialize_schema(self):
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_files (
                file_hash VARCHAR PRIMARY KEY,
                raw_content BLOB
            )
            """
        )

    def save_file(self, file_path: pathlib.Path, file_hash: str):
        raw_data = file_path.read_bytes()
        self.con.execute(
            "INSERT INTO raw_files VALUES (?, ?)", (file_hash, raw_data)
        )
        self.con.commit()

    def close(self):
        if hasattr(self, "con") and self.con:
            self.con.close()
