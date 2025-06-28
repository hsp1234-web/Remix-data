import hashlib
import pathlib
from typing import Iterator, Tuple
from tqdm import tqdm

class FileScanner:
    """
    A utility class for scanning directories and calculating file hashes.
    """

    @staticmethod
    def scan_directory(directory_path: str) -> Iterator[Tuple[str, pathlib.Path]]:
        """
        Scans a directory recursively for files, calculates their SHA256 hash,
        and yields the hash along with the file path.

        Args:
            directory_path: The path to the directory to scan.

        Yields:
            A tuple containing the SHA256 hash (hex string) and the pathlib.Path object for each file.

        Raises:
            FileNotFoundError: If the provided directory_path does not exist or is not a directory.
        """
        path_obj = pathlib.Path(directory_path)

        if not path_obj.exists():
            raise FileNotFoundError(f"Directory not found: {directory_path}")
        if not path_obj.is_dir():
            raise FileNotFoundError(f"Path is not a directory: {directory_path}")

        # First, collect all files to be processed to have an accurate total for tqdm
        all_files = [f for f in path_obj.rglob('*') if f.is_file()]

        # Use tqdm for progress bar
        for file_path in tqdm(all_files, desc=f"Scanning {directory_path}", unit="file"):
            try:
                hasher = hashlib.sha256()
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(4096)  # Read in 4KB chunks
                        if not chunk:
                            break
                        hasher.update(chunk)
                file_hash = hasher.hexdigest()
                yield file_hash, file_path
            except IOError as e:
                # Optionally, log this error or handle it more gracefully
                # For now, we'll print a warning and skip the file.
                print(f"Warning: Could not read or hash file {file_path}: {e}")
                continue
            except Exception as e:
                print(f"Warning: An unexpected error occurred while processing file {file_path}: {e}")
                continue

if __name__ == '__main__':
    # Example Usage (for direct testing of this script)
    # Create a dummy directory structure for testing
    current_script_path = pathlib.Path(__file__).parent
    test_scan_dir = current_script_path / "test_scan_area"

    # Clean up previous test run if any
    if test_scan_dir.exists():
        for item in test_scan_dir.rglob('*'):
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                # For rmdir, directory must be empty
                pass # Let's remove files first, then attempt to remove dirs if needed
        # A more robust cleanup would remove subdirs first, then parent
        # For simplicity, we'll just ensure files are gone for now.
        # If subdirs were created, they might persist.
        try:
            # Attempt to remove subdirectories (if they are empty)
            for item in sorted(list(test_scan_dir.rglob('*')), key=lambda p: len(p.parts), reverse=True):
                if item.is_dir():
                    try:
                        item.rmdir()
                    except OSError: # Directory not empty
                        pass
            test_scan_dir.rmdir() # remove main test dir if empty
        except OSError:
             pass


    test_scan_dir.mkdir(parents=True, exist_ok=True)
    (test_scan_dir / "file1.txt").write_text("This is file 1.")
    (test_scan_dir / "file2.csv").write_text("col1,col2\nval1,val2")

    sub_dir = test_scan_dir / "subdir"
    sub_dir.mkdir(exist_ok=True)
    (sub_dir / "file3.log").write_text("Log entry 1\nLog entry 2")
    (sub_dir / "empty_file.txt").write_text("")


    print(f"Scanning directory: {test_scan_dir}")
    try:
        for f_hash, f_path in FileScanner.scan_directory(str(test_scan_dir)):
            print(f"Hash: {f_hash}, Path: {f_path}")
    except FileNotFoundError as e:
        print(e)

    print("\nScanning non-existent directory:")
    try:
        for f_hash, f_path in FileScanner.scan_directory("non_existent_dir_123"):
            print(f"Hash: {f_hash}, Path: {f_path}")
    except FileNotFoundError as e:
        print(e)

    print("\nScanning a file instead of a directory:")
    try:
        # Create a dummy file to test this case
        dummy_file_for_test = current_script_path / "dummy.txt"
        dummy_file_for_test.write_text("I am a file.")
        for f_hash, f_path in FileScanner.scan_directory(str(dummy_file_for_test)):
            print(f"Hash: {f_hash}, Path: {f_path}")
    except FileNotFoundError as e:
        print(e)
    finally:
        if 'dummy_file_for_test' in locals() and dummy_file_for_test.exists():
            dummy_file_for_test.unlink()


    # Clean up after example run (optional)
    # print(f"\nCleaning up {test_scan_dir}...")
    # (sub_dir / "file3.log").unlink()
    # (sub_dir / "empty_file.txt").unlink()
    # sub_dir.rmdir()
    # (test_scan_dir / "file1.txt").unlink()
    # (test_scan_dir / "file2.csv").unlink()
    # test_scan_dir.rmdir()
    # print("Cleanup complete.")
    print("\nFileScanner example usage complete.")
