import pytest
import hashlib
import pathlib
from src.sp_data_v16.ingestion.scanner import FileScanner

@pytest.fixture
def test_files_structure(tmp_path: pathlib.Path):
    """Creates a temporary directory structure with some files for testing."""
    base_dir = tmp_path / "scan_test_area"
    base_dir.mkdir()

    # Create some files in the base directory
    (base_dir / "file1.txt").write_text("Content of file1")
    (base_dir / "file2.dat").write_bytes(b"Binary content for file2")

    # Create a subdirectory with a file
    sub_dir = base_dir / "subdir1"
    sub_dir.mkdir()
    (sub_dir / "file3.txt").write_text("Content of file3 in subdir")

    # Create an empty file
    (base_dir / "empty.txt").write_text("")

    # Create another subdirectory, potentially empty or with more files
    sub_dir2 = base_dir / "subdir2"
    sub_dir2.mkdir()
    (sub_dir2 / "another_file.log").write_text("Log data here")

    # Expected files and their content for manual hash calculation if needed
    # file1.txt: "Content of file1"
    # file2.dat: b"Binary content for file2"
    # subdir1/file3.txt: "Content of file3 in subdir"
    # empty.txt: ""
    # subdir2/another_file.log: "Log data here"

    return base_dir

def manual_sha256_hash(file_path: pathlib.Path) -> str:
    """Helper function to manually calculate SHA256 hash of a file."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(4096)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()

def test_scan_directory(test_files_structure: pathlib.Path):
    """Tests the FileScanner.scan_directory method."""

    results = list(FileScanner.scan_directory(str(test_files_structure)))

    # 1. Verify the number of files found
    # Expected: file1.txt, file2.dat, subdir1/file3.txt, empty.txt, subdir2/another_file.log
    assert len(results) == 5, "Incorrect number of files scanned."

    # Create a dictionary for easier lookup: path -> hash
    scanned_files_map = {path.name: file_hash for file_hash, path in results}

    # 2. Manually calculate hash for one file and assert
    file1_path = test_files_structure / "file1.txt"
    expected_file1_hash = manual_sha256_hash(file1_path)
    assert "file1.txt" in scanned_files_map, "file1.txt not found in scan results"
    assert scanned_files_map["file1.txt"] == expected_file1_hash, "Hash for file1.txt does not match."

    # 3. Verify path objects and their existence for all found files
    found_paths_set = set()
    for file_hash, file_path_obj in results:
        assert isinstance(file_path_obj, pathlib.Path), "Returned path is not a pathlib.Path object."
        assert file_path_obj.exists(), f"Returned path {file_path_obj} does not exist."
        assert file_path_obj.is_file(), f"Returned path {file_path_obj} is not a file."
        found_paths_set.add(file_path_obj.name)

    expected_filenames = {"file1.txt", "file2.dat", "file3.txt", "empty.txt", "another_file.log"}
    assert found_paths_set == expected_filenames, "The set of found filenames does not match expected."

    # Test scanning empty.txt
    empty_file_path = test_files_structure / "empty.txt"
    expected_empty_hash = manual_sha256_hash(empty_file_path)
    assert "empty.txt" in scanned_files_map, "empty.txt not found in scan results"
    assert scanned_files_map["empty.txt"] == expected_empty_hash, "Hash for empty.txt does not match."

    # Test binary file
    file2_path = test_files_structure / "file2.dat"
    expected_file2_hash = manual_sha256_hash(file2_path)
    assert "file2.dat" in scanned_files_map, "file2.dat not found in scan results"
    assert scanned_files_map["file2.dat"] == expected_file2_hash, "Hash for file2.dat does not match."

def test_scan_directory_non_existent(tmp_path: pathlib.Path):
    """Tests scanning a non-existent directory."""
    non_existent_dir = tmp_path / "this_dir_does_not_exist"
    with pytest.raises(FileNotFoundError) as excinfo:
        list(FileScanner.scan_directory(str(non_existent_dir)))
    assert f"Directory not found: {non_existent_dir}" in str(excinfo.value)

def test_scan_directory_path_is_file(tmp_path: pathlib.Path):
    """Tests scanning a path that is a file, not a directory."""
    file_path = tmp_path / "iam_a_file.txt"
    file_path.write_text("I am not a directory.")
    with pytest.raises(FileNotFoundError) as excinfo: # Assuming FileNotFoundError is raised, adjust if different error
        list(FileScanner.scan_directory(str(file_path)))
    assert f"Path is not a directory: {file_path}" in str(excinfo.value)

def test_scan_empty_directory(tmp_path: pathlib.Path):
    """Tests scanning an empty directory."""
    empty_dir = tmp_path / "empty_scan_dir"
    empty_dir.mkdir()
    results = list(FileScanner.scan_directory(str(empty_dir)))
    assert len(results) == 0, "Scan of empty directory should yield no results."

def test_scan_directory_io_error_on_one_file(test_files_structure: pathlib.Path, mocker, capsys):
    """
    測試當掃描目錄中某個檔案時發生 IOError，程式應能捕捉錯誤、繼續處理，
    並從結果中排除該錯誤檔案。
    """
    # 選擇一個檔案來模擬IOError，例如 file2.dat
    error_file_name = "file2.dat"
    error_file_path = test_files_structure / error_file_name

    # 使用 mocker.patch 來模擬 open 函數在遇到特定檔案時拋出 IOError
    # 我們需要儲存原始的 open
    original_open = builtins.open

    def mock_open_with_io_error(file, mode='r', *args, **kwargs):
        if pathlib.Path(file) == error_file_path:
            raise IOError(f"Mocked IOError for {file}")
        return original_open(file, mode, *args, **kwargs)

    mocker.patch('builtins.open', mock_open_with_io_error)
    # tqdm 會影響 capsys 的捕捉，暫時禁用或 mock掉 tqdm
    mocker.patch('src.sp_data_v16.ingestion.scanner.tqdm', lambda x, **kwargs: x)


    results = list(FileScanner.scan_directory(str(test_files_structure)))

    # 預期找到 5 個檔案，但 file2.dat 會因 IOError 而被跳過
    assert len(results) == 4, "掃描結果應包含4個檔案（排除出錯的檔案）。"

    found_paths_set = {path.name for _, path in results}
    assert error_file_name not in found_paths_set, f"錯誤檔案 {error_file_name} 不應出現在結果中。"

    expected_remaining_filenames = {"file1.txt", "file3.txt", "empty.txt", "another_file.log"}
    assert found_paths_set == expected_remaining_filenames, "掃描到的檔案集合不符合預期（已排除錯誤檔案）。"

    # 驗證錯誤訊息是否被印出 (FileScanner 中的 print)
    captured = capsys.readouterr()
    assert f"Warning: Could not read or hash file {error_file_path}" in captured.out
    assert f"Mocked IOError for {error_file_path}" in captured.out

    # 確保其他檔案的雜湊仍然正確（抽查一個）
    file1_path = test_files_structure / "file1.txt"
    expected_file1_hash = manual_sha256_hash(file1_path) # Re-calculate hash as original_open is used

    file1_in_results = next((item_hash for item_hash, item_path in results if item_path.name == "file1.txt"), None)
    assert file1_in_results is not None, "file1.txt 應在結果中"
    assert file1_in_results == expected_file1_hash, "file1.txt 的雜湊值不正確"

# 需要 import builtins
import builtins
