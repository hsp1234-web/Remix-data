import pytest
from pathlib import Path
import json
import io
import zipfile

# pytest 內建的 tmp_path_factory fixture 可供測試使用，無需自訂。
# 若測試函式需要暫存目錄，可以直接在其參數中宣告 tmp_path_factory。
# 例如: def test_something(tmp_path_factory):
#           temp_dir = tmp_path_factory.mktemp("data")
#           ...

@pytest.fixture(scope="session")
def project_root() -> Path:
    """提供專案根目錄的路徑 (包含 pyproject.toml 的目錄)。"""
    # __file__ 是目前 conftest.py 的路徑
    # .parent 是 tests/ 目錄
    # .parent.parent 是專案根目錄
    return Path(__file__).parent.parent


@pytest.fixture
def schemas_json_content(project_root: Path):
    """
    提供 config/schemas.json 的內容。
    這個 fixture會讀取並解析 JSON 檔案，供測試函式使用。
    它使用 project_root fixture 來確保路徑的正確性。
    """
    schemas_file_path = project_root / "config" / "schemas.json"
    # 實際應用中，最好檢查檔案是否存在，並在找不到時引發明確錯誤
    # assert schemas_file_path.exists(), f"Schemas JSON 檔案未在預期路徑找到: {schemas_file_path}"
    with open(schemas_file_path, "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def create_zip_in_memory():
    """
    提供一個函式，用於在記憶體中建立 ZIP 檔案。

    這個 fixture 本身是一個函式，該函式接受一個字典作為參數。
    字典的鍵是 ZIP 檔案內的相對路徑 (字串，例如 'my_file.csv' 或 'folder/data.txt')。
    字典的值是檔案內容 (位元組字串)。

    返回:
        一個 io.BytesIO 物件，其中包含所建立 ZIP 檔案的完整二進位內容。
        測試函式可以將此 BytesIO 物件傳遞給需要類檔案物件的函式 (例如 zipfile.ZipFile)。
    """
    def _create_zip(file_contents_map: dict[str, bytes]) -> io.BytesIO:
        """
        實際建立記憶體中 ZIP 檔案的內部函式。

        參數:
            file_contents_map: 一個字典，鍵為檔案路徑(字串)，值為檔案內容(位元組)。
                               例如: {"file1.txt": b"content1", "data/file2.csv": b"col1,col2\n1,2"}

        返回:
            io.BytesIO: 包含 ZIP 檔案資料的 BytesIO 物件。
        """
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for filename, content in file_contents_map.items():
                # 確保檔案名稱使用 UTF-8 編碼，雖然 zipfile 通常會處理，但明確指定更好
                # ZipInfo 物件允許更細緻的控制，但直接使用 writestr 通常足夠
                zf.writestr(filename, content)

        # 將緩衝區的指標移至開頭，以便後續讀取
        zip_buffer.seek(0)
        return zip_buffer

    return _create_zip

# 如何在測試中使用 create_zip_in_memory:
#
# def test_processing_zip_data(create_zip_in_memory, schemas_json_content, tmp_path_factory):
#     # 1. 準備 ZIP 檔案的內容
#     mock_zip_files = {
#         "file1.csv": b"header1,header2\ndata1,data2",
#         "another_folder/file2.txt": b"Hello, world!",
#         "empty_file.txt": b""
#     }
#
#     # 2. 使用 fixture 建立記憶體中的 ZIP 檔案
#     zip_file_bytes_io = create_zip_in_memory(mock_zip_files)
#
#     # 3. (假設) worker_process_file 或其他函式可以直接處理 BytesIO 物件
#     #    如果不行，可能需要在測試中修補 (patch) zipfile.ZipFile 的開啟方式，
#     #    使其能夠接受 BytesIO 物件。
#     #
#     #    例如，如果 file_parser.py 中的程式碼是:
#     #    import zipfile
#     #    def my_func(path_to_zip):
#     #        with zipfile.ZipFile(path_to_zip, 'r') as zf:
#     #            ...
#     #
#     #    在測試中，你可以這樣做:
#     #    from unittest.mock import patch
#     #    @patch('your_module.zipfile.ZipFile') # 路徑取決於 zipfile 在哪裡被匯入並使用
#     #    def test_my_func(mock_zipfile_constructor, create_zip_in_memory):
#     #        zip_file_bytes_io = create_zip_in_memory({"file.txt": b"test"})
#     #
#     #        # 設定 mock_zipfile_constructor 在被呼叫時返回一個使用 BytesIO 物件初始化的 ZipFile 實例
#     #        # __enter__ 和 __exit__ 是為了模擬 'with' 陳述式的行為
#     #        mock_zip_instance = zipfile.ZipFile(zip_file_bytes_io, 'r')
#     #        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance
#     #
#     #        # 呼叫你的函式
#     #        your_module.my_func("dummy_path.zip") # 路徑現在是虛設的
#     #
#     #        # 進行斷言
#     #        mock_zipfile_constructor.assert_called_once_with("dummy_path.zip", 'r')
#     #        # ... 其他基於 mock_zip_instance 操作的斷言
#
#     # 4. 使用 schemas_json_content
#     #    schema = schemas_json_content['my_schema_key']
#
#     # 5. 使用 tmp_path_factory 建立暫存輸出目錄
#     #    output_dir = tmp_path_factory.mktemp("output_parquet")
#     #    result_path = worker_process_file(zip_file_bytes_io, "file1.csv", schema, output_dir)
#     #    assert (output_dir / "expected_file.parquet").exists()

# 如何在測試中使用 schemas_json_content:
#
# def test_schema_usage(schemas_json_content):
#     assert "some_key" in schemas_json_content  # 假設的鍵
#     # ... 使用載入的 schema 進行測試 ...

# 如何在測試中使用 tmp_path_factory (pytest 內建):
#
# def test_output_to_temp_dir(tmp_path_factory):
#     # tmp_path_factory 是一個 factory fixture，用於建立唯一的暫存目錄
#     my_temp_dir = tmp_path_factory.mktemp("parsed_files")
#     # my_temp_dir 是一個 pathlib.Path 物件
#     output_file = my_temp_dir / "output.txt"
#     output_file.write_text("這是一個測試輸出檔案。")
#     assert output_file.exists()
#     assert output_file.read_text() == "這是一個測試輸出檔案。"
