import pytest
import pathlib
import json
from src.sp_data_v16.transformation.schema_manager import SchemaManager

@pytest.fixture
def schema_manager_env(tmp_path: pathlib.Path):
    """
    Sets up a temporary SchemaManager with a dummy schemas.json file.
    """
    schemas_data = {
        "schema_utf8_test": {"keywords": ["UTF8Keyword", "測試UTF8"]},
        "schema_big5_test": {"keywords": ["BIG5Keyword", "測試BIG5"]},
        "schema_ascii_only_test": {"keywords": ["ASCIIOnly"]}
    }
    schema_file = tmp_path / "temporary_schemas.json"
    with open(schema_file, 'w', encoding='utf-8') as f:
        json.dump(schemas_data, f)
    manager = SchemaManager(str(schema_file))
    return manager

def test_identifies_utf8_schema(schema_manager_env: SchemaManager):
    """Tests identification of a schema using UTF-8 encoded content."""
    manager = schema_manager_env
    raw_content = "This is a file containing UTF8Keyword.".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) == "schema_utf8_test"

    raw_content_alt = "另一個包含 測試UTF8 的檔案".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content_alt) == "schema_utf8_test"

def test_identifies_big5_schema(schema_manager_env: SchemaManager):
    """Tests identification of a schema using BIG5 encoded content."""
    manager = schema_manager_env
    # BIG5Keyword needs to be representable in BIG5. Assuming it's ASCII.
    # If "BIG5Keyword" itself contains non-ASCII chars not in BIG5, this part of test would be problematic.
    # For simplicity, using an ASCII keyword here for the first assertion.
    raw_content_ascii_in_big5 = "File with ASCIIOnly keyword".encode('big5') # Assuming ASCIIOnly is in schema_big5_test or a new one
    # Let's adjust the fixture to have a schema that makes more sense for this
    # For the provided fixture, let's test "BIG5Keyword" as ASCII
    raw_content_big5_kwd = "A file that has BIG5Keyword here.".encode('big5')
    assert manager.identify_schema_from_content(raw_content_big5_kwd) == "schema_big5_test"

    raw_content_alt = "另一個包含 測試BIG5 的檔案".encode('big5')
    assert manager.identify_schema_from_content(raw_content_alt) == "schema_big5_test"

def test_returns_none_for_no_match(schema_manager_env: SchemaManager):
    """Tests that None is returned when no keywords match."""
    manager = schema_manager_env
    raw_content = "This file has no relevant keywords.".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) is None

def test_handles_decode_error_gracefully(schema_manager_env: SchemaManager):
    """Tests that None is returned if content cannot be decoded by UTF-8 or BIG5."""
    manager = schema_manager_env
    raw_content = b'\xff\xfe\x41\x00'  # UTF-16LE BOM + 'A', will fail UTF-8 and BIG5 decoding
    assert manager.identify_schema_from_content(raw_content) is None

def test_empty_content_returns_none(schema_manager_env: SchemaManager):
    """Tests that empty byte content returns None."""
    manager = schema_manager_env
    raw_content = b''
    assert manager.identify_schema_from_content(raw_content) is None

def test_schema_file_not_found(tmp_path: pathlib.Path):
    """Tests behavior when the schema file is not found."""
    manager = SchemaManager(str(tmp_path / "non_existent_schemas.json"))
    assert manager.schemas == {}  # Expect schemas to be empty

    raw_content = "Any content".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) is None # No schemas to match against

def test_schema_with_no_keywords(tmp_path: pathlib.Path):
    """Tests that a schema definition with no keywords is skipped."""
    schemas_data = {
        "schema_with_keywords": {"keywords": ["RelevantKeyword"]},
        "schema_no_keywords": {"keywords": []},
        "schema_missing_keywords_key": {}
    }
    schema_file = tmp_path / "test_no_keywords_schema.json"
    with open(schema_file, 'w', encoding='utf-8') as f:
        json.dump(schemas_data, f)

    manager = SchemaManager(str(schema_file))

    content_for_no_keywords = "This content should not match schema_no_keywords".encode('utf-8')
    assert manager.identify_schema_from_content(content_for_no_keywords) is None

    content_for_missing_keywords = "This content should not match schema_missing_keywords_key".encode('utf-8')
    assert manager.identify_schema_from_content(content_for_missing_keywords) is None

    content_for_with_keywords = "RelevantKeyword is here".encode('utf-8')
    assert manager.identify_schema_from_content(content_for_with_keywords) == "schema_with_keywords"

def test_json_decode_error(tmp_path: pathlib.Path):
    """Tests behavior when the schema file contains invalid JSON."""
    schema_file = tmp_path / "invalid_schemas.json"
    with open(schema_file, 'w', encoding='utf-8') as f:
        f.write("{'keywords': ['bad json, this is not good']}") # Invalid JSON (single quotes)

    manager = SchemaManager(str(schema_file))
    assert manager.schemas == {} # Expect schemas to be empty due to decode error

    raw_content = "Any content".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) is None

# Note: The original test_identifies_big5_schema had a slight ambiguity
# with "BIG5Keyword" if it wasn't ASCII. The fixture was updated to
# schema_ascii_only_test to make one part of such a test clearer,
# but then the test was re-focused on testing BIG5 encoded keywords directly.
# The current `schema_manager_env` fixture is:
# {
#     "schema_utf8_test": {"keywords": ["UTF8Keyword", "測試UTF8"]},
#     "schema_big5_test": {"keywords": ["BIG5Keyword", "測試BIG5"]},
#     "schema_ascii_only_test": {"keywords": ["ASCIIOnly"]} # Added this for clarity
# }
# The tests above should work with this updated fixture.
# The `test_identifies_big5_schema` uses "BIG5Keyword" which is ASCII and thus BIG5 compatible.
# The Chinese characters "測試BIG5" are valid BIG5 characters.

def test_schema_manager_init_io_error(tmp_path: pathlib.Path, mocker, capsys):
    """
    測試 SchemaManager 在初始化時，如果讀取 schema 檔案發生 IOError (例如 PermissionError)，
    能夠妥善處理，並使 schemas 屬性為空。
    """
    schema_file_path = tmp_path / "permission_denied_schemas.json"
    # 不需要實際建立檔案，因為 open 會被 mock

    # 模擬 builtins.open 在被呼叫時拋出 PermissionError
    mocker.patch('builtins.open', side_effect=PermissionError("Mocked Permission Denied"))

    # manager = SchemaManager(str(schema_file_path)) # 錯誤的：如果__init__拋異常，manager可能未定義

    # 根據原始碼，發生 FileNotFoundError 或 json.JSONDecodeError 時，
    # self.schemas 會是 {}，並且會印出警告。
    # 對於其他 IOError (如 PermissionError)，原始碼中沒有明確的 try-except，
    # 但由於 open() 本身就會拋出，所以 SchemaManager 的 __init__ 應該會讓這個異常直接拋出。
    # 因此，我們預期 PermissionError 會被拋出。
    # 經過思考，原始碼的 try-except 只捕捉 FileNotFoundError 和 JSONDecodeError。
    # 任何其他的 IOError (包括 PermissionError) 都會導致 open() 失敗並直接拋出，
    # 這意味著 SchemaManager 的 __init__ 不會捕捉它，而是會讓它傳播出去。
    # 所以測試應該斷言 PermissionError 被 raise。

    # 更新：重新檢視 src/sp_data_v16/transformation/schema_manager.py，
    # 其 __init__ 中的 try-except 區塊只明確處理 FileNotFoundError 和 json.JSONDecodeError。
    # PermissionError 是 IOError 的子類別。如果 open() 拋出 PermissionError，
    # 它不會被現有的 except 區塊捕捉，因此會向外傳播。

    # 所以，這裡的測試應該是斷言 PermissionError 被 raise
    # 這個預期與最初的「schemas 屬性應為空」不同，這是基於對原始碼錯誤處理邏輯的更仔細分析。

    # 讓我們確認一下 pytest.raises 的用法。如果 __init__ 拋出異常，那麼 manager 物件可能不會被完全初始化。
    # 所以我們應該把 SchemaManager 的實例化放到 pytest.raises 中。
    with pytest.raises(PermissionError, match="Mocked Permission Denied"):
        SchemaManager(str(schema_file_path))

    # 如果我們希望它能處理 PermissionError 並將 schemas 設為空，
    # 則原始碼需要修改。但根據目前的任務，我們是測試現有程式碼的健壯性。
    # 如果現有程式碼會直接拋出，那測試就應該驗證這一點。

    # 假設任務是希望它 *能* 處理，且不拋出，那麼原始碼的 try-except 需要調整。
    # 但目前是「增加模擬...的測試」，所以是測試現狀。

    # 再次檢查原始碼：
    # try:
    #     with open(...)
    # except FileNotFoundError: ...
    # except json.JSONDecodeError: ...
    # 確實，PermissionError 會直接從 with open() 拋出，且不被捕捉。

    # 因此，上面的 pytest.raises(PermissionError) 是正確的。
    # 我們不需要檢查 manager.schemas，因為如果異常拋出，manager 可能未成功建立。

def test_identify_schema_keyword_case_insensitivity(tmp_path: pathlib.Path):
    """測試 identify_schema_from_content 對於 schema 關鍵字的大小寫不敏感。"""
    schemas_data = {
        "case_test_schema": {"keywords": ["MyKeyword", "AnotherKEY"]}
    }
    schema_file = tmp_path / "case_test_schemas.json"
    with open(schema_file, 'w', encoding='utf-8') as f:
        json.dump(schemas_data, f)
    manager = SchemaManager(str(schema_file))

    # 測試不同的關鍵字大小寫組合
    content_lower = "this is mykeyword here".encode('utf-8')
    content_upper = "AND ANOTHERKEY IS PRESENT".encode('utf-8')
    content_mixed = "Some MyKeYwOrD text".encode('utf-8')
    content_unrelated = "no relevant words".encode('utf-8')

    assert manager.identify_schema_from_content(content_lower) == "case_test_schema", "小寫關鍵字應匹配"
    assert manager.identify_schema_from_content(content_upper) == "case_test_schema", "大寫關鍵字應匹配"
    assert manager.identify_schema_from_content(content_mixed) == "case_test_schema", "混合大小寫關鍵字應匹配"
    assert manager.identify_schema_from_content(content_unrelated) is None, "不相關內容不應匹配"

def test_identify_schema_multiple_matches_priority(tmp_path: pathlib.Path):
    """
    測試當檔案內容可能匹配到多個 schema 關鍵字時，
    identify_schema_from_content 會返回字典中第一個匹配到的 schema。
    """
    # 注意：Python 3.7+ 字典保持插入順序。
    # 'schema_A' 先定義。
    # 'schema_B' 後定義。
    schemas_data_order1 = {
        "schema_A": {"keywords": ["KeywordA", "CommonKeyword"]},
        "schema_B": {"keywords": ["KeywordB", "CommonKeyword"]},
        "schema_C": {"keywords": ["KeywordC"]}
    }
    schema_file_order1 = tmp_path / "priority_schemas1.json"
    with open(schema_file_order1, 'w', encoding='utf-8') as f:
        json.dump(schemas_data_order1, f)
    manager_order1 = SchemaManager(str(schema_file_order1))

    # 內容包含 "CommonKeyword"，它存在於 schema_A 和 schema_B。
    # 因為 schema_A 先定義，所以應該匹配到 schema_A。
    content_common = "This content has CommonKeyword.".encode('utf-8')
    assert manager_order1.identify_schema_from_content(content_common) == "schema_A"

    # 內容只包含 "KeywordB"，應該匹配到 schema_B。
    content_b_only = "This content has KeywordB only.".encode('utf-8')
    assert manager_order1.identify_schema_from_content(content_b_only) == "schema_B"

    # 內容包含 "KeywordA" 和 "KeywordB"。因為 schema_A 先，所以匹配 schema_A。
    content_a_and_b = "This content has KeywordA and KeywordB.".encode('utf-8')
    assert manager_order1.identify_schema_from_content(content_a_and_b) == "schema_A"

    # 更改 schema 順序來驗證行為
    schemas_data_order2 = {
        "schema_B": {"keywords": ["KeywordB", "CommonKeyword"]}, # schema_B 現在先定義
        "schema_A": {"keywords": ["KeywordA", "CommonKeyword"]},
        "schema_C": {"keywords": ["KeywordC"]}
    }
    schema_file_order2 = tmp_path / "priority_schemas2.json"
    with open(schema_file_order2, 'w', encoding='utf-8') as f:
        json.dump(schemas_data_order2, f)
    manager_order2 = SchemaManager(str(schema_file_order2))

    # 內容包含 "CommonKeyword"。因為 schema_B 現在先定義，所以應該匹配到 schema_B。
    assert manager_order2.identify_schema_from_content(content_common) == "schema_B"

    # 內容包含 "KeywordA" 和 "KeywordB"。因為 schema_B 先，所以匹配 schema_B。
    assert manager_order2.identify_schema_from_content(content_a_and_b) == "schema_B"

    # 內容只包含 "KeywordA"，仍然匹配 schema_A。
    content_a_only = "This content has KeywordA only.".encode('utf-8')
    assert manager_order2.identify_schema_from_content(content_a_only) == "schema_A"
