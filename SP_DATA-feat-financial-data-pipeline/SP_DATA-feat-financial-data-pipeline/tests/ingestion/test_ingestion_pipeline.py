import pytest
import zipfile
from pathlib import Path
from taifex_pipeline.ingestion.pipeline import IngestionPipeline

# ==============================================================================
# 1. 測試輔助函式 (Test Helper Function)
# ==============================================================================

def create_test_zip(zip_path: Path, content_spec: dict):
    """
    遞迴地創建一個測試用的 zip 檔案，可以包含巢狀的 zip 檔案。

    Args:
        zip_path: 要創建的 zip 檔案的完整路徑。
        content_spec: 一個字典，描述了壓縮檔的內容。
                      - key (str): 檔案名稱。
                      - value (str): 檔案的文本內容。
                      - value (dict): 一個巢狀的 content_spec，用於創建巢狀 zip。
    """
    with zipfile.ZipFile(zip_path, 'w') as zf:
        for name, content in content_spec.items():
            if isinstance(content, dict):
                # 處理巢狀 zip 檔案
                # 在臨時位置創建巢狀 zip
                nested_zip_path = zip_path.parent / f"_{name}" # Use a temp prefix
                create_test_zip(nested_zip_path, content)
                # 將創建好的巢狀 zip 添加到父 zip 中
                zf.write(nested_zip_path, arcname=name)
                # 刪除臨時的巢狀 zip 檔案
                nested_zip_path.unlink()
            elif isinstance(content, str):
                # 處理普通文本檔案
                zf.writestr(name, content)
            # Add handling for bytes content if needed, e.g. for binary files
            elif isinstance(content, bytes):
                zf.writestr(name, content)


# ==============================================================================
# 2. Pytest Fixture (測試環境)
# ==============================================================================

@pytest.fixture
def ingestion_pipeline_env(tmp_path):
    """
    為 ingestion pipeline 測試創建一個隔離的檔案環境。
    """
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    output_dir.mkdir()

    # Basic config, can be overridden by tests if needed
    config = {
        "ingestion": {
            "source_dir": str(source_dir),
            "output_dir": str(output_dir),
        }
        # Add other pipeline configs if necessary, e.g. logging
    }
    pipeline = IngestionPipeline(config)

    return {
        "pipeline": pipeline,
        "source_dir": source_dir,
        "output_dir": output_dir,
        "config": config # Make config available if tests want to modify it
    }


# ==============================================================================
# 3. 測試案例 (Test Cases)
# ==============================================================================

def test_process_single_non_nested_zip(ingestion_pipeline_env):
    """
    測試場景 1：處理一個不含巢狀結構的簡單壓縮檔。
    """
    # Arrange: 準備環境和測試檔案
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    zip_spec = {
        "file1.csv": "col1,col2\n1,2",
        "file2.txt": "some text data"
    }
    create_test_zip(source_dir / "simple.zip", zip_spec)

    # Act: 執行管線
    pipeline.run()

    # Assert: 驗證輸出結果
    assert (output_dir / "file1.csv").exists()
    assert (output_dir / "file2.txt").exists()
    assert (output_dir / "file1.csv").read_text() == "col1,col2\n1,2"
    assert (output_dir / "file2.txt").read_text() == "some text data"
    assert len(list(output_dir.iterdir())) == 2  # 確保沒有多餘的檔案


def test_process_with_single_level_nesting(ingestion_pipeline_env, caplog):
    """
    測試場景 2：處理包含單層巢狀結構的壓縮檔。
    這是一個完整的範例，展示了如何使用輔助工具和 fixture。
    """
    # Arrange: 準備環境和具有巢狀結構的測試檔案
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    # 定義一個包含巢狀 zip 的檔案結構
    zip_spec = {
        "file_in_level1.csv": "level1 data",
        "level2.zip": { # This is the nested zip
            "file_in_level2.csv": "level2 data"
        }
    }
    create_test_zip(source_dir / "level1.zip", zip_spec)

    # Act: 執行管線
    # Assuming logger is setup in a way that caplog can capture it
    # If IngestionPipeline uses a logger obtained via get_logger(name),
    # ensure caplog is configured for that logger name or its parent.
    # For simplicity, let's assume default root logger or a known one.
    # import logging # If needed to set level for specific loggers for caplog
    # caplog.set_level(logging.INFO, logger="taifex_pipeline.ingestion.pipeline")


    pipeline.run()

    # Assert: 驗證所有檔案是否被正確提取
    # 1. 驗證 level1 的檔案
    assert (output_dir / "file_in_level1.csv").exists()
    assert (output_dir / "file_in_level1.csv").read_text() == "level1 data"

    # 2. 驗證巢狀的 zip 本身也被提取出來了 (this is the critical part for the queue)
    assert (output_dir / "level2.zip").exists(), "Nested zip 'level2.zip' should be extracted to output_dir first"

    # 3. 驗證 level2 的檔案 (from the processed nested zip)
    assert (output_dir / "file_in_level2.csv").exists()
    assert (output_dir / "file_in_level2.csv").read_text() == "level2 data"

    # 4. 驗證日誌記錄 (optional, but good for confirming behavior)
    # Note: caplog might need specific setup if logger names are involved.
    # This basic check assumes messages go to a logger caplog can capture.
    # For more robust log testing, you might need to patch the logger within the pipeline instance.

    # Example of patching logger if default caplog doesn't work:
    # with patch.object(pipeline.logger, 'info') as mock_log_info:
    # pipeline.run()
    # mock_log_info.assert_any_call("Found nested archive: level2.zip. Extracting...")

    # Using caplog directly (ensure the pipeline's logger is compatible)
    # For this to work well, the pipeline's logger should be accessible or a child of the root logger
    # and caplog should be at the correct level (e.g. INFO or DEBUG)
    if caplog: # Check if caplog fixture is active and working
        assert "Found nested archive: level2.zip. Extracting..." in caplog.text
        assert "Added 1 new nested archives to the processing queue." in caplog.text

    # 5. 確保輸出目錄中總共有 3 個項目 (file_in_level1.csv, level2.zip, file_in_level2.csv)
    # The number of items depends on whether the nested zip itself is kept or only its contents
    # Based on the pipeline code, the nested zip is extracted, and *its path* is added to the queue.
    # The *contents* of the nested zip are then extracted.
    # The nested zip file *itself* (level2.zip) will be in the output_dir because it was extracted there.
    # And its content (file_in_level2.csv) will also be in output_dir.
    assert len(list(output_dir.iterdir())) == 3, \
        f"Output dir should contain 3 items. Found: {[p.name for p in output_dir.iterdir()]}"

# Placeholder for other tests to be added by the agent
# test_process_multi_level_nesting
def test_process_multi_level_nesting(ingestion_pipeline_env, caplog):
    """
    測試場景 3：處理多層巢狀結構 (A.zip -> B.zip -> C.zip -> final.csv)
    """
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    zip_spec = {
        "A.zip": {
            "file_in_A.txt": "Content of A",
            "B.zip": {
                "file_in_B.txt": "Content of B",
                "C.zip": {
                    "final.csv": "final content"
                }
            }
        }
    }
    create_test_zip(source_dir / "entry.zip", zip_spec) # The entry point zip

    pipeline.run()

    # Assertions
    assert (output_dir / "A.zip").exists(), "A.zip should be extracted"
    assert (output_dir / "file_in_A.txt").exists()
    assert (output_dir / "file_in_A.txt").read_text() == "Content of A"

    assert (output_dir / "B.zip").exists(), "B.zip should be extracted from A.zip"
    assert (output_dir / "file_in_B.txt").exists()
    assert (output_dir / "file_in_B.txt").read_text() == "Content of B"

    assert (output_dir / "C.zip").exists(), "C.zip should be extracted from B.zip"
    assert (output_dir / "final.csv").exists()
    assert (output_dir / "final.csv").read_text() == "final content"

    # Check total items in output: A.zip, file_in_A, B.zip, file_in_B, C.zip, final.csv
    # Total = 6 items
    # entry.zip is the source, A.zip is its content.
    # So output should have A.zip, file_in_A.txt (from A.zip)
    # B.zip (from A.zip), file_in_B.txt (from B.zip)
    # C.zip (from B.zip), final.csv (from C.zip)

    # Let's verify the number of items in output_dir
    # A.zip, file_in_A.txt
    # B.zip, file_in_B.txt
    # C.zip, final.csv
    # This is 6 items.
    # The initial zip "entry.zip" is in source_dir, its content "A.zip" is extracted to output_dir.
    # Then A.zip is processed from output_dir.
    # Its contents file_in_A.txt and B.zip are extracted to output_dir.
    # Then B.zip is processed. Its contents file_in_B.txt and C.zip are extracted to output_dir.
    # Then C.zip is processed. Its content final.csv is extracted to output_dir.

    # The structure of create_test_zip for entry.zip:
    # entry.zip contains:
    #   A.zip (as a file, which itself is a zip structure)
    # So, when entry.zip is processed, A.zip is extracted to output_dir.
    # Then A.zip is added to queue.
    # When A.zip is processed:
    #   file_in_A.txt is extracted to output_dir.
    #   B.zip is extracted to output_dir.
    # Then B.zip is added to queue.
    # When B.zip is processed:
    #   file_in_B.txt is extracted to output_dir.
    #   C.zip is extracted to output_dir.
    # Then C.zip is added to queue.
    # When C.zip is processed:
    #   final.csv is extracted to output_dir.

    # Expected files in output_dir:
    # A.zip (extracted from entry.zip)
    # file_in_A.txt (extracted from A.zip)
    # B.zip (extracted from A.zip)
    # file_in_B.txt (extracted from B.zip)
    # C.zip (extracted from B.zip)
    # final.csv (extracted from C.zip)
    # Total 6 files.

    output_files = sorted([p.name for p in output_dir.iterdir()])
    expected_output_files = sorted([
        "A.zip", "file_in_A.txt",
        "B.zip", "file_in_B.txt",
        "C.zip", "final.csv"
    ])
    assert output_files == expected_output_files
    assert len(output_files) == 6

    if caplog:
        assert "Found nested archive: A.zip. Extracting..." in caplog.text # From entry.zip
        assert "Found nested archive: B.zip. Extracting..." in caplog.text # From A.zip
        assert "Found nested archive: C.zip. Extracting..." in caplog.text # From B.zip
        # Check how many times new archives were added
        # Entry.zip -> adds A.zip (1)
        # A.zip -> adds B.zip (1)
        # B.zip -> adds C.zip (1)
        # C.zip -> adds nothing
        queue_add_logs = [rec.message for rec in caplog.records if "new nested archives to the processing queue" in rec.message]
        assert len(queue_add_logs) == 3 # A.zip, B.zip, C.zip
        assert "Added 1 new nested archives to the processing queue." in queue_add_logs[0] # A.zip
        assert "Added 1 new nested archives to the processing queue." in queue_add_logs[1] # B.zip
        assert "Added 1 new nested archives to the processing queue." in queue_add_logs[2] # C.zip


def test_process_multiple_nested_zips(ingestion_pipeline_env, caplog):
    """
    測試場景 4：處理包含多個巢狀檔案的結構 (A.zip內含B.zip, C.zip和file_A.csv)
    B.zip 內含 file_B.csv
    C.zip 內含 file_C.csv
    """
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    zip_spec = {
        "A.zip": {
            "file_A.csv": "Content of A",
            "B.zip": {
                "file_B.csv": "Content of B"
            },
            "C.zip": {
                "file_C.csv": "Content of C"
            }
        }
    }
    create_test_zip(source_dir / "entry_multiple.zip", zip_spec)

    pipeline.run()

    # Assertions for extracted files
    assert (output_dir / "A.zip").exists()
    assert (output_dir / "file_A.csv").exists()
    assert (output_dir / "file_A.csv").read_text() == "Content of A"

    assert (output_dir / "B.zip").exists()
    assert (output_dir / "file_B.csv").exists()
    assert (output_dir / "file_B.csv").read_text() == "Content of B"

    assert (output_dir / "C.zip").exists()
    assert (output_dir / "file_C.csv").exists()
    assert (output_dir / "file_C.csv").read_text() == "Content of C"

    # Expected files in output_dir:
    # A.zip (from entry_multiple.zip)
    # file_A.csv (from A.zip)
    # B.zip (from A.zip)
    # file_B.csv (from B.zip)
    # C.zip (from A.zip)
    # file_C.csv (from C.zip)
    # Total 6 files.
    output_files = sorted([p.name for p in output_dir.iterdir()])
    expected_output_files = sorted([
        "A.zip", "file_A.csv",
        "B.zip", "file_B.csv",
        "C.zip", "file_C.csv"
    ])
    assert output_files == expected_output_files
    assert len(output_files) == 6

    if caplog:
        assert "Found nested archive: A.zip. Extracting..." in caplog.text
        # When A.zip is processed, it finds B.zip and C.zip
        assert "Found nested archive: B.zip. Extracting..." in caplog.text
        assert "Found nested archive: C.zip. Extracting..." in caplog.text

        queue_add_logs = [rec.message for rec in caplog.records if "new nested archives to the processing queue" in rec.message]
        # entry_multiple.zip -> adds A.zip (1)
        # A.zip -> adds B.zip and C.zip (2)
        # B.zip -> adds nothing
        # C.zip -> adds nothing
        assert len(queue_add_logs) == 2 # A.zip; then B.zip & C.zip together
        assert "Added 1 new nested archives to the processing queue." in queue_add_logs[0] # A.zip
        assert "Added 2 new nested archives to the processing queue." in queue_add_logs[1] # B.zip and C.zip


def test_infinite_loop_direct_recursion(ingestion_pipeline_env, caplog):
    """
    測試場景 5：防範無限迴圈（直接遞迴 A.zip 內含 A.zip）
    """
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    # Create A.zip which contains itself. This is tricky with create_test_zip.
    # We'll create A.zip, then manually add a reference to itself.
    # Simpler: A.zip contains B.zip, and B.zip contains A.zip (circular, next test)
    # For direct recursion, the zip file would have to contain its own path as a member,
    # which is not typical.
    # A more realistic direct recursion is if processing A.zip (path1) extracts a file
    # also named A.zip (path2, but same name) into the output, and path2 gets added to queue.
    # The current `processed_paths` uses absolute paths, so this should be handled if
    # path1 (source) and path2 (output) are different.
    # If A.zip from source is processed, and it extracts another A.zip to output,
    # then output/A.zip is added to queue. When output/A.zip is processed, it's a new path.

    # Let's test the case where a zip extracts another zip *of the same name* into the output dir,
    # and that extracted zip is then queued.
    # source_dir/A.zip extracts A.zip to output_dir/A.zip
    # Then output_dir/A.zip is processed.

    zip_spec_A = {
        "A.zip": {"dummy.txt": "this is to make A.zip a valid zip"}
        # This spec means A.zip contains a file named "A.zip" which is a zip
    }
    create_test_zip(source_dir / "A_entry.zip", zip_spec_A)

    pipeline.run()

    # Assertions
    # A_entry.zip is processed, extracts A.zip to output_dir/A.zip
    # output_dir/A.zip is added to queue.
    # output_dir/A.zip is processed, extracts dummy.txt to output_dir/dummy.txt
    # It also extracts its member "A.zip" (which is a zip) to output_dir/A.zip (overwriting itself)
    # This new output_dir/A.zip is added to queue.
    # Now, when output_dir/A.zip is picked again, it's in processed_paths.

    assert (output_dir / "A.zip").exists()
    assert (output_dir / "dummy.txt").exists()
    assert (output_dir / "dummy.txt").read_text() == "this is to make A.zip a valid zip"

    # Check logs for skipping
    if caplog:
        # The first time output_dir/A.zip is processed, it's not skipped.
        # The second time it's picked from queue, it should be skipped.
        # Path in processed_paths will be output_dir/A.zip
        # A_entry.zip (source) -> extracts A.zip to output/A.zip (path P1)
        # P1 is added to queue. P1 is processed.
        # P1 extracts dummy.txt. P1 also extracts its member "A.zip" to output/A.zip (path P1 again).
        # P1 is added to queue again.
        # P1 is popped from queue. P1 is IN processed_paths. Skipped.
        assert f"Skipping already processed file to prevent infinite loop: A.zip" in caplog.text
        # Count occurrences of "Processing archive: A.zip"
        processing_logs = [rec.message for rec in caplog.records if "Processing archive: A.zip" in rec.message]
        assert len(processing_logs) == 1, "A.zip from output should only be processed once"


def test_infinite_loop_circular_dependency(ingestion_pipeline_env, caplog):
    """
    測試場景 6：防範無限迴圈（循環依賴 A.zip -> B.zip -> A.zip）
    """
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    # entry.zip contains A.zip
    # A.zip contains B.zip
    # B.zip contains A.zip (this A.zip is a different actual file but named the same)

    zip_spec_B_contains_A = {
        "A.zip": {"dummy_in_B_A.txt": "content"} # B will contain an A.zip
    }
    # To make this work with create_test_zip, we'll have B.zip contain a *file* named A.zip
    # which itself is a zip.

    # entry.zip -> extracts actual_A.zip (named A.zip)
    # actual_A.zip -> extracts actual_B.zip (named B.zip)
    # actual_B.zip -> extracts another_A.zip (named A.zip)
    # another_A.zip -> contains dummy.txt
    # The infinite loop check is based on *path* of the zip being processed.
    # So, source/entry.zip is processed. output/A.zip (path P_A1) is queued.
    # P_A1 is processed. output/B.zip (path P_B1) is queued.
    # P_B1 is processed. output/A.zip (path P_A2, but same as P_A1 if B extracts to same output) is queued.
    # If P_A2 is same as P_A1, it will be skipped.

    # Structure:
    # entry.zip:
    #   A.zip:
    #     file_in_A.txt
    #     B.zip:
    #       file_in_B.txt
    #       A_from_B.zip:  (This is the problematic one, named A.zip for conflict)
    #         final_dummy.txt

    # Redefine spec for clarity of paths
    # source_dir/entry.zip
    #   output_dir/A.zip (from entry.zip) -> P_A
    #     output_dir/file_in_A.txt
    #     output_dir/B.zip (from A.zip) -> P_B
    #       output_dir/file_in_B.txt
    #       output_dir/A_from_B.zip (from B.zip, this is the 'A.zip' that B contains) -> P_A_from_B
    #         output_dir/final_dummy.txt
    #
    # The critical part is if P_A_from_B is added to queue and has the same *path* as P_A.
    # Our current pipeline extracts nested zips with their original names into self.output_dir.
    # So if B.zip contains an archive member also named "A.zip", it will be extracted to
    # self.output_dir / "A.zip". This *is* the same path as P_A.

    zip_spec = {
        "A.zip": { # This will be extracted as output_dir/A.zip (P_A)
            "file_in_A.txt": "text in A",
            "B.zip": { # This will be extracted as output_dir/B.zip (P_B)
                "file_in_B.txt": "text in B",
                # B.zip contains an archive member that is *also* named A.zip
                "A.zip": { # This will attempt to extract to output_dir/A.zip, same as P_A
                    "dummy_final.txt": "final text"
                }
            }
        }
    }
    create_test_zip(source_dir / "entry_circular.zip", zip_spec)

    pipeline.run()

    # Assertions
    # Expected files in output_dir:
    # A.zip (P_A, extracted from entry_circular.zip)
    # file_in_A.txt (from P_A)
    # B.zip (P_B, extracted from P_A)
    # file_in_B.txt (from P_B)
    # dummy_final.txt (from the A.zip that was inside B.zip, which overwrote/was P_A)

    assert (output_dir / "A.zip").exists()
    assert (output_dir / "file_in_A.txt").exists()
    assert (output_dir / "B.zip").exists()
    assert (output_dir / "file_in_B.txt").exists()
    assert (output_dir / "dummy_final.txt").exists()

    # Check log for skipping message.
    # Order:
    # 1. entry_circular.zip processed. Extracts A.zip to output/A.zip (P_A). P_A queued.
    # 2. P_A processed. Extracts file_in_A.txt and B.zip to output/B.zip (P_B). P_B queued. (P_A added to processed_paths)
    # 3. P_B processed. Extracts file_in_B.txt and A.zip (from B) to output/A.zip (P_A again). P_A queued. (P_B added to processed_paths)
    # 4. P_A popped from queue. P_A is IN processed_paths. Skipped.
    if caplog:
        assert "Skipping already processed file to prevent infinite loop: A.zip" in caplog.text

        # A.zip (P_A) should be processed once.
        # B.zip (P_B) should be processed once.
        processing_A_logs = [rec.message for rec in caplog.records if "Processing archive: A.zip" in rec.message]
        processing_B_logs = [rec.message for rec in caplog.records if "Processing archive: B.zip" in rec.message]
        assert len(processing_A_logs) == 1, "A.zip should only be processed once."
        assert len(processing_B_logs) == 1, "B.zip should only be processed once."

def test_corrupted_zip_file(ingestion_pipeline_env, caplog):
    """
    測試場景 7：處理損壞的壓縮檔。
    管線應跳過損壞的檔案，記錄錯誤，並繼續處理其他檔案。
    """
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    # Create a good zip file
    good_zip_spec = {"good_file.txt": "this is good data"}
    create_test_zip(source_dir / "good.zip", good_zip_spec)

    # Create a corrupted (or just non-zip) file and name it .zip
    bad_zip_path = source_dir / "bad.zip"
    with open(bad_zip_path, "w") as f:
        f.write("this is not a zip file content")

    pipeline.run()

    # Assertions
    # Good file should be processed
    assert (output_dir / "good_file.txt").exists()
    assert (output_dir / "good_file.txt").read_text() == "this is good data"

    # Bad file should not produce output (other than possibly the bad.zip itself if it was queued then failed)
    # Check that no files *from* bad.zip are in output
    # The bad.zip itself might be in processed_paths if it was picked from queue.
    # The crucial part is that the pipeline doesn't crash and logs the error.

    assert not (output_dir / "bad.zip").exists(), "Corrupted bad.zip should not be extracted or left in output."
                                                # Or, if it was extracted before error, it might exist.
                                                # The important part is that its *contents* aren't processed
                                                # and the error is logged.
                                                # The current pipeline extracts nested zips first, then queues them.
                                                # If bad.zip is a top-level non-nested zip, it's processed directly.

    # Let's refine: if bad.zip is a source file, _process_single_zip will try to open it.
    # It will fail with BadZipFile. Nothing from it will be extracted.
    # It will be added to processed_paths. No new zips will be added to queue from it.

    if caplog:
        assert f"Failed to process bad.zip: Corrupted zip file." in caplog.text

    # Ensure only good_file.txt is in output
    output_items = list(output_dir.iterdir())
    self.assertEqual(len(output_items), 1)
    self.assertEqual(output_items[0].name, "good_file.txt")


def test_empty_zip_file(ingestion_pipeline_env, caplog):
    """
    測試場景 8：處理空的壓縮檔。
    管線應能正常結束，不產生任何輸出檔案，且佇列應為空。
    """
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    # Create an empty zip file
    create_test_zip(source_dir / "empty.zip", {})

    pipeline.run()

    # Assertions
    # No files should be in the output directory
    output_items = list(output_dir.iterdir())
    self.assertEqual(len(output_items), 0, f"Output directory should be empty, found: {output_items}")

    if caplog:
        # Check that it was processed
        assert "Processing archive: empty.zip" in caplog.text
        # Check that no new archives were added from it
        queue_add_logs = [rec.message for rec in caplog.records if "new nested archives to the processing queue" in rec.message]
        # Only the initial scan might add empty.zip to queue if it's from source.
        # If empty.zip was itself a nested zip, then its parent would have added it.
        # The log "Added X new nested archives" comes *after* processing a zip.
        # So, if empty.zip is processed and yields no nested zips, this log shouldn't appear for it.

        # More precise: check that after "Processing archive: empty.zip", there's no
        # "Added ... new nested archives" log entry immediately related to it,
        # or that the count is 0 if such a log is always made.
        # The current code only logs "Added..." if newly_found_zips is not empty.
        # So we should not see that log for empty.zip.

        # Ensure no errors were logged for this specific file
        error_logs_for_empty = [rec for rec in caplog.records if "empty.zip" in rec.message and rec.levelno >= 30] # WARNING or ERROR
        self.assertEqual(len(error_logs_for_empty), 0, "Should be no errors for processing an empty zip.")

def test_zip_with_only_directories(ingestion_pipeline_env, caplog):
    """
    測試處理只包含目錄的壓縮檔。
    """
    source_dir = ingestion_pipeline_env["source_dir"]
    output_dir = ingestion_pipeline_env["output_dir"]
    pipeline = ingestion_pipeline_env["pipeline"]

    # zipfile library doesn't directly create directory entries in the same way
    # as files via writestr or write. Directories are implicitly created by file paths.
    # To create a zip that "contains" a directory, you usually add a file within that dir.
    # Or, the ZipInfo object can be manipulated.
    # For create_test_zip, we can't directly specify a directory as content.
    # However, the pipeline's infolist loop has `if member_info.is_dir(): continue`
    # So, this test is more about ensuring that part works and no error occurs.

    zip_path = source_dir / "only_dirs.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        # Create a directory entry explicitly
        dir_info = zipfile.ZipInfo("some_dir/")
        # dir_info.external_attr = 0o40755 << 16  # typical directory permissions
        # dir_info.compress_type = zipfile.ZIP_STORED # Dirs are not compressed
        zf.writestr(dir_info, b"") # Add directory entry
        zf.writestr("some_dir/file_in_dir.txt", "dummy")


    pipeline.run()

    # Assertions
    # The directory itself is not "extracted" as a folder by pipeline logic,
    # but its contained files are.
    assert (output_dir / "file_in_dir.txt").exists()
    assert (output_dir / "file_in_dir.txt").read_text() == "dummy"

    # Ensure no other unexpected files or directories are created at top level of output
    output_items = list(output_dir.iterdir())
    self.assertEqual(len(output_items), 1)

    if caplog:
        assert "Processing archive: only_dirs.zip" in caplog.text
        # No errors should be logged for directories
        error_logs_for_file = [rec for rec in caplog.records if "only_dirs.zip" in rec.message and rec.levelno >= 40] # ERROR
        self.assertEqual(len(error_logs_for_file), 0, "Should be no errors for processing a zip with directories.")

# test_zip_with_only_directories
