#!/bin/bash

# 當任何命令失敗時立即終止腳本，確保原子性
set -e

echo "--- [1/4] 開始執行原子化測試規格書 ---"

# 檢查當前目錄結構，用於除錯
echo "--- 當前工作目錄: $(pwd) ---"
echo "--- 專案根目錄檔案列表: ---"
ls -l

# --- 步驟一：安裝所有依賴 ---
echo "\n--- [2/4] 正在從 requirements.txt 安裝依賴... ---"
# 使用 --user 選項可能有助於在某些受限環境中安裝
# 移除了 --disable-pip-version-check 和 --no-cache-dir 以簡化，如果需要可以加回
pip install -r requirements.txt

# --- 步驟二：設定 Python 環境 ---
# 雖然在此腳本中，當前目錄就是專案根目錄，但這是一個好習慣
export PYTHONPATH=$(pwd)
echo "--- PYTHONPATH 已設定為: ${PYTHONPATH} ---"

# --- 步驟三：執行主協調器 ---
echo "\n--- [3/4] 正在執行 main_orchestrator.py... ---"
# 確保 Python 能夠找到 main_orchestrator.py
# 如果 PYTHONPATH 正確設定，直接執行即可
python main_orchestrator.py

# --- 步驟四：驗證結果 ---
echo "\n--- [4/4] 正在驗證數據庫結果... ---"

# 驗證 raw_lake 資料庫和目標表
RAW_DB_PATH="data_workspace/raw_lake/raw_lake.duckdb"
# 根據 taifex_format_catalog.json 和 sample_daily_options_20231220.csv,
# raw 表名應為 raw_taifex_options_daily_quotes
RAW_TABLE_NAME="raw_taifex_options_daily_quotes"

if [ -f "$RAW_DB_PATH" ]; then
    echo "✅ 驗證成功: $RAW_DB_PATH 檔案已創建。"
    echo "--- 正在查詢 raw_lake 資料庫內容... ---"

    # 檢查 raw_taifex_options_daily_quotes 表是否存在並且有數據
    # 我們的範例檔案 sample_daily_options_20231220.csv 有5行數據 (不含表頭)
    # TaifexService 在 ingest_single_file 中會為 DataFrame 添加 'raw_source_file' 和 'ingested_at_raw' 欄位
    # 所以原始的19個欄位會變成21個
    EXPECTED_ROW_COUNT=5
    ROW_COUNT=$(duckdb "$RAW_DB_PATH" -c "SELECT COUNT(*) FROM $RAW_TABLE_NAME;")

    if [ "$ROW_COUNT" -eq "$EXPECTED_ROW_COUNT" ]; then
        echo "✅ 驗證成功: 表 $RAW_TABLE_NAME 在 $RAW_DB_PATH 中包含 $ROW_COUNT 行數據 (符合預期)。"
        echo "--- 表 $RAW_TABLE_NAME 的前2行數據: ---"
        duckdb "$RAW_DB_PATH" -c "SELECT * FROM $RAW_TABLE_NAME LIMIT 2;"
    else
        echo "❌ 驗證失敗: 表 $RAW_TABLE_NAME 在 $RAW_DB_PATH 中包含 $ROW_COUNT 行數據，預期為 $EXPECTED_ROW_COUNT。"
        echo "--- 表 $RAW_TABLE_NAME 的所有數據 (如果存在): ---"
        duckdb "$RAW_DB_PATH" -c "SELECT * FROM $RAW_TABLE_NAME;" || echo "查詢 $RAW_TABLE_NAME 失敗或表不存在。"
        exit 1
    fi
else
    echo "❌ 驗證失敗: $RAW_DB_PATH 檔案未找到！"
    exit 1
fi

# 驗證 curated_mart 資料庫和目標表
CURATED_DB_PATH="data_workspace/curated_mart/curated_mart.duckdb"
# 根據 database_schemas.json，其中一個表是 fact_daily_market_summary
CURATED_TABLE_NAME="fact_daily_market_summary"

if [ -f "$CURATED_DB_PATH" ]; then
    echo "\n✅ 驗證成功: $CURATED_DB_PATH 檔案已創建。"
    echo "--- 正在查詢 curated_mart 資料庫內容... ---"

    # 檢查 fact_daily_market_summary 表是否存在（應由 initialize_schema 創建）
    # 由於 TaifexService 的轉換邏輯是佔位符，此表目前應為空
    TABLE_EXISTS_COUNT=$(duckdb "$CURATED_DB_PATH" -c "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='$CURATED_TABLE_NAME';")

    if [ "$TABLE_EXISTS_COUNT" -eq 1 ]; then
        echo "✅ 驗證成功: 表 $CURATED_TABLE_NAME 在 $CURATED_DB_PATH 中已創建。"
        ROW_COUNT_CURATED=$(duckdb "$CURATED_DB_PATH" -c "SELECT COUNT(*) FROM $CURATED_TABLE_NAME;")
        if [ "$ROW_COUNT_CURATED" -eq 0 ]; then
            echo "✅ 驗證成功: 表 $CURATED_TABLE_NAME 為空 (符合預期，因轉換邏輯為佔位符)。"
        else
            echo "⚠️ 驗證警告: 表 $CURATED_TABLE_NAME 包含 $ROW_COUNT_CURATED 行數據，預期為0。"
        fi
    else
        echo "❌ 驗證失敗: 表 $CURATED_TABLE_NAME 未在 $CURATED_DB_PATH 中找到！"
        exit 1
    fi
else
    echo "❌ 驗證失敗: $CURATED_DB_PATH 檔案未找到！"
    exit 1
fi

echo "\n--- 原子化測試規格書執行成功！ ---"
