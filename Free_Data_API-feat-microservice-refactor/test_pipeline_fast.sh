#!/bin/bash
# test_pipeline_fast.sh
# 一個用於在 Jules 沙箱中快速、可靠地測試數據處理流程的原子化腳本。

# 如果任何命令失敗，立即退出。
set -e

# --- 輔助函數 ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [FastTest] - $1"
}

log "🚀 快速檢測流程啟動..."

# --- 1. 環境準備 ---
# 定義腳本和模擬資料庫的路徑
PROCESSOR_SCRIPT="data_pipeline/processor_service.py"
VERIFIER_SCRIPT="verify_db.py"
MOCK_DATA_DIR="test_data"
RAW_DB_PATH="$MOCK_DATA_DIR/mock_raw_data.duckdb"
FEATURES_DB_PATH="$MOCK_DATA_DIR/mock_features_data.duckdb"

# 清理並創建一個乾淨的測試數據目錄
log "正在準備乾淨的測試環境..."
rm -rf $MOCK_DATA_DIR
mkdir -p $MOCK_DATA_DIR
log "測試目錄 '$MOCK_DATA_DIR' 已創建。"

# --- 2. 建立「模擬」原始數據 ---
# 我們不執行真實的 fetcher，而是用一個 Python 命令直接創建一個包含可預測數據的資料庫。
log "正在創建模擬的原始數據庫 (mock raw database)..."
python -c "
import pandas as pd
import duckdb

# 這是我們已知的、用於測試的假數據
mock_data = {
    'date': pd.to_datetime(['2023-01-02', '2023-01-03', '2023-01-04']),
    'symbol': ['MOCK', 'MOCK', 'MOCK'],
    'open': [100, 102, 101],
    'high': [103, 104, 102],
    'low': [99, 101, 100],
    'close': [102, 103, 101.5],
    'adjClose': [102, 103, 101.5],
    'volume': [1000, 1200, 1100]
}
df = pd.DataFrame(mock_data)

con = duckdb.connect('$RAW_DB_PATH')
con.execute('CREATE TABLE raw_prices AS SELECT * FROM df')
con.close()
print('✅ 模擬原始數據庫創建成功: $RAW_DB_PATH')
"

# --- 3. 執行數據處理服務 ---
# 讓 processor_service 指向我們的模擬資料庫
log "正在執行數據處理服務 (data_pipeline.processor_service)..."
python -m data_pipeline.processor_service --input-db "$RAW_DB_PATH" --output-db "$FEATURES_DB_PATH"

# --- 4. 驗證輸出結果 ---
# 使用 verify_db.py 來檢查處理後的特徵資料庫
log "正在執行數據驗證 (verify_db.py)..."
python $VERIFIER_SCRIPT --symbol MOCK --raw-db-path "$RAW_DB_PATH" --features-db-path "$FEATURES_DB_PATH"

log "✅ 快速檢測流程執行完畢！"
echo "-----------------------------------------------------"
echo "請檢查以上日誌以確認所有步驟是否成功。"
echo "如果所有檢查都通過，代表核心處理邏輯在新架構下運作正常。"
echo "-----------------------------------------------------"
