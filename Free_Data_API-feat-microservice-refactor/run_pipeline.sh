#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# --- 配置 ---
FETCHER_SCRIPT="data_pipeline/fetcher_service.py"
PROCESSOR_SCRIPT="data_pipeline/processor_service.py"
RAW_DB_PATH="data/raw_market_data.duckdb"
FEATURES_DB_PATH="data/features_market_data.duckdb"
DEFAULT_SYMBOLS="AAPL,GOOG,NVDA,TSLA"

# --- 輔助函數 ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] - $1"
}

usage() {
    echo "Usage: $0 --stage <fetch|process|full_pipeline> [--symbol <SYMBOL_LIST>]"
    exit 1
}

# --- 解析參數 ---
STAGE=""
SYMBOLS=$DEFAULT_SYMBOLS
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --stage) STAGE="$2"; shift ;;
        --symbol) SYMBOLS="$2"; shift ;;
        *) echo "Unknown parameter: $1"; usage ;;
    esac
    shift
done

if [ -z "$STAGE" ]; then
    log "錯誤: --stage 參數為必需項 (fetch, process, full_pipeline)"
    usage
fi

# --- 確保數據目錄存在 ---
mkdir -p data

# --- 階段執行 ---
fetch_data() {
    log "--- STAGE: FETCH ---"
    log "正在為股票 [${SYMBOLS}] 獲取原始數據..."
    python -m data_pipeline.fetcher_service --symbols "$SYMBOLS" --output-db "$RAW_DB_PATH"
    log "數據獲取服務執行完畢。"
}

process_data() {
    log "--- STAGE: PROCESS ---"
    log "正在處理原始數據..."
    python -m data_pipeline.processor_service --input-db "$RAW_DB_PATH" --output-db "$FEATURES_DB_PATH"
    log "數據處理服務執行完畢。"
}

# --- 主流程控制 ---
log "管線執行啟動，階段: $STAGE"
case $STAGE in
    fetch)
        fetch_data
        ;;
    process)
        process_data
        ;;
    full_pipeline)
        fetch_data
        process_data
        log "完整管線執行成功！"
        ;;
    *)
        log "錯誤: 未知的階段 '$STAGE'"
        usage
        ;;
esac
log "管線執行結束。"
