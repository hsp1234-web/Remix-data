#!/bin/bash
# run_pipeline.sh - The Orchestrator
# This script controls the entire data pipeline flow.
# It will exit immediately if any command fails.
set -e

# --- Configuration ---
SYMBOL="2330.TW" # 台積電
START_DATE="2024-01-01"
END_DATE="2024-06-28"
DATA_DIR="data"
RAW_DB_PATH="$DATA_DIR/raw_market_data.db"
FEATURE_DB_PATH="$DATA_DIR/market_features.db"

echo "=== Pipeline Started for $SYMBOL ==="
echo "--- Ensuring data directory exists ---"
mkdir -p $DATA_DIR

# --- Step 1: Execute Fetcher Service ---
echo "[Orchestrator] Calling Fetcher Service..."
python3 services/fetcher_service.py \
    --symbol "$SYMBOL" \
    --start "$START_DATE" \
    --end "$END_DATE" \
    --db "$RAW_DB_PATH"

# --- Step 2: Execute Processor Service ---
echo "[Orchestrator] Calling Processor Service..."
python3 services/processor_service.py \
    --raw-db "$RAW_DB_PATH" \
    --feature-db "$FEATURE_DB_PATH" \
    --symbol "$SYMBOL"

echo "=== Pipeline Finished Successfully for $SYMBOL ==="
