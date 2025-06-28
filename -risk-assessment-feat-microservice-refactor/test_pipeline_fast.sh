#!/bin/bash

# 當任何指令失敗時，立即終止腳本
set -e

# --- 環境設定 ---
# 讓 Python 能夠找到 'services' 和 'tests' 目錄下的模組
export PYTHONPATH=$(pwd)
echo "PYTHONPATH set to: $PYTHONPATH"

# --- 執行 Pytest ---
echo "--- Running Fast Offline Tests with Pytest ---"
pytest -v tests/

echo "All tests passed successfully!"
