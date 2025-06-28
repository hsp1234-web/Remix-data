#!/bin/bash
set -e

echo "===== [1/3] 正在根據完整的 requirements.txt 安裝依賴... ====="
pip install -r panoramic-market-analyzer/requirements.txt

echo "===== [2/3] 正在設定 PYTHONPATH 以解決模組導入問題... ====="
export PYTHONPATH=$(pwd)/panoramic-market-analyzer

echo "===== [3/3] 正在以模組模式執行模擬測試... ====="
python -m panoramic-market-analyzer.data_pipeline.test_commander_mocked

echo "===== ✅ 測試規格書執行成功！ ====="
