#!/bin/bash
# lint_check.sh
# 一個用於快速掃描所有 Python 原始碼以檢查語法錯誤和代碼風格的腳本。

# 如果任何命令失敗，立即退出。
set -e

# --- 輔助函數 ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [LintCheck] - $1"
}

log "🚀 快速語法健檢啟動..."

# --- 1. 安裝檢查工具 ---
log "正在安裝 flake8..."
pip install flake8 > /dev/null
log "✅ flake8 安裝完畢。"

# --- 2. 執行語法檢查 ---
# 我們將對整個 data_pipeline 目錄和根目錄下的 .py 檔案進行檢查。
TARGET_DIRS="data_pipeline/ verify_db.py"
log "正在對目標 [${TARGET_DIRS}] 執行 flake8 檢查..."

# flake8 會在發現問題時返回非零退出碼。如果沒有問題，則無輸出。
if flake8 ${TARGET_DIRS}; then
    log "🎉🎉🎉 語法檢查通過！所有 Python 檔案均符合語法和基本風格要求。"
else
    log "❌ 語法檢查發現問題，請查看上面的輸出。"
    # 由於 `set -e`，如果 flake8 失敗，腳本會在此處自動停止。
    exit 1
fi

log "✅ 快速語法健檢成功完成！"
