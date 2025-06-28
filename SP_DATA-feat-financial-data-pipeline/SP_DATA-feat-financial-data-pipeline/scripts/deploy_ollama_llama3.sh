#!/bin/bash

# deploy_ollama_llama3.sh
# 此腳本用於在 Linux 環境 (特別是 Google Colab) 中安裝 Ollama 並拉取 Llama 3 模型。

# --- 設定 ---
# set -e: 當任何命令執行失敗時立即退出腳本。
# set -u: 當使用未定義的變數時視為錯誤並退出。
# set -o pipefail: 如果管道中的任何命令失敗，則整個管道視為失敗。
set -euo pipefail

# Ollama 模型名稱和標籤 (Llama 3 8B instruction-tuned)
# llama3 或 llama3:8b 通常會拉取最新的 8B instruct 模型。
# 可以根據 Ollama Hub (ollama.com/library/llama3) 的最新資訊調整。
OLLAMA_MODEL_NAME="llama3:8b"
# OLLAMA_MODEL_NAME="llama3:latest" # 或者使用 latest，通常也是 8B instruct
# OLLAMA_MODEL_NAME="llama3:8b-instruct-q4_K_M" # 一個更明確的4位元量化版本範例，如果需要特定量化

# Ollama 服務檢查參數
MAX_RETRIES=20       # 最大重試次數
RETRY_INTERVAL=5     # 每次重試間隔秒數
OLLAMA_API_URL="http://localhost:11434"

# --- 函數定義 ---
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# --- 主腳本流程 ---

log_message "開始 Ollama 與 Llama 3 部署流程..."

# 1. 檢查是否已安裝 Ollama
if command -v ollama &> /dev/null; then
    log_message "Ollama 已安裝。版本資訊："
    ollama --version
else
    log_message "Ollama 未安裝。開始安裝 Ollama..."
    # 使用官方安裝腳本
    curl -fsSL https://ollama.com/install.sh | sh
    if [ $? -ne 0 ]; then
        log_message "錯誤：Ollama 安裝失敗。"
        exit 1
    fi
    log_message "Ollama 安裝成功。版本資訊："
    ollama --version
fi

# 2. 檢查 Ollama 服務是否已在運行
# 如果 ollama serve 已經在運行 (例如，由 Colab 環境自動啟動或之前手動啟動)，
# 我們可能不需要再次啟動它，或者需要先停止現有的。
# 為了簡化，此腳本假設如果服務不在運行，則啟動它。
# 在 Colab 中，ollama serve 可能在安裝後自動運行，或者需要手動啟動。
# `pgrep -f "ollama serve"` 可以檢查進程是否存在。
if pgrep -f "ollama serve" > /dev/null; then
    log_message "Ollama 服務已在運行中。"
else
    log_message "啟動 Ollama 服務 (背景模式)..."
    # 使用 nohup 使其在背景運行，並將輸出重定向到日誌檔案
    # 在 Colab 中，這有助於在儲存格執行完畢後保持服務運行
    nohup ollama serve > ollama_server.log 2>&1 &
    OLLAMA_PID=$! # 獲取背景進程的 PID
    log_message "Ollama 服務已以 PID $OLLAMA_PID 在背景啟動。日誌輸出到 ollama_server.log。"
    # 給服務一點時間啟動
    sleep 5
fi

# 3. 增強服務啟動驗證
log_message "等待 Ollama 服務就緒 (最多等待 $((MAX_RETRIES * RETRY_INTERVAL)) 秒)..."
RETRY_COUNT=0
SERVICE_READY=false
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    # curl -sS --fail 檢查 URL。如果成功 (HTTP 200-299)，返回 0。失敗則返回非零。
    if curl --silent --show-error --fail "${OLLAMA_API_URL}" > /dev/null 2>&1; then
        log_message "Ollama 服務已就緒並在 ${OLLAMA_API_URL} 上回應。"
        SERVICE_READY=true
        break
    else
        log_message "Ollama 服務尚未就緒 (嘗試 $((RETRY_COUNT + 1))/${MAX_RETRIES})... 等待 ${RETRY_INTERVAL} 秒。"
        sleep $RETRY_INTERVAL
        RETRY_COUNT=$((RETRY_COUNT + 1))
    fi
done

if [ "$SERVICE_READY" = false ]; then
    log_message "錯誤：Ollama 服務在等待超時後仍未就緒。請檢查 ollama_server.log 以獲取更多資訊。"
    # 如果 Ollama 是由這個腳本啟動的，可以考慮 kill 它
    if [ -n "${OLLAMA_PID-}" ]; then # 檢查 OLLAMA_PID 是否已設定且非空
        log_message "嘗試停止由本腳本啟動的 Ollama 服務 (PID: $OLLAMA_PID)..."
        kill "$OLLAMA_PID" || log_message "警告：停止 Ollama 服務 (PID: $OLLAMA_PID) 失敗。"
    fi
    exit 1
fi

# 4. 拉取 Llama 3 模型
log_message "開始拉取 Llama 3 模型: ${OLLAMA_MODEL_NAME}..."
# ollama pull 的輸出會顯示進度
ollama pull "${OLLAMA_MODEL_NAME}"
if [ $? -ne 0 ]; then
    log_message "錯誤：拉取模型 ${OLLAMA_MODEL_NAME} 失敗。"
    # 這裡可以考慮是否需要停止 Ollama 服務
    exit 1
fi
log_message "模型 ${OLLAMA_MODEL_NAME} 拉取成功。"

# 5. 驗證模型下載
log_message "目前已下載的 Ollama 模型列表："
ollama list

log_message "Ollama 與 Llama 3 部署流程完成。"
log_message "Ollama 服務應在背景運行。如果是由此腳本啟動，PID 為 ${OLLAMA_PID:-'未知 (可能先前已運行)'}。"
log_message "您現在可以使用 'ollama run ${OLLAMA_MODEL_NAME}' 或透過 API 與模型互動。"
log_message "要查看 Ollama 服務日誌，請執行: tail -f ollama_server.log (如果是由此腳本啟動)"
log_message "要手動停止由此腳本啟動的 Ollama 服務，請執行: kill ${OLLAMA_PID:-'<OLLAMA_PID>'} (如果 PID 已知)"

exit 0
