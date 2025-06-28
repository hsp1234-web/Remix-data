#!/bin/bash

# deploy_ollama_llama3.sh
# 用於在類 Linux 環境 (如 Google Colab) 中部署 Ollama 並加載 Llama 3 模型的腳本。

# --- 配置變量 ---
# Ollama 安裝腳本 URL
OLLAMA_INSTALL_URL="https://ollama.com/install.sh"

# Google Drive 中預期存放模型的路徑 (相對於 Drive 根目錄)
# 用戶需要根據實際情況修改此路徑，或者將模型放在此預設路徑
GDRIVE_MODEL_BASE_PATH="/content/drive/MyDrive" # Colab 掛載點
GDRIVE_PROJECT_MODELS_SUBDIR="AI_Backtesting_Models" # 項目特定的模型子目錄
LLAMA3_MODEL_FILENAME="llama3-8b-instruct-q4_K_M.gguf" # 假設的 GGUF 文件名 (q4_K_M 是常見量化級別)
# 或者，如果我們直接使用 Ollama 的 manifest 文件，可能不需要 .gguf，而是 Ollama 的內部表示

# Ollama 中 Llama 3 模型的名稱 (用於 ollama pull 或 ollama run)
# Ollama Hub 上的 Llama3 8B instruct q4_K_M 可能有特定名稱，例如 llama3:8b-instruct-q4_K_M
# 或者更通用的 llama3:latest (如果已配置為拉取量化版)
# 這裡我們用一個常見的量化版本名稱，實際使用時可能需要調整
OLLAMA_MODEL_NAME="llama3:8b-instruct-q4_K_M"
# 如果我們是從 .gguf 文件創建，則創建時可以指定這個名字

# Ollama 模型存放的預期本地路徑 (Ollama 通常自己管理)
# ~/.ollama/models/manifests/registry.ollama.ai/library/{model_name}/{tag}
# ~/.ollama/models/blobs/sha256-...
# 我們主要關心的是能讓 Ollama 服務找到模型

# 腳本執行時的日誌文件 (可選)
LOG_FILE="${HOME}/ollama_deployment_log_$(date +%Y%m%d_%H%M%S).txt"
# exec > >(tee -a "${LOG_FILE}") 2>&1 # 將所有輸出同時打印到控制台和日誌文件 (在某些環境可能導致問題)

# --- 輔助函數 ---
log_message() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [OLLAMA_DEPLOY] $1"
}

# --- 主邏輯 ---
set -e # 命令失敗時立即退出

log_message "開始 Ollama 和 Llama 3 部署流程..."

# 1. 檢查並安裝 Ollama
if ! command -v ollama &> /dev/null; then
    log_message "Ollama 未安裝。正在嘗試從 ${OLLAMA_INSTALL_URL} 安裝..."
    curl -fsSL ${OLLAMA_INSTALL_URL} | sh
    if [ $? -ne 0 ]; then
        log_message "錯誤：Ollama 安裝失敗。"
        exit 1
    fi
    log_message "Ollama 安裝成功。"
    # Ollama 安裝後，可能需要將其添加到 PATH 或重新加載 shell 配置
    # 在 Colab 等環境中，安裝腳本通常會處理好，或者 ollama 命令會立即在當前會話可用
    # 如果不行，可能需要 source ~/.bashrc 或類似操作，但這在腳本中難以通用處理
    # 安裝腳本通常會提示如何使 ollama 命令可用
    log_message "Ollama 安裝腳本執行完畢。如果 'ollama' 命令仍然找不到，您可能需要重新啟動終端或 source shell 配置文件。"
else
    log_message "Ollama 已安裝。版本信息: $(ollama --version)"
fi

# 2. 啟動 Ollama 服務 (如果尚未運行)
# Ollama 通常作為一個後台服務運行。安裝後可能已啟動。
# `ollama serve` 會在前台啟動服務，除非用 & 後台化或 systemd 管理
# 簡單檢查方法：嘗試 `ollama list`，如果服務未運行它會提示
log_message "正在嘗試啟動/確認 Ollama 服務..."
if ! pgrep -x "ollama" > /dev/null; then
    log_message "Ollama 服務似乎未運行。正在嘗試在後台啟動 Ollama 服務..."
    # 在 Colab 中，直接後台運行 `ollama serve` 可能是一個簡單的方法
    # 但要注意 Colab 環境的生命週期和資源限制
    ollama serve > /tmp/ollama_serve.log 2>&1 &
    OLLAMA_PID=$!
    log_message "Ollama 服務已嘗試在後台啟動 (PID: ${OLLAMA_PID})。等待幾秒鐘讓服務初始化..."
    sleep 15 # 等待服務啟動，時間可能需要調整

    # 再次檢查進程是否存在
    if ! kill -0 ${OLLAMA_PID} 2>/dev/null || ! pgrep -x "ollama" > /dev/null; then
        log_message "錯誤：Ollama 服務未能成功在後台啟動。請檢查 /tmp/ollama_serve.log"
        log_message "您可能需要手動運行 'ollama serve' 在一個單獨的終端。"
        # exit 1 # 先不退出，讓後續模型檢查嘗試，Ollama list 會提示
    else
        log_message "Ollama 服務已在後台啟動。"
    fi
else
    log_message "Ollama 服務已在運行中。"
fi


# 3. 檢查 Llama 3 模型是否已存在於 Ollama 中
log_message "檢查模型 '${OLLAMA_MODEL_NAME}' 是否已存在於 Ollama 中..."
if ollama list | grep -q "${OLLAMA_MODEL_NAME}"; then
    log_message "模型 '${OLLAMA_MODEL_NAME}' 已存在於 Ollama 中。無需操作。"
else
    log_message "模型 '${OLLAMA_MODEL_NAME}' 不在 Ollama 本地列表中。"

    # 4. 如果本地不存在，嘗試從 Google Drive 複製模型文件 (如果提供了路徑和文件名)
    #    這一步比較複雜，因為 Ollama 通常使用 manifest 和 blob 的方式管理模型，
    #    直接複製 .gguf 文件到 Ollama 的模型目錄並不能直接讓 Ollama 識別它。
    #    正確的做法是使用 `ollama create ${OLLAMA_MODEL_NAME} -f Modelfile` 從 GGUF 創建，
    #    或者如果 Ollama 支持直接導入 GGUF (需要確認 Ollama 版本和功能)。
    #    假設我們有一個 Modelfile 指向 GGUF。

    GDRIVE_FULL_MODEL_PATH="${GDRIVE_MODEL_BASE_PATH}/${GDRIVE_PROJECT_MODELS_SUBDIR}/${LLAMA3_MODEL_FILENAME}"
    MODELS_DIR_CREATED_BY_SCRIPT=false

    if [ -n "${LLAMA3_MODEL_FILENAME}" ] && [ -f "${GDRIVE_FULL_MODEL_PATH}" ]; then
        log_message "在 Google Drive 中找到模型文件: ${GDRIVE_FULL_MODEL_PATH}"
        log_message "正在嘗試通過 Modelfile 從 GGUF 創建 Ollama 模型 '${OLLAMA_MODEL_NAME}'..."

        # 創建一個臨時的 Modelfile
        TEMP_MODELFILE_DIR=$(mktemp -d) # 創建一個臨時目錄
        MODELFILE_PATH="${TEMP_MODELFILE_DIR}/Modelfile_llama3_gdrive"

        # Modelfile 內容，FROM 指向 GGUF 文件的絕對路徑
        # 注意：Modelfile 中的 FROM 通常指向一個基礎模型或一個 .gguf 文件的相對或絕對路徑。
        # 如果是絕對路徑，Ollama 需要能訪問到它。
        cat <<EOF > "${MODELFILE_PATH}"
FROM ${GDRIVE_FULL_MODEL_PATH}
TEMPLATE "{{ .System }}<|eot_id|><|start_header_id|>user<|end_header_id|>

{{ .Prompt }}<|eot_id|><|start_header_id|>assistant<|end_header_id|>

{{ .Response }}<|eot_id|>"
# PARAMETER temperature 0.7
# PARAMETER top_k 50
# PARAMETER top_p 0.9
# (可以根據需要添加更多參數和系統提示)
EOF
        log_message "已創建臨時 Modelfile 內容如下:"
        cat "${MODELFILE_PATH}"
        log_message "--- Modelfile 結束 ---"

        log_message "執行 ollama create ${OLLAMA_MODEL_NAME} -f ${MODELFILE_PATH} ..."
        if ollama create "${OLLAMA_MODEL_NAME}" -f "${MODELFILE_PATH}"; then
            log_message "成功從 Google Drive 的 GGUF 文件創建了 Ollama 模型 '${OLLAMA_MODEL_NAME}'。"
            MODELS_DIR_CREATED_BY_SCRIPT=true # 標記模型是我們創建的
        else
            log_message "錯誤：從 Google Drive 的 GGUF 文件創建 Ollama 模型 '${OLLAMA_MODEL_NAME}' 失敗。"
            log_message "將嘗試從 Ollama Hub 拉取模型作為備選方案。"
            MODELS_DIR_CREATED_BY_SCRIPT=false # 重置標記
        fi
        rm -rf "${TEMP_MODELFILE_DIR}" # 清理臨時文件和目錄
    else
        log_message "在 Google Drive 路徑 '${GDRIVE_FULL_MODEL_PATH}' 未找到指定的模型文件，或文件名未配置。"
        log_message "將嘗試從 Ollama Hub 拉取模型。"
        MODELS_DIR_CREATED_BY_SCRIPT=false
    fi

    # 5. 如果模型仍然不存在 (例如 Drive 中沒有，或從 Drive 創建失敗)，則從 Ollama Hub 拉取
    if ! ${MODELS_DIR_CREATED_BY_SCRIPT} && ! ollama list | grep -q "${OLLAMA_MODEL_NAME}"; then
        log_message "正在從 Ollama Hub 拉取模型 '${OLLAMA_MODEL_NAME}'。這可能需要一些時間..."
        if ollama pull "${OLLAMA_MODEL_NAME}"; then
            log_message "成功從 Ollama Hub 拉取模型 '${OLLAMA_MODEL_NAME}'。"
        else
            log_message "錯誤：從 Ollama Hub 拉取模型 '${OLLAMA_MODEL_NAME}' 失敗。"
            log_message "請檢查模型名稱是否正確，以及網絡連接。也可能是 Ollama Hub 暫時不可用。"
            # exit 1 # 先不退出，讓後續驗證步驟有機會運行
        fi
    fi
fi

# 6. 最終驗證：嘗試運行模型或列出模型，確認服務和模型可用
log_message "最終驗證：列出 Ollama 中的所有模型..."
ollama list
if ollama list | grep -q "${OLLAMA_MODEL_NAME}"; then
    log_message "模型 '${OLLAMA_MODEL_NAME}' 已成功部署並在 Ollama 中可用！"
    log_message "可以嘗試用 'ollama run ${OLLAMA_MODEL_NAME} \"你好嗎?\"' 來測試交互。"
else
    log_message "警告：部署流程結束，但模型 '${OLLAMA_MODEL_NAME}' 似乎仍未在 Ollama 中完全可用。"
    log_message "請檢查之前的日誌輸出以獲取詳細信息。"
    # exit 1 # 部署未完全成功
fi

log_message "Ollama 和 Llama 3 部署流程結束。"
exit 0 # 即使有警告，也返回0，讓調用者判斷是否真的失敗
