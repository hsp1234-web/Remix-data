#!/bin/bash

# run_full_simulation.sh
# 主控 Bash 腳本，用於啟動完整的歷史回溯與 AI 決策模擬流程。

# --- 配置 ---
# 腳本執行時的日誌文件 (可選)
SIMULATION_LOG_FILE="${HOME}/main_simulation_run_$(date +%Y%m%d_%H%M%S).log"
# exec > >(tee -a "${SIMULATION_LOG_FILE}") 2>&1 # 同時輸出到控制台和日誌

# Python 解釋器
PYTHON_EXECUTABLE="python3"

# 項目根目錄 (假設此腳本位於 project_root/run_full_simulation.sh 或 project_root/scripts/run_full_simulation.sh)
# 如果此腳本在 project_root/scripts/ 下：
# SCRIPT_DIR_SIM="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PROJECT_ROOT_SIM="$(cd "${SCRIPT_DIR_SIM}/.." && pwd)"
# 如果此腳本在 project_root/ 下：
PROJECT_ROOT_SIM="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # 假設運行時，此腳本在項目根目錄

# 主 Python 模擬腳本的路徑
MAIN_SIM_PY_SCRIPT="${PROJECT_ROOT_SIM}/src/main_simulation.py"

# Ollama 部署腳本路徑 (可選，取決於是否希望此腳本管理 Ollama 部署)
# DEPLOY_OLLAMA_SCRIPT="${PROJECT_ROOT_SIM}/scripts/deploy_ollama_llama3.sh"

# 數據庫初始化腳本路徑
INIT_DB_SCRIPT="${PROJECT_ROOT_SIM}/scripts/initialize_database.sh"

# --- 輔助函數 ---
log_sim_message() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [SIM_RUNNER] $1"
}

# --- 環境檢查與準備 ---
log_sim_message "開始執行完整模擬流程..."

# 1. 檢查 Python 主模擬腳本是否存在
if [ ! -f "${MAIN_SIM_PY_SCRIPT}" ]; then
    log_sim_message "錯誤: 主 Python 模擬腳本未找到: ${MAIN_SIM_PY_SCRIPT}"
    exit 1
fi
log_sim_message "主 Python 模擬腳本找到: ${MAIN_SIM_PY_SCRIPT}"

# 2. (可選) 檢查/執行 Ollama 部署
#    為簡化，這裡假設用戶已手動運行 deploy_ollama_llama3.sh 或 Ollama 服務已在運行。
#    如果需要自動化，可以取消以下註釋：
# if [ -f "${DEPLOY_OLLAMA_SCRIPT}" ]; then
#     log_sim_message "檢測到 Ollama 部署腳本，將嘗試執行以確保 Ollama 和模型可用..."
#     # bash "${DEPLOY_OLLAMA_SCRIPT}" # 確保它有執行權限
#     # if [ $? -ne 0 ]; then
#     #     log_sim_message "警告：Ollama 部署腳本執行似乎有問題。模擬可能失敗。"
#     # fi
# else
#     log_sim_message "警告：Ollama 部署腳本 ${DEPLOY_OLLAMA_SCRIPT} 未找到。假設 Ollama 服務已手動準備就緒。"
# fi
log_sim_message "假設 Ollama 服務 (http://localhost:11434) 及所需模型已準備就緒。"
log_sim_message "如果 Ollama 未運行或模型不可用，Python 腳本中的 AI 交互部分會失敗。"

# 3. (可選) 檢查/執行數據庫初始化
#    用戶可以選擇是否在每次運行模擬前強制重新初始化數據庫。
#    默認情況下，如果數據庫已存在，initialize_database.sh (及其調用的 Python 腳本)
#    由於 schema.sql 使用 IF NOT EXISTS，不會重複創建表，但也不會清空數據。
FORCE_DB_INIT=false # 設為 true 以在每次運行前強制重新創建數據庫 (會刪除舊數據庫)

if [ -f "${INIT_DB_SCRIPT}" ]; then
    log_sim_message "檢測到數據庫初始化腳本。"
    if [ "$FORCE_DB_INIT" = true ]; then
        log_sim_message "將強制重新初始化數據庫 (舊數據將被刪除)..."
        bash "${INIT_DB_SCRIPT}" --force
    else
        log_sim_message "將執行數據庫初始化腳本 (如果表不存在則創建)..."
        bash "${INIT_DB_SCRIPT}"
    fi
    if [ $? -ne 0 ]; then
        log_sim_message "錯誤：數據庫初始化腳本執行失敗。模擬終止。"
        exit 1
    fi
    log_sim_message "數據庫初始化腳本執行完畢。"
else
    log_sim_message "警告：數據庫初始化腳本 ${INIT_DB_SCRIPT} 未找到。假設數據庫已手動準備就緒。"
fi


# 4. 設置必要的環境變量 (示例)
#    實際的 API Keys/Tokens 應該通過更安全的方式管理，例如 Colab Secrets 或 .env 文件 (需 gitignore)
#    這裡僅為演示，假設連接器會從這些標準名稱的環境變量讀取。
# export FRED_API_KEY="your_fred_api_key" # 在 FredConnector 中是 FRED_API_KEY_TEST 或 FRED_API_KEY
# export FINMIND_API_TOKEN="your_finmind_api_token" # 在 FinMindConnector 中是 FINMIND_API_TOKEN_TEST 或 FINMIND_API_TOKEN

# 檢查是否有傳遞給此 shell 腳本的額外參數，並將它們傳遞給 Python 腳本
# 例如，用戶可以運行 ./run_full_simulation.sh --start "YYYY-MM-DDTHH:MM..." --end "..." --symbol "XYZ"
# main_simulation.py 內部需要用 argparse 解析這些參數。
# 目前 main_simulation.py 的 __main__ 是硬編碼的，需要修改以接收參數。
# 為了簡單，這裡我們先不處理複雜的參數傳遞，假設 Python 腳本使用其內部的預設值。
PYTHON_SCRIPT_ARGS=""
# if [ "$#" -gt 0 ]; then
#    PYTHON_SCRIPT_ARGS="$@"
# fi

log_sim_message "所有準備工作完成。即將啟動 Python 主模擬程序..."
log_sim_message "日誌級別等配置請在 src/main_simulation.py 中調整。"
log_sim_message "模擬的起止時間和標的目前在 src/main_simulation.py 的 __main__ 部分硬編碼。"

# --- 執行主模擬 ---
# 確保 Python 腳本能夠找到其依賴的模塊
# main_simulation.py 內部已經處理了 sys.path
"${PYTHON_EXECUTABLE}" "${MAIN_SIM_PY_SCRIPT}" ${PYTHON_SCRIPT_ARGS}

SIM_EXIT_CODE=$?

if [ ${SIM_EXIT_CODE} -eq 0 ]; then
  log_sim_message "Python 主模擬程序成功執行完畢。"
else
  log_sim_message "Python 主模擬程序執行失敗，退出碼: ${SIM_EXIT_CODE}。"
  log_sim_message "請檢查日誌以獲取詳細錯誤信息。"
  exit ${SIM_EXIT_CODE}
fi

log_sim_message "完整模擬流程結束。"
exit 0
