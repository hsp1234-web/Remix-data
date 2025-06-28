#!/bin/bash

# diagnose_nyfed.sh
# 執行 NYFedConnector 的 Python 診斷腳本，並將輸出保存到日誌檔案。

# --- 設定 ---
# set -e: 當任何命令執行失敗時立即退出腳本。
# set -u: 當使用未定義的變數時視為錯誤並退出。
# set -o pipefail: 如果管道中的任何命令失敗，則整個管道視為失敗。
set -euo pipefail

# 獲取腳本所在的目錄
# 如果此腳本位於 project_root/scripts/diagnose_nyfed.sh
# SCRIPT_DIR 會是 project_root/scripts
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# PROJECT_ROOT 應該是 SCRIPT_DIR 的父目錄 (如果此腳本在 scripts/ 下)
# 或者就是 SCRIPT_DIR (如果此腳本在專案根目錄，但我們計畫放在 scripts/ 下)
# 這裡我們假設此腳本 diagnose_nyfed.sh 將被放置在專案根目錄下的 scripts/ 子目錄中。
# 因此，專案根目錄是 SCRIPT_DIR 的上一層。
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Python 診斷腳本的路徑 (相對於 PROJECT_ROOT)
# 我們的 Python 腳本也放在 scripts/ 目錄下
PYTHON_DIAGNOSIS_SCRIPT="scripts/diagnose_nyfed_logic.py"
FULL_PYTHON_SCRIPT_PATH="${PROJECT_ROOT}/${PYTHON_DIAGNOSIS_SCRIPT}"

# 輸出日誌檔案的路徑 (相對於 PROJECT_ROOT)
REPORT_OUTPUT_DIR="${PROJECT_ROOT}/logs" # 確保 logs 目錄存在
mkdir -p "$REPORT_OUTPUT_DIR" # -p 可以避免在目錄已存在時報錯，並能創建父目錄
REPORT_FILE="${REPORT_OUTPUT_DIR}/nyfed_diagnosis_bash_report.txt" # Bash 腳本本身的報告


# --- 函數定義 ---
log_message_bash() {
    # 此函數的輸出會同時到控制台和 REPORT_FILE
    echo "$(date '+%Y-%m-%d %H:%M:%S') [BashDiagnose] - $1" | tee -a "$REPORT_FILE"
}

# --- 主腳本流程 ---

# 清理舊的 Bash 報告檔案 (如果存在)
if [ -f "$REPORT_FILE" ]; then
    rm "$REPORT_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S') [BashDiagnose] - Cleared old report file: ${REPORT_FILE}" > "$REPORT_FILE" # 新建並寫入第一行
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') [BashDiagnose] - Starting new report file: ${REPORT_FILE}" > "$REPORT_FILE"
fi

# Python 腳本會自行處理其詳細日誌檔案 (logs/nyfed_diagnosis_detail.log)
# 我們可以在這裡記錄一下該詳細日誌的路徑
PYTHON_DETAILED_LOG="${PROJECT_ROOT}/logs/nyfed_diagnosis_detail.log"


log_message_bash "開始執行 NYFed Connector 診斷..."
log_message_bash "專案根目錄 (PROJECT_ROOT) 被設定為: ${PROJECT_ROOT}"
log_message_bash "Python 診斷腳本的預期完整路徑: ${FULL_PYTHON_SCRIPT_PATH}"
log_message_bash "此 Bash 腳本的報告將輸出到: ${REPORT_FILE}"
log_message_bash "Python 腳本的詳細日誌將輸出到: ${PYTHON_DETAILED_LOG}"
log_message_bash "(如果 Python 腳本成功執行，它會自行清理舊的 ${PYTHON_DETAILED_LOG})"


# 檢查 Python 診斷腳本是否存在
if [ ! -f "$FULL_PYTHON_SCRIPT_PATH" ]; then
    log_message_bash "錯誤：Python 診斷腳本 ${FULL_PYTHON_SCRIPT_PATH} 未找到。"
    exit 1
fi

# 關於 PYTHONPATH: Python 腳本 (diagnose_nyfed_logic.py) 內部已經通過 sys.path.insert
# 處理了使其能夠找到 src 目錄下的模組。所以這裡通常不需要額外設定 PYTHONPATH。
# 如果 Python 腳本無法正確導入模組，則應首先檢查 Python 腳本內部的路徑設定邏輯。

log_message_bash "INFO: (diagnose_nyfed.sh) Bypassing pip install in Bash script. Python script will handle dependencies."
# 註解掉 Bash 層面的 pip install，因為 Python 腳本內部會處理
# echo "INFO: Force installing dependencies with verbose logging..."
# python3 -m pip install --force-reinstall --no-cache-dir -vvv requests pandas openpyxl beautifulsoup4 PyYAML
# INSTALL_EXIT_CODE=$?
# if [ $INSTALL_EXIT_CODE -ne 0 ]; then
#     log_message_bash "ERROR: pip install command failed with exit code $INSTALL_EXIT_CODE. Aborting."
#     exit $INSTALL_EXIT_CODE
# fi
# log_message_bash "INFO: Dependencies installation reported success."
INSTALL_EXIT_CODE=0 # 假設此步驟不再由 Bash 執行，因此預設為成功

log_message_bash "執行 Python 診斷腳本 (diagnose_nyfed_logic.py)..."
# 執行 Python 腳本。
# Python 腳本的 logging 設定會同時輸出到控制台和它自己的詳細日誌檔案。
# 我們這裡也捕獲 Python 腳本的 stdout 和 stderr 到 Bash 的報告檔案中，以防 Python 的日誌設定有問題。

PYTHON_SCRIPT_EXIT_CODE=0 # 初始化 Python 腳本的退出碼
# 優先使用 poetry run (如果可用且專案是 poetry 管理的)
if command -v poetry &> /dev/null && [ -f "${PROJECT_ROOT}/pyproject.toml" ]; then
    log_message_bash "檢測到 Poetry 和 pyproject.toml，將使用 'poetry run python3 ...' 執行。"
    PYTHONIOENCODING=utf-8 poetry run python3 "$FULL_PYTHON_SCRIPT_PATH" 2>&1 | tee -a "$REPORT_FILE" || PYTHON_SCRIPT_EXIT_CODE=$?
else
    log_message_bash "未檢測到 Poetry 或 pyproject.toml，將使用 'python3 ...' 直接執行。"
    PYTHONIOENCODING=utf-8 python3 "$FULL_PYTHON_SCRIPT_PATH" 2>&1 | tee -a "$REPORT_FILE" || PYTHON_SCRIPT_EXIT_CODE=$?
fi

# 決定最終的 Bash 腳本退出碼
# 如果 pip 安裝失敗，則 Bash 腳本應指示失敗。
# 否則，Bash 腳本的退出碼應反映 Python 腳本的退出碼。
FINAL_EXIT_CODE=0
if [ $INSTALL_EXIT_CODE -ne 0 ]; then
    log_message_bash "由於 pip install 步驟失敗 (返回碼: $INSTALL_EXIT_CODE)，將此診斷標記為失敗。"
    FINAL_EXIT_CODE=$INSTALL_EXIT_CODE
elif [ $PYTHON_SCRIPT_EXIT_CODE -ne 0 ]; then
    log_message_bash "Python 診斷腳本執行失敗 (返回碼: $PYTHON_SCRIPT_EXIT_CODE)。"
    FINAL_EXIT_CODE=$PYTHON_SCRIPT_EXIT_CODE
else
    log_message_bash "Python 診斷腳本執行成功完成 (返回碼 0)。"
    FINAL_EXIT_CODE=0
fi


if [ $FINAL_EXIT_CODE -ne 0 ]; then
    log_message_bash "警告：NYFed 診斷流程遇到問題 (最終返回碼: $FINAL_EXIT_CODE)。"
    log_message_bash "請檢查 ${REPORT_FILE} 的內容，以及 Python 詳細日誌 ${PYTHON_DETAILED_LOG} (如果存在) 以獲取錯誤詳情。"
fi

log_message_bash "NYFed Connector 診斷 Bash 腳本結束。"
log_message_bash "主要診斷輸出 (來自 Python 腳本的 stdout/stderr) 位於: ${REPORT_FILE}"
log_message_bash "Python 腳本更詳細的內部日誌位於: ${PYTHON_DETAILED_LOG}"
log_message_bash "如果下載了任何檔案，它們可能位於: ${PROJECT_ROOT}/temp_diagnostics/"

# 根據 Python 腳本的退出碼來決定此 Bash 腳本的退出碼
exit $EXIT_CODE
