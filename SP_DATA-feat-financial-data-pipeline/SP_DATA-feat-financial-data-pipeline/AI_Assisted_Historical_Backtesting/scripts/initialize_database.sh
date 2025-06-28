#!/bin/bash

# initialize_database.sh
# 用於初始化項目 DuckDB 數據庫的腳本。
# 它調用 python 腳本 src/scripts/init_db.py 來執行 config/schema.sql。

# 強制腳本在任何命令失敗時立即退出
set -e

# 獲取腳本所在的目錄 (scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 推斷項目根目錄 (AI_Assisted_Historical_Backtesting/)
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Python 解釋器 (假設 python3 在 PATH 中)
PYTHON_EXECUTABLE="python3"

# Python 初始化腳本的路徑
INIT_DB_PY_SCRIPT="${PROJECT_ROOT}/src/scripts/init_db.py"

# Schema SQL 文件的預設路徑 (如果 init_db.py 沒有自己的預設或需要覆蓋)
# DEFAULT_SCHEMA_FILE="${PROJECT_ROOT}/config/schema.sql"
# init_db.py 已經有預設的 schema 文件路徑，所以這裡不需要再定義，除非要覆蓋

# 數據庫文件的預設路徑 (如果 init_db.py 沒有自己的預設或需要覆蓋)
# DEFAULT_DB_PATH="${PROJECT_ROOT}/data/project_data.duckdb"
# init_db.py 也會處理預設的 db 路徑

# 檢查 Python 初始化腳本是否存在
if [ ! -f "${INIT_DB_PY_SCRIPT}" ]; then
    echo "錯誤: Python 初始化腳本未找到: ${INIT_DB_PY_SCRIPT}"
    exit 1
fi

# 腳本使用說明
print_usage() {
  echo "用法: $0 [--dbpath <database_file_path>] [--schema <schema_sql_file_path>] [--force]"
  echo ""
  echo "選項:"
  echo "  --dbpath <path>    指定數據庫文件的完整路徑。"
  echo "                     預設: ${PROJECT_ROOT}/data/${DEFAULT_DB_FILENAME:-project_data.duckdb} (由Python腳本內部決定)"
  echo "  --schema <path>    指定 Schema SQL 文件的完整路徑。"
  echo "                     預設: ${PROJECT_ROOT}/config/schema.sql (由Python腳本內部決定)"
  echo "  --force            如果數據庫文件已存在，則強制刪除並重新初始化。"
  echo "  -h, --help         顯示此幫助信息。"
  echo ""
  echo "示例:"
  echo "  $0                                           # 使用預設路徑初始化"
  echo "  $0 --dbpath /mnt/my_drive/my_project.duckdb    # 指定數據庫路徑"
  echo "  $0 --force                                   # 強制重新初始化"
}

# 解析命令行參數 (直接傳遞給 Python 腳本)
# Bash 本身不直接解析 --dbpath 等，而是將它們完整傳遞給 python 腳本
# 如果需要 Bash 做更複雜的參數解析，可以使用 getopts 或手動循環

if [[ " $* " == *" --help "* ]] || [[ " $* " == *" -h "* ]]; then
    print_usage
    exit 0
fi

echo "開始執行數據庫初始化腳本..."
echo "項目根目錄: ${PROJECT_ROOT}"
echo "將調用 Python 腳本: ${INIT_DB_PY_SCRIPT}"
echo "傳遞參數: $@" # 打印所有傳遞給此 shell 腳本的參數

# 執行 Python 初始化腳本，並將所有傳遞給 shell 腳本的參數原樣傳遞給 Python 腳本
# 確保 Python 腳本能夠找到其依賴的模塊 (init_db.py 內部已處理 sys.path)
"${PYTHON_EXECUTABLE}" "${INIT_DB_PY_SCRIPT}" "$@"

INIT_EXIT_CODE=$?

if [ ${INIT_EXIT_CODE} -eq 0 ]; then
  echo "數據庫初始化腳本成功執行。"
else
  echo "數據庫初始化腳本執行失敗，退出碼: ${INIT_EXIT_CODE}。"
  exit ${INIT_EXIT_CODE}
fi

exit 0
