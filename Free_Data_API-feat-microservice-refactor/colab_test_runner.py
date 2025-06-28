# -*- coding: utf-8 -*-
# @title 🚀 金融數據框架部署與測試平台 v1.0
# @markdown ### 系統介紹
# @markdown 本腳本專為在 Colab 環境中快速部署、測試及驗證 `financial_data_framework` 專案而設計。
# @markdown - **一鍵部署**: 從您的 GitHub 儲存庫精準拉取指定分支的最新程式碼。
# @markdown - **自動化測試**: 自動安裝依賴、執行核心數據管道，並對結果進行初步驗證。
# @markdown - **即時監控**: 監控並報告 CPU、RAM 及磁碟使用狀況。
# @markdown - **詳細日誌**: 生成完整的執行日誌，方便追蹤與除錯。
# @markdown ---
# @markdown ### 使用說明
# @markdown 1.  **設定參數**: 在下方填寫您的 GitHub 儲存庫 URL 和要部署的分支名稱。
# @markdown 2.  **執行儲存格**: 點擊左側的播放按鈕或使用 `Ctrl+Enter` / `Cmd+Enter`。
# @markdown 3.  **檢視結果**: 觀察下方的輸出日誌，了解每個步驟的執行情況和最終的驗證結果。

# ==============================================================================
# @markdown ### 步驟 1: 🛠️ 設定部署參數
# @markdown 請在下方輸入您的 GitHub 資訊。
# ==============================================================================
# --- Git 部署相關設定 ---
# !!! 重要：請將 GITHUB_REPO_URL 替換成您實際的儲存庫 URL !!!
# 例如： "https://github.com/YOUR_USERNAME/financial_data_framework.git"
GITHUB_REPO_URL = "https://github.com/YOUR_USERNAME/YOUR_REPOSITORY_NAME.git"  # @param {type:"string"}
# !!! 重要：請將 BRANCH_NAME 替換成您要測試的分支名稱 !!!
# 例如："main" 或 "feat/financial-data-framework-init"
BRANCH_NAME = "main"  # @param {type:"string"}
LOCAL_CLONE_PATH = "/content/financial_data_framework_deployment"  # @param {type:"string"}

# --- 測試執行參數 ---
# 用於 `run_pipeline_in_colab.py` 的參數
TEST_SYMBOLS = "AAPL,GOOG"  # @param {type:"string"}
TEST_START_DATE = "2024-01-01" # @param {type:"string"}
TEST_END_DATE = "2024-01-15"   # @param {type:"string"}


# ==============================================================================
# @markdown ### 步驟 2: ▶️ 點擊執行此儲存格 (如果您是直接貼到Colab)
# @markdown 或者，如果您是通過 `!python colab_test_runner.py` 執行，請確保已在此文件頂部修改參數。
# ==============================================================================

# --- 核心函式庫導入與自動安裝 ---
import os
import sys
import subprocess
import shutil
import io
import time
import threading
import logging
from datetime import datetime
from typing import Optional

try:
    import psutil
except ImportError:
    print("正在安裝 psutil...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "psutil"])
    import psutil
try:
    import pytz
except ImportError:
    print("正在安裝 pytz...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pytz"])
    import pytz
try:
    import duckdb
except ImportError:
    print("正在安裝 duckdb...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "duckdb"])
    import duckdb

try:
    from google.colab import files
    from IPython.display import display, HTML, Javascript
    IS_COLAB = True
except ImportError:
    IS_COLAB = False
    display = lambda x: print(str(x))
    HTML = lambda x: str(x) # In non-Colab, HTML will just be a string
    class MockFiles:
        def download(self, path): print(f"下載模擬: 請手動從 {path} 獲取檔案。")
    files = MockFiles()

# --- 全域設定與輔助函式 ---
TAIPEI_TZ = pytz.timezone('Asia/Taipei')
LOG_STREAM = io.StringIO()

def get_taipei_time_str(ts=None) -> str:
    dt = datetime.fromtimestamp(ts) if ts else datetime.now()
    return dt.astimezone(TAIPEI_TZ).strftime('%Y-%m-%d %H:%M:%S')

# --- 雙軌制日誌系統 ---
class ColabLogger:
    def __init__(self, log_stream: io.StringIO):
        self.tz = TAIPEI_TZ
        self.log_stream = log_stream
        self.lock = threading.Lock()
        self.html_logs_for_display = [] # Only for final display if needed

        # Configure standard logging module
        logging.basicConfig(
            stream=self.log_stream,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(name)s:%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        root_logger = logging.getLogger()
        # Clear existing handlers from Colab's default setup if any, to avoid duplicate console output
        if IS_COLAB:
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

        # Add our stream handler for StringIO
        stream_handler_stringio = logging.StreamHandler(self.log_stream)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(filename)s:%(lineno)d] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        stream_handler_stringio.setFormatter(formatter)
        root_logger.addHandler(stream_handler_stringio)

        # Add a handler for console output (sys.stdout)
        # This ensures logs appear in console when run as script AND in Colab cell output
        stream_handler_stdout = logging.StreamHandler(sys.stdout)
        stream_handler_stdout.setFormatter(formatter)
        root_logger.addHandler(stream_handler_stdout)

        root_logger.setLevel(logging.INFO)
        self.logger = root_logger # Use the configured root logger

    def _get_timestamp(self) -> str: # Not directly used by self.logger but kept for direct calls if any
        return datetime.now(self.tz).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Methods to directly use the configured logger
    def info(self, msg: str): self.logger.info(f"⚪ {msg}")
    def success(self, msg: str): self.logger.info(f"✅ {msg}") # Using info level for success too, prefix indicates type
    def warning(self, msg: str): self.logger.warning(f"⚠️ {msg}")
    def error(self, msg: str): self.logger.error(f"❌ {msg}")
    def header(self, msg: str):
        plain_header = f"\n{'='*80}\n=== {msg.strip()} ===\n{'='*80}\n"
        self.logger.info(plain_header) # Log header as info
        if IS_COLAB: # Also display HTML header in Colab
             display(HTML(f'<h3 style="color:black; border-bottom:2px solid #64B5F6; padding-bottom:5px; font-family:sans-serif; margin-top:1em;">{msg}</h3>'))


    def hw_log(self, msg: str): self.logger.info(f"⏱️ {msg}") # Hardware logs as info

    def get_plain_logs(self) -> str:
        return self.log_stream.getvalue()

# --- 硬體管理與監控 ---
class HardwareMonitor:
    def __init__(self, logger: ColabLogger):
        self.logger = logger
        self.stop_event = threading.Event()
        self.thread = None
        try:
            self.total_ram_gb = psutil.virtual_memory().total / (1024**3)
        except Exception as e:
            self.logger.warning(f"無法獲取總 RAM 大小: {e}")
            self.total_ram_gb = 0


    def _monitor(self):
        while not self.stop_event.is_set():
            try:
                cpu_percent = psutil.cpu_percent()
                ram = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                status_line = (f"CPU: {cpu_percent:.1f}% | "
                               f"RAM: {ram.percent:.1f}% ({ram.used/(1024**3):.2f}/{self.total_ram_gb:.2f} GB) | "
                               f"Disk: {disk.percent:.1f}%")
                self.logger.hw_log(status_line)
            except Exception as e:
                self.logger.warning(f"硬體監控錯誤: {e}")
            time.sleep(10) # 每 10 秒記錄一次

    def start(self):
        self.logger.info("啟動硬體監控執行緒...")
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()

    def stop(self):
        if self.thread and self.thread.is_alive():
            self.logger.info("停止硬體監控執行緒...")
            self.stop_event.set()
            self.thread.join(timeout=3)

# --- 主執行流程 ---
def run_deployment_and_test():
    start_time = time.time()
    logger = ColabLogger(LOG_STREAM) # Logger now prints to stdout and captures to StringIO
    hw_monitor = HardwareMonitor(logger)

    if IS_COLAB:
        from IPython.display import clear_output
        clear_output(wait=True)

    logger.header("🚀 金融數據框架部署與測試平台 v1.0 啟動")
    hw_monitor.start()

    logger.info(f"腳本執行於: {sys.executable}")
    logger.info(f"目前工作目錄: {os.getcwd()}")

    # === 階段一: Git 專案部署 ===
    logger.header("階段一: Git 專案部署")
    logger.info(f"目標儲存庫: {GITHUB_REPO_URL}")
    logger.info(f"目標分支: {BRANCH_NAME}")
    logger.info(f"本地路徑: {LOCAL_CLONE_PATH}")

    if "YOUR_USERNAME" in GITHUB_REPO_URL or "YOUR_REPOSITORY_NAME" in GITHUB_REPO_URL :
        logger.error("重要：請先在此腳本頂部修改 GITHUB_REPO_URL 和 BRANCH_NAME 參數再執行！")
        hw_monitor.stop()
        return logger

    if os.path.exists(LOCAL_CLONE_PATH):
        logger.info(f"偵測到舊目錄，正在清理 '{LOCAL_CLONE_PATH}'...")
        try:
            shutil.rmtree(LOCAL_CLONE_PATH)
            logger.success(f"舊目錄 {LOCAL_CLONE_PATH} 清理完畢。")
        except Exception as e:
            logger.error(f"清理舊目錄 {LOCAL_CLONE_PATH} 失敗: {e}")
            hw_monitor.stop()
            return logger

    git_command = ["git", "clone", "--branch", BRANCH_NAME, "--single-branch", GITHUB_REPO_URL, LOCAL_CLONE_PATH]
    logger.info(f"執行 Git 指令: {' '.join(git_command)}")
    try:
        # Using subprocess.run for simpler stdout/stderr handling
        git_result = subprocess.run(git_command, capture_output=True, text=True, encoding='utf-8', timeout=120)

        if git_result.stdout:
            for line in git_result.stdout.splitlines(): logger.info(f"[Git STDOUT] {line}")
        if git_result.stderr: # Git clone often uses stderr for progress messages
            for line in git_result.stderr.splitlines(): logger.info(f"[Git STDERR] {line}") # Log as info

        if git_result.returncode != 0:
            logger.error(f"❌ Git clone 失敗！Return code: {git_result.returncode}")
            if git_result.stderr: logger.error(f"Git 錯誤訊息摘要: {git_result.stderr.strip().splitlines()[-3:]}")
            hw_monitor.stop()
            return logger
        else:
            logger.success("✅ 專案分支成功拉取至本地！")
            expected_config_path = os.path.join(LOCAL_CLONE_PATH, 'config.yaml')
            if not os.path.exists(expected_config_path):
                logger.warning(f"警告：config.yaml 未在 {expected_config_path} 找到。")
            else:
                logger.info(f"找到 config.yaml 於 {expected_config_path}")

    except subprocess.TimeoutExpired:
        logger.error("Git clone 超時！")
        hw_monitor.stop()
        return logger
    except Exception as e:
        logger.error(f"Git clone 過程中發生未知錯誤: {e}")
        hw_monitor.stop()
        return logger

    # === 階段二: 安裝依賴 ===
    logger.header("階段二: 安裝專案依賴")
    requirements_path = os.path.join(LOCAL_CLONE_PATH, "requirements.txt")
    if not os.path.exists(requirements_path):
        logger.error(f"❌ 找不到 requirements.txt 於 {requirements_path}。無法安裝依賴。")
        hw_monitor.stop()
        return logger

    pip_command = [sys.executable, "-m", "pip", "install", "-r", requirements_path, "-q"]
    logger.info(f"執行 Pip 指令: {' '.join(pip_command)}")
    try:
        pip_result = subprocess.run(pip_command, capture_output=True, text=True, encoding='utf-8', timeout=300)

        if pip_result.stdout: # Some info might still go to stdout with -q
            for line in pip_result.stdout.splitlines(): logger.info(f"[Pip STDOUT] {line}")
        if pip_result.stderr: # Errors will be in stderr
            for line in pip_result.stderr.splitlines(): logger.error(f"[Pip STDERR] {line}")

        if pip_result.returncode != 0:
            logger.error(f"❌ Pip install 失敗！Return code: {pip_result.returncode}")
            hw_monitor.stop()
            return logger
        else:
            logger.success("✅ 專案依賴安裝成功！")
    except subprocess.TimeoutExpired:
        logger.error("Pip install 超時！")
        hw_monitor.stop()
        return logger
    except Exception as e:
        logger.error(f"Pip install 過程中發生未知錯誤: {e}")
        hw_monitor.stop()
        return logger

    # === 階段三: 執行數據管道測試 ===
    logger.header("階段三: 執行數據管道測試")
    run_script_path = os.path.join(LOCAL_CLONE_PATH, "run_pipeline_in_colab.py") # This is the script from the cloned repo

    if not os.path.exists(run_script_path):
        logger.error(f"❌ 找不到執行腳本 run_pipeline_in_colab.py 於 {run_script_path}。")
        hw_monitor.stop()
        return logger

    db_file_name = "data_hub.duckdb"
    expected_db_path = os.path.join(LOCAL_CLONE_PATH, db_file_name)
    logger.info(f"預期 DuckDB 數據庫檔案將位於: {expected_db_path}")
    if os.path.exists(expected_db_path):
        logger.info(f"偵測到舊的數據庫檔案 {expected_db_path}，將其刪除。")
        try:
            os.remove(expected_db_path)
            logger.success(f"舊數據庫檔案 {expected_db_path} 已刪除。")
        except Exception as e:
            logger.warning(f"刪除舊數據庫檔案 {expected_db_path} 失敗: {e}")

    python_command = [
        sys.executable,
        os.path.basename(run_script_path), # run_pipeline_in_colab.py
        "--task", "fetch",
        "--symbols", TEST_SYMBOLS,
        "--start_date", TEST_START_DATE,
        "--end_date", TEST_END_DATE,
        "--config", "config.yaml"
    ]
    logger.info(f"在目錄 {LOCAL_CLONE_PATH} 中執行 Python 指令: {' '.join(python_command)}")
    try:
        # Execute the target script within its own directory context
        pipeline_result = subprocess.run(python_command, capture_output=True, text=True, encoding='utf-8', timeout=300, cwd=LOCAL_CLONE_PATH)

        # The script's own logging (to stdout/stderr) will be captured here
        if pipeline_result.stdout:
            for line in pipeline_result.stdout.splitlines(): logger.info(f"[Pipeline STDOUT] {line}")
        if pipeline_result.stderr:
            for line in pipeline_result.stderr.splitlines(): logger.error(f"[Pipeline STDERR] {line}")

        if pipeline_result.returncode != 0:
            logger.error(f"❌ 數據管道執行失敗！Return code: {pipeline_result.returncode}")
            hw_monitor.stop()
            return logger
        else:
            logger.success("✅ 數據管道執行成功！")
    except subprocess.TimeoutExpired:
        logger.error("數據管道執行超時！")
        hw_monitor.stop()
        return logger
    except Exception as e:
        logger.error(f"數據管道執行過程中發生未知錯誤: {e}")
        hw_monitor.stop()
        return logger

    # === 階段四: 數據驗證 ===
    logger.header("階段四: 數據驗證")
    if not os.path.exists(expected_db_path):
        logger.error(f"❌ 驗證失敗：預期的數據庫檔案 {expected_db_path} 未找到！")
        hw_monitor.stop()
        return logger

    logger.info(f"嘗試連接到 DuckDB 數據庫: {expected_db_path}")
    try:
        con = duckdb.connect(database=expected_db_path, read_only=True)
        logger.success("✅ 成功連接到 DuckDB 數據庫。")

        table_check = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ohlcv_daily';").fetchone()
        if not table_check:
            logger.error("❌ 驗證失敗：'ohlcv_daily' 表在數據庫中不存在！")
            con.close()
            hw_monitor.stop()
            return logger
        logger.info("'ohlcv_daily' 表存在。")

        symbols_to_check = [s.strip().upper() for s in TEST_SYMBOLS.split(',')]
        all_symbols_data_found = True
        for symbol in symbols_to_check:
            logger.info(f"查詢股票 {symbol} 的數據...")
            count_result = con.execute(f"SELECT COUNT(*) FROM ohlcv_daily WHERE symbol = ?;", [symbol]).fetchone()
            if count_result and count_result[0] > 0:
                logger.success(f"✅ 找到 {symbol} 的 {count_result[0]} 筆數據。")
                sample_data = con.execute(f"SELECT * FROM ohlcv_daily WHERE symbol = ? ORDER BY date DESC LIMIT 3;", [symbol]).df()
                logger.info(f"  {symbol} 最新 3 筆數據範例:\n{sample_data.to_string()}")
            else:
                logger.warning(f"⚠️ 未找到股票 {symbol} 的數據，或數據為空。")
                all_symbols_data_found = False

        if all_symbols_data_found:
             logger.success("✅ 所有測試股票的數據均已成功載入並初步驗證！")
        else:
             logger.warning("⚠️ 部分或全部測試股票的數據未找到或為空。請檢查管道日誌。")

        con.close()
        logger.info("DuckDB 連接已關閉。")

    except Exception as e:
        logger.error(f"❌ 數據驗證過程中發生錯誤: {e}")
        hw_monitor.stop()
        return logger

    # === 收尾工作 ===
    hw_monitor.stop()
    duration = time.time() - start_time
    logger.header(f"🏁 全部流程在 {duration:.2f} 秒內執行完畢")

    log_filename_prefix = f"financial_framework_test_{datetime.now(TAIPEI_TZ).strftime('%Y%m%d_%H%M%S')}"
    plain_log_filename = f"{log_filename_prefix}.log"

    # Save the captured log stream to file
    # (The logger already printed to stdout during execution)
    try:
        with open(plain_log_filename, "w", encoding="utf-8") as f:
            f.write(LOG_STREAM.getvalue()) # Use LOG_STREAM which captured all logs
        logger.info(f"完整的純文字日誌已保存至: {plain_log_filename}")

        if IS_COLAB:
            print(f"\n若需下載純文字日誌檔案，請在下一個儲存格執行以下指令：")
            print(f"from google.colab import files; files.download('{plain_log_filename}')")
        else:
            print(f"純文字日誌位於: {plain_log_filename}")
    except Exception as e:
        logger.error(f"保存日誌檔案時出錯: {e}")


    return logger

# --- 執行主函數 ---
if __name__ == '__main__':
    # This allows the script to be run directly for testing
    # In Colab, pasting this into a cell and running it will execute run_deployment_and_test()
    final_logger = run_deployment_and_test()
    # The logger instance itself is returned, mainly for potential programmatic use if imported.
    # All visual output is handled within run_deployment_and_test via prints and display()
    print("\n--- 腳本執行完畢 ---")
    if GITHUB_REPO_URL == "https://github.com/YOUR_USERNAME/YOUR_REPOSITORY_NAME.git":
         print("提醒：您似乎使用的是預設的 GITHUB_REPO_URL。請修改腳本頂部的此參數以指向您的儲存庫。")

```
