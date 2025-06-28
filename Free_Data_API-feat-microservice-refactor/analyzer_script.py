# -*- coding: utf-8 -*-
# @title 🚀 全景市場分析儀 v1.6 - 強制重組與測試平台
# @markdown ### 系統介紹
# @markdown 本腳本是為「全景市場分析儀」量身定做的**本地化**自動部署與測試平台。它專注於在不依賴任何外部儲存（如 Google Drive）的情況下，快速驗證核心程式碼的邏輯正確性。
# @markdown - **本地優先**: 所有數據庫與快取檔案均在 Colab 的高速本地臨時目錄 `/content/temp_data` 中生成與讀寫。
# @markdown - **一鍵執行**: 自動完成部署、安裝、執行與驗證的全過程。
# @markdown - **精準部署**: 已根據您的指示，預設部署您指定的 **`feat/initial-project-structure`** 分支。
# @markdown ---
# @markdown ### v1.6 開發日誌 (強制重組)
# @markdown - **新增「強制重組」步驟**: 在 `git clone` 之後，腳本會自動檢測「雙層包裹」結構，並將內層專案的所有內容移動到頂層，從根本上解決 `ModuleNotFoundError`。
# @markdown - **適應性增強**: 此腳本現在能夠處理您當前分支的混亂結構。

# ==============================================================================
# @markdown ### 步驟 1: 🎯 確認部署目標 (參數已為您設定)
# ==============================================================================
# --- Git 部署相關設定 ---
GITHUB_REPO_URL = "https://github.com/hsp1234-web/Free_Data_API.git" #@param {type:"string"}
BRANCH_NAME = "feat/initial-project-structure" #@param {type:"string"}
# 修改路徑到 /tmp
OUTER_CLONE_PATH = "/tmp/panoramic-market-analyzer" #@param {type:"string"}


# --- 數據管道測試參數 ---
TEST_SYMBOLS = "SPY,TLT,GLD,BTC-USD"  # @param {type:"string"}
TEST_START_DATE = "2023-01-01" # @param {type:"string"}
TEST_END_DATE = "2023-12-31"   # @param {type:"string"}

# --- 本地臨時數據路徑設定 ---
# 修改路徑到 /tmp
LOCAL_DATA_PATH = "/tmp/temp_data" #@param {type:"string"}

# ==============================================================================
# @markdown ### 步驟 2: ▶️ 點擊執行此儲存格
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

# --- 依賴安裝 ---
try:
    import psutil
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "psutil"])
    import psutil
try:
    import pytz
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pytz"])
    import pytz
try:
    import duckdb
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "duckdb"])
    import duckdb

try:
    from google.colab import drive, files
    from IPython.display import display, HTML
    IS_COLAB = True
except ImportError:
    IS_COLAB = False
    display, HTML = lambda x: print(str(x)), lambda x: x # type: ignore
    class MockFiles:
        def download(self, path): print(f"下載模擬: 請手動從 {path} 獲取檔案。")
    files = MockFiles() # type: ignore

# --- 全域設定與日誌系統 ---
TAIPEI_TZ = pytz.timezone('Asia/Taipei')
LOG_STREAM = io.StringIO()

class ColabLogger:
    def __init__(self, log_stream: io.StringIO):
        self.log_stream = log_stream
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.StreamHandler(self.log_stream)
            ]
        )
        self.logger = logging.getLogger("PlatformLogger")

    def info(self, msg, **kwargs): self.logger.info(f"⚪ {msg}", **kwargs)
    def success(self, msg, **kwargs): self.logger.info(f"✅ {msg}", **kwargs)
    def warning(self, msg, **kwargs): self.logger.warning(f"⚠️ {msg}", **kwargs)
    def error(self, msg, **kwargs): self.logger.error(f"❌ {msg}", **kwargs)
    def header(self, msg):
        plain_header = f"\n{'='*80}\n=== {msg.strip()} ===\n{'='*80}"
        self.logger.info(plain_header)
        if IS_COLAB:
            display(HTML(f'<h3 style="color:black; border-bottom:2px solid #64B5F6; padding-bottom:5px; font-family:sans-serif; margin-top:1em;">{msg}</h3>'))

    def hw_log(self, msg): self.logger.info(f"⏱️ {msg}")
    def get_full_log(self): return self.log_stream.getvalue()

# --- 硬體監控 ---
class HardwareMonitor:
    def __init__(self, logger: ColabLogger):
        self.logger = logger
        self.stop_event = threading.Event()
        self.thread = None
        self.total_ram_gb = psutil.virtual_memory().total / (1024**3)

    def _monitor(self):
        while not self.stop_event.is_set():
            cpu_percent = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            status = f"CPU: {cpu_percent:.1f}% | RAM: {ram.percent:.1f}% ({ram.used/(1024**3):.2f}/{self.total_ram_gb:.2f} GB) | Disk: {disk.percent:.1f}%"
            self.logger.hw_log(status)
            time.sleep(15)

    def start(self):
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=3)

# --- 主執行流程 ---
def run_deployment_and_test():
    start_time = time.time()
    logger = ColabLogger(LOG_STREAM)
    hw_monitor = HardwareMonitor(logger)

    if IS_COLAB:
        from IPython.display import clear_output
        clear_output(wait=True)

    logger.header("🚀 全景市場分析儀 v1.6 - 強制重組與測試平台啟動")
    hw_monitor.start()

    # === 階段零: 環境準備 (本地模式) ===
    logger.header("階段零: 環境準備 (本地模式)")
    logger.warning("Google Drive 未掛載。所有數據將寫入 Colab 本地臨時目錄，會話結束後將被清除。")
    if not os.path.exists(LOCAL_DATA_PATH):
        os.makedirs(LOCAL_DATA_PATH)
        logger.info(f"已創建本地臨時數據目錄: {LOCAL_DATA_PATH}")
    else:
        logger.info(f"本地臨時數據目錄已存在: {LOCAL_DATA_PATH}")


    # === 階段一: Git 專案部署 ===
    logger.header("階段一: Git 專案部署")
    # 修改基礎部署目錄到 /tmp
    base_deployment_path = "/tmp"
    os.chdir(base_deployment_path)
    logger.info(f"切換到基礎部署目錄: {base_deployment_path}")

    if os.path.exists(OUTER_CLONE_PATH):
        logger.info(f"偵測到舊目錄，正在清理 '{OUTER_CLONE_PATH}'...")
        shutil.rmtree(OUTER_CLONE_PATH)

    git_command = ["git", "clone", "--branch", BRANCH_NAME, "--single-branch", GITHUB_REPO_URL, OUTER_CLONE_PATH]
    logger.info(f"執行指令: {' '.join(git_command)}")
    git_result = subprocess.run(git_command, capture_output=True, text=True, encoding='utf-8')

    if git_result.returncode != 0:
        logger.error(f"❌ Git clone 失敗！\n{git_result.stderr.strip()}", exc_info=False)
        hw_monitor.stop()
        return
    else:
        logger.success("✅ 專案分支成功拉取至本地！")

    # === 階段二: 強制重組與路徑設置 ===
    logger.header("階段二: 強制重組與路徑設置")

    inner_project_path = os.path.join(OUTER_CLONE_PATH, "panoramic-market-analyzer")

    if os.path.isdir(inner_project_path):
        logger.warning(f"偵測到雙層包裹結構！內層路徑: {inner_project_path}。正在執行強制重組...")
        items_to_move = os.listdir(inner_project_path)
        logger.info(f"準備從 '{inner_project_path}' 移動以下項目到 '{OUTER_CLONE_PATH}': {items_to_move}")

        all_moved_successfully = True
        for item in items_to_move:
            source_item_path = os.path.join(inner_project_path, item)
            destination_item_path_final = os.path.join(OUTER_CLONE_PATH, item)

            logger.info(f"準備移動 '{source_item_path}' -> '{destination_item_path_final}'")

            try:
                if item == "data_pipeline" and os.path.isdir(destination_item_path_final):
                    logger.warning(f"目標目錄 '{destination_item_path_final}' 已存在。正在刪除以確保正確覆蓋...")
                    shutil.rmtree(destination_item_path_final)
                    logger.success(f"✅ 已刪除舊的 '{destination_item_path_final}'。")

                shutil.move(source_item_path, destination_item_path_final)
                logger.info(f"✅ 成功移動 '{source_item_path}' -> '{destination_item_path_final}'")
            except Exception as e:
                logger.error(f"❌ 移動 '{source_item_path}' 到 '{destination_item_path_final}' 失敗: {e}", exc_info=True)
                all_moved_successfully = False

        if all_moved_successfully:
            logger.success(f"✅ 所有項目已成功從 '{inner_project_path}' 移動到 '{OUTER_CLONE_PATH}'。")
            try:
                os.rmdir(inner_project_path)
                logger.success(f"✅ 空的內層資料夾 '{inner_project_path}' 已成功移除。")
            except Exception as e:
                logger.error(f"❌ 移除空的內層資料夾 '{inner_project_path}' 失敗: {e}", exc_info=True)
        else:
            logger.error("❌ 強制重組過程中部分項目移動失敗。請檢查以上日誌。")

        expected_config_path = os.path.join(OUTER_CLONE_PATH, 'config.yaml')
        expected_commander_path = os.path.join(OUTER_CLONE_PATH, 'data_pipeline', 'commander.py')

        if os.path.exists(expected_config_path):
            logger.success(f"✅ 驗證成功: 關鍵文件 'config.yaml' 存在於 '{OUTER_CLONE_PATH}'。")
        else:
            logger.error(f"❌ 驗證失敗: 關鍵文件 'config.yaml' 未在 '{OUTER_CLONE_PATH}' 中找到。")

        if os.path.exists(expected_commander_path):
            logger.success(f"✅ 驗證成功: 關鍵模組 'data_pipeline/commander.py' 存在於 '{OUTER_CLONE_PATH}'。")
        else:
            logger.error(f"❌ 驗證失敗: 關鍵模組 'data_pipeline/commander.py' 未在 '{OUTER_CLONE_PATH}' 中找到。")
            logger.info(f"請檢查 '{os.path.join(OUTER_CLONE_PATH, 'data_pipeline')}' 目錄的內容。")
            if os.path.exists(os.path.join(OUTER_CLONE_PATH, 'data_pipeline')):
                 logger.info(f"'{os.path.join(OUTER_CLONE_PATH, 'data_pipeline')}' 目錄內容: {os.listdir(os.path.join(OUTER_CLONE_PATH, 'data_pipeline'))}")
            else:
                logger.warning(f"'{os.path.join(OUTER_CLONE_PATH, 'data_pipeline')}' 目錄不存在。")
    else:
        logger.info(f"未偵測到雙層包裹結構於 '{inner_project_path}'。跳過強制重組。")
        expected_commander_path = os.path.join(OUTER_CLONE_PATH, 'data_pipeline', 'commander.py')
        if not os.path.exists(expected_commander_path):
            logger.warning(f"注意: 未執行重組，且 'data_pipeline/commander.py' 未在 '{OUTER_CLONE_PATH}' 中找到。")

    # === 階段 2.5: 使用修正後的版本覆寫 YFinanceFetcher.py ===
    logger.header("階段 2.5: 使用修正後的版本覆寫 YFinanceFetcher.py")
    yfinance_fetcher_path = os.path.join(OUTER_CLONE_PATH, "data_pipeline", "fetchers", "yfinance_fetcher.py")

    correct_yfinance_fetcher_content = """\
# fetchers/yfinance_fetcher.py
# Modified to include more robust MultiIndex and date column handling
import yfinance as yf
import pandas as pd
import time
import random
import logging
from typing import Optional
from ..interfaces.data_fetcher_interface import DataFetcherInterface

class YFinanceFetcher(DataFetcherInterface):
    \"\"\"使用 yfinance 獲取金融數據的穩健實現。\"\"\"

    def __init__(self, robustness_config: dict):
        self.config = robustness_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        current_delay = self.config['delay_min_seconds']
        for attempt in range(self.config['retries']):
            try:
                self.logger.info(f"Attempt {{attempt + 1}}/{{self.config['retries']}} to fetch {{symbol}} from yfinance...")
                # 1. 明確設定 auto_adjust=False
                data = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=False)

                # 2. 添加最簡調試日誌
                self.logger.info(f"YF_DEBUG Symbol passed to yf.download: {{symbol}}")
                if not data.empty:
                    self.logger.info(f"YF_DEBUG Raw data columns for {{symbol}}: {{data.columns.tolist()}}")
                    self.logger.info(f"YF_DEBUG Raw data index for {{symbol}}: {{data.index}}")
                    self.logger.info(f"YF_DEBUG Raw data head for {{symbol}}:\\n{{data.head().to_string()}}")
                else:
                    self.logger.info(f"YF_DEBUG Raw data for {{symbol}} is empty.")

                if data.empty:
                    self.logger.warning(f"No data found for {{symbol}} on yfinance for the given period: {{start_date}} to {{end_date}} (with auto_adjust=False).")
                    return None

                # 標準化數據格式
                data.reset_index(inplace=True)

                # 3. 處理 MultiIndex 列
                if isinstance(data.columns, pd.MultiIndex):
                    self.logger.info(f"YF_DEBUG Detected MultiIndex columns for {{symbol}}. Attempting to flatten.")
                    new_columns = []
                    for col_tuple in data.columns:
                        if isinstance(col_tuple, tuple):
                            # 優先取元組的第一個元素，如果為空則取第二個 (適用於 ('Date', '') 的情況)
                            new_columns.append(col_tuple[0] if col_tuple[0] else col_tuple[1])
                        else:
                            new_columns.append(col_tuple)
                    data.columns = new_columns
                    self.logger.info(f"YF_DEBUG Columns after flattening MultiIndex for {{symbol}}: {{data.columns.tolist()}}")
                    self.logger.info(f"YF_DEBUG Data head after flattening MultiIndex for {{symbol}}:\\n{{data.head().to_string()}}")

                # 將所有列名轉為小寫並替換空格為下劃線
                data.columns = [str(col).lower().replace(' ', '_') for col in data.columns]

                # 確保 'date' 列是主要的日期時間列
                if 'date' not in data.columns:
                    if 'index' in data.columns:
                        self.logger.info(f"YF_DEBUG Renaming 'index' column to 'date' for {{symbol}}.")
                        data.rename(columns={{'index': 'date'}}, inplace=True)
                    elif 'datetime' in data.columns:
                        self.logger.info(f"YF_DEBUG Renaming 'datetime' column to 'date' for {{symbol}}.")
                        data.rename(columns={{'datetime': 'date'}}, inplace=True)

                expected_cols = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
                for col in expected_cols:
                    if col not in data.columns:
                        self.logger.warning(f"Column '{{col}}' not found in standardized data for {{symbol}}. Filling with NaN.")
                        data[col] = pd.NA

                if 'date' in data.columns:
                    data['date'] = pd.to_datetime(data['date'])
                else:
                    self.logger.error(f"Critical: 'date' column still not found after all standardization attempts for {{symbol}}. Columns: {{data.columns.tolist()}}")
                    return None

                return data

            except Exception as e:
                self.logger.error(f"Error fetching {{symbol}} from yfinance on attempt {{attempt + 1}}: {{e}}", exc_info=True)
                if attempt < self.config['retries'] - 1:
                    sleep_time = current_delay + random.uniform(0, current_delay * 0.1)
                    self.logger.info(f"Waiting {{sleep_time:.2f}} seconds before retrying...")
                    time.sleep(sleep_time)
                    current_delay = min(current_delay * self.config.get('backoff_factor', 2), self.config['delay_max_seconds'])
                else:
                    self.logger.critical(f"Failed to fetch {{symbol}} from yfinance after all {{self.config['retries']}} retries.")
                    return None
        return None
"""
    # Correctly prepare the content for writing (unescape f-string like parts for the target file)
    final_yfinance_fetcher_code = correct_yfinance_fetcher_content.replace("{{symbol}}", "{symbol}") \
                                                               .replace("{{attempt + 1}}", "{attempt + 1}") \
                                                               .replace("{{self.config['retries']}}", "{self.config['retries']}") \
                                                               .replace("{{start_date}}", "{start_date}") \
                                                               .replace("{{end_date}}", "{end_date}") \
                                                               .replace("{{data.columns.tolist()}}", "{data.columns.tolist()}") \
                                                               .replace("{{data.index}}", "{data.index}") \
                                                               .replace("{{data.head().to_string()}}", "{data.head().to_string()}") \
                                                               .replace("{{col}}", "{col}") \
                                                               .replace("{{sleep_time:.2f}}", "{sleep_time:.2f}") \
                                                               .replace("{{e}}", "{e}")

    if not os.path.isdir(OUTER_CLONE_PATH):
        logger.error(f"❌ 克隆目錄 '{OUTER_CLONE_PATH}' 不存在。無法繼續。")
        hw_monitor.stop()
        return

    os.chdir(OUTER_CLONE_PATH) # 切換到克隆下來的目錄
    sys.path.insert(0, OUTER_CLONE_PATH) # 將專案路徑加入 sys.path

    # 確保 fetchers 目錄存在
    fetchers_dir = os.path.join(OUTER_CLONE_PATH, "data_pipeline", "fetchers")
    if not os.path.exists(fetchers_dir):
        os.makedirs(fetchers_dir)
        logger.info(f"已創建目錄: {fetchers_dir}")

    if os.path.exists(os.path.dirname(yfinance_fetcher_path)):
        try:
            with open(yfinance_fetcher_path, 'w', encoding='utf-8') as f:
                f.write(final_yfinance_fetcher_code)
            logger.success(f"✅ '{yfinance_fetcher_path}' 已被修正後的版本成功覆寫。")
        except Exception as e:
            logger.error(f"❌ 覆寫 '{yfinance_fetcher_path}' 失敗: {e}", exc_info=True)
    else:
        logger.error(f"❌ 目錄 '{os.path.dirname(yfinance_fetcher_path)}' 不存在。無法覆寫 YFinanceFetcher。")

    # === 階段 2.6: 修改 config.yaml 以設定 max_workers = 1 ===
    logger.header("階段 2.6: 修改 config.yaml 以設定 max_workers = 1")
    config_yaml_path = os.path.join(OUTER_CLONE_PATH, "config.yaml")
    if os.path.exists(config_yaml_path):
        try:
            import yaml # 確保導入 yaml
            with open(config_yaml_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)

            if 'concurrency' not in config_data:
                config_data['concurrency'] = {}
            config_data['concurrency']['max_workers'] = 1

            with open(config_yaml_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, sort_keys=False)
            logger.success(f"✅ '{config_yaml_path}' 已更新，設定 max_workers = 1。")

            # 驗證修改
            # with open(config_yaml_path, 'r', encoding='utf-8') as f:
            #     logger.info(f"驗證 config.yaml 內容:\n{f.read()}")

        except Exception as e:
            logger.error(f"❌ 修改 '{config_yaml_path}' 失敗: {e}", exc_info=True)
    else:
        logger.error(f"❌ config.yaml 文件未在 '{config_yaml_path}' 找到，無法修改 max_workers。")

    logger.info(f"已將專案路徑 '{OUTER_CLONE_PATH}' 加入系統路徑。")
    logger.info(f"當前工作目錄已切換至: {os.getcwd()}")
    logger.info(f"當前 sys.path: {sys.path}")

    # === 階段三: 安裝依賴 ===
    logger.header("階段三: 安裝依賴")
    requirements_path = "requirements.txt" # 現在應該在 OUTER_CLONE_PATH 下
    if not os.path.exists(requirements_path):
        logger.error(f"❌ 在 '{os.getcwd()}' 中找不到 {requirements_path}。無法安裝依賴。")
        hw_monitor.stop()
        return

    logger.info("正在安裝依賴...")
    pip_result = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", requirements_path], capture_output=True, text=True)
    if pip_result.returncode != 0:
        logger.error(f"❌ Pip install 失敗！\n{pip_result.stderr}", exc_info=False)
        hw_monitor.stop()
        return
    logger.success("✅ 專案依賴安裝成功！")

    # === 階段四: 執行數據管道 ===
    logger.header("階段四: 執行數據管道 (本地模式)")
    commander = None
    try:
        # 清理可能已緩存的舊模組
        modules_to_clear = ['data_pipeline.commander', 'data_pipeline', 'data_pipeline.fetchers.yfinance_fetcher']
        for module_name in modules_to_clear:
            if module_name in sys.modules:
                logger.info(f"從 sys.modules 中移除已緩存的模組: {module_name}")
                del sys.modules[module_name]

        from data_pipeline.commander import Commander # type: ignore
        logger.info("初始化指揮官...")

        db_full_path = os.path.join(LOCAL_DATA_PATH, "panoramic_analyzer.duckdb")
        cache_full_path = os.path.join(LOCAL_DATA_PATH, "api_cache.sqlite")

        logger.info(f"數據庫將創建於本地: {db_full_path}")
        logger.info(f"快取將創建於本地: {cache_full_path}")

        if os.path.exists(db_full_path): os.remove(db_full_path)
        if os.path.exists(cache_full_path): os.remove(cache_full_path)

        commander = Commander( # type: ignore
            config_path='config.yaml', # 現在應該在 OUTER_CLONE_PATH 下
            db_path=db_full_path,
            cache_path=cache_full_path
        )

        logger.info(f"指揮官下達指令：執行批次數據獲取與儲存，目標: {TEST_SYMBOLS}")
        symbols_map_for_run = {'equity': TEST_SYMBOLS.split(',')}

        commander.run_batch_fetch_and_store( # type: ignore
            symbols_map=symbols_map_for_run,
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE
        )
        logger.success("✅ 指揮官批次任務執行完畢！")

    except Exception as e:
        logger.error(f"❌ 數據管道執行過程中發生嚴重錯誤: {e}", exc_info=True)
        hw_monitor.stop()
        return
    finally:
        if commander:
            commander.close() # type: ignore

    # === 階段五: 數據驗證 ===
    logger.header("階段五: 數據庫驗證 (本地模式)")
    db_full_path = os.path.join(LOCAL_DATA_PATH, "panoramic_analyzer.duckdb")
    if not os.path.exists(db_full_path):
        logger.error(f"❌ 驗證失敗：預期的本地數據庫檔案 {db_full_path} 未找到！")
        hw_monitor.stop()
        return

    try:
        con = duckdb.connect(database=db_full_path, read_only=True)
        logger.info("✅ 成功連接到本地 DuckDB 數據庫進行驗證。")

        tables = con.execute("SHOW TABLES;").fetchdf()
        logger.info(f"數據庫中的表格:\n{tables}")

        table_to_check = "ohlcv_daily"
        if table_to_check in tables['name'].values: # type: ignore
            logger.success(f"✅ 表格 '{table_to_check}' 存在。")
            for symbol in TEST_SYMBOLS.split(','):
                symbol_upper = symbol.strip().upper() # 確寶股票代碼是大寫且無空格
                count_result = con.execute(f"SELECT COUNT(*) FROM {table_to_check} WHERE symbol = ?", [symbol_upper]).fetchone()
                if count_result and count_result[0] > 0:
                    logger.success(f"  - 找到 {symbol_upper}: {count_result[0]} 筆記錄。")
                else:
                    logger.warning(f"  - 未在數據庫中找到 {symbol_upper} 的記錄。")
        else:
            logger.error(f"❌ 驗證失敗：核心表格 '{table_to_check}' 不存在！")

        con.close()
    except Exception as e:
        logger.error(f"❌ 數據庫驗證過程中發生錯誤: {e}", exc_info=True)

    # === 收尾工作 ===
    hw_monitor.stop()
    duration = time.time() - start_time
    logger.header(f"🏁 全部流程在 {duration:.2f} 秒內執行完畢")

    log_filename = f"local_deployment_log_{datetime.now(TAIPEI_TZ).strftime('%Y%m%d_%H%M%S')}.log"
    # 修改日誌保存路徑到 /tmp
    final_log_path = f"/tmp/{log_filename}"
    with open(final_log_path, "w", encoding="utf-8") as f:
        f.write(logger.get_full_log())
    logger.success(f"完整的純文字日誌已保存至: {final_log_path}")

    if IS_COLAB:
        print("\n若需下載日誌檔案，請在下一個儲存格執行以下指令：")
        print(f"from google.colab import files; files.download('{final_log_path}')")

if __name__ == '__main__':
    run_deployment_and_test()
