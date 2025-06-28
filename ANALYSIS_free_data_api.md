### 專案 `Free_Data_API-feat-microservice-refactor` 分析報告

**整體描述**: 此專案看起來是 `-risk-assessment-feat-microservice-refactor` 專案的一個演進版本或分支，旨在建立一個更通用的金融數據處理框架。它引入了更清晰的模組化結構，例如 `financial_data_framework` 和 `panoramic-market-analyzer` 子目錄，並繼續使用 DuckDB 進行數據存儲。腳本中出現了 FMP (Financial Modeling Prep) API 的使用跡象。

**主要功能模組與檔案分析**:

1.  **README.md**:
    *   **功能**: 提供專案概覽、微服務架構理念說明、環境設定指南以及如何通過 `run_pipeline.sh` 執行完整或部分數據流程的指令。
    *   **核心架構**: 強調了 `Fetcher Service` (從外部 API 抓取原始數據存入「原始數據層」) 和 `Processor Service` (從「原始數據層」讀取、處理並存入「特徵層」) 的分離。
    *   **API 金鑰**: 指示需要在 `.env` 文件中設定 `API_KEY_FMP`。
    *   **數據庫驗證**: 提及使用 `verify_db.py` 進行驗證。

2.  **數據管道核心 (`data_pipeline/`)**:
    *   `__init__.py`: 空文件，標記此目錄為 Python 包。
    *   `_temp_init_db.py`:
        *   **功能**: 臨時的數據庫初始化腳本。
        *   **核心函式**: `initialize_all_tables`，用於創建核心表格，包括 `fmp_profiles`, `fmp_financial_statements`, 以及特定股票的價格表 (日和週)。
    *   `aggregator.py`:
        *   **功能**: 提供 `resample_ohlcv` 函式，用於將日頻 OHLCV 數據重新採樣到新的時間頻率 (例如，週、月、季)。
        *   **處理**: 處理 DatetimeIndex，列名映射，並對 OHLCV 數據應用正確的聚合規則 (open->first, high->max, low->min, close->last, volume->sum)。
    *   `data_processor.py`:
        *   **功能**: 處理原始價格數據。
        *   **核心函式**: `process_price_data`，接收日頻 DataFrame，進行列名標準化，計算 MA20 (20日移動平均線)，並調用 `aggregator.resample_ohlcv` 生成週頻數據。
        *   **輸出**: 返回處理後的日頻 DataFrame 和週頻 DataFrame，均添加了 'symbol' 列。
    *   `db_utils.py`:
        *   **功能**: 數據庫工具函式，主要用於將 Pandas DataFrame 數據通過 UPSERT 邏輯存入 DuckDB。
        *   **核心函式**: `save_dataframe`，會自動從 DataFrame 推斷並創建表格 (如果不存在，包含主鍵定義)，然後使用 `ON CONFLICT DO UPDATE` 進行數據的插入或更新。
        *   **類型映射**: 包含基本的 Pandas dtype 到 SQL type 的映射。
    *   `fetcher_service.py`:
        *   **功能**: 從 FMP API 獲取日頻價格數據。
        *   **核心函式**: `fetch_single_symbol` (獲取單個股票數據) 和 `main` (解析命令列參數，並行獲取多個股票數據)。
        *   **並行處理**: 使用 `ThreadPoolExecutor` 並行獲取數據。
        *   **數據儲存**: 調用 `db_utils.save_dataframe` 將獲取的原始價格數據存儲到 `raw_prices` 表。
    *   `market_data_fmp.py`:
        *   **功能**: 直接與 FMP API 交互的底層函式。
        *   **核心函式**: `get_daily_price_data` (獲取日頻歷史價格), `get_company_profile` (獲取公司簡介), `get_financial_statements` (獲取財務報表，如損益表)。
        *   **API Key**: 從環境變數 `API_KEY_FMP` 讀取。
        *   **快取與重試**: 使用 `requests-cache` (fmp_cache.sqlite) 和 `requests.packages.urllib3.util.retry` 實現 API 請求的快取和重試。
    *   `processor_service.py`:
        *   **功能**: 數據處理服務的主入口。
        *   **流程**: 從原始數據庫 (`raw_prices` 表) 讀取數據，調用 `data_processor.process_price_data` 進行處理 (計算 MA20，生成週線數據)，然後將結果分別存儲到 `daily_features` 和 `weekly_features` 表中。

3.  **金融數據框架 (`financial_data_framework/`)**: (這似乎是一個更抽象的框架層)
    *   `commander.py`:
        *   **功能**: 系統的協調器 (Facade 模式)。負責讀取配置、實例化數據獲取器 (Fetcher) 和數據庫倉儲 (Repository)，並協調數據流程。
        *   **動態實例化**: 根據 `config.yaml` 中的設定 (`fetcher.type`, `database.type`) 動態選擇並創建 Fetcher (目前支持 `yfinance`) 和 Repository (目前支持 `duckdb`)。
        *   **並行獲取**: 使用 `ThreadPoolExecutor` 並行獲取多個股票代號的數據。
        *   **數據儲存**: 調用 Repository 的 `save_ohlcv` 方法存儲數據。
    *   `config.yaml`:
        *   **功能**: 此框架的配置文件。
        *   **內容**: 數據庫類型和路徑 (`data_hub.duckdb`)，數據獲取器類型 (`yfinance`) 及其穩健性參數 (重試次數、延遲)，並行處理的 worker 數量。
    *   `data_fetchers/`:
        *   `yfinance_fetcher.py`: 實現了 `DataFetcherInterface`，使用 `yfinance` 庫獲取數據，包含延遲和基本的錯誤處理。
    *   `data_storage/`:
        *   `duckdb_repository.py`: 實現了 `DatabaseInterface`，使用 DuckDB 進行數據存儲，包含 `_create_ohlcv_table_if_not_exists` (創建表) 和 `save_ohlcv` (UPSERT 邏輯)。
    *   `interfaces/`:
        *   `data_fetcher_interface.py`: 定義了數據獲取器的抽象基類 (ABC)，包含 `fetch_data` 抽象方法。
        *   `database_interface.py`: 定義了數據庫倉儲的抽象基類，包含 `connect`, `disconnect`, `save_ohlcv`, `get_ohlcv` 等抽象方法。
    *   `run_pipeline_in_colab.py`:
        *   **功能**: 在 Colab 環境中執行此框架數據管道的腳本。
        *   **參數化**: 通過 `argparse` 接收任務類型 (`fetch`)、股票代號、日期範圍和配置文件路徑。
        *   **流程**: 實例化 `Commander` 並調用其 `fetch_and_store_symbols` 方法。

4.  **全景市場分析儀子專案 (`panoramic-market-analyzer/`)**: (這似乎是基於 `financial_data_framework` 的一個具體應用或更進一步的抽象)
    *   `config.yaml`: 此子專案的配置文件，結構類似頂層的 `financial_data_framework/config.yaml`，但可能針對不同的數據源或路徑。
    *   `data_pipeline/`: 包含此子專案的數據管道邏輯。
        *   `commander.py`: 與頂層 `financial_data_framework/commander.py` 類似，但導入的是此子目錄下的 fetchers 和 database 實現。
        *   `database/duckdb_repository.py`: DuckDB 的實現，與頂層框架中的類似。
        *   `fetchers/`: 包含 `yfinance_fetcher.py`, `fred_fetcher.py`, `crypto_fetcher.py` (使用 CoinGecko API)。這些 fetchers 實現了 `DataFetcherInterface`。
        *   `interfaces/`: 同頂層框架。
        *   `processing/processor.py`: 包含 `DataProcessor` 類，用於數據清洗和計算技術指標 (SMA, EMA, RSI)。
        *   `test_commander_mocked.py`: 使用 `unittest.mock` 對 `Commander` 進行模擬測試，專注於測試 `fetch_single_symbol_data` 的邏輯，模擬了 `YFinanceFetcher` 的行為。
    *   `notebooks/Market_Analysis_Runner.ipynb`:
        *   **功能**: Colab Notebook，作為一個用戶界面來部署和執行 `panoramic-market-analyzer` 的數據管道。
        *   **流程**: 掛載 Google Drive -> 從 GitHub clone 指定專案和分支 -> 安裝依賴 -> 設定 Python 環境 -> 導入 `Commander` -> 提供參數輸入界面 (股票代號、日期等) -> 執行數據獲取和存儲。

5.  **頂層腳本與文件**:
    *   `analyzer_script.py`:
        *   **功能**: 一個 Colab 自動化部署與測試平台腳本，用於快速驗證核心程式碼邏輯。
        *   **強制重組**: 包含檢測和修復 "雙層包裹" 專案結構的邏輯。
        *   **覆寫**: 包含使用修正後的 `YFinanceFetcher.py` 覆寫克隆下來的文件的邏輯。
        *   **配置修改**: 自動修改 `config.yaml` 以設定 `max_workers = 1` (可能是為了簡化測試或避免 Colab 資源限制)。
        *   **流程**: Git clone -> 強制重組 -> 覆寫文件 -> 修改配置 -> 安裝依賴 -> 執行數據管道 (調用 `data_pipeline.commander.Commander`) -> 數據庫驗證。
        *   **監控與日誌**: 包含硬體監控和詳細的日誌記錄。
    *   `colab_test_runner.py`:
        *   **功能**: 另一個 Colab 執行器腳本，更側重於通過命令列參數分階段執行數據管道的不同部分 (初始化數據庫、處理價格、處理基本面、完整流程、驗證數據庫)。
        *   **API Key**: 從 Colab Secrets 讀取多個 API Key (ALPHA_VANTAGE, FINMIND, FINNHUB, FMP, FRED, POLYGON, DEEPSEEK, GOOGLE) 並設定為環境變數。
        *   **模組導入**: 導入 `data_pipeline` 下的各個模組 (`db_utils`, `market_data_fmp`, `data_processor`) 和 `verify_db`。
        *   **命令分派**: 根據 `--stage` 參數調用不同的處理函式。
    *   `file_utils.py`: 與 `-risk-assessment-feat-microservice-refactor` 中的版本相同，用於增量更新。
    *   `main_analyzer.py`, `market_data_fred.py`, `market_data_yfinance.py`, `api_deep_tester.py`, `sample_api_test_report.txt`: 這些文件看起來與 `-risk-assessment-feat-microservice-refactor` 中的版本非常相似或相同，可能是在專案演進過程中被複製或共享的。它們的功能在之前的分析中已涵蓋。
    *   `requirements.txt`: 專案的頂層依賴 (pandas, duckdb, pyyaml, python-dotenv, requests, requests-cache)。
    *   `run_colab_tests.py`: 可能是 `colab_test_runner.py` 的一個別名或早期版本。
    *   `run_data_pipeline.sh`, `run_pipeline.sh`: Bash 腳本，用於在本地或伺服器環境執行數據管道的不同階段。
    *   `setup_and_test.sh`: 可能是用於設定環境並執行 `test_commander_mocked.py` 的腳本。
    *   `test_fetcher_service.sh`, `test_pipeline_fast.sh`: 類似於 `-risk-assessment` 專案中的測試腳本，但可能針對此專案的服務。
    *   `verify_db.py`: 用於驗證 DuckDB 中表格是否存在、是否為空、特定股票數據是否存在，並抽樣查看數據。

**與 `-risk-assessment-feat-microservice-refactor` 的比較和演進**:

*   **抽象層次更高**: `financial_data_framework` 和 `panoramic-market-analyzer` 的引入表明專案試圖建立更通用和模組化的數據處理能力。
*   **數據源擴展**: 明確引入了 FMP API (`market_data_fmp.py`) 和 CoinGecko API (`crypto_fetcher.py`)，擴展了數據來源。
*   **配置驅動**: `config.yaml` 的使用更加突出，用於配置數據源、數據庫、並行度等。
*   **接口化設計**: `interfaces` 目錄定義了 `DataFetcherInterface` 和 `DatabaseInterface`，這是良好的面向接口編程實踐。
*   **數據處理器**: `processing/processor.py` 的引入，為數據清洗和指標計算提供了專門的模組。
*   **更完善的 Colab 工具**: `analyzer_script.py` 和 `colab_test_runner.py` 提供了更複雜的 Colab 環境部署、測試和執行流程。
*   **代碼複用與演進**: 一些核心工具 (如 `file_utils.py`) 和早期數據獲取腳本 (如 `market_data_yfinance.py`, `market_data_fred.py`) 被保留和複用。

**潛在微服務邊界 (與之前分析類似，但更清晰)**:

1.  **數據獲取服務 (Fetcher Service)**:
    *   **職責**: 從 yfinance, FRED, FMP, CoinGecko 等獲取數據。
    *   **對應**: `data_pipeline/fetcher_service.py` (FMP), `financial_data_framework/data_fetchers/`, `panoramic-market-analyzer/data_pipeline/fetchers/`。
2.  **數據處理服務 (Processor Service)**:
    *   **職責**: 數據清洗，計算 MA, EMA, RSI 等。
    *   **對應**: `data_pipeline/data_processor.py`, `panoramic-market-analyzer/data_pipeline/processing/processor.py`。
3.  **數據存儲服務 (Storage Service / Repository)**:
    *   **職責**: 與 DuckDB 交互，提供數據的 CRUD 和 UPSERT 操作。
    *   **對應**: `data_pipeline/db_utils.py`, `financial_data_framework/data_storage/duckdb_repository.py`, `panoramic-market-analyzer/data_pipeline/database/duckdb_repository.py`。
4.  **任務協調服務 (Orchestrator Service)**:
    *   **職責**: 協調 fetch, process, store 等任務。
    *   **對應**: `financial_data_framework/commander.py`, `panoramic-market-analyzer/data_pipeline/commander.py`，以及各類 `run_*.sh` 和 Colab 執行器腳本。
5.  **API 測試/監控服務**: `api_deep_tester.py`。
6.  **配置管理服務**: (隱式) 通過 `config.yaml` 和相應的加載邏輯。

**總體印象**:
`Free_Data_API-feat-microservice-refactor` 專案在 `-risk-assessment-feat-microservice-refactor` 的基礎上進行了顯著的結構優化和功能擴展，更加符合一個可擴展金融數據平台的設計。它通過接口定義、模組劃分和配置驅動，提高了系統的靈活性和可維護性。Colab 相關的工具也更加成熟。
