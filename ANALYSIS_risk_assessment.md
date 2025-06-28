### 專案 `-risk-assessment-feat-microservice-refactor` 分析報告

**整體描述**: 此專案旨在建立一個小型的金融數據處理管道，採用了基礎的微服務概念，將數據獲取和數據處理分離。它使用 yfinance API 獲取股價數據，並使用 DuckDB 作為本地數據庫。專案包含運行腳本和 pytest 測試。

**主要功能模組與檔案分析**:

1.  **README.md**:
    *   **功能**: 提供專案概覽、架構說明、環境設定指南以及如何運行的指令。
    *   **架構**: 描述了兩個主要服務 (`fetcher_service.py`, `processor_service.py`) 和兩個協調腳本 (`run_pipeline.sh`, `test_pipeline_fast.sh`)。
    *   **數據流**: `fetcher_service` (yfinance API -> `raw_data.db`) -> `processor_service` (`raw_data.db` -> 計算指標 -> `features.db`)。
    *   **技術棧**: Python, yfinance, DuckDB, pytest, Bash。

2.  **服務 (Services)** (`services/`):
    *   `fetcher_service.py`:
        *   **功能**: 數據獲取服務。負責從 yfinance API 抓取指定股票代號 (symbol) 在指定日期範圍內的 OHLCV (開高低收成交量) 數據。
        *   **輸出**: 將原始數據存儲到 DuckDB 資料庫 (`raw_data.db`) 中，表名為股票代號的小寫。
        *   **參數**: `--symbol`, `--start-date`, `--end-date`, `--db-path`。
        *   **列名處理**: 將 yfinance 返回的列名（可能為元組或混合大小寫）轉換為統一的小寫字符串（例如 'Open' -> 'open'）。
    *   `processor_service.py`:
        *   **功能**: 數據處理服務。負責從 `raw_data.db` 讀取原始數據，計算技術指標（目前是 20 日和 60 日移動平均線 `ma20`, `ma60`），並將結果存儲到 `features.db`。
        *   **輸入**: 原始數據 DuckDB 路徑 (`--input_db`)。
        *   **輸出**: 特徵數據 DuckDB 路徑 (`--output_db`)，表名為 `{symbol}_features`。
        *   **處理**: 計算收盤價 (`close`) 的移動平均，並移除因計算窗口導致的初始 NaN 值。
        *   **錯誤處理**: 包含對數據庫文件不存在、表不存在、數據為空或缺少 'close' 列的檢查。

3.  **配置 (Config)** (`config/`):
    *   `api_endpoints.yaml`:
        *   **功能**: 定義統一 API 請求器 (`UnifiedAPIFetcher`，在此專案的 `api_fetcher.py` 中定義) 如何從不同數據源獲取指標。
        *   **結構**: 頂層為 `indicators`，下有各指標名稱 (例如 `VIX_YF`)，每個指標可有多個備選數據源 (`api_provider`) 及相應參數 (`series_id`, `ticker`, `data_column`)。
        *   **支持的 Provider (根據 `api_fetcher.py` 分析)**: `fred`, `yfinance`。
    *   `dq_rules.yaml`:
        *   **功能**: 定義數據質量 (DQ) 驗證規則。
        *   **規則類型**: `range_check` (範圍檢查), `spike_check` (尖峰檢查), `not_null_check` (非空檢查), `stale_check` (過期檢查)。
        *   **參數**: 每個規則類型有其特定參數 (如 `min`, `max`, `window`, `threshold_std`, `max_days_stale`) 和嚴重性 (`severity`: ERROR, WARNING)。
    *   `project_config.yaml`:
        *   **功能**: 專案級別的設定檔。
        *   **內容**: 項目名稱、版本、日誌級別、時區、API Key 環境變數名映射、數據獲取設定 (快取目錄、過期時間、重試參數)、壓力指數模型參數 (窗口期、權重、平滑、閾值)、Prompt 工程設定、回測設定路徑等。

4.  **核心邏C庫 (Python 檔案)**:
    *   `api_deep_tester.py`:
        *   **功能**: 用於深度測試多個金融數據 API 的連通性、數據獲取能力、時間週期支持、歷史數據長度以及速率限制。
        *   **支持的 API (根據函式)**: FRED, Alpha Vantage, Finnhub, News API, FMP, Polygon.io, ECB, World Bank, OECD, US Treasury FiscalData, BEA, BLS, TWSE, CoinGecko, yfinance。
        *   **環境變數**: 依賴 `.env` 文件加載各 API Key。
        *   **輸出**: 生成詳細的日誌和測試報告文本文件。
    *   `api_fetcher.py`:
        *   **功能**: 定義了一個統一的 API 數據獲取框架 (`UnifiedAPIFetcher`)。
        *   **設計**: 使用適配器模式 (Adapter Pattern)，為不同的 API Provider (FRED, yfinance) 實現了 `BaseAdapter` 的子類 (`FredAdapter`, `YFinanceAdapter`)。
        *   **核心類**:
            *   `BaseAdapter`: 適配器基類，定義了 `_fetch_data` (由子類實現) 和 `get_data` (包含重試邏輯) 方法。
            *   `FredAdapter`: FRED API 適配器，支持有金鑰 (使用 `fredapi` 庫) 和無金鑰 (使用 `market_data_fred.py`) 兩種模式。
            *   `YFinanceAdapter`: yfinance 適配器，使用 `yfinance` 庫。
            *   `UnifiedAPIFetcher`: 統一請求器，根據 `api_endpoints.yaml` 配置，選擇合適的適配器獲取數據，並實現了基於 Parquet 格式的本地快取機制。
        *   **錯誤處理**: 定義了 `APIFetchError`, `APIAuthenticationError`, `APIRateLimitError`, `APIDataNotFoundError` 等自定義異常。
        *   **重試機制**: 使用 `tenacity` 庫對 API 請求進行重試。
    *   `config_loader.py`:
        *   **功能**: 負責載入、解析和驗證 `project_config.yaml`。
        *   **API Key 處理**: 從環境變數中讀取 `project_config.yaml` 中定義的 API Key 名稱對應的實際金鑰，並將其填充到配置字典的 `runtime_api_keys` 下。
    *   `data_validator.py`:
        *   **功能**: 數據驗證器。根據 `dq_rules.yaml` 中定義的規則，對傳入的 Pandas DataFrame 中的數據指標進行驗證。
        *   **核心類**: `DataValidator`。
        *   **驗證方法**: 實現了 `_apply_range_check`, `_apply_spike_check`, `_apply_not_null_check`, `_apply_stale_check` 等私有方法。
        *   **輸出**: 在原始 DataFrame 基礎上附加 DQ 狀態 (`_dq_status`) 和註釋 (`_dq_notes`) 列。
    *   `file_utils.py`:
        *   **功能**: 提供文件處理相關的工具函式，主要用於管理已處理文件的清單 (manifest)。
        *   **機制**: 使用 SHA-256 雜湊值來判斷文件內容是否變更，以實現增量更新。
        *   **檔案**: 清單儲存於 `OUT_Processed_Prompts/processed_files_manifest.json`。
    *   `local_data_fetcher.py`:
        *   **功能**: 一個本地腳本，用於從 FRED 和 yfinance 獲取一批預定義的指標數據，並將其保存為 CSV 文件到 `local_sample_data` 目錄。
        *   **目的**: 可能用於生成離線測試數據或樣本數據。
        *   **依賴**: 需要本地環境設定 `FRED_API_KEY`。
    *   `main_analyzer.py`:
        *   **功能**: 主分析流程協調器。掃描輸入目錄 (`IN_Source_Reports`) 中的文本報告檔案，解析檔名獲取年份和週數，然後：
            1.  獲取報告對應的日期範圍。
            2.  讀取報告的質化內容。
            3.  使用 `market_data_yfinance.py` 和 `market_data_fred.py` 獲取相關的量化市場數據 (S&P 500, 台指, TLT, HYG, LQD, VIX, TWD/USD, FRED VIX, 10Y 公債, 聯邦基金利率, 台指期貨連續合約)。
            4.  將質化內容和量化數據組合成一個 Prompt 文本。
            5.  將 Prompt 儲存到 `OUT_Processed_Prompts` 目錄。
        *   **增量更新**: 使用 `file_utils.py` 中的 manifest 機制，只處理新的或已修改的報告檔案。
        *   **檔名解析**: 依賴 `YYYY年第WW週` 的檔名格式。
    *   `market_data_fred.py`:
        *   **功能**: 提供從 FRED 獲取經濟數據序列的功能，**無需 API 金鑰**。
        *   **方法**: 通過構造直接下載 CSV 的 URL (`https://fred.stlouisfed.org/graph/fredgraph.csv`) 來獲取數據。
        *   **快取**: 使用 `requests-cache` 為 FRED 請求建立獨立的 SQLite 快取 (`CACHE_Market_Data/fred_data_cache.sqlite`)。
        *   **穩定性**: 包含重試邏ozygous (基於 `requests.exceptions`) 和超時設定。
        *   **數據處理**: 解析 CSV，轉換日期索引，將數據列轉為數值型 (處理 FRED CSV 中用 '.' 表示缺失的情況)。
    *   `market_data_yfinance.py`:
        *   **功能**: 提供從 Yahoo Finance 獲取市場數據的功能，並包含台指期貨連續合約的拼接邏輯。
        *   **核心函式**: `fetch_yfinance_data_stable` (獲取股票/ETF/指數數據，含重試和小時線降級邏輯) 和 `fetch_continuous_taifex_futures` (獲取台指期貨連續近月合約數據，使用向後調整法)。
        *   **快取**: 通過 `yf.set_session(SESSION)` 讓 yfinance 使用配置了 `requests-cache` 的全局 Session (`CACHE_Market_Data/yfinance_cache.sqlite`)。
        *   **穩定性**: `fetch_yfinance_data_stable` 包含錯誤處理、重試 (基於 `yfinance` 可能拋出的錯誤和網路錯誤) 與小時線數據獲取失敗時降級到日線的邏輯。
        *   **台指期貨**: `fetch_continuous_taifex_futures` 涉及計算最後交易日、生成合約代號、循環獲取各月份合約數據、並進行向後價格調整以生成連續數據。
        *   **選擇權檢查**: `check_taifex_options_support_yfinance` 用於檢查 yfinance 對台指選擇權的數據支持情況 (結論通常是不支持歷史 OCHLV 或希臘字母)。

5.  **測試 (Tests)** (`tests/`):
    *   `conftest.py`:
        *   **功能**: 提供 pytest fixtures，例如 `mock_raw_data`，用於生成測試用的假原始股價 DataFrame。欄位名已處理為小寫。
    *   `test_fetcher_service.py`:
        *   **功能**: 測試 `fetcher_service.py` 中的 `save_to_duckdb` 函數。
        *   **方法**: 使用 `mock_raw_data` fixture 和 pytest 提供的臨時目錄 (`tmp_path`)，驗證數據能否成功寫入 DuckDB 並讀取回來。
    *   `test_processor_service.py`:
        *   **功能**: 測試 `processor_service.py` 中的 `process_data` 函數。
        *   **方法**: 使用 `mock_raw_data` fixture，驗證移動平均線 (`ma20`, `ma60`) 是否能正確計算，以及因 dropna() 導致的行數變化是否符合預期。使用 `np.isclose` 比較浮點數。

6.  **腳本 (Scripts)**:
    *   `run_pipeline.sh`:
        *   **功能**: 執行完整的數據獲取和處理管道。
        *   **流程**: 清理舊數據庫 -> 運行 `fetcher_service.py` -> 運行 `processor_service.py`。
        *   **環境**: 設定 `PYTHONPATH`。
    *   `test_pipeline_fast.sh`:
        *   **功能**: 執行快速的離線測試。
        *   **流程**: 設定 `PYTHONPATH` -> 運行 `pytest -v tests/`。

7.  **Jupyter Notebooks**:
    *   `一級交易pro.ipynb`: (內容根據Cell分析)
        *   **Cell 1 (專案初始化與全局設定)**: 安裝/導入庫，定義 `PROJECT_CONFIG` (含 API Key, 權重, URL 等), `EXECUTION_TRACKER`, 設定時區，實現互動式日期選擇與儲存 (使用 `ipywidgets` 和 `date_config.json`)。
        *   **Cell 2 (核心函式定義)**: 定義繪圖函式 `plot_results` (5x2 時間序列圖), `plot_gemini_gauge_mpl` (壓力儀表板), `plot_trend_colored` (近期趨勢圖)，均支持多種輸出格式 ('show', 'save', 'base64')。
        *   **Cell 3 (日期處理與 API 驗證)**: 驗證 Cell 1 的日期範圍，使用 `PROJECT_CONFIG` 中的 FRED API Key 初始化 `Fred` 物件並執行測試請求以驗證 Key。
        *   **Cell 4 (抓取 FRED 經濟數據)**: 使用 `Fred` 物件和驗證後的日期，抓取 SOFR, DGS10, DGS2, RRP, VIX, WRESBAL 等數據，處理不同頻率並對齊至業務日，存儲到 `fred_data_df`。
        *   **Cell 5 (抓取 Yahoo Finance 數據)**: 使用 `yfinance` 獲取 MOVE 指數和可選的長債 ETF (如 TLT) 數據，存儲到 `yahoo_data_df`。
        *   **Cell 6 (抓取並處理 NY Fed 持有量)**: 從 NY Fed 網站下載多個 Excel 文件，解析、加總並合併一級交易商公債持有量數據，存儲到 `nyfed_positions_series`。**警告混合了 SBN/SBP 數據。**
        *   **Cell 7 (合併所有數據源)**: 將 `fred_data_df`, `yahoo_data_df`, `nyfed_positions_series` 合併到 `merged_data_df`，處理欄位重命名和低頻數據的向前填充。
        *   **Cell 8 (計算衍生指標與壓力指數)**: 基於 `merged_data_df` 計算利差、SOFR 偏差、持有/準備金比率，然後計算各成分的滾動百分位排名，最後根據權重計算原始及平滑壓力指數，並可選計算 MACD。結果存入 `final_df`。
        *   **Cell 9 (產生時間序列圖表)**: 調用 `plot_results` 顯示 5x2 網格的詳細時間序列圖。
        *   **Cell 10 (產生趨勢圖與儀表板)**: 顯示 `final_df` 尾部數據預覽，調用 `plot_gemini_gauge_mpl` 和 `plot_trend_colored` 顯示儀表板和近期趨勢。
        *   **Cell 11 (顯示文字分析結果)**: 根據最新壓力指數值，顯示歷史事件對比提醒和市場壓力情境分析文字。
        *   **Cell 12 (互動式報告參數設定)**: 使用 `ipywidgets` (Dropdown, DatePicker, Checkbox) 讓使用者選擇 HTML 報告的日期範圍和圖表內容。
        *   **(Cell_Debug_Consolidated, Cell_Test_YFinance)**: 除錯和單獨測試用的 Notebook Cell。
    *   `risk_assessment/main.ipynb`: (內容較簡單，像是早期原型)
        *   **功能**: 導入 `commander` 並調用 `generate_report()`。
        *   **commander.py**: 包含 `Commander` 類，使用 `FredFetcher` 和 `SQLiteRepository` (模擬) 執行一個簡化的數據獲取和儲存流程。
        *   **utils/config_loader.py**: 載入 `risk_assessment/config.yaml`。
        *   **modules/**: 包含 `database` (接口和 SQLite 實現) 和 `fetchers` (接口和 FRED 實現) 的抽象。

8.  **其他**:
    *   `requirements.txt`: 列出專案依賴 (pandas, yfinance, duckdb, pytest)。
    *   `sample_api_test_report.txt`, `風險可視化.txt`: 可能是 `api_deep_tester.py` 或其他分析腳本的示例文本輸出。
    *   `data/`: 可能包含 `raw_data.db`, `features.db` (由管道生成) 或其他數據文件。

**潛在微服務邊界**:

根據目前的分析，可以考慮以下幾個潛在的微服務邊界：

1.  **數據獲取服務 (Fetcher Service)**:
    *   **職責**: 專門負責從各種外部 API (yfinance, FRED, Alpha Vantage, NY Fed 等) 獲取原始數據。
    *   **輸入**: 指標請求 (例如，股票代號、序列ID、日期範圍)。
    *   **輸出**: 原始數據，可以存儲到一個共享的原始數據層 (例如，獨立的數據庫、消息隊列或文件存儲)。
    *   **對應現有**: `fetcher_service.py`, `market_data_yfinance.py`, `market_data_fred.py`, `api_fetcher.py` 中的適配器邏輯，以及 `main_analyzer.py` 和 Notebooks 中的數據獲取部分。`local_data_fetcher.py` 的邏輯也可以歸入此類。

2.  **數據處理與特徵工程服務 (Processor Service)**:
    *   **職責**: 讀取原始數據，進行清洗、轉換、計算技術指標、衍生指標、百分位排名等。
    *   **輸入**: 原始數據。
    *   **輸出**: 清洗和處理後的特徵數據，存儲到特徵數據庫或特徵存儲。
    *   **對應現有**: `processor_service.py`, `main_analyzer.py` 和 Notebooks 中的數據計算部分 (例如，計算利差、SOFR偏差、壓力指數、MACD)。

3.  **數據驗證服務 (Validator Service)**:
    *   **職責**: 對原始數據或特徵數據執行數據質量檢查。
    *   **輸入**: 待驗證的數據集，DQ 規則配置。
    *   **輸出**: 驗證報告，或標記了 DQ 問題的數據。
    *   **對應現有**: `data_validator.py`。

4.  **API 測試與監控服務 (APITest Service)**:
    *   **職責**: 定期或按需測試外部 API 的可用性和數據質量。
    *   **輸入**: API 端點列表，測試規則。
    *   **輸出**: API 狀態報告，監控指標。
    *   **對應現有**: `api_deep_tester.py`。

5.  **報告與可視化服務 (Reporting/Visualization Service)**:
    *   **職責**: 基於處理後的數據生成圖表、儀表板和分析報告 (文字或 HTML)。
    *   **輸入**: 特徵數據，壓力指數結果。
    *   **輸出**: 圖表圖片，HTML 報告，文本分析。
    *   **對應現有**: Notebooks 中的繪圖函式 (`plot_results`, `plot_gemini_gauge_mpl`, `plot_trend_colored`) 和文字分析生成邏輯。

6.  **任務協調/工作流服務 (Orchestrator Service)**:
    *   **職責**: 管理和協調各個服務的執行順序和依賴關係。
    *   **對應現有**: `run_pipeline.sh`, `main_analyzer.py` 的頂層邏輯。

**配置文件管理**:
*   `project_config.yaml`, `api_endpoints.yaml`, `dq_rules.yaml` 可以作為一個配置服務或共享配置庫的一部分，供各微服務按需讀取。`config_loader.py` 是其基礎。

**通信機制**:
*   服務間可以通過 REST API, gRPC 或消息隊列 (如 Kafka, RabbitMQ) 進行通信。
*   數據庫 (DuckDB, 或更換為其他如 PostgreSQL) 可以作為數據共享的一種方式。

**進一步思考**:
*   **日誌聚合**: 需要一個中心化的日誌系統。
*   **部署與擴展**: 每個微服務可以獨立部署和擴展。
*   **安全性**: API Key 管理需要更安全的機制，例如使用 Vault。

此初步分析為後續的微服務架構設計提供了基礎。
