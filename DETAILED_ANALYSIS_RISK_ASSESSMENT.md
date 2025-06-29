# 專案分析報告：-risk-assessment-feat-microservice-refactor

**報告日期**: 2024年6月27日

**分析員**: Jules (AI Software Engineer)

## 1. 專案整體概覽

`-risk-assessment-feat-microservice-refactor` 子專案旨在建立一個小型的、概念驗證級別的金融數據處理管道。其核心設計思想是將數據獲取 (Fetcher) 與數據處理 (Processor) 兩個階段分離，初步體現了微服務的解耦理念。該專案主要使用 yfinance API 作為數據源獲取股票的歷史 OHLCV (開盤價、最高價、最低價、收盤價、成交量) 數據，並利用 DuckDB 作為本地數據庫來存儲原始數據和計算後的特徵數據。專案結構清晰，包含了運行整體流程的 Bash 腳本和用於驗證核心組件功能的 pytest 單元測試。

主要的子系統和功能模組包括：

*   **數據獲取服務 (`services/fetcher_service.py`)**: 負責從 yfinance API 抓取指定股票的時間序列數據，並將其存儲到一個 DuckDB 資料庫 (`raw_data.db`) 中。
*   **數據處理服務 (`services/processor_service.py`)**: 負責從 `raw_data.db` 讀取原始數據，計算技術指標（目前實現了20日和60日移動平均線），並將結果存儲到另一個 DuckDB 資料庫 (`features.db`) 中。
*   **配置文件 (`config/`)**: 包含 `project_config.yaml` (專案級配置)、`api_endpoints.yaml` (用於更通用的 `UnifiedAPIFetcher`，定義指標和數據源) 和 `dq_rules.yaml` (數據質量驗證規則)。這些配置文件展示了向配置驅動設計演進的意圖。
*   **核心庫與工具**:
    *   `api_fetcher.py`: 定義了一個 `UnifiedAPIFetcher` 框架，採用適配器模式來支持多種數據源 (FRED, yfinance)，並包含快取和重試邏輯。
    *   `data_validator.py`: 根據 `dq_rules.yaml` 執行數據質量檢查。
    *   `config_loader.py`: 負責加載和驗證 `project_config.yaml`。
    *   `market_data_yfinance.py` 和 `market_data_fred.py`: 提供了直接與 yfinance 和 FRED API 交互的功能，包含快取和錯誤處理。
    *   `main_analyzer.py`: 一個用於掃描報告、獲取相關市場數據並生成 Prompt 的主分析流程。
    *   `api_deep_tester.py`: 用於深度測試多個金融API的工具。
*   **測試 (`tests/`)**: 包含針對 `fetcher_service.py` 和 `processor_service.py` 的單元測試。
*   **Jupyter Notebooks**: 包含 `一級交易pro.ipynb` (一個複雜的、用於計算和可視化金融壓力指數的 Notebook) 和 `risk_assessment/main.ipynb` (一個早期原型)。

該專案雖然功能相對基礎，但其架構設計（服務分離、配置驅動、單元測試）為後續更複雜的金融數據平台演進奠定了良好的基礎。

## 2. 核心數據處理流程 (`services/` 及相關腳本) 分析

此子專案的核心數據處理流程由 `services/fetcher_service.py` 和 `services/processor_service.py` 兩個服務以及協調它們運行的 `run_pipeline.sh` 腳本構成。

### 2.1. 數據獲取服務 (`services/fetcher_service.py`)

*   **職責**: 從 yfinance API 獲取指定股票代號 (`symbol`) 在指定日期範圍 (`start-date`, `end-date`) 內的日頻 OHLCV 數據。
*   **數據輸出**: 將獲取的原始數據存儲到由 `--db-path` 參數指定的 DuckDB 資料庫中（預設或腳本指定為 `data/raw_data.db`）。數據表名直接使用股票代號的小寫形式 (例如，若 `symbol` 為 "AAPL"，則表名為 "aapl")。
*   **欄位名處理**: 腳本會將從 yfinance 返回的 DataFrame 的欄位名（例如 'Open', 'High', 'Adj Close'）統一轉換為小寫（'open', 'high', 'adj_close'）。
*   **主要函式**: `save_to_duckdb(symbol, start_date, end_date, db_path)`。
*   **執行方式**: 通過命令列參數接收股票代號、日期範圍和資料庫路徑。

### 2.2. 數據處理服務 (`services/processor_service.py`)

*   **職責**: 從原始數據庫 (`--input_db`，通常是 `fetcher_service.py` 產生的 `data/raw_data.db`) 讀取指定股票的原始價格數據，計算技術指標，並將包含原始數據和新計算指標的 DataFrame 存儲到特徵數據庫 (`--output_db`，通常是 `data/features.db`)。
*   **技術指標**: 目前實現了基於收盤價 (`close`) 的20日移動平均線 (`ma20`) 和60日移動平均線 (`ma60`)。
*   **數據表命名**: 輸出的特徵數據表名格式為 `{symbol}_features` (例如 "aapl_features")。
*   **數據處理細節**:
    *   在計算移動平均線後，會使用 `.dropna()` 移除因滾動窗口計算而產生的初始 NaN 值行。
*   **錯誤處理**: 包含對輸入資料庫文件不存在、指定的股票數據表不存在、數據表為空或缺少必要的 'close' 欄位的檢查和處理。
*   **主要函式**: `process_data(symbol, input_db_path, output_db_path)`。
*   **執行方式**: 通過命令列參數接收股票代號、輸入資料庫路徑和輸出資料庫路徑。

### 2.3. 管道執行腳本 (`run_pipeline.sh`)

*   **職責**: 作為一個簡單的協調器，按順序執行數據獲取和數據處理流程。
*   **流程**:
    1.  設定 `PYTHONPATH` 以確保能正確導入專案內的模組。
    2.  (可選) 刪除舊的 `data/raw_data.db` 和 `data/features.db` 以確保每次運行都是從乾淨的狀態開始。
    3.  定義要處理的股票代號列表 (例如 "AAPL", "GOOGL", "MSFT") 和日期範圍。
    4.  循環遍歷股票代號列表：
        *   調用 `python services/fetcher_service.py` 為當前股票獲取數據。
        *   調用 `python services/processor_service.py` 為當前股票處理數據並計算特徵。
*   **靈活性**: 股票代號和日期範圍可以直接在腳本中修改，方便測試不同標的。

## 3. 配置文件 (`config/`) 分析

此子專案的 `config/` 目錄下包含三個主要的 YAML 配置文件，它們為專案提供了靈活性和可配置性，並支持了更高級的功能模組（如 `api_fetcher.py` 和 `data_validator.py`）。

*   **`project_config.yaml`**:
    *   **作用**: 專案級別的全局配置文件。
    *   **內容**:
        *   `project_name`, `version`
        *   `logging`: 日誌級別 (`level`)。
        *   `timezone`: 時區設定 (例如 `Asia/Taipei`)。
        *   `api_keys_env_names`: 將內部使用的 API Key 名稱 (如 `FMP_API_KEY`) 映射到實際的環境變量名稱 (如 `API_KEY_FMP`)。這是由 `config_loader.py` 處理，用於從環境中安全加載API金鑰。
        *   `data_fetching`: 數據獲取相關參數，如 `cache_directory`, `cache_expire_after_seconds`, `max_retries`, `backoff_factor`。這些參數主要由 `api_fetcher.py` 中的 `UnifiedAPIFetcher` 使用。
        *   `financial_stress_model_params` (雖然此專案核心服務未使用，但配置存在): 用於計算金融壓力指數的參數，包括各成分的窗口期 (`window`)、權重 (`weights`)、平滑參數 (`smoothing_alpha`) 以及壓力等級的閾值 (`thresholds`)。
        *   `prompt_engineering` (雖然此專案核心服務未使用，但配置存在): AI Prompt工程相關配置，如AI服務提供商 (`ai_provider`)、模型名稱 (`model_name`)、溫度 (`temperature`) 等。
        *   `backtesting_config_path` (雖然此專案核心服務未使用，但配置存在): 指向回測設定檔的路徑。
*   **`api_endpoints.yaml`**:
    *   **作用**: 為 `api_fetcher.py` 中的 `UnifiedAPIFetcher` 提供數據源和指標的詳細配置。
    *   **結構**: 頂層鍵為 `indicators`，其下是各個想要獲取的金融指標的內部名稱 (例如 `VIX_YF`, `US_TREASURY_10Y_FRED`)。
    *   每個指標可以配置一個或多個 `api_provider` 作為備選數據源。
    *   每個 `api_provider` 條目包含：
        *   `type`: API提供商的類型 (例如 `fred`, `yfinance`)，對應 `api_fetcher.py` 中的適配器。
        *   `series_id` 或 `ticker`: 在該API提供商處的具體指標代號。
        *   `data_column` (可選): 如果API返回多列數據，指定要提取的列名。
        *   `priority` (可選): 獲取該指標時此數據源的優先級。
*   **`dq_rules.yaml`**:
    *   **作用**: 為 `data_validator.py` 中的 `DataValidator` 提供數據質量 (DQ) 驗證規則。
    *   **結構**: 頂層鍵為 `metrics`，其下是各個需要進行DQ檢查的指標的內部名稱 (應與 `api_endpoints.yaml` 中的指標名或 DataFrame 中的欄位名對應)。
    *   每個指標可以配置多條DQ規則，每條規則包含：
        *   `rule_type`: 規則類型，如 `range_check` (數值範圍檢查), `spike_check` (尖峰/突變檢查), `not_null_check` (非空檢查), `stale_check` (數據過期檢查)。
        *   `severity`: 違反規則時的嚴重等級 (`ERROR` 或 `WARNING`)。
        *   以及該規則類型特定的參數，例如：
            *   `range_check`: `min_value`, `max_value`。
            *   `spike_check`: `window` (滾動窗口大小), `threshold_std` (標準差倍數閾值)。
            *   `not_null_check`: (無額外參數)。
            *   `stale_check`: `max_days_stale` (數據允許的最大過期天數)。

## 4. 核心庫與工具分析

除了 `services/` 目錄下的核心服務外，專案根目錄還包含一系列 Python 腳本，它們提供了更通用的功能或用於特定分析任務。

*   **`api_fetcher.py` (`UnifiedAPIFetcher`)**:
    *   **設計思想**: 採用適配器模式 (Adapter Pattern)，旨在提供一個統一的接口來從不同的金融數據API獲取數據。
    *   **核心類**:
        *   `BaseAdapter`: 定義了所有數據源適配器的通用接口，包括 `_fetch_data` (由子類實現以處理特定API的請求細節) 和 `get_data` (包含通用的重試邏輯，使用 `tenacity` 庫)。
        *   `FredAdapter`: 實現了從 FRED 獲取數據的邏輯。它支持兩種模式：如果提供了API金鑰，則使用 `fredapi` 庫；如果未提供金鑰，則回退到使用 `market_data_fred.py` 中實現的直接CSV下載方法。
        *   `YFinanceAdapter`: 實現了從 Yahoo Finance 獲取數據的邏輯，使用 `yfinance` 庫。
        *   `UnifiedAPIFetcher`: 這是主要的外部接口類。它在初始化時讀取 `api_endpoints.yaml` 和 `project_config.yaml` (主要獲取API金鑰和快取設置)。其 `get_indicator_data` 方法會根據請求的指標名稱，從 `api_endpoints.yaml` 中查找配置的數據源，並按優先級嘗試使用對應的適配器獲取數據。
    *   **快取機制**: `UnifiedAPIFetcher` 實現了一個基於 Parquet 檔案的本地快取系統。獲取到的數據會被存儲在 `project_config.yaml` 中 `data_fetching.cache_directory` 指定的目錄下，並根據 `cache_expire_after_seconds` 設定過期時間。
    *   **錯誤處理**: 定義了一組自定義異常 (`APIFetchError`, `APIAuthenticationError`, `APIRateLimitError`, `APIDataNotFoundError`)，用於更精確地表示API請求過程中可能發生的錯誤。
*   **`data_validator.py` (`DataValidator`)**:
    *   **職責**: 根據 `dq_rules.yaml` 中定義的規則，對輸入的 Pandas DataFrame 中的特定指標列進行數據質量驗證。
    *   **核心方法**: `validate_data(df, metric_name)`。
    *   **驗證規則實現**: 內部包含 `_apply_range_check`, `_apply_spike_check`, `_apply_not_null_check`, `_apply_stale_check` 等私有方法來執行具體的檢查邏輯。
    *   **輸出**: 該驗證器會直接修改輸入的 DataFrame，在其中添加兩列：`{metric_name}_dq_status` (記錄該行數據的DQ狀態，如 "PASS", "ERROR_RANGE", "WARNING_SPIKE") 和 `{metric_name}_dq_notes` (記錄詳細的DQ問題描述)。
*   **`config_loader.py`**:
    *   **職責**: 負責安全地加載 `project_config.yaml` 文件，並特別處理其中的 `api_keys_env_names` 部分。
    *   **API金鑰處理**: 它會遍歷 `api_keys_env_names` 中定義的內部API金鑰名到環境變量名的映射，嘗試從環境中讀取實際的API金鑰，並將其填充到返回的配置字典中的 `runtime_api_keys` 鍵下。如果環境變量未設置，則對應的API金鑰值為 `None`。
*   **`market_data_yfinance.py` 和 `market_data_fred.py`**:
    *   **`market_data_yfinance.py`**:
        *   提供了更底層的、直接與 `yfinance` 庫交互的功能。
        *   `fetch_yfinance_data_stable`: 封裝了數據獲取、錯誤處理（包括對特定`yfinance`異常的捕獲）、重試邏輯，以及在獲取小時線數據失敗時自動降級嘗試獲取日線數據的機制。
        *   `fetch_continuous_taifex_futures`: 專門用於獲取和拼接台灣加權股價指數期貨 (TAIFEX) 的連續近月合約數據。它包含了計算合約到期日、生成合約代碼、循環獲取各月份數據並使用向後調整法 (backward adjusting) 進行價格拼接的複雜邏輯。
        *   `check_taifex_options_support_yfinance`: 用於檢查 `yfinance` 對台指選擇權歷史數據的支持情況。
        *   **快取**: 利用 `requests-cache` 建立了一個名為 `yfinance_cache.sqlite` 的本地快取，並通過 `yf.set_session()` 讓 `yfinance` 的所有請求都通過這個快取會話。
    *   **`market_data_fred.py`**:
        *   提供了不依賴官方 `fredapi` 庫（即無需API金鑰）的 FRED 數據獲取方法。
        *   `fetch_fred_series_no_key`: 通過直接構造 FRED 圖表數據的 CSV 下載 URL (`https://fred.stlouisfed.org/graph/fredgraph.csv?id=SERIES_ID&cosd=START_DATE&coed=END_DATE`) 來獲取數據。
        *   **數據處理**: 解析下載的 CSV 內容，將日期列設為索引，並將數據列轉換為數值型（特別處理了 FRED CSV 中用 `.` 表示缺失值的情況）。
        *   **快取**: 同樣使用 `requests-cache` 建立了一個名為 `fred_data_cache.sqlite` 的獨立本地快取。
*   **`main_analyzer.py`**:
    *   **定位**: 一個較為複雜的分析流程協調器，更像是一個具體的應用案例而非通用庫。
    *   **核心流程**:
        1.  掃描輸入目錄 (`IN_Source_Reports/`) 中的文本報告檔案（預期檔名格式為 `YYYY年第WW週*.txt`）。
        2.  使用 `file_utils.py` 中的清單機制 (`processed_files_manifest.json`) 進行增量處理，只處理新的或內容已更改的報告。
        3.  從檔名中解析出報告的年份和週次，並計算對應的日期範圍。
        4.  讀取報告的文本內容（質化數據）。
        5.  調用 `market_data_yfinance.py` 和 `market_data_fred.py` 中的函數，獲取該報告日期範圍內的一系列預定義市場指標和宏觀經濟數據（量化數據），例如 S&P 500, 台股指數, 各類債券ETF (TLT, HYG, LQD), VIX指數, 美元兌台幣匯率, FRED的VIX, 10年期美債利率, 聯邦基金利率, 以及台指期貨連續合約數據。
        6.  將質化報告內容和獲取的量化市場數據整合成一個結構化的 Prompt 文本。
        7.  將生成的 Prompt 文本保存到輸出目錄 (`OUT_Processed_Prompts/`)，以報告的原始檔名（加上 `.prompt.txt` 後綴）命名。
*   **`api_deep_tester.py`**:
    *   **定位**: 一個全面的API測試工具，用於評估多種金融數據API的可用性、數據質量和限制。
    *   **支持的API**: 涵蓋了 FRED, Alpha Vantage, Finnhub, News API, FMP, Polygon.io, ECB, World Bank, OECD, 美國財政部 FiscalData, BEA, BLS, 台灣證交所 (TWSE), CoinGecko, yfinance 等多個數據源。
    *   **測試維度**:
        *   **連通性 (Connectivity)**
        *   **數據獲取 (Data Retrieval)**: 針對特定指標或股票代號。
        *   **時間週期支持 (Time Period Support)**: 日線、週線、月線等。
        *   **歷史數據長度 (Historical Data Length)**
        *   **速率限制 (Rate Limiting)**: 通過連續請求測試。
    *   **配置與執行**:
        *   依賴 `.env` 文件加載各API所需的金鑰。
        *   包含詳細的日誌記錄和錯誤捕獲機制。
        *   可以選擇性地運行針對特定API的測試。
    *   **輸出**: 生成詳細的測試報告文本文件（例如 `sample_api_test_report.txt`），記錄每個API測試的結果、成功獲取的數據樣本、遇到的錯誤等。
*   **`local_data_fetcher.py`**:
    *   **定位**: 一個簡單的本地數據獲取腳本。
    *   **功能**: 從 FRED (使用 `fredapi`) 和 yfinance 獲取一組預定義的經濟指標和股票數據，並將它們分別保存為 CSV 檔案到 `local_sample_data/` 目錄下。
    *   **目的**: 主要用於快速生成一些本地的樣本數據，可能用於離線開發、測試或演示。
    *   **依賴**: 需要在環境中設定 `FRED_API_KEY` 才能成功運行 FRED 部分的數據獲取。
*   **`file_utils.py`**:
    *   **職責**: 提供文件處理相關的輔助功能，核心是管理一個已處理文件的清單 (manifest)。
    *   **清單機制**:
        *   維護一個 JSON 檔案 (`processed_files_manifest.json`)，記錄每個已處理檔案的原始路徑和其內容的 SHA-256 雜湊值。
        *   `is_file_processed_and_unchanged(filepath, manifest_data)`: 檢查給定檔案是否已在清單中，並且其當前內容雜湊值與記錄的雜湊值是否一致。
        *   `update_manifest(filepath, manifest_data)`: 如果檔案是新的或內容已更改，則更新清單中該檔案的雜湊值。
    *   **用途**: 被 `main_analyzer.py` 用於實現對輸入報告的增量處理，避免重複處理未更改的檔案。

## 5. 測試 (`tests/`) 分析

專案包含了一套使用 `pytest` 編寫的單元測試，主要針對 `services/` 目錄下的核心服務。

*   **`conftest.py`**:
    *   **作用**: 為測試提供共享的 fixtures (測試固件)。
    *   **`mock_raw_data` fixture**:
        *   生成一個模擬的 Pandas DataFrame，包含 'Date', 'Open', 'High', 'Low', 'Close', 'Volume' 等欄位，用於模擬從 yfinance API 獲取的原始股價數據。
        *   注意：此 fixture 生成的 DataFrame 的欄位名已經是小寫的，這與 `fetcher_service.py` 中對 yfinance 原始返回數據進行小寫處理後的格式一致。
*   **`test_fetcher_service.py`**:
    *   **測試目標**: `services/fetcher_service.py` 中的 `save_to_duckdb` 函數。
    *   **測試方法**:
        *   使用 `mock_raw_data` fixture 生成測試數據。
        *   使用 `mocker.patch` 來模擬 `yfinance.Ticker().history()` 方法，使其返回 `mock_raw_data`，從而避免實際的網路請求。
        *   使用 `pytest` 提供的 `tmp_path` fixture 創建一個臨時的資料庫文件路徑。
        *   調用 `save_to_duckdb` 將模擬數據寫入臨時資料庫。
        *   再從該臨時資料庫中讀取剛寫入的表，驗證表名是否正確（股票代號小寫），以及讀取出的數據內容和欄位名是否與預期一致。
*   **`test_processor_service.py`**:
    *   **測試目標**: `services/processor_service.py` 中的 `process_data` 函數。
    *   **測試方法**:
        *   同樣使用 `mock_raw_data` fixture 生成輸入數據，並先將其存入一個臨時的 "原始" DuckDB 資料庫中。
        *   調用 `process_data` 處理這個臨時的原始數據庫，將結果輸出到另一個臨時的 "特徵" DuckDB 資料庫。
        *   從 "特徵" 資料庫中讀取處理後的數據表。
        *   驗證：
            *   是否正確計算了 `ma20` 和 `ma60` 移動平均線。
            *   由於計算移動平均線和隨後的 `.dropna()` 操作，處理後的數據行數是否符合預期（例如，對於20日MA，行數應比原始數據少19行）。
            *   使用 `numpy.isclose` 來比較計算出的移動平均線浮點數值是否與手動計算的預期值在允許的誤差範圍內相符。

測試覆蓋了核心服務的數據寫入、讀取和基本計算邏輯，採用了 mock 和臨時文件系統資源，是良好的單元測試實踐。

## 6. Jupyter Notebooks 分析

專案中包含兩個主要的 Jupyter Notebooks，它們在數據分析和原型驗證中扮演了重要角色。

*   **`一級交易pro.ipynb`**:
    *   **定位**: 一個功能非常全面的、用於計算和可視化「金融壓力指數」的交互式分析工具和報告生成器。它整合了數據獲取、處理、指標計算、可視化以及參數化報告生成的完整流程。
    *   **核心流程與功能**:
        1.  **環境初始化與配置 (Cell 1)**:
            *   導入必要的庫 (pandas, numpy, matplotlib, seaborn, plotly, yfinance, fredapi, ipywidgets 等)。
            *   定義 `PROJECT_CONFIG` 全局字典，硬編碼了多個API的URL模板、指標代號、計算權重、圖表顏色、字體路徑等大量配置信息。**注意：API金鑰 (如FRED_API_KEY) 也直接硬編碼在此處，這在生產環境中是不安全的。**
            *   定義 `EXECUTION_TRACKER` 字典用於記錄各階段耗時。
            *   設定時區為 `Asia/Taipei`。
            *   使用 `ipywidgets` 創建日期選擇器，允許用戶交互式地選擇分析的開始和結束日期，並將選擇結果保存到 `date_config.json` 文件中。
        2.  **核心繪圖函式定義 (Cell 2)**:
            *   `plot_results`: 生成一個 5x2 的網格圖，包含壓力指數本身、其成分指標（如VIX、MOVE、利差、SOFR偏差、交易商持倉）、以及相關市場數據（如S&P500、美元指數）的時間序列圖。支持將圖表顯示、保存為圖片或轉換為Base64編碼。
            *   `plot_gemini_gauge_mpl`: 使用 Matplotlib 創建一個類似儀表盤的圖形，顯示當前壓力指數的數值和其所處的壓力等級（正常、中度、高度、極度）。
            *   `plot_trend_colored`: 繪製壓力指數近期的趨勢圖，並根據其斜率使用不同顏色標示上升、下降或盤整。
        3.  **日期處理與API驗證 (Cell 3)**:
            *   從 `date_config.json` 或 `PROJECT_CONFIG` 加載日期範圍。
            *   使用 `PROJECT_CONFIG` 中硬編碼的 FRED API Key 初始化 `fredapi.Fred` 物件，並執行一次測試請求以驗證金鑰的有效性。
        4.  **數據獲取 (Cells 4, 5, 6)**:
            *   **FRED數據 (Cell 4)**: 使用 `fredapi` 獲取一系列宏觀經濟指標 (SOFR, DGS10, DGS2, 美聯儲隔夜逆回購操作量 RRPONTSYD, VIXCLS, 美聯儲總資產 WALCL, 商業銀行準備金 WRESBAL)。對獲取的數據進行頻率對齊 (到業務日)、向前填充缺失值。
            *   **Yahoo Finance數據 (Cell 5)**: 使用 `yfinance` 獲取 MOVE 指數 (債券市場波動率) 和可選的長期限美國公債ETF (如 TLT) 的價格數據。
            *   **NY Fed一級交易商持倉數據 (Cell 6)**: 這部分邏輯較為複雜。它從紐約聯儲網站下載多個按年份組織的 Excel 文件 (Primary Dealer Statistics)，解析這些文件，提取特定工作表 (通常是 "UST Positions" 或類似名稱) 中的數據，篩選出美國公債（Treasury Coupons, Bills, FRNs, TIPS）的淨持倉數據，並將其加總（**警告：腳本中提到此處可能混合了 SOMA Broker-Dealer Net Purchases (SBN/SBP) 和 Primary Dealer Positions，需要仔細核對數據定義**）。最終匯總為每週的總淨持倉序列。
        5.  **數據合併與預處理 (Cell 7)**: 將從 FRED, Yahoo Finance, NY Fed 獲取的所有數據序列合併到一個名為 `merged_data_df` 的 Pandas DataFrame 中。進行欄位重命名 (例如，將 VIXCLS 重命名為 VIX_FRED)，並對低頻數據（如每週的交易商持倉）應用向前填充 (`.ffill()`) 以匹配更高頻的數據。
        6.  **衍生指標計算與壓力指數構建 (Cell 8)**:
            *   基於合併後的數據計算一系列衍生指標，例如：
                *   10年期與2年期美債利差 (`Spread_10Y_2Y`)。
                *   SOFR利率與其90天移動平均線的偏差 (`SOFR_Deviation_from_MA`)。
                *   NY Fed 一級交易商公債淨持倉與美聯儲準備金的比率 (`NYFED_Dealer_NetPosition_to_FedReserve`)。
            *   對這些衍生指標以及部分原始指標 (如 VIX_FRED, MOVE_Index) 計算其在過去一年滾動窗口內的百分位排名 (percentile rank)。
            *   根據 `PROJECT_CONFIG` 中為每個成分指標設定的權重 (`weights_stress_index`)，對這些百分位排名進行加權平均，得到「原始壓力指數」。
            *   對原始壓力指數應用指數移動平均 (EMA) 進行平滑處理，得到最終的「平滑壓力指數」。
            *   (可選) 計算平滑壓力指數的 MACD 指標。
            *   所有結果存儲在 `final_df` DataFrame 中。
        7.  **結果可視化 (Cells 9, 10)**:
            *   調用 `plot_results` 函式，生成並顯示包含壓力指數及其各成分指標的 5x2 時間序列圖表。
            *   顯示 `final_df` 的尾部數據預覽。
            *   調用 `plot_gemini_gauge_mpl` 函式，生成並顯示壓力指數儀表盤。
            *   調用 `plot_trend_colored` 函式，生成並顯示壓力指數的近期趨勢圖。
        8.  **文字分析結果展示 (Cell 11)**: 根據最新的平滑壓力指數值，提供對應的市場壓力等級描述，並與歷史上特定事件發生時的壓力指數值進行對比，給出簡要的市場情境分析。
        9.  **互動式報告參數設定 (Cell 12)**: 使用 `ipywidgets` (Dropdown, DatePicker, Checkbox) 創建一個用戶界面，允許用戶選擇生成HTML報告的日期範圍、是否包含各類圖表等參數。**但實際的HTML報告生成邏輯在此Notebook中並未完全實現或展示。**
        10. **除錯與測試單元 (Cell_Debug_Consolidated, Cell_Test_YFinance)**: 包含一些用於開發過程中進行除錯和單獨測試特定功能 (如YFinance數據獲取) 的代碼片段。
*   **`risk_assessment/main.ipynb`**:
    *   **定位**: 一個更早期的、結構相對簡單的原型或概念驗證 Notebook。
    *   **功能**:
        *   導入位於同目錄 `commander.py` 中的 `Commander` 類。
        *   實例化 `Commander` (它會加載 `risk_assessment/config.yaml`，並根據配置初始化 `FredFetcher` 和 `SQLiteRepository`)。
        *   調用 `commander.generate_report()` 方法。
    *   **`commander.py` 內容**:
        *   `Commander` 類在其 `__init__` 中：
            *   加載配置。
            *   根據配置實例化一個數據庫倉儲 (`SQLiteRepository`) 和一個數據獲取器 (`FredFetcher`)。
        *   `generate_report()` 方法：
            *   定義一個股票代號列表 (硬編碼為 "AAPL", "MSFT")。
            *   循環獲取這些股票的數據 (但 `FredFetcher` 是用於FRED序列的，此處邏輯可能不匹配或僅為示意)。
            *   將獲取到的數據 (DataFrame) 保存到 SQLite 資料庫。
    *   **`modules/` 目錄**:
        *   `database/database_interface.py` 和 `database/sqlite_repository.py`: 定義了數據庫操作的接口和 SQLite 的具體實現。
        *   `fetchers/fetcher_interface.py` 和 `fetchers/fred_fetcher.py`: 定義了數據獲取器的接口和 FRED 獲取器的具體實現 (使用 `fredapi` 庫)。
    *   **總體評價**: 此 Notebook 和相關的 `risk_assessment/` 目錄下的代碼，展示了一個更早期的、基於接口和依賴注入思想的數據處理流程原型，但其功能相對簡單，且與 `一級交易pro.ipynb` 的複雜度和目標不完全一致。

## 7. 當前問題與改進建議

(此部分與主報告中的 "7.2. 主要問題列表" 和 "7.4. 改進建議" 重疊較多，此處針對 `SP_DATA-feat-financial-data-pipeline` 子專案的特性做一些補充和聚焦)

### 7.1. 當前主要問題 (聚焦於此子專案內部)

*   **核心流程依賴問題**:
    *   `src/main.py` 流程因 `data_master.py` 遺失和 `src/connectors/__init__.py` 被修改而無法正常執行數據獲取和壓力指數計算。這是影響此子專案核心功能發揮的最大障礙。
*   **子專案間的隔離與協同不足**:
    *   `MyTaifexDataProject` 和 `AI_Assisted_Historical_Backtesting` 雖然功能強大，但它們與 `src/main.py` 代表的核心數據流以及 `panoramic-market-analyzer` 之間，在數據共享、配置統一、代碼複用方面顯得較為隔離。
    *   例如，`AI_Assisted_Historical_Backtesting` 內部有自己的一套數據連接器，這與 `src/connectors/` 中的共享庫存在潛在的冗餘。
*   **配置文件管理的複雜性**:
    *   如前所述，多個 `config.yaml`, `config.py`, JSON配置文件 (`format_catalog.json`, `schemas.json`), SQL DDL (`schema.sql`) 分散在專案各處，增加了理解和維護的難度。需要一個更清晰、統一的配置管理策略。
*   **AI模型選型與配置的不一致**:
    *   `src/main.py` (通過 `src/configs/project_config.yaml`) 指向使用 Claude AI。
    *   `AI_Assisted_Historical_Backtesting` 則明確設計使用 Llama 3 (通過 Ollama)。
    *   這種不一致性使得AI相關功能的整合和未來發展方向不明確。
*   **舊版本數據管道的狀態**:
    *   `src/data_pipeline_v15/` 和 `src/sp_data_v16/` 的存在意義和當前可用性需要澄清。如果已廢棄，應考慮移除或歸檔。
*   **`一級交易pro.ipynb` 的生產化問題**:
    *   該 Notebook 雖然功能強大，但其分析邏輯、配置（尤其是API金鑰硬編碼）和執行方式高度依賴Jupyter環境，不利於自動化和生產部署。其核心算法和數據處理步驟應被重構為可獨立調用和測試的Python模組。
    *   NY Fed數據處理部分存在潛在的數據定義混淆問題，需要仔細審查和修正。

### 7.2. 針對此子專案的改進建議

1.  **修復核心數據流程 (`src/main.py`)**:
    *   **恢復 `DataMaster`**: 這是首要任務。應根據 `tests/test_data_master.py` 的接口提示，盡快找到或重寫 `src/data_master.py`，使其能夠正確加載和調度 `src/connectors/` 中的連接器。
    *   **修正 `src/connectors/__init__.py`**: 將此文件恢復到能夠正常導入所有連接器並使 `get_connector_class` 函數按預期工作的狀態。
2.  **整合與重構數據連接器**:
    *   對比 `src/connectors/` 和 `AI_Assisted_Historical_Backtesting/src/connectors/` 中的同類連接器，消除冗餘，建立一套統一的、可被所有子系統共享的數據連接器庫。
    *   確保所有連接器都通過 `DataMaster` (或類似的中心化服務) 進行調用和管理。
3.  **統一配置管理**:
    *   設計一個全局的、分層的配置方案。例如，一個主 `config.yaml` 存放通用配置，各子專案或特定環境可以有選擇地覆蓋或擴展這些配置。
    *   將所有數據庫 Schema 定義（`config/schemas.json`, `AI_Assisted_Historical_Backtesting/config/schema.sql`）整合或建立清晰的關聯。
    *   API金鑰等敏感信息必須從所有配置文件中移除，統一使用環境變數或專用的秘密管理工具。
4.  **明確各子專案的定位與數據接口**:
    *   **`MyTaifexDataProject`**: 可作為一個獨立的ETL服務，專門負責TAIFEX數據的獲取、處理和供給。其產出的 `processed_data.duckdb` 可以作為下游分析（如AI回測）的標準化數據源之一。
    *   **`AI_Assisted_Historical_Backtesting`**: 定位為核心的AI分析與回測引擎。應明確其數據輸入來源（是依賴 `DataMaster` 獲取，還是直接消費 `MyTaifexDataProject` 的產出，或其他方式）。
    *   **`src/main.py` 流程**: 在 `DataMaster` 恢復後，它可以作為一個通用的宏觀經濟與市場數據的獲取、處理和初步分析（如壓力指數計算）的核心服務。
    *   **`panoramic-market-analyzer`**: 如果其功能有獨特性，可以作為一個獨立的分析工具；如果功能與其他部分重疊較多，考慮將其核心分析邏輯整合到更通用的服務中。
5.  **AI策略的統一與抽象**:
    *   如果專案需要支持多種AI模型，應設計一個通用的AI代理接口 (`AI Agent Interface`)，使得上層應用（如回測引擎、報告服務）可以透明地與不同的AI模型實現（Llama 3, Claude, etc.）進行交互。
6.  **生產化 `一級交易pro.ipynb`**:
    *   將其核心的數據處理、指標計算、壓力指數構建邏輯重構為獨立的、可測試的Python模組和函數。
    *   將其可視化部分分離出來，可以作為「報告生成與可視化服務」的一部分。
    *   參數化其執行過程，使其可以被自動化腳本或工作流引擎調用。
7.  **強化日誌與監控**:
    *   確保所有主要的數據處理流程和服務都使用統一的、結構化的日誌記錄標準。
    *   考慮引入簡單的監控機制來追蹤關鍵數據管道的運行狀態和健康度。

通過解決上述核心問題並實施改進建議，`SP_DATA-feat-financial-data-pipeline` 子專案有潛力被打造成為一個強大、穩定且易於擴展的金融智能平台。

---
報告完畢。
