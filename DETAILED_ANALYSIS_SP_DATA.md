# 專案分析報告：SP_DATA-feat-financial-data-pipeline

**報告日期**: 2024年6月27日

**分析員**: Jules (AI Software Engineer)

## 1. 專案整體概覽

`SP_DATA-feat-financial-data-pipeline` 子專案是一個功能豐富、結構較為複雜的金融數據分析與處理平台，旨在整合從數據獲取、清洗、轉換、存儲到高級分析（如指標計算、AI輔助的歷史回溯和交易策略生成）的全鏈路功能。專案由多個相對獨立的子系統和數據管道構成，每個部分都有其特定的數據來源、處理邏輯和目標用戶場景。

主要的子系統和功能模組包括：

*   一個核心的數據處理流程 (原由 `main.py` 或 `src/main.py` 驅動，但目前存在問題)，設計用於從多種財經API獲取數據，計算自定義的金融壓力指數，並生成市場簡報。
*   一個高度成熟的、專用於處理台灣期貨交易所 (TAIFEX) 數據的管道 (**`MyTaifexDataProject/`**)。
*   一個AI輔助的歷史回溯與交易策略生成系統 (**`AI_Assisted_Historical_Backtesting/`**)，利用大型語言模型 (LLM) 進行市場分析與決策模擬。
*   一個相對獨立的、基於yfinance的市場分析工具 (**`panoramic-market-analyzer/`**)。
*   此外，專案中還包含了多個版本的數據管道 (如 `data_pipeline.py` (舊版), `src/sp_data_v16/` 的相關組件)，以及一個共享的數據連接器庫 (位於 `src/connectors/` 但目前初始化受限)和多個配置文件。

儘管專案展現了強大的功能潛力，但目前存在一些核心問題，顯著影響了部分功能的正常運行。最主要的問題包括關鍵模組 `data_master.py` 的遺失，以及共享數據連接器庫的初始化腳本 `src/connectors/__init__.py` 被置於一個特殊的診斷模式，導致多數數據連接器無法被正常加載。此外，專案的配置文件管理也顯得有些分散和混亂。這些問題共同阻礙了數據在不同子系統間的有效流動和整合。

## 2. 核心數據處理流程 (`src/main.py` 及相關) 分析

位於 `src/main.py` (或專案根目錄的 `main.py`，根據上下文判斷應為前者) 的腳本設計為一個核心的數據處理和分析流程，其主要目標是：

*   從多種金融數據源 (如 FRED, NYFed, YFinance, FinMind) 獲取宏觀經濟數據和市場數據。
*   對獲取的數據進行存儲和管理 (使用 DuckDB，由 `src/database/database_manager.py` 管理)。
*   利用 `src/engine/indicator_engine.py` 計算特定的金融指標，尤其是「交易商壓力指數 (Dealer Stress Index)」。
*   基於計算結果生成 JSON 格式的市場簡報。

### 2.1. 核心組件

*   **`DataMaster` (預期存在於 `src/data_master.py`)**:
    *   **角色**: 設計為一個關鍵的數據獲取抽象層，負責統一管理和調度各種位於 `src/connectors/` 下的數據連接器。它應根據配置文件中的 API 優先級和類型，動態選擇合適的連接器來獲取數據，並可能提供數據緩存和回退機制。
    *   **現狀**: 此 `data_master.py` 檔案目前在程式庫中遺失。雖然 `tests/test_data_master.py` 的存在及其內容暗示了 `DataMaster` 類的設計和功能，但其實際代碼的缺失是導致 `src/main.py` 流程無法正常運行的首要原因。測試文件表明 `DataMaster` 應位於 `src/data_master.py`。
*   **`DatabaseManager` (位於 `src/database/database_manager.py`)**:
    *   **角色**: 負責與 DuckDB 資料庫的交互，包括連接、數據的批量插入/更新 (`bulk_insert_or_replace`) 以及查詢操作。
    *   **配置**: 資料庫的路徑（例如 `data/financial_data.duckdb`，此路徑相對於 `src/main.py` 執行時的PROJECT_ROOT）和其他相關參數通過傳遞給其實例的 `config` 物件來設定，該 `config` 物件源於 `src/main.py` 加載的設定檔。
*   **`IndicatorEngine` (位於 `src/engine/indicator_engine.py`)**:
    *   **角色**: 封裝了計算各種技術指標和衍生因子的邏輯。其核心功能之一是根據設定檔 (`src/configs/project_config.yaml` 中的 `indicator_engine_params`) 中定義的權重和閾值，計算「交易商壓力指數」。
    *   **輸入**: 接收從 `DatabaseManager` 獲取的宏觀數據和市場數據作為輸入。
*   **數據連接器 (位於 `src/connectors/`)**:
    *   **角色**: 提供對不同金融數據 API（如 FRED, NYFed, YFinance, FinMind, Alpha Vantage 等）的底層訪問接口。每個連接器封裝了特定 API 的請求邏輯、認證處理和數據格式轉換。
    *   **現狀**: `src/connectors/__init__.py` 目前被修改為一個特殊的「NYFed 診斷模式」，導致 `get_connector_class` 函數在被 `DataMaster`（如果存在）調用時，只能成功返回 `NYFedConnector`。任何加載其他連接器的嘗試都會失敗。

### 2.2. 配置依賴

`src/main.py` 的運行高度依賴設定檔。

*   它通過 `load_config()` 函數載入設定檔。該函數名義上預期從專案根目錄讀取 `config.yaml`。
*   然而，從 `src/main.py` 的上下文和 `src/configs/project_config.yaml` 的內容來看，後者 (`src/configs/project_config.yaml`) 才是實際驅動 `src/main.py` 流程的主要設定檔。它包含了 API 端點、目標獲取的指標列表、數據獲取的時間範圍、指標引擎的詳細參數以及 AI 服務（Claude模型）的配置。
*   根目錄的 `config.yaml` 主要用於 `MyTaifexDataProject`。這種配置分離可能導致混淆。

### 2.3. 當前問題

*   **`data_master.py` 遺失**: 這是最嚴重的問題，直接導致 `src/main.py` 因無法導入 `DataMaster` 類而失敗。沒有 `DataMaster`，整個數據獲取和調度邏輯都無法執行。
*   **`src/connectors/__init__.py` 被修改**: 即使 `data_master.py` 被恢復，`src/connectors/__init__.py` 的「NYFed 診斷模式」也會阻止 `DataMaster` 初始化和使用除 `NYFedConnector` 以外的任何連接器。這將使得 `src/main.py` 無法獲取絕大部分預期數據（如 FRED, YFinance, FinMind 的數據），從而導致後續的指標計算和市場簡報生成功能嚴重受限或失敗。

## 3. `MyTaifexDataProject/` (源於 `src/taifex_pipeline/`) 分析

`MyTaifexDataProject/` 是一個設計精良、功能完備的自動化數據管道，專門用於處理來自台灣期貨交易所 (TAIFEX) 的大量且格式多樣的公開數據。其核心目標是實現數據的高效獲取、可靠的格式識別、精確的數據清洗與轉換，並將最終結果存儲於結構化的分析型資料庫中。

### 3.1. 核心設計 (基於 `Program_Development_Project.txt`)

該子專案的設計文檔 (`Program_Development_Project.txt`) 詳細闡述了其核心架構和運作原理：

*   **格式指紋目錄 (Format Fingerprint Catalog)**:
    *   通過分析檔案標頭（前N行內容進行正規化處理後計算 SHA256 雜湊值）為每種獨特的檔案格式生成「指紋」。
    *   一個中央化的 JSON 設定檔 (`config/format_catalog.json`) 將這些「指紋」映射到詳細的「處理配方」，配方中包含目標資料庫表名、Pandas 解析參數 (`parser_config`)、對應的數據清洗函數名稱 (`cleaner_function`) 以及必要的欄位列表 (`required_columns`)。
*   **兩階段自動化管線 (Two-Stage Automated Pipeline)**:
    1.  **第一階段 - 汲取 (Ingestion)**: 此階段的核心原則是「極速、穩定、零解析」。它負責掃描指定的來源資料夾，對於新檔案，計算其內容的 SHA256 雜湊值以避免重複處理，然後將其未經修改的原始二進位內容完整存入一個名為 `raw_lake.db` 的 DuckDB 資料庫的 `raw_files` 表中。同時，在 `manifest.db` 資料庫（`file_processing_log` 表）中登記該檔案的元數據和狀態 (例如 `RAW_INGESTED`)。
    2.  **第二階段 - 轉換 (Transformation)**: 此階段的核心原則是「智慧、平行、可重跑」。它查詢 `manifest.db` 中狀態為 `RAW_INGESTED`（或特定重跑狀態如 `QUARANTINED`）的檔案。使用 `ProcessPoolExecutor` 將任務分配給所有可用的 CPU 核心進行平行處理。每個處理單元從 `raw_lake.db` 讀取原始檔案內容，計算其格式指紋，查找對應的處理配方，然後使用配方中的配置進行數據解析 (Pandas)、調用指定的清洗函數進行數據轉換和驗證，最後將乾淨的 DataFrame 載入到最終的目標資料庫 (例如 `processed_data.duckdb`) 中對應的表格。處理完成後更新 `manifest.db` 中的檔案狀態 (如 `TRANSFORMATION_SUCCESS`, `QUARANTINED`, `TRANSFORMATION_FAILED`) 及相關元數據。
*   **狀態管理與審計 (`manifest.db`)**: `file_processing_log` 表不僅追蹤每個檔案的處理狀態，還記錄了詳細的審計信息，如檔案雜湊值、原始路徑、格式指紋、各階段時間戳、目標表名、處理行數、錯誤訊息以及管線執行ID，確保了數據處理流程的完全透明和可追溯性。
*   **資源最大化與錯誤處理**: 管道設計考慮了 CPU 核心的動態偵測和充分利用，以及對大檔案的記憶體管理。對於無法識別格式或處理失敗的檔案，會將其隔離 (`QUARANTINED`)，並提供了手動註冊新格式 (`scripts/register_format.py`) 和重處理隔離檔案的機制。
*   **日誌系統**: 採用雙軌制日誌，包括便於操作者監控的即時主控台報告，以及供開發者和機器分析的詳細結構化 JSON 日誌檔案。

### 3.2. 原始碼位置與執行入口

*   該數據管道的核心 Python 原始碼位於專案根目錄下的 `src/taifex_pipeline/`。
*   `MyTaifexDataProject/` 目錄則更像是一個此管道的具體應用實例或部署包，它包含了執行腳本 `run.py`、該實例的 `README.md`、以及可能的特定配置文件。
*   主要執行入口是 `MyTaifexDataProject/run.py`，它提供了一個命令列介面，允許用戶執行 `ingest` (汲取)、`transform` (轉換)、`run_all` (完整流程)、`init_db` (初始化資料庫)、`scan_metadata` (掃描已處理數據生成元數據) 等操作。

### 3.3. 配置依賴

*   **主要作業配置**: 由位於專案根目錄的 `config.yaml` 文件定義（通過 `project_folder: "MyTaifexDataProject"` 關聯）。此文件指定了輸入/輸出資料夾路徑、資料庫名稱（如 `processed_data.duckdb` 用於存儲最終結果）、日誌文件名、並行處理的 worker 數量限制等。
*   **格式定義與處理配方**: 由位於專案根目錄 `config/` 文件夾下的 `format_catalog.json` 文件提供。這是管道能夠自動識別和處理不同 TAIFEX 數據檔案格式的關鍵。
*   **特定實例配置**: `MyTaifexDataProject/config.yaml` 包含了一些針對此特定實例的配置，例如 `metadata_scanner` 的路徑設定。

### 3.4. 數據產出

*   **原始數據湖**: 未經修改的原始檔案內容存儲在 `raw_lake.db` 中。
*   **處理後的結構化數據**: 經過清洗、轉換的最終數據存儲在名為 `processed_data.duckdb` 的 DuckDB 資料庫中，其內部表結構根據 `format_catalog.json` 中各配方定義的 `target_table` 生成。
*   詳細的處理日誌與 Manifest 資料庫。

## 4. `AI_Assisted_Historical_Backtesting/` 子專案分析

`AI_Assisted_Historical_Backtesting/` 子專案旨在構建一個利用大型語言模型 (LLM) 輔助進行倒推式歷史金融市場回溯測試和交易策略生成的系統。其核心理念是在模擬的歷史時間點，僅使用當時可獲得的數據，讓 AI 分析市場狀況並提出決策建議。

### 4.1. 功能與設計目標

*   **倒推式歷史回溯**: 系統以一定的時間間隔（例如12小時）從最近的歷史數據點開始，逐步向過去回溯，模擬歷史市場的演進。
*   **AI 決策生成**: 在每一個模擬的歷史時間點，系統會準備一份結構化的「市場簡報」，將其提交給本地部署的大型語言模型 (根據 README 和腳本，計劃使用 Llama 3，通過 Ollama 部署) 進行分析。AI 的任務是基於簡報內容生成交易決策、推薦策略及其推理過程。
*   **歷史決策日誌**: AI 的每一次決策（包括輸入的簡報、AI的原始回應、解析後的策略、信心評分等）都會被詳細記錄到一個持久化的 SQLite 資料庫中 (根據 `schema.sql` 和 `db_manager.py`，表名如 `ai_historical_judgments`)。
*   **分層報告系統**: 基於 AI 的歷史決策日誌和相關的特徵數據，系統能夠自動生成每日、每週、每月等多層次的市場分析報告，旨在從數據中提煉洞察和模式。
*   **零依賴原則**: 該子專案在其 `README.md` 中強調遵循「零依賴」原則，目標是最大限度地依賴 Python 標準庫，以確保在嚴苛的沙箱環境中的可移植性和穩定性。

### 4.2. 核心組件

*   **主模擬邏輯 (`src/main_simulation.py`)**: 這是整個回溯和 AI 決策流程的核心 Python 腳本。其職責包括：
    *   控制歷史時間的回溯步進。
    *   在每個時間點，調用數據處理模組準備 AI 分析所需的市場數據和特徵。
    *   調用提示生成器 (`prompt_generator.py`) 構建「市場簡報」。
    *   與 AI 代理 (`llama_agent.py`) 交互，發送簡報並獲取 AI 的分析和決策。
    *   將 AI 的決策和相關元數據記錄到資料庫。
*   **AI 邏輯 (`src/ai_logic/`)**:
    *   `llama_agent.py`: 包含了與本地部署的 Ollama Llama 3 模型進行交互的客戶端邏輯。
    *   `prompt_generator.py`: (雖然檔案列表中缺失，但從導入推斷存在) 負責根據當前的市場數據和回溯點，動態生成結構化的、適合 LLM 理解和分析的提示文本（即「市場簡報」）。
*   **數據連接器 (`src/connectors/`)**: 此子專案內部包含了一組數據連接器 (`finmind_connector.py`, `fred_connector.py`, `yfinance_connector.py`)。考慮到「零依賴」原則，這些很可能是為該子專案特別實現或簡化的版本，用於獲取回溯所需的基本金融數據。
*   **數據處理 (`src/data_processing/`)**:
    *   `aligners.py`, `cleaners.py`, `feature_calculator.py`: (雖然檔案列表中缺失，但從導入推斷存在) 這些模組共同負責將原始獲取的數據轉換為 AI 分析和市場簡報所需的格式和內容。
*   **資料庫管理 (`src/database/db_manager.py`)**: 負責管理該子專案自身的 SQLite 資料庫。`config/schema.sql` 文件定義了此資料庫中（例如 `ai_historical_judgments` 表）的表結構。
*   **報告生成**: (推測) 可能有未列出的報告生成模組，用於實現分層報告系統。

### 4.3. 執行入口與配置

*   **主要執行腳本**: `run_full_simulation.sh` 是啟動完整回溯模擬流程的 Bash 腳本。
*   **AI 環境部署**: `scripts/deploy_ollama_llama3.sh` 輔助部署本地 Ollama 和 Llama 3。
*   **資料庫初始化**: `scripts/initialize_database.sh` 配合 `config/schema.sql` 用於創建 SQLite 資料庫表。
*   **配置**: 資料庫表結構由 `config/schema.sql` 定義。AI 模型連接等配置可能在 `llama_agent.py` 或通過環境變數設定。回溯參數可能在 `src/main_simulation.py` 中硬編碼或通過命令行傳遞。

### 4.4. 數據依賴

*   **輸入數據**: 需要歷史金融數據（股價、宏觀指標等），可能通過其內建的連接器從外部 API 獲取。
*   **輸出數據**: AI 歷史決策日誌、市場分析報告。

## 5. `panoramic-market-analyzer/` 子專案分析

`panoramic-market-analyzer/` 子專案致力於構建一個金融數據處理管道，其 `README.md` 表明它遵循基於微服務（更準確地說是獨立的命令列工具集合）的架構理念，並強調職責分離和程式碼品質。

### 5.1. 功能與設計目標

*   從名稱「全景市場分析儀」推測，該子專案的目標是提供對市場的全面分析視角。
*   其設計強調通過一系列獨立的命令列工具來實現數據的獲取、處理和可能的分析功能。
*   非常注重開發流程中的品質保證，包括靜態程式碼分析 (linting) 和單元測試。

### 5.2. 架構與核心組件

(由於此子專案的 Python 檔案未在提供的列表中，以下分析主要基於其 `README.md` 和推測)

*   **Fetcher Service (推測)**: 負責從外部數據源獲取金融數據。考慮到 `README.md` 中提到了 `yfinance`，這可能是主要的數據源。
*   **Processor Service (推測)**: 負責對獲取的原始數據進行後續處理，如數據清洗、特徵計算等。
*   **數據存儲 (推測)**: 可能使用 DuckDB 或類似的本地數據庫。

### 5.3. 主要依賴與執行入口

*   **主要依賴**: `yfinance`, `duckdb`, `pandas`, `pytest`, `flake8` (從 `README.md` 推測)。
*   **執行入口**: `run_pipeline.sh` (執行完整數據管道), `run_lint.sh`, `run_tests.sh`, `run_quality_checks.sh`。

### 5.4. 在整體專案中的定位

*   **獨立性**: 看起來是一個相對獨立的子專案，有自己的依賴和執行流程。
*   **數據源**: 主要依賴 `yfinance`。
*   **潛在角色**: 可能作為輕量級的、專注於特定市場分析的工具，或作為開發和測試新分析模型的平台。

## 6. 其他共用模組、工具及配置分析

### 6.1. 共享數據連接器庫 (`src/connectors/`)

*   **豐富的實現**: 包含針對多種金融數據 API 的連接器。
*   **基類**: `base.py` 和 `base_connector.py` 可能定義了通用接口。
*   **`__init__.py` 的特殊狀態**: 目前被修改為「NYFed 診斷模式」，嚴重限制了除 `NYFedConnector` 外其他連接器的加載。
*   **影響**: 阻礙了 `DataMaster` (如果存在) 的正常工作，進而影響 `src/main.py` 的數據獲取。

### 6.2. 通用資料庫模組 (`src/database/`)

*   `database_manager.py`: 提供與 DuckDB 交互的通用管理器。
*   `writer.py`: (內容未知) 可能專注於數據寫入。

### 6.3. 指標計算引擎 (`src/engine/indicator_engine.py`)

*   封裝了金融指標（如「交易商壓力指數」）的計算邏輯，具有良好的複用潛力。

### 6.4. AI 相關模組

*   `src/ai_agent.py`: 頂層 AI 代理，可能配置為使用 Claude AI。
*   `AI_Assisted_Historical_Backtesting/src/ai_logic/`: 包含針對 Llama 3 的特定實現。
*   兩者關係需釐清，是否存在統一AI接口的可能。

### 6.5. 設定檔管理概覽

專案的設定檔管理呈現分散狀態：

*   `config.yaml` (根目錄): 主要用於 `MyTaifexDataProject`。
*   `config.py` (根目錄): 定義全局路徑常量，可能與 YAML 配置衝突。
*   `src/configs/project_config.yaml`: `src/main.py` 依賴的主要設定檔。
*   `MyTaifexDataProject/config.yaml`: `MyTaifexDataProject` 內部 `metadata_scanner.py` 的配置。
*   `config/format_catalog.json`: `MyTaifexDataProject` 的核心格式定義。
*   `config/schemas.json`: DuckDB 的表結構定義，非常關鍵。
*   `AI_Assisted_Historical_Backtesting/config/schema.sql`: 該子專案的 SQLite DDL。

### 6.6. 其他數據管道版本

*   `src/data_pipeline_v15/` 和 `src/sp_data_v16/`: 可能是歷史版本或特定數據源的處理管道，其當前狀態和與主流程的關係需要確認。

### 6.7. 日誌系統概覽

*   **全局日誌**: `src/scripts/initialize_global_log.py` 提供全局日誌初始化，被 `src/main.py` 調用。
*   **`MyTaifexDataProject` 日誌**: 設計了獨立的雙軌制日誌系統。
*   **一致性**: 需要確保整個專案的日誌記錄風格、級別和格式的一致性。

## 7. 總結與建議

### 7.1. 整體架構評估

本專案展現了構建一個綜合性金融數據分析與 AI 輔助決策平台的雄心。其架構具有模組化嘗試、數據驅動設計等優點，但同時也存在核心模組遺失、共享庫初始化問題、配置管理混亂、子專案依賴不明確等挑戰。

### 7.2. 主要問題列表

1.  **核心模組遺失 (`data_master.py`)**: 導致 `src/main.py` 無法運行。
2.  **共享連接器庫初始化問題 (`src/connectors/__init__.py`)**: 限制了數據源的訪問。
3.  **設定檔管理混亂且分散**: 易導致配置錯誤和維護困難。
4.  **數據流與子專案依賴關係不明確**: 難以評估整體一致性和數據生命周期。
5.  **AI 實現與配置不一致** (Llama 3 vs Claude): 需釐清使用策略。
6.  **潛在的冗餘代碼或過時版本** (v15, v16 數據管道)。

### 7.3. 數據流推測

理想情況下，數據應從外部源通過統一的 `DataMaster` 和連接器流入，經過處理和特徵工程後，供給AI模型、回測引擎和報告系統。目前由於核心問題，各子專案更像獨立運行。

### 7.4. 改進建議

1.  **恢復核心功能模組**:
    *   **首要任務**: 找到或重新實現 `data_master.py`。
    *   **修復連接器初始化**: 恢復 `src/connectors/__init__.py` 的正常功能。
2.  **標準化和集中化設定檔管理**:
    *   選擇或設計一個主設定檔，整合各模組配置。
    *   統一路徑管理，推薦使用相對於專案根目錄的路徑。
    *   使用環境變數或 `.env` 文件管理敏感信息。
3.  **明確數據流與子專案依賴關係**:
    *   繪製詳細的專案架構圖和數據流程圖。
    *   定義清晰的服務間接口（若考慮微服務化）。
4.  **代碼庫清理與重構**:
    *   評估並處理舊版本數據管道 (`v15`, `v16`)。
    *   統一或整合不同地方實現的相似功能連接器。
    *   評估 `panoramic-market-analyzer` 的整合可能性。
5.  **統一或協調 AI 實現**:
    *   明確專案對不同AI模型的使用策略，考慮設計通用AI代理接口。
6.  **完善項目級文檔**:
    *   創建或更新根目錄的 `README.md`，提供專案概覽、架構、配置和運行指南。
    *   為核心共享模組補充文檔字符串。

通過實施這些建議，可以顯著提高專案的穩定性、可維護性、可理解性和整體效率，使其更好地實現其作為一個綜合性金融數據分析與 AI 策略平台的目標。

---
報告完畢。
