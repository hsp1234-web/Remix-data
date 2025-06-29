# 金融數據分析與AI策略平台

本倉儲是一個多功能的金融數據處理、分析與AI策略輔助平台。它整合了來自多個子專案的功能，旨在提供一個從數據獲取、處理、特徵工程到AI分析和策略回測的完整解決方案。

## 專案結構與分析

本倉儲主要由以下三個核心子專案/目錄構成，它們代表了平台功能的不同方面和演進階段：

1.  **`-risk-assessment-feat-microservice-refactor`**:
    *   一個初期的金融數據處理管道原型，專注於從yfinance獲取數據，計算移動平均線，並使用DuckDB存儲結果。此專案為服務分離概念的初步探索。
    *   詳細分析請見：[ANALYSIS_risk_assessment.md](./ANALYSIS_risk_assessment.md)

2.  **`Free_Data_API-feat-microservice-refactor`**:
    *   作為 `-risk-assessment` 的進階版本，此專案致力於構建一個更通用的金融數據框架。它引入了更清晰的模組化結構 (如 `financial_data_framework`)，擴展了數據源 (如FMP API, CoinGecko API)，並通過接口定義提升了系統的靈活性和可擴展性。
    *   詳細分析請見：[ANALYSIS_free_data_api.md](./ANALYSIS_free_data_api.md)

3.  **`SP_DATA-feat-financial-data-pipeline`**:
    *   一個功能全面的系統，集成了金融數據分析與AI輔助的交易策略回測。其亮點包括多樣化的數據連接器、基於Llama 3的AI決策邏輯、歷史回溯引擎，以及一個專門處理台灣期貨交易所數據的獨立管道 (`MyTaifexDataProject`)。此專案特別強調了在受限環境（如沙箱）中進行開發的實戰經驗和對「零依賴」原則的追求。
    *   詳細分析請見：[ANALYSIS_sp_data.md](./ANALYSIS_sp_data.md)

對整個倉儲的**綜合分析**，包括各子專案的詳細功能拆解、技術棧評估以及它們之間的關聯與演進脈絡，請參閱以下主要分析文檔：

*   **[專案整體分析 (ANALYSIS.md)](./ANALYSIS.md)**

## 微服務架構建議

為了將本倉儲的各項功能整合成一個更現代化、可擴展且易於維護的平台，我們基於對現有程式碼的深入分析，提出了一套微服務架構方案。該架構旨在將複雜的系統拆分為一系列獨立、專注的服務模組。

**核心微服務模組建議包括：**

1.  **數據採集網關服務 (Data Ingestion Gateway Service)**: 統一管理外部API數據獲取。
2.  **期交所檔案處理服務 (Taifex File Processing Service)**: 專門處理期交所的批次數據檔案。
3.  **核心數據處理與特徵工程服務 (Core Data Processing & Feature Engineering Service)**: 負責數據清洗、轉換及特徵計算。
4.  **AI決策與分析服務 (AI Decision & Analytics Service)**: 利用LLM等AI模型進行市場分析與決策。
5.  **歷史回溯與模擬引擎服務 (Backtesting & Simulation Engine Service)**: 執行交易策略的歷史回測。
6.  **報告生成與可視化服務 (Reporting & Visualization Service)**: 產生分析報告和圖表。
7.  **數據質量監控服務 (Data Quality Monitoring Service)**: 確保數據的準確性和一致性。
8.  **(可選) 配置與元數據管理服務 (Configuration & Metadata Management Service)**: 集中管理系統配置和元數據。

此架構的目的是提高系統的模組化程度，使得各功能可以獨立開發、部署和擴展，同時也考慮到了在資源受限環境下的適應性。

關於此微服務架構的**詳細設計原則、各服務模組的職責、潛在技術選型、數據流圖、服務間通信機制以及沙箱環境適應性考量**，請參閱以下專門的架構文檔：

*   **[微服務架構詳解 (MICROSERVICE_ARCHITECTURE.md)](./MICROSERVICE_ARCHITECTURE.md)**

## 如何開始

(此部分可根據專案的實際運行方式進行填充，例如：)

1.  **環境設定**:
    *   確保已安裝 Python [版本號]。
    *   建議使用 Poetry 進行依賴管理: `poetry install`
    *   設定必要的環境變數 (例如 API 金鑰，參考各子專案的說明)。
2.  **數據庫初始化**:
    *   (根據最終選擇的數據庫和初始化方式填寫，例如 `python run.py init_db` 或特定子專案的初始化腳本)。
3.  **執行數據管道/分析**:
    *   (根據主要執行入口填寫，例如 `bash run_pipeline.sh` 或 `python src/main.py`)。

詳細的執行指令和配置方法，請參考各子專案目錄下的 `README.md` 文件以及 `ANALYSIS.md` 中對各部分的描述。

---
本文檔由 AI 輔助生成和分析。
