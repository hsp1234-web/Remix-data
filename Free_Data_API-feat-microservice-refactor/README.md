# 全景市場分析儀 v2.0 - 微服務架構

歡迎來到「全景市場分析儀」的全新微服務架構。本專案旨在提供一個穩健、可擴展、易於維護的金融數據工程平台。

## 核心架構理念

本專案遵循「微服務」設計理念，將過去單一、龐大的數據處理流程，拆分為一系列獨立、職責單一的服務。每個服務都可以獨立開發、測試與部署，大大提高了系統的穩定性與開發效率。

- **Fetcher Service**: 數據獲取服務，負責從外部 API (如 Financial Modeling Prep) 抓取最原始的數據，並將其存入一個獨立的「原始數據層」資料庫。
- **Processor Service**: 數據處理服務，負責從「原始數據層」讀取數據，進行清洗、計算指標、特徵工程等，並將結果存入一個乾淨的「特徵層」資料庫。
- **Orchestrator (`run_pipeline.sh`)**: 流程協調器，作為總指揮，按正確順序調用各個服務，管理整個數據管線的執行流程。

## 環境設置

1.  **複製專案**:
    ```bash
    git clone <your-repository-url>
    cd panoramic-market-analyzer
    ```

2.  **安裝依賴**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **設定 API 金鑰**:
    請創建一個 `.env` 檔案，並在其中填寫必要的 API 金鑰：
    ```
    API_KEY_FMP="YOUR_FINANCIAL_MODELING_PREP_API_KEY"
    ```

## 如何執行

使用 `run_pipeline.sh` 腳本來執行完整的數據流程。

**執行完整管線 (推薦)**:
```bash
bash run_pipeline.sh --stage full_pipeline --symbol "AAPL,MSFT,GOOG"
```

**分步執行**:

```bash
# 僅獲取數據
bash run_pipeline.sh --stage fetch --symbol "AAPL,MSFT"

# 僅處理數據 (假設已獲取)
bash run_pipeline.sh --stage process
```

## 資料庫驗證

執行完管線後，可使用 `verify_db.py` 腳本來驗證資料庫中的內容。

```bash
python verify_db.py --symbol AAPL
```
