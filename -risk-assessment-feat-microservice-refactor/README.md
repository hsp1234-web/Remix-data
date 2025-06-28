# 全景市場分析儀 - 微服務化架構

本專案旨在建立一個穩健、可擴展的金融數據處理管道。

## 架構總覽

本專案採用微服務化架構，將數據處理流程拆分為獨立、可測試的服務單元：

1.  **`fetcher_service.py`**: 數據獲取服務，負責從 `yfinance` API 抓取原始股價數據 (OHLCV)，並存入一個獨立的 DuckDB 資料庫 (`raw_data.db`)。
2.  **`processor_service.py`**: 數據處理服務，負責讀取 `raw_data.db` 中的原始數據，計算技術指標（例如移動平均線），並將結果存入另一個 DuckDB 資料庫 (`features.db`)。
3.  **`run_pipeline.sh`**: 流程協調器，作為總指揮，依序執行數據獲取和數據處理服務，完成端到端的數據流程。
4.  **`test_pipeline_fast.sh`**: 快速測試協調器，用於在**完全離線**的環境下，使用預先定義的模擬數據 (Mock Data) 來驗證所有服務的內部邏輯是否正確。

## 環境設定

1.  **安裝依賴**:
    ```bash
    pip install -r requirements.txt
    ```

## 如何運行

1.  **執行完整的數據管道 (聯網)**:
    ```bash
    bash run_pipeline.sh
    ```
    此命令將會：
    - 刪除舊的資料庫檔案。
    - 執行 `fetcher_service.py` 從網路抓取 SPY 的最新數據。
    - 執行 `processor_service.py` 處理數據並生成特徵。

2.  **執行快速離線測試**:
    ```bash
    bash test_pipeline_fast.sh
    ```
    此命令將會執行 `pytest`，並利用 `tests/conftest.py` 中定義的模擬數據來測試服務邏輯，全程不需要網路連線。
