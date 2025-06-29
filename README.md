# Financial Forensics Engine (金融市場洞察引擎)

本專案是一個事件驅動的數據處理與分析引擎，旨在從多源數據（如社交媒體討論、金融市場行情）中提取洞察，並利用大型語言模型 (LLM) 生成分析報告，輔助金融市場決策。

## 核心功能

*   **多源數據汲取**: 自動從 CSV 等格式導入社交媒體貼文和金融市場數據 (如台指期行情)。
*   **分層數據處理**: 採用 Input -> Bronze -> Silver -> Gold 的數據分層架構，對數據進行逐步清洗、轉換、聚合與增強。
*   **情境感知分析包生成**: 針對特定分析目標（例如某個交易週），整合結構化的市場數據與非結構化的社交輿情數據，並進行初步的 NLP 分析（情感、關鍵詞），形成綜合分析包。
*   **AI 驅動的報告生成**: 利用 Gemini 等大型語言模型，對生成的分析包進行深度分析，自動撰寫包含宏觀背景、微觀複盤及策略洞察的金融分析報告。
*   **事件驅動架構**: 基於檔案系統的事件總線，實現各數據處理模組（微應用）的解耦和彈性調度。
*   **可配置化與可擴展性**: 通過外部設定檔管理數據庫表結構、數據格式映射等；微應用架構易於擴展新的數據源或分析模組。

## 專案結構總覽

Financial_Forensics_Engine/
  runner.py                     # 主執行器，事件驅動任務調度核心
  requirements.txt              # Python 依賴包列表
  taifex_format_catalog.json    # 台指期數據格式轉換設定檔
  database_schemas.json         # 資料庫表結構定義檔

  apps/                         # 微應用程式目錄
    00_ingest_social_posts/   # 汲取社交媒體貼文
      run.py
    01_ingest_taifex/         # 汲取台指期數據
      run.py
    02_transform_taifex/      # 轉換台指期數據 (Bronze -> Silver)
      run.py
    03_aggregate_to_gold/     # 聚合台指期數據 (Silver -> Gold)
      run.py
    10_create_weekly_context/ # 生成目標週分析包
      run.py
    11_analyze_weekly_context/  # AI 分析每週情境，生成報告
      run.py
    20_generate_synthesis_report/ # AI 生成跨週期綜合報告
      run.py

  config/                       # (建議新增) 存放專案設定檔
    project_config.yaml       # (建議新增) 專案級設定，如 API 金鑰名稱映射

  utils/                        # (建議新增) 通用工具模組
    config_loader.py          # (建議新增) 統一的設定加載器

  data/                         # 數據存儲目錄 (分層)
    input/                    # 原始輸入數據
      social_posts/social_posts.csv
      taifex/unzipped/taifex_data.csv
    bronze/                   # 初步轉換後的數據 (Parquet)
      social_posts/threads_posts.parquet
      taifex/taifex_data.parquet
    silver/                   # 清洗和結構化後的數據
      analysis_packages/    # App 10 生成的 JSON 分析包
        2022-W30_AnalysisPackage.json (示例)
    gold/                     # 高度聚合和分析後的數據
      analysis_reports/     # App 11 生成的 AI 分析報告
        2022-W30_AnalysisReport.txt (示例)
    reports/                  # App 20 生成的綜合報告
    financial_data.duckdb     # DuckDB 資料庫檔案

  event_bus/                    # 事件/任務檔案傳遞目錄
    queue/                    # 新任務佇列
    in_progress/              # 處理中任務
    completed/                # 已完成任務
    failed/                   # 失敗任務

  logs/                         # 日誌檔案目錄
    runner_YYYYMMDD_HHMMSS.log
    app_XX_script_name.log    # 各微應用的日誌

## 主要功能模組說明

### `runner.py` (主執行器)
*   功能：作為整個系統的中央調度器，採用事件驅動模式。
*   監控 `event_bus/queue/` 目錄中的新任務（以 `.json` 檔案形式定義）。
*   根據任務定義中的 `app_name`，動態調用對應的 `apps/` 子目錄下的 `run.py` 腳本。
*   管理任務生命週期：將任務檔案在 `queue`, `in_progress`, `completed`, `failed` 目錄間移動。
*   記錄詳細的執行日誌。

### `apps/` (微應用程式)
每個微應用都是一個獨立的 Python 腳本 (`run.py`)，執行特定的數據處理或分析任務。

*   **`00_ingest_social_posts`**: 讀取 `data/input/social_posts/social_posts.csv`，轉換為 Parquet 並存儲到 `data/bronze/social_posts/`。
*   **`01_ingest_taifex`**: 讀取 `data/input/taifex/unzipped/` 下的 CSV，轉換為 Parquet 並存儲到 `data/bronze/taifex/`。
*   **`02_transform_taifex`**: 讀取 Bronze 層期交所數據，參考 `taifex_format_catalog.json` 和 `database_schemas.json`，在 `data/financial_data.duckdb` 中創建/更新 `silver_fact_taifex_quotes` 表。
*   **`03_aggregate_to_gold`**: 從 DuckDB 的 `silver_fact_taifex_quotes` 讀取日度數據，按週聚合，並寫入 DuckDB 的 `gold_weekly_market_summary` 表。
*   **`10_create_weekly_context`**:
    *   接收目標週 `target_week_id`。
    *   整合市場數據、社交貼文，進行 NLP 分析（情感、關鍵詞）。
    *   將結果組合成 JSON 分析包，存儲到 `data/silver/analysis_packages/`。
    *   觸發 `11_analyze_weekly_context` 任務。
*   **`11_analyze_weekly_context`**:
    *   讀取分析包 JSON。
    *   格式化數據為 Prompt，調用 Google Gemini API 進行分析。
    *   將 AI 生成的分析文本保存到 `data/gold/analysis_reports/`。
*   **`20_generate_synthesis_report`**: (低優先級) 合併多個週度分析報告，調用 AI 生成跨週期綜合報告，存儲到 `data/reports/`。

### 設定檔
*   `requirements.txt`: Python 依賴庫。
*   `taifex_format_catalog.json`: 期交所數據 CSV 欄位映射與類型定義。
*   `database_schemas.json`: DuckDB 表結構定義。
*   `config/project_config.yaml` (建議): 專案級設定，如 API 金鑰環境變數名稱映射。

### 工具模組
*   `utils/config_loader.py` (建議): 加載 `project_config.yaml` 並處理 API 金鑰讀取。

### 數據分層 (`data/`)
*   **Input**: 原始數據。
*   **Bronze**: 初步轉換和基本清洗後的數據。
*   **Silver**: 清洗、結構化，可供分析的數據 (包含分析包)。
*   **Gold**: 高度聚合、AI 分析後的最終報告或洞察。

### 事件總線 (`event_bus/`)
*   基於檔案系統的隊列，用於微應用解耦和任務調度。

### 日誌 (`logs/`)
*   集中存放所有組件的運行日誌。

## 使用說明 (模擬演習)

1.  **環境準備**：
    *   確保已安裝 Python 及 `requirements.txt` 中列出的所有依賴包。
    *   （若使用 AI 功能）確保相關的 API 金鑰已在環境中正確設置 (例如，通過環境變數配置 `GOOGLE_API_KEY` 等，並在 `config/project_config.yaml` 中進行映射)。
2.  **數據準備**：
    *   將原始社交貼文數據放入 `data/input/social_posts/social_posts.csv`。
    *   將原始台指期數據放入 `data/input/taifex/unzipped/taifex_data.csv`。
3.  **觸發初始任務**：
    *   根據需求，在 `event_bus/queue/` 目錄下創建任務 JSON 檔案。例如，創建 `task_ingest_social_001.json` 和 `task_ingest_taifex_001.json` 來啟動數據汲取流程。
    *   任務 JSON 範例:
        ```json
        // task_ingest_social_001.json
        {
          "app_name": "00_ingest_social_posts",
          "params": {
            "input_file": "data/input/social_posts/social_posts.csv",
            "output_file": "data/bronze/social_posts/threads_posts.parquet"
          }
        }
        ```
4.  **啟動執行器**：
    *   在專案根目錄 (`Financial_Forensics_Engine/`) 下運行 `python runner.py`。
    *   `runner.py` 將會監控任務隊列並依次執行。
5.  **查看結果**：
    *   處理過程中的日誌會記錄在 `logs/` 目錄下。
    *   各階段的數據產出會存放在 `data/` 目錄對應的分層子目錄中。
    *   最終的 AI 分析報告位於 `data/gold/analysis_reports/`。

## 注意事項 (沙箱環境)

在特定的沙箱環境（如 Google Colab 或類似的雲端 Notebook）中執行時，可能會遇到檔案系統操作限制。如果遇到此類問題，可能需要將部署和執行步驟進一步分解。

---
本文檔由 AI 輔助生成和分析。
