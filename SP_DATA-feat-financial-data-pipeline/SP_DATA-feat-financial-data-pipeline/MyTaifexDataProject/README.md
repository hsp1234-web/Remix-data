# 全自動本地智慧期交所數據管道 (Taifex Intelligent Data Pipeline) v3.0.1

## 專案概述

本專案旨在開發一套完整、全自動、且高效的本地智慧數據解決方案，專門用於處理來自台灣期貨交易所 (TAIFEX) 的大量且多樣化的公開數據。系統的設計目標是實現高度的適應性、穩定性和運算效能，能夠自動辨識不同的檔案格式，執行精確的數據清洗與轉換，並將最終結果儲存於結構化的分析型資料庫中。此外，本專案也強調提供完善的日誌記錄、狀態監控以及錯誤管理機制，以確保數據處理流程的透明度與可維護性。

## 核心設計與功能

本數據管道的核心設計基於以下幾個關鍵組件與原則：

1.  **格式指紋目錄 (Format Fingerprint Catalog)**：
    *   透過對檔案標頭內容進行正規化處理並計算 SHA256 雜湊值，為每種獨特的檔案格式生成「指紋」。
    *   一個中央化的 `config/format_catalog.json` 設定檔，將「指紋」映射到詳細的「處理配方」。

2.  **兩階段自動化管線 (Two-Stage Automated Pipeline)**：
    *   **第一階段 - 汲取 (Ingestion)**：將原始檔案完整存入 `data/01_raw_lake/raw_lake_and_manifest.duckdb` 中的 `raw_files` 表，並在 `file_processing_log` 表 (manifest) 中登記。
    *   **第二階段 - 轉換 (Transformation)**：根據 manifest 記錄，平行處理已汲取但未轉換的檔案。流程包括：格式識別、解析、清洗、載入到 `data/02_processed/processed_data.duckdb`。

3.  **資源最大化與狀態管理**：
    *   動態偵測 CPU 核心數並行化處理轉換任務。
    *   `file_processing_log` 表 (manifest) 追蹤每個檔案的完整處理生命週期。

4.  **日誌輸出 (Logging Output)**：
    *   **主控台即時報告**：提供操作者直觀進度。
    *   **結構化日誌檔案 (JSON)**：存於 `logs/` 目錄，記錄詳細事件，便於分析。檔名格式如 `pipeline_run_YYYYMMDD_HHMMSS_EXECUTIONID[:8].log.json`。

5.  **專案結構與程式碼品質**：
    *   標準化 Python 專案目錄結構。
    *   使用 `black`, `Ruff`, `mypy` 配合 `pre-commit` 進行程式碼品質控制。

## 詳細設計文檔

關於本專案更詳細的設計決策、架構藍圖、各模組介面定義以及具體實施細節，請參閱位於本倉儲根目錄下的 `Program_Development_Project.txt` 文件。該文件是本專案開發的核心指導文獻。

## 環境設定

1.  **Python 版本**: 需要 Python >=3.9, <3.13。
2.  **Poetry**: 建議使用 [Poetry](https://python-poetry.org/) 進行依賴管理。
    *   安裝 Poetry (參考其官方文件)。
3.  **安裝依賴**: 在專案根目錄 (`MyTaifexDataProject/`) 下執行：
    ```bash
    poetry install
    ```
4.  **Pre-commit 掛鉤 (可選但強烈建議)**:
    ```bash
    poetry run pre-commit install
    ```
    這會在您每次提交程式碼時自動執行程式碼檢查。

## 如何執行數據管道

數據管道通過位於專案根目錄的 `run.py` 腳本啟動。

**基本用法：**

```bash
poetry run python run.py [action] [options]
```
或如果您的 Poetry 環境已激活：
```bash
python run.py [action] [options]
```

**主要操作 (`action`)：**

*   `run_all`: （預設最常用）依次完整執行數據汲取和數據轉換兩個階段。
    ```bash
    python run.py run_all
    ```
*   `ingest`: 只執行數據汲取階段。將新檔案從來源目錄（預設 `data/01_input_files/` 和 `data/00_landing_zone/`）汲取到原始數據湖。
    ```bash
    python run.py ingest
    ```
*   `transform`: 只執行數據轉換階段。處理已汲取但尚未轉換的檔案。
    ```bash
    python run.py transform
    ```
*   `init_db`: 初始化或重新建立所有必要的資料庫表格結構（例如 `raw_files`, `file_processing_log`）。
    ```bash
    python run.py init_db
    ```
*   `show_config`: 顯示當前 `config/format_catalog.json` 的內容，方便檢查。
    ```bash
    python run.py show_config
    ```

**常用選項 (`options`)：**

*   `--reprocess-quarantined`: 在執行 `transform` 或 `run_all` 時，指示轉換管線嘗試重新處理之前被標記為 `QUARANTINED`（通常是因格式未知或解析/清洗失敗）的檔案。這在您更新了 `format_catalog.json` 或修復了清洗函式後非常有用。
    ```bash
    python run.py transform --reprocess-quarantined
    ```
*   `--source-dirs path/to/dir1 path/to/dir2`: 指定一個或多個來源資料夾路徑（相對於專案根目錄），覆蓋汲取管線的預設掃描目錄。
    ```bash
    python run.py ingest --source-dirs custom_input_data/ new_api_data/
    ```
*   `--max-workers <number>`: 指定轉換管線平行處理時使用的最大 CPU 核心數。
    ```bash
    python run.py transform --max-workers 4
    ```
*   `--log-level <LEVEL>`: 設定主控台日誌的級別 (DEBUG, INFO, WARNING, ERROR, CRITICAL)。預設 INFO。
*   `--log-file-level <LEVEL>`: 設定檔案日誌的級別。預設 DEBUG。
*   `--config-dir <directory>`: 指定 `format_catalog.json` 所在的目錄。預設 `config`。
*   `--catalog-file <filename>`: 指定格式目錄設定檔的名稱。預設 `format_catalog.json`。

**範例：完整運行，並重新處理隔離檔案，同時指定日誌級別**
```bash
python run.py run_all --reprocess-quarantined --log-level DEBUG
```

## 如何註冊新檔案格式

當管線遇到一個它無法識別格式指紋的檔案時，該檔案會被標記為 `QUARANTINED`，並在日誌中提示。您需要手動為這個新格式註冊一個處理配方。

使用位於 `scripts/` 目錄下的 `register_format.py` 輔助腳本：

1.  **準備一個範例檔案**：將無法識別的檔案（或其同格式的代表性範例）放在一個您可以訪問的路徑下。
2.  **執行註冊腳本**：
    ```bash
    poetry run python scripts/register_format.py /path/to/your/sample_file.csv
    ```
    或者（如果 Poetry 環境已激活）：
    ```bash
    python scripts/register_format.py /path/to/your/sample_file.csv
    ```
3.  **按照提示操作**：
    *   腳本會計算檔案的格式指紋。
    *   如果指紋已存在，它會顯示現有配方並詢問您是否要更新。
    *   您將被引導輸入或修改該格式的描述、目標資料庫表名、Pandas 解析參數 (`parser_config`)、對應的清洗函式名稱 (`cleaner_function`) 以及必要的欄位列表 (`required_columns`)。
    *   完成後，配方將被保存到 `config/format_catalog.json`。
4.  **編寫或確認清洗函式**：
    *   確保您在 `format_catalog.json` 中指定的 `cleaner_function` (例如 `my_cleaners.clean_new_data_v1`) 實際存在於 `src/taifex_pipeline/transformation/cleaners/` 路徑下的某個 Python 模組中 (例如 `my_cleaners.py`)，並且該函式遵循 `(df: pd.DataFrame) -> pd.DataFrame` 的介面。
5.  **重新處理**：一旦新格式註冊完畢且清洗函式準備就緒，您可以使用 `run.py` 的 `--reprocess-quarantined` 選項來處理之前被隔離的檔案：
    ```bash
    python run.py transform --reprocess-quarantined
    ```

## 日誌文件

*   詳細的結構化 JSON 日誌會記錄在專案根目錄下的 `logs/` 資料夾中。
*   每個管線運行實例都會產生一個獨立的日誌檔案，檔名包含執行時間和唯一的執行ID，例如 `pipeline_run_20231027_153000_a1b2c3d4.log.json`。
*   這些日誌對於追蹤處理細節、診斷問題和監控效能非常有用。

## 版本與分支

*   當前開發分支： `main_v3.0.1`

## 語言

本專案所有程式碼註解、日誌訊息以及相關文檔（包括本 README 和 `Program_Development_Project.txt`）均優先使用**繁體中文**。專有名詞和無法避免的技術術語可能會保留英文原文以確保精確性。

---
*本 README.md 文件由 Jules (AI Software Engineer) 協助產生。*
*最後更新時間: (Jules 自動填寫)*
