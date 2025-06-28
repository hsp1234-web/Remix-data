#!/bin/bash

# run_historical_simulation.sh
# 腳本目的：執行大規模歷史回溯，生成 AI 歷史決策日誌。

echo "--- 開始歷史回溯模擬 ---"

# 設定參數 (未來可以考慮從命令列參數讀取)
# 注意：此腳本假設 project_config.yaml 已手動設定為所需的完整歷史回溯期間。
# 例如，在 src/configs/project_config.yaml 中設定:
# data_fetch_range:
#   start_date: "2020-01-01"
#   end_date: "2023-12-31" # 確保結束日期已更新以進行完整回溯

# 創建 src/configs/project_config.yaml (與 run_prototype.sh 中的定義保持一致)
# 這確保了 AI API 金鑰是從檔案讀取的，而不是環境變數。
mkdir -p src/configs # 確保目錄存在
cat <<EOF > src/configs/project_config.yaml
# Configuration for the Financial Data Processing Prototype

database:
  path: "data/financial_data.duckdb"

data_fetch_range:
  start_date: "2020-01-01" # 根據需要調整進行完整回溯
  end_date: "2025-06-26" # 根據需要調整，此處為先前範例的結束日期

api_endpoints:
  fred:
    api_key_env: "FRED_API_KEY" # main.py 中目前是硬編碼，但保留此結構
    base_url: "https://api.stlouisfed.org/fred/"

target_metrics:
  fred_series_ids:
    - "DGS10"
    - "DGS2"
    - "SOFR"
    - "VIXCLS"
    - "WRESBAL"
  yfinance_tickers:
    - "^MOVE"

nyfed_primary_dealer_urls:
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2024.xlsx"
    file_pattern: "prideal2024.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx"
    file_pattern: "prideal2023.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2022.xlsx"
    file_pattern: "prideal2022.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"

nyfed_format_recipes:
  "PD_STATS_FORMAT_2013_ONWARDS":
    header_row: 3
    date_column: "As of Date"
    columns_to_sum:
      - "U.S. Treasury coupons"
      - "U.S. Treasury bills"
      - "U.S. Treasury floating rate notes (FRNs)"
      - "Federal agency debt securities (MBS)"
      - "Federal agency debt securities (non-MBS)"
      - "Commercial paper"
      - "Certificates of deposit"
      - "Bankers acceptances"
      - "Equities"
      - "Corporate bonds (investment grade)"
      - "Corporate bonds (below investment grade)"
      - "Municipal securities"
      - "Other assets"
    data_unit_multiplier: 1000000

indicator_engine_params:
  rolling_window_days: 252
  stress_index_weights:
    sofr_deviation: 0.20
    spread_10y2y: 0.20
    primary_dealer_position: 0.15
    move_index: 0.25
    vix_index: 0.15
    pos_res_ratio: 0.05
  stress_threshold_moderate: 40
  stress_threshold_high: 60
  stress_threshold_extreme: 80

requests_config:
  max_retries: 3
  base_backoff_seconds: 1
  timeout: 30
  download_timeout: 120

ai_service:
  api_key: "YOUR_API_KEY_HERE" # AI Agent 將從此處讀取
  api_endpoint: "https://api.anthropic.com/v1/messages"
  default_model: "claude-3-opus-20240229"
  max_retries: 3
  retry_delay_seconds: 5
  api_call_delay_seconds: 1.0
EOF

echo "src/configs/project_config.yaml for historical simulation has been created/updated."
echo "AI API Key should be configured in src/configs/project_config.yaml (ai_service.api_key)."
echo "If it remains 'YOUR_API_KEY_HERE', AI decisions will be skipped by the agent."

# 執行主應用程式
echo "執行 src/main.py 進行歷史數據處理與 AI 決策生成..."
# 假設 Python 環境和依賴已經準備好 (例如，已執行過 pip install -r requirements.txt)
python src/main.py

# 執行結束後的日誌記錄
# main.py 內部會記錄已處理的日期和 AI API 調用次數。
# 此腳本可以簡單地指示使用者查看日誌。
echo ""
echo "--- 歷史回溯模擬執行完畢 ---"
echo "詳細執行日誌請查看 market_briefing_log.txt (總體日誌) 以及 api_test_logs/ 目錄下的時間戳記日誌 (應用程式日誌)。"
echo "AI API 調用次數統計應記錄在應用程式日誌中。"

# 未來可擴展功能：
# 1. 從命令列接收開始和結束日期，並動態修改 project_config.yaml (較複雜，需注意備份和還原)。
# 2. 更精細的進度追蹤和錯誤處理。
# 3. 執行後自動從日誌中提取 AI API 調用總數並顯示。
# 4. 檢查 Python 和 pip 是否存在。
# 5. 檢查 requirements.txt 是否已安裝。

exit 0
