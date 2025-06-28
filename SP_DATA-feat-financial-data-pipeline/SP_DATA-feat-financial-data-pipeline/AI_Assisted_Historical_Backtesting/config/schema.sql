-- SQL DDL Script for AI Assisted Historical Backtesting System
-- Target Database: SQLite

-- 說明:
-- 1. 所有時間戳 (_utc 結尾) 均應存儲為 TEXT，格式為 ISO8601 (YYYY-MM-DD HH:MM:SS.SSSZ)。應用程序負責時區處理和格式化。
--    SQLite 的 DATETIME 類型親和性會將其存儲為 TEXT, REAL 或 INTEGER。推薦使用 TEXT 存儲 ISO8601 字符串。
-- 2. JSON 字段存儲為 TEXT，應用程序層面負責序列化和反序列化。
-- 3. 字段命名風格：小寫下劃線。
-- 4. SQLite 中，INTEGER PRIMARY KEY 默認是 AUTOINCREMENT 的別名 (如果沒有指定 WITHOUT ROWID)。

--------------------------------------------------------------------------------
-- 表1: raw_market_data - 存儲從各數據源抓取的原始數據
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_market_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,                          -- SQLite 自增主鍵
    source TEXT NOT NULL,                                          -- 例如: "yfinance_csv_daily_aapl", "finmind_json_institutional_2330", "fred_json_cpi"
    symbol_or_series_id TEXT NOT NULL,                             -- 股票代碼、經濟序列ID等
    data_retrieved_utc TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%fZ', 'now')), -- 此條原始數據的抓取入庫時間 (ISO8601 UTC)
    original_data_timestamp_utc TEXT,                              -- 數據本身的原始時間戳 (ISO8601 UTC)
    data_payload TEXT NOT NULL,                                    -- 原始數據內容 (CSV 字符串, JSON 字符串)
    metadata_json TEXT,                                            -- 關於此原始數據的元信息 (JSON字符串格式), 例如API參數、文件名
    processing_status TEXT DEFAULT 'pending',                      -- 例如: "pending", "processing", "success", "failed"
    processed_at_utc TEXT,                                         -- 此原始數據被處理的時間 (ISO8601 UTC)
    error_message TEXT                                             -- 如果 processing_status 是 "failed"，記錄錯誤信息
);

--------------------------------------------------------------------------------
-- 表2: processed_features_hourly - 經預處理後，每個分析週期的特徵數據
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_features_hourly (
    timestamp_period_start_utc TEXT NOT NULL,                      -- 特徵窗口的開始時間 (ISO8601 UTC)
    symbol TEXT NOT NULL,                                          -- 產品代碼 (例如股票代碼)

    price_open REAL,
    price_high REAL,
    price_low REAL,
    price_close REAL,
    price_change_abs REAL,
    price_change_pct REAL,
    volatility_12hr_atr REAL,

    volume_total INTEGER, -- SQLite 中 INTEGER 可以存儲大整數
    open_interest_change INTEGER,

    oi_put_total INTEGER,
    oi_call_total INTEGER,
    pc_ratio_oi REAL,
    pc_ratio_vol REAL,

    rsi_14 REAL,
    sma_20 REAL,
    ema_12 REAL,
    macd_signal REAL,
    bollinger_upper REAL,
    bollinger_lower REAL,

    iv_skew_cboe REAL,
    vix_value REAL,

    news_sentiment_score REAL,
    economic_event_proximity_hours INTEGER,

    fred_interest_rate REAL,
    fred_cpi_yoy_pct REAL,

    data_source_references TEXT, -- JSON 列表字符串，包含生成這些特徵所依賴的 raw_market_data 的 id
    feature_generated_at_utc TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%fZ', 'now')), -- 此特徵生成的時間 (ISO8601 UTC)

    PRIMARY KEY (timestamp_period_start_utc, symbol)
);
CREATE INDEX IF NOT EXISTS idx_processed_features_timestamp_symbol ON processed_features_hourly (timestamp_period_start_utc DESC, symbol);

--------------------------------------------------------------------------------
-- 表3: ai_historical_judgments - AI 在每個歷史時間點的決策日誌
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_historical_judgments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    judgment_timestamp_utc TEXT NOT NULL,                          -- AI 做出判斷的時間 (ISO8601 UTC)
    market_briefing_json TEXT NOT NULL,                            -- 提交給 AI 的 JSON 市場簡報快照 (JSON字符串)
    ai_model_name TEXT,
    ai_decision_category TEXT,
    ai_recommended_strategy TEXT,
    ai_rationale_text TEXT,
    confidence_score REAL,
    key_warnings_json TEXT,                                        -- AI 判斷的潛在風險 (JSON 列表字符串)
    raw_llm_response_text TEXT,
    feature_period_start_utc TEXT,                                 -- 該判斷基於哪個 processed_features_hourly 的時間窗口 (ISO8601 UTC)
    symbol_judged TEXT,
    processing_time_seconds REAL,
    log_created_at_utc TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%fZ', 'now')) -- 此條日誌的創建時間 (ISO8601 UTC)
);
CREATE INDEX IF NOT EXISTS idx_ai_judgments_timestamp_symbol ON ai_historical_judgments (judgment_timestamp_utc DESC, symbol_judged);

--------------------------------------------------------------------------------
-- 表4: daily_reports_log - 每日報告
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_reports_log (
    report_date TEXT PRIMARY KEY,                                  -- 報告對應的日期 (YYYY-MM-DD, UTC)
    report_text_markdown TEXT,
    source_judgment_ids_json TEXT,                                 -- JSON 列表字符串
    source_feature_day_utc TEXT,                                   -- YYYY-MM-DD
    ai_model_name_reporter TEXT,
    generation_parameters_json TEXT,                               -- JSON 字符串
    generated_at_utc TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%fZ', 'now'))
);

--------------------------------------------------------------------------------
-- 表5: weekly_reports_log - 每週報告
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weekly_reports_log (
    report_week_start_date TEXT PRIMARY KEY,                       -- 報告對應週的第一天 (YYYY-MM-DD, UTC)
    report_text_markdown TEXT,
    source_daily_report_dates_json TEXT,                           -- JSON 列表字符串
    source_feature_week_descriptor TEXT,                           -- 例如 "2023-W51"
    ai_model_name_reporter TEXT,
    generation_parameters_json TEXT,
    generated_at_utc TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%fZ', 'now'))
);

--------------------------------------------------------------------------------
-- 表6: monthly_reports_log - 每月報告
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monthly_reports_log (
    report_month_start_date TEXT PRIMARY KEY,                      -- 報告對應月的第一天 (YYYY-MM-DD, UTC)
    report_text_markdown TEXT,
    source_weekly_report_dates_json TEXT,                          -- JSON 列表字符串
    source_feature_month_descriptor TEXT,                          -- 例如 "2023-12"
    ai_model_name_reporter TEXT,
    generation_parameters_json TEXT,
    generated_at_utc TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%fZ', 'now'))
);

-- 結束 --
