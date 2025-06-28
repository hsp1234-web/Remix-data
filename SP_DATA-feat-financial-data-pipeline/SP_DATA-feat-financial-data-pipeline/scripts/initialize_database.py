import duckdb
import logging
from pathlib import Path
from typing import Dict, Any # For type hinting
from datetime import datetime, timezone
import sys # For __main__ StreamHandler

# Configure logger for this script
logger = logging.getLogger(f"project_logger.{__name__}")
if not logger.handlers and not logging.getLogger().hasHandlers(): # Check root logger as well
    logger.addHandler(logging.NullHandler())
    logger.debug(f"Logger for {__name__} (initialize_database) configured with NullHandler as no other handlers were found.")


# --- SQL DDL 定義 ---
SQL_DDL_FINANCIAL_DATA = """
CREATE SEQUENCE IF NOT EXISTS dim_security_internal_id_seq START 1;

CREATE TABLE IF NOT EXISTS dim_security (
    internal_id BIGINT PRIMARY KEY DEFAULT nextval('dim_security_internal_id_seq'),
    security_id VARCHAR UNIQUE NOT NULL,
    name VARCHAR,
    asset_class VARCHAR,
    exchange VARCHAR,
    currency VARCHAR,
    country VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    description TEXT,
    first_seen_date DATE,
    last_seen_date DATE,
    delisted_date DATE,
    source_api_info JSON,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp
);
CREATE INDEX IF NOT EXISTS idx_dim_security_security_id ON dim_security(security_id);
CREATE INDEX IF NOT EXISTS idx_dim_security_asset_class ON dim_security(asset_class);

CREATE TABLE IF NOT EXISTS fact_stock_price (
    price_date DATE NOT NULL,
    security_id VARCHAR NOT NULL,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    adj_close_price DOUBLE,
    volume BIGINT,
    turnover DOUBLE,
    dividends DOUBLE DEFAULT 0.0,
    stock_splits DOUBLE DEFAULT 1.0,
    vwap DOUBLE,
    transactions INTEGER,
    source_api VARCHAR NOT NULL,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (price_date, security_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_security_id_date ON fact_stock_price(security_id, price_date DESC);

CREATE TABLE IF NOT EXISTS dim_financial_metric (
    source_api VARCHAR NOT NULL,
    source_metric_name VARCHAR NOT NULL,
    canonical_metric_name VARCHAR NOT NULL,
    metric_description TEXT,
    metric_unit VARCHAR,
    statement_type_hint VARCHAR,
    is_growth_metric BOOLEAN DEFAULT FALSE,
    last_updated_in_db_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (source_api, source_metric_name),
    UNIQUE (canonical_metric_name)
);
CREATE INDEX IF NOT EXISTS idx_dim_financial_metric_canonical_name ON dim_financial_metric(canonical_metric_name);

CREATE TABLE IF NOT EXISTS fact_financial_statement (
    security_id VARCHAR NOT NULL,
    fiscal_period VARCHAR NOT NULL,
    announcement_date DATE NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE,
    currency VARCHAR,
    report_date DATE NOT NULL,
    filing_date DATE,
    statement_type VARCHAR,
    source_api VARCHAR NOT NULL,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (security_id, fiscal_period, announcement_date, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_fact_financial_statement_sec_period_ann_metric ON fact_financial_statement(security_id, fiscal_period, announcement_date, metric_name);
CREATE INDEX IF NOT EXISTS idx_fact_financial_statement_sec_metric_ann ON fact_financial_statement(security_id, metric_name, announcement_date DESC);

CREATE TABLE IF NOT EXISTS fact_macro_economic_data (
    metric_date DATE NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE,
    frequency VARCHAR,
    unit VARCHAR,
    notes TEXT,
    source_api VARCHAR NOT NULL,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (metric_date, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_fact_macro_metric_name_date ON fact_macro_economic_data(metric_name, metric_date DESC);

CREATE TABLE IF NOT EXISTS fact_alternative_data (
    data_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    security_id VARCHAR,
    factor_name VARCHAR NOT NULL,
    factor_value_numeric DOUBLE,
    factor_value_text TEXT,
    factor_value_json JSON,
    source_calculation_id VARCHAR,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (data_timestamp, security_id, factor_name)
);
CREATE INDEX IF NOT EXISTS idx_fact_alt_data_sec_factor_time ON fact_alternative_data(security_id, factor_name, data_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_alt_data_factor_time ON fact_alternative_data(factor_name, data_timestamp DESC);
"""

SQL_DDL_AI_JUDGMENTS = """
CREATE TABLE IF NOT EXISTS ai_simulation_log (
    simulation_timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    market_briefing TEXT NOT NULL,
    ai_model_used VARCHAR NOT NULL,
    prompt_details TEXT,
    ai_raw_response TEXT,
    extracted_strategy JSON,
    extracted_factors JSON,
    processing_time_seconds DOUBLE,
    backtest_result_summary JSON
);
CREATE INDEX IF NOT EXISTS idx_ai_simulation_log_model_time ON ai_simulation_log(ai_model_used, simulation_timestamp DESC);
"""

try:
    PROJECT_ROOT_DIR = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT_DIR = Path(".").resolve()
    logger.warning(f"__file__ not defined, using CWD {PROJECT_ROOT_DIR} as project root for log path construction.")

DEFAULT_DATA_DIR = PROJECT_ROOT_DIR / "data"
FINANCIAL_DATA_DB_PATH_DEFAULT = DEFAULT_DATA_DIR / "financial_data.duckdb"
AI_JUDGMENTS_DB_PATH_DEFAULT = DEFAULT_DATA_DIR / "ai_historical_judgments.duckdb"


def prefill_dim_financial_metric(con: duckdb.DuckDBPyConnection):
    logger.info("Prefilling dim_financial_metric table...")
    initial_metrics = [
        {"source_api": "finmind", "source_metric_name": "營業收入", "canonical_metric_name": "revenue", "metric_description": "Total operating revenue", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "營業毛利（毛損）", "canonical_metric_name": "gross_profit", "metric_description": "Gross profit or loss", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "營業利益（損失）", "canonical_metric_name": "operating_income", "metric_description": "Operating income or loss", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "繼續營業單位稅前淨利（淨損）", "canonical_metric_name": "pretax_income", "metric_description": "Income before tax from continuing operations", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "本期淨利（淨損）歸屬於母公司業主", "canonical_metric_name": "net_income_parent", "metric_description": "Net income attributable to owners of parent", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "基本每股盈餘", "canonical_metric_name": "eps_basic", "metric_description": "Basic earnings per share", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "流動資產", "canonical_metric_name": "current_assets", "metric_description": "Total current assets", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "非流動資產", "canonical_metric_name": "non_current_assets", "metric_description": "Total non-current assets", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "資產總計", "canonical_metric_name": "total_assets", "metric_description": "Total assets", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "流動負債", "canonical_metric_name": "current_liabilities", "metric_description": "Total current liabilities", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "非流動負債", "canonical_metric_name": "non_current_liabilities", "metric_description": "Total non-current liabilities", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "負債總計", "canonical_metric_name": "total_liabilities", "metric_description": "Total liabilities", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "歸屬於母公司業主之權益合計", "canonical_metric_name": "equity_parent", "metric_description": "Equity attributable to owners of parent", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "權益總計", "canonical_metric_name": "total_equity", "metric_description": "Total equity", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "營業活動之淨現金流入（流出）", "canonical_metric_name": "net_cash_ops", "metric_description": "Net cash flow from operating activities", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},
        {"source_api": "finmind", "source_metric_name": "投資活動之淨現金流入（流出）", "canonical_metric_name": "net_cash_inv", "metric_description": "Net cash flow from investing activities", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},
        {"source_api": "finmind", "source_metric_name": "籌資活動之淨現金流入（流出）", "canonical_metric_name": "net_cash_fin", "metric_description": "Net cash flow from financing activities", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},
        {"source_api": "finmind", "source_metric_name": "本期現金及約當現金增加（減少）數", "canonical_metric_name": "net_change_cash", "metric_description": "Net change in cash and cash equivalents", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},
        {"source_api": "calculated", "source_metric_name": "ROE", "canonical_metric_name": "roe", "metric_description": "Return on Equity", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "ROA", "canonical_metric_name": "roa", "metric_description": "Return on Assets", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "GrossProfitMargin", "canonical_metric_name": "gross_profit_margin", "metric_description": "Gross Profit Margin", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "OperatingProfitMargin", "canonical_metric_name": "operating_profit_margin", "metric_description": "Operating Profit Margin", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "NetProfitMargin", "canonical_metric_name": "net_profit_margin", "metric_description": "Net Profit Margin", "metric_unit": "%", "statement_type_hint": "ratios"},
    ]
    now_utc = datetime.now(timezone.utc)
    data_to_insert = [
        (
            item["source_api"], item["source_metric_name"], item["canonical_metric_name"],
            item.get("metric_description"), item.get("metric_unit"),
            item.get("statement_type_hint"), item.get("is_growth_metric", False),
            now_utc
        ) for item in initial_metrics
    ]
    insert_sql = """
    INSERT INTO dim_financial_metric
        (source_api, source_metric_name, canonical_metric_name, metric_description, metric_unit, statement_type_hint, is_growth_metric, last_updated_in_db_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (source_api, source_metric_name) DO UPDATE SET
        canonical_metric_name = excluded.canonical_metric_name,
        metric_description = excluded.metric_description,
        metric_unit = excluded.metric_unit,
        statement_type_hint = excluded.statement_type_hint,
        is_growth_metric = excluded.is_growth_metric,
        last_updated_in_db_timestamp = excluded.last_updated_in_db_timestamp;
    """
    try:
        con.executemany(insert_sql, data_to_insert)
        logger.info(f"Successfully inserted/updated {len(data_to_insert)} records into dim_financial_metric.")
    except duckdb.Error as e:
        logger.error(f"Error prefilling dim_financial_metric: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during prefill_dim_financial_metric: {e}", exc_info=True)


def initialize_all_databases(
    financial_db_path: Path = FINANCIAL_DATA_DB_PATH_DEFAULT,
    ai_judgments_db_path: Path = AI_JUDGMENTS_DB_PATH_DEFAULT
    ):
    logger.info("Starting initialization of all databases...")

    data_dir = financial_db_path.parent
    data_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Data directory '{data_dir}' confirmed/created.")

    logger.info(f"Initializing database: {financial_db_path}")
    con_financial = None
    try:
        con_financial = duckdb.connect(database=str(financial_db_path), read_only=False)
        logger.info(f"Successfully connected to/created {financial_db_path}")
        con_financial.execute(SQL_DDL_FINANCIAL_DATA)
        logger.info(f"Executed core table and index DDLs in {financial_db_path}.")
        prefill_dim_financial_metric(con_financial)
        logger.info(f"{financial_db_path} initialization complete.")
    except duckdb.Error as e:
        logger.error(f"DuckDB error initializing {financial_db_path}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error initializing {financial_db_path}: {e}", exc_info=True)
    finally:
        if con_financial:
            con_financial.close()
            logger.info(f"Connection to {financial_db_path} closed.")

    logger.info(f"Initializing database: {ai_judgments_db_path}")
    con_ai = None
    try:
        con_ai = duckdb.connect(database=str(ai_judgments_db_path), read_only=False)
        logger.info(f"Successfully connected to/created {ai_judgments_db_path}")
        con_ai.execute(SQL_DDL_AI_JUDGMENTS)
        logger.info(f"Executed core table and index DDLs in {ai_judgments_db_path}.")
        logger.info(f"{ai_judgments_db_path} initialization complete.")
    except duckdb.Error as e:
        logger.error(f"DuckDB error initializing {ai_judgments_db_path}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error initializing {ai_judgments_db_path}: {e}", exc_info=True)
    finally:
        if con_ai:
            con_ai.close()
            logger.info(f"Connection to {ai_judgments_db_path} closed.")

    logger.info("All database initialization processes finished.")


if __name__ == "__main__":
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )

    logger.info(f"Running database initialization with default paths: "
                f"FinancialDB='{FINANCIAL_DATA_DB_PATH_DEFAULT}', AIJudgmentsDB='{AI_JUDGMENTS_DB_PATH_DEFAULT}'")
    initialize_all_databases()
```
**對草案的增強和調整摘要（V2 更新）：**
*   **日誌記錄器初始化：** 模組級別的 logger 初始化與其他模組保持一致。
*   **DDL 整合：** 將所有表的 `CREATE TABLE` 和 `CREATE INDEX` 語句整合到 `SQL_DDL_FINANCIAL_DATA` 和 `SQL_DDL_AI_JUDGMENTS` 這兩個多行字串變數中。DuckDB 的 `execute()` 方法可以執行包含多條 SQL 語句的字串。
*   **資料庫路徑：** 預設的資料庫路徑現在基於 `__file__` 計算得出，指向專案根目錄下的 `data/` 子目錄。這使得腳本在不同環境中（只要它位於 `scripts/` 子目錄下）更具可移植性。`initialize_all_databases` 函數也接受可選的路徑參數，以便將來可以從 `config.yaml` 讀取路徑。
*   **`prefill_dim_financial_metric` 更新：**
    *   擴充了 `initial_metrics` 列表，包含了更多基於 FinMind（損益表、資產負債表、現金流量表示例）和「計算所得」的指標，並添加了 `metric_unit` 和 `statement_type_hint`。
    *   修正了 `INSERT ... ON CONFLICT` 語句，使其與 `dim_financial_metric` 的主鍵 `(source_api, source_metric_name)` 和 `UNIQUE (canonical_metric_name)` 約束更穩健地工作。現在它會基於 `(source_api, source_metric_name)` 進行衝突判斷，並更新其他欄位。這也意味著如果嘗試用不同的 `(source_api, source_metric_name)` 映射到一個已存在的 `canonical_metric_name`，會因違反 `UNIQUE` 約束而失敗（這是期望的行為，以保證 `canonical_metric_name` 的唯一性）。
*   **`initialize_all_databases` 函數：**
    *   現在是主執行函數，負責創建 `data` 目錄，並分別初始化兩個資料庫。
    *   在創建 `financial_data.duckdb` 的表之後調用 `prefill_dim_financial_metric`。
*   **`if __name__ == "__main__":` 塊：**
    *   簡化了，直接調用 `initialize_all_databases()` 使用預設路徑。
    *   日誌配置確保在直接運行此腳本時，INFO 及以上級別的日誌會輸出到控制台。

這個版本的 `initialize_database.py` 更加完整，並且能獨立運行以建立所有必要的資料庫結構和初始維度數據。
