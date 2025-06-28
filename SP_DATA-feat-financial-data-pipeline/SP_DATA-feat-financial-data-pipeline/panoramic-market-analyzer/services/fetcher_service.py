# services/fetcher_service.py
import argparse
import yfinance as yf
import duckdb
import os
import pandas as pd

def fetch_and_store(symbol: str, start_date: str, end_date: str, db_path: str):
    """從 yfinance 獲取數據，清理欄位名後，再存儲到 DuckDB。"""
    print(f"[Fetcher] Fetching {symbol} from {start_date} to {end_date}...")
    df = yf.download(symbol, start=start_date, end=end_date)

    if df.empty:
        print(f"[Fetcher] No data returned for {symbol}. Exiting.")
        return

    # --- 關鍵修復：標準化欄位名稱 ---
    # yfinance 可能返回多層級欄位 (MultiIndex)，例如 ('Close', '2330.TW')。
    # 這行程式碼會將其「扁平化」，只取第一個層級作為新的欄位名，例如 'Close'。
    # 這確保了存入資料庫的欄位永遠是乾淨、單一的字串。
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    # ------------------------------------

    # 確保目錄存在
    # 在 df.empty 檢查之後，如果 df 不是空的，我們才需要確保目錄存在
    # 並且在連接數據庫之前。
    # 同時，如果 db_path 是 "data/raw_market_data.db"，os.path.dirname(db_path) 是 "data"
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(db_path)

    # 將 symbol 中的 '-' 和 '.' 都替換為 '_' 以創建安全的資料表名稱
    safe_symbol_for_table = symbol.replace('-', '_').replace('.', '_')
    table_name = f"{safe_symbol_for_table}_raw"

    # 重設索引，以便將日期索引保存為資料表中的一欄
    df_to_store = df.reset_index() # 確保 'Date' 索引成為一個欄位

    # 為資料表名稱加上雙引號，以處理可能以數字開頭或包含特殊字元的情況
    # 並使用 df_to_store 進行儲存
    con.execute(f"""CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM df_to_store""")
    con.close()
    print(f"""[Fetcher] Data for {symbol} saved to table "{table_name}" in {db_path}""")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Fetcher Service")
    parser.add_argument("--symbol", required=True, help="Stock symbol, e.g., '2330.TW'")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--db", required=True, help="Path to raw data DuckDB file")
    args = parser.parse_args()

    fetch_and_store(args.symbol, args.start, args.end, args.db)
