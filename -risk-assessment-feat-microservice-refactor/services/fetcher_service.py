import argparse
import yfinance as yf
import duckdb
from datetime import datetime
import os
import pandas as pd

def fetch_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """從 yfinance API 獲取股價數據"""
    print(f"Fetching data for {symbol} from {start_date} to {end_date}...")
    # auto_adjust=True is the default and generally recommended.
    # Removed repair=True as it might lead to non-DataFrame returns on certain errors.
    df = yf.download(symbol, start=start_date, end=end_date, auto_adjust=True)

    if df.empty:
        # This check might be insufficient if yf.download returns something other than an empty DataFrame on error
        raise ValueError(f"No data fetched for symbol {symbol}. Check symbol or date range. yfinance might have returned an empty DataFrame or encountered an issue.")

    # Ensure df is a DataFrame before trying to access df.columns
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"yf.download did not return a pandas DataFrame. Received type: {type(df)}. Content: {str(df)}")

    # 處理列名
    new_cols = []
    for col in df.columns:
        if isinstance(col, tuple) and len(col) > 0:
            # 如果列名是元組，例如 ('Close', 'SPY')，我們只取第一個元素並轉小寫
            new_cols.append(str(col[0]).lower())
        else:
            # 否則，直接轉換為字符串並轉小寫
            new_cols.append(str(col).lower())
    df.columns = new_cols

    # 確保必要的列存在 (轉換後檢查)
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    if not all(col_name in df.columns for col_name in required_cols):
        missing = [col_name for col_name in required_cols if col_name not in df.columns]
        raise ValueError(f"Downloaded data is missing required columns after processing: {missing}. Current columns: {df.columns.tolist()}")

    print("Data fetched successfully and columns processed to simple lowercase strings.")
    return df

def save_to_duckdb(df: pd.DataFrame, symbol: str, db_path: str):
    """將 DataFrame 存儲到 DuckDB 資料庫"""
    # 確保目錄存在，如果 db_path 包含目錄
    if os.path.dirname(db_path):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    table_name = symbol.lower().replace('-', '_')
    print(f"Saving data to table '{table_name}' in '{db_path}'...")
    con = duckdb.connect(db_path)

    # Ensure all columns from df are used with quoted lowercase names in the SQL query
    # df.columns should already be lowercase here due to previous step in fetch_data
    cols = df.columns
    select_cols_sql = ", ".join([f'"{col}"' for col in cols]) # "col1", "col2", ...

    # DuckDB's to_df() or from_df() is often easier, but if constructing SQL:
    # We need to register the DataFrame df as a temporary table, then select from it.
    con.register('temp_df_to_save', df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT {select_cols_sql} FROM temp_df_to_save")
    con.unregister('temp_df_to_save') # Clean up temporary registration

    con.close()
    print("Data saved successfully.")

def main():
    """主執行函數，解析命令列參數並協調流程"""
    parser = argparse.ArgumentParser(description="Fetch stock data and save to DuckDB.")
    parser.add_argument("--symbol", type=str, required=True, help="Stock symbol (e.g., SPY)")
    parser.add_argument("--start-date", type=str, required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", type=str, default=datetime.now().strftime('%Y-%m-%d'), help="End date in YYYY-MM-DD format (defaults to today)")
    parser.add_argument("--db-path", type=str, required=True, help="Path to the output DuckDB file")

    args = parser.parse_args()

    try:
        # 1. 獲取數據
        raw_df = fetch_data(args.symbol, args.start_date, args.end_date)

        # 2. 儲存數據
        save_to_duckdb(raw_df, args.symbol, args.db_path)

    except Exception as e:
        print(f"An error occurred in fetcher_service: {e}")
        exit(1) # Exit with a non-zero code to indicate failure

if __name__ == "__main__":
    main()
