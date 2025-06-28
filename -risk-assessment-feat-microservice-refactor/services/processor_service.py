import argparse
import duckdb
import os
import pandas as pd

def load_from_duckdb(symbol: str, db_path: str) -> pd.DataFrame:
    """從 DuckDB 加載數據到 DataFrame，並增加表格存在性檢查"""
    table_name = symbol.lower().replace('-', '_')
    print(f"Loading data from table '{table_name}' in '{db_path}'...")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found at {db_path}")

    con = duckdb.connect(db_path)
    try:
        # 檢查表格是否存在
        table_check = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table_name]
        ).fetchone()

        if table_check is None:
            raise ValueError(f"Table '{table_name}' not found in database '{db_path}'.")

        df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    finally:
        con.close()

    if df.empty:
        raise ValueError(f"No data found in table {table_name}.")

    # 列名應該直接從 DuckDB 以小寫形式獲取 (由於 fetcher_service.py 中的保存方式)
    print("Data loaded successfully.")
    return df

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """計算技術指標（特徵工程）"""
    print("Processing data to calculate moving averages...")
    # Removed debugging line: print(f"Columns in DataFrame received by process_data: {df.columns.tolist()}")
    if 'close' not in df.columns:
        raise ValueError("'close' column not found in the input data.")

    df_processed = df.copy()
    df_processed['ma20'] = df_processed['close'].rolling(window=20).mean()
    df_processed['ma60'] = df_processed['close'].rolling(window=60).mean()
    print("Processing completed.")
    return df_processed.dropna()

def save_features_to_duckdb(df: pd.DataFrame, symbol: str, db_path: str):
    """將處理過的特徵 DataFrame 存儲到 DuckDB"""
    # 確保目錄存在，如果 db_path 包含目錄
    if os.path.dirname(db_path):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    table_name = f"{symbol.lower().replace('-', '_')}_features"
    print(f"Saving features to table '{table_name}' in '{db_path}'...")
    con = duckdb.connect(db_path)
    # 在保存特徵時，列名 ma20, ma60 已經是小寫
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    con.close()
    print("Features saved successfully.")

def main():
    """主執行函數，解析命令列參數並協調流程"""
    parser = argparse.ArgumentParser(description="Process raw stock data and save features.")
    parser.add_argument("--input_db", type=str, required=True, help="Path to the input raw data DuckDB file")
    parser.add_argument("--output_db", type=str, required=True, help="Path to the output features DuckDB file")
    parser.add_argument("--symbol", type=str, required=True, help="Stock symbol to process")

    args = parser.parse_args()

    try:
        # 1. 加載原始數據
        raw_df = load_from_duckdb(args.symbol, args.input_db)

        # 2. 處理數據
        features_df = process_data(raw_df)

        # 3. 儲存特徵
        save_features_to_duckdb(features_df, args.symbol, args.output_db)

    except Exception as e:
        print(f"An error occurred in processor_service: {e}")
        exit(1)

if __name__ == "__main__":
    main()
