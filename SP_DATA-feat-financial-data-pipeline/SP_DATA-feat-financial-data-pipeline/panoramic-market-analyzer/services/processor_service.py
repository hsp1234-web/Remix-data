# services/processor_service.py
import argparse
import duckdb
import os
import pandas as pd

def process_and_store(raw_db_path: str, feature_db_path: str, symbol: str):
    """從原始資料庫讀取、處理並存儲到特徵資料庫。"""
    print(f"[Processor] Processing data for {symbol}...")
    if not os.path.exists(raw_db_path):
        print(f"[Processor] Error: Raw DB not found at {raw_db_path}")
        exit(1)

    # 將 symbol 中的 '-' 和 '.' 都替換為 '_' 以匹配 fetcher 中創建的資料表名稱
    safe_symbol_for_table = symbol.replace('-', '_').replace('.', '_')
    raw_table_name = f"{safe_symbol_for_table}_raw"

    raw_con = duckdb.connect(raw_db_path, read_only=True)

    try:
        # 為資料表名稱加上雙引號
        df = raw_con.execute(f"""SELECT * FROM "{raw_table_name}" """).fetchdf()
    except duckdb.CatalogException:
        print(f"""[Processor] Error: Table "{raw_table_name}" not found in {raw_db_path}""")
        exit(1)
    finally:
        raw_con.close()

    # --- 核心處理邏輯 ---
    # 確保使用正確的欄位名稱（yfinance 通常返回首字母大寫的欄位）
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    df.dropna(inplace=True)
    # ---------------------

    # 確保目錄存在
    os.makedirs(os.path.dirname(feature_db_path), exist_ok=True)

    feature_con = duckdb.connect(feature_db_path)
    # 使用與 raw_table_name 同樣的邏輯來創建 feature_table_name
    feature_table_name = f"{safe_symbol_for_table}_features"
    # 為資料表名稱加上雙引號
    feature_con.execute(f"""CREATE OR REPLACE TABLE "{feature_table_name}" AS SELECT * FROM df""")
    feature_con.close()
    print(f"""[Processor] Features for {symbol} saved to table "{feature_table_name}" in {feature_db_path}""")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Processor Service")
    parser.add_argument("--raw-db", required=True, help="Path to raw data DB")
    parser.add_argument("--feature-db", required=True, help="Path to feature store DB")
    parser.add_argument("--symbol", required=True, help="Stock symbol to process")
    args = parser.parse_args()

    process_and_store(args.raw_db, args.feature_db, args.symbol)
