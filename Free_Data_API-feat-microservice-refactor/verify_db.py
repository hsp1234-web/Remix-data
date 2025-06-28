# verify_db.py
import argparse
import logging
import duckdb
# import pandas as pd # Not used

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def verify_db_table(db_path: str, table_name: str, symbol_to_check: str = None):
    """
    驗證指定的 DuckDB 資料庫中的一個表格。

    - 檢查表格是否存在。
    - 檢查是否有數據。
    - 如果提供了 symbol，檢查該 symbol 的數據是否存在。
    """
    logging.info(f"\n--- 正在驗證: 資料庫='{db_path}', 表格='{table_name}' ---")
    con = None  # 初始化 con 以確保 finally 區塊可以存取
    try:
        con = duckdb.connect(database=db_path, read_only=True)

        # 1. 檢查表格是否存在
        tables_df = con.execute("SHOW TABLES;").fetchdf()
        if table_name not in tables_df['name'].values:
            logging.error(f"❌ 驗證失敗: 表格 '{table_name}' 不存在於 '{db_path}'。")
            return False
        logging.info(f"✅ 表格存在性檢查通過。")

        # 2. 檢查表格是否為空
        # 為表名加上引號以處理特殊字元或大小寫敏感的表名（儘管 DuckDB 預設不敏感）
        total_rows_query = con.execute(f'SELECT COUNT(*) FROM "{table_name}";')
        if total_rows_query is None:
            logging.error(f"❌ 無法從表格 '{table_name}' 獲取行數查詢對象。")
            return False
        total_rows_result = total_rows_query.fetchone()
        if total_rows_result is None:
            logging.error(f"❌ 無法從表格 '{table_name}' 的查詢結果中獲取行數。")
            return False
        total_rows = total_rows_result[0]

        if total_rows == 0:
            logging.warning(f"⚠️ 驗證注意: 表格 '{table_name}' 存在但為空。")
            # con.close() # 不在這裡關閉，讓 finally 處理
            return True  # 不視為失敗，但給予警告
        logging.info(f"✅ 表格非空檢查通過，總行數: {total_rows}。")

        # 3. 檢查特定 symbol (如果提供)
        # 假設 'symbol' 欄位存在於所有相關表格中
        # 首先檢查 'symbol' 列是否存在於表中
        table_columns_df = con.execute(
            f"PRAGMA table_info('{table_name}');").fetchdf()
        if 'symbol' not in table_columns_df['name'].values:
            logging.warning(
                f"⚠️ 在表格 '{table_name}' 中未找到 'symbol' 欄位，跳過特定股票檢查。")
        elif symbol_to_check:
            # 使用參數化查詢以防止 SQL 注入，並確保股票代碼正確處理
            symbol_rows_query = con.execute(
                f'SELECT COUNT(*) FROM "{table_name}" WHERE symbol = ?;', [symbol_to_check.upper()])
            if symbol_rows_query is None:
                logging.error(
                    f"❌ 無法在表格 '{table_name}' 中查詢股票 '{symbol_to_check}' 的查詢對象。")
                return False
            symbol_rows_result = symbol_rows_query.fetchone()
            if symbol_rows_result is None:
                logging.error(
                    f"❌ 無法從表格 '{table_name}' 的股票 '{symbol_to_check}' 查詢結果中獲取行數。")
                return False
            symbol_rows = symbol_rows_result[0]

            if symbol_rows == 0:
                logging.error(
                    f"❌ 驗證失敗: 在表格 '{table_name}' 中未找到股票 '{symbol_to_check.upper()}' 的任何記錄。")
                # con.close() # 不在這裡關閉
                return False
            logging.info(
                f"✅ 特定股票檢查通過，找到 {symbol_rows} 筆 '{symbol_to_check.upper()}' 的記錄。")

        # 4. 抽樣查看數據
        sample_df_query = con.execute(f'SELECT * FROM "{table_name}" LIMIT 5;')
        if sample_df_query is None:
            logging.warning(f"⚠️ 無法從表格 '{table_name}' 抽樣數據查詢對象。")
        else:
            sample_df = sample_df_query.fetchdf()
            if sample_df.empty:
                logging.info("表格 '" + table_name + "' 中沒有數據可供抽樣預覽。")  # Reverted to string concatenation
            else:
                logging.info("抽樣數據預覽:")
                try:
                    # 使用 Pandas 的 to_string 以獲得更好的格式
                    print(sample_df.to_string())
                except Exception as e:
                    logging.warning(f"打印抽樣數據時發生錯誤: {e}")

        # con.close() # 讓 finally 處理
        return True

    except duckdb.Error as e:  # 更具體地捕獲 DuckDB 錯誤
        logging.error(
            f"❌ 驗證 '{db_path}' 中的 '{table_name}' 時發生 DuckDB 錯誤: {e}", exc_info=True)
        return False
    except Exception as e:
        logging.error(
            f"❌ 驗證 '{db_path}' 中的 '{table_name}' 時發生非預期錯誤: {e}", exc_info=True)
        return False
    finally:
        if con:
            try:
                con.close()
            except Exception as close_exc:
                logging.error(f"關閉資料庫 '{db_path}' 連接時發生額外錯誤: {close_exc}")


def main():
    parser = argparse.ArgumentParser(
        description="Verification script for the data pipeline databases.")
    parser.add_argument(
        "--symbol", help="A specific symbol to check for in the tables (e.g., AAPL).")
    parser.add_argument("--raw-db-path", default="data/raw_market_data.duckdb",
                        help="Path to the raw data DuckDB file.")
    parser.add_argument("--features-db-path", default="data/features_market_data.duckdb",
                        help="Path to the features data DuckDB file.")
    args = parser.parse_args()

    all_checks_passed = True

    logging.info("開始資料庫驗證流程...")
    logging.info(f"將驗證原始數據庫: {args.raw_db_path}")
    logging.info(f"將驗證特徵數據庫: {args.features_db_path}")

    # 驗證原始數據層
    if not verify_db_table(args.raw_db_path, "raw_prices", args.symbol):
        all_checks_passed = False

    # 驗證特徵層
    if not verify_db_table(args.features_db_path, "daily_features", args.symbol):
        all_checks_passed = False
    if not verify_db_table(args.features_db_path, "weekly_features", args.symbol):
        all_checks_passed = False

    print("\n" + "="*50)
    if all_checks_passed:
        logging.info("🎉 所有驗證檢查均已成功通過！")
    else:
        logging.error("🔥 部分驗證檢查失敗，請檢閱以上日誌！")
    print("="*50)


if __name__ == "__main__":
    main()
