# -*- coding: utf-8 -*-
"""
此模組包含用於清洗和轉換從 TAIFEX 原始檔案汲取的數據的函數。
每個函數通常對應 taifex_format_catalog.json 中的一個 'cleaner_function'條目。

主要職責：
- 數據類型轉換
- 欄位重命名與選擇 (基於 recipe['column_mapping_curated'])
- 處理缺失值/預設值 (基於 recipe['data_type_defaults'])
- 業務邏輯衍生的欄位計算 (如果適用且不複雜)
- 壞數據行的識別與隔離
"""
import pandas as pd
from typing import Dict, Any, Tuple, List
import logging

# 獲取一個模組級別的 logger
# logger = logging.getLogger(__name__)
# 為了簡化，假設 logger 由調用方 (TaifexService) 傳入或服務本身有 logger
# 如果需要獨立測試此模組，可以取消註解上面的 logger

def clean_options_daily_data(df_raw: pd.DataFrame, recipe: Dict[str, Any], logger: logging.Logger) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    清洗「每日選擇權行情」數據。

    Args:
        df_raw (pd.DataFrame): 從 raw_lake 讀取的原始 DataFrame。
        recipe (Dict[str, Any]): 此數據類型的處理配方，來自 taifex_format_catalog.json。
                                 應包含 'column_mapping_curated', 'data_type_defaults',
                                 以及 'schema_curated_ref' (間接指向 database_schemas.json 中的類型定義)。
        logger (logging.Logger): 用於日誌記錄的 logger 實例。

    Returns:
        Tuple[pd.DataFrame, List[Dict[str, Any]]]:
            - cleaned_df (pd.DataFrame): 清洗和轉換後的 DataFrame，準備寫入 curated_mart。
            - quarantined_rows (List[Dict[str, Any]]): 包含壞數據行及其錯誤信息的字典列表。
    """
    logger.info(f"開始清洗選擇權每日數據，共 {len(df_raw)} 行。配方描述: {recipe.get('description')}")

    # 複製一份 DataFrame 以避免修改原始數據 (雖然 df_raw 本身也是副本)
    df = df_raw.copy()

    # 預期 curated_mart 中的欄位名 (來自配方的 column_mapping_curated 的值)
    # 以及它們的目標類型 (來自 database_schemas.json，通過 recipe['schema_curated_ref'] 獲取)
    # 這部分邏輯可能需要在 TaifexService 中預先解析 schema，然後傳遞給 cleaner，
    # 或者 cleaner 依賴配方中明確的類型指示。
    # 為了簡化此範例，我們假設配方或 schema 中有足夠的類型信息。

    column_mapping_curated = recipe.get('column_mapping_curated', {})
    data_type_defaults = recipe.get('data_type_defaults', {})

    # 1. 欄位選擇和重命名 (確保只保留目標欄位，並使用標準名稱)
    # column_mapping_curated 的鍵是原始（或汲取時已初步重命名）欄位名，值是 curated 標準名
    # 但在 TaifexService 的 ingest_single_file 中，我們已經根據 recipe['column_mapping_raw'] 進行了重命名
    # 所以這裡的 df.columns 應該是 recipe['column_mapping_raw'] 的結果
    # 而 column_mapping_curated 的鍵應該是這些已初步重命名的欄位，值是最終 curated 名稱
    # 為了此範例的清晰，我們假設 column_mapping_curated 的鍵是 df 中已有的欄位名

    # 過濾掉不在 column_mapping_curated 鍵中的欄位 (如果需要嚴格按映射選擇)
    # df = df[list(column_mapping_curated.keys())] # 如果 column_mapping_curated 的鍵是原始欄位

    # 重命名欄位到 curated_mart 的標準名稱
    df.rename(columns=column_mapping_curated, inplace=True)
    logger.debug(f"欄位重命名後: {df.columns.tolist()}")

    # 確保所有在 mapping 值中出現的欄位都存在，不足的以 None 填充 (或基於 schema)
    expected_curated_columns = list(column_mapping_curated.values())
    for col in expected_curated_columns:
        if col not in df.columns:
            df[col] = None
            logger.debug(f"添加缺失的目標欄位 '{col}' 並填充為 None。")

    # 只保留最終需要的欄位
    df = df[expected_curated_columns]

    quarantined_rows: List[Dict[str, Any]] = []
    cleaned_rows_list: List[Dict[str, Any]] = []

    # 2. 逐行處理數據類型轉換和驗證 (更穩健，但可能較慢)
    # 或者可以嘗試列式操作，並捕獲錯誤

    # 獲取 DataFrame 中原始欄位名到 curated 標準欄位名的完整映射
    # column_mapping_curated 的鍵是 DataFrame 中的欄位名 (已是 _raw 後綴)
    # 值是最終 curated 表的欄位名
    final_curated_names_map = recipe.get('column_mapping_curated', {})

    # 獲取所有最終期望的 curated 欄位名列表
    # 這是 cleaned_df 最終應該包含的欄位
    expected_curated_columns = list(final_curated_names_map.values())

    # 1. 先進行欄位重命名 (從 _raw 後綴名到最終 curated 名)
    #    只對存在於 DataFrame 中的欄位進行重命名
    rename_map_for_df = {raw_col: curated_col for raw_col, curated_col in final_curated_names_map.items() if raw_col in df.columns}
    df.rename(columns=rename_map_for_df, inplace=True)
    logger.debug(f"欄位初步重命名後 (raw -> curated names if exists): {df.columns.tolist()}")

    # 2. 確保所有 expected_curated_columns 都存在於 DataFrame 中，不足的以 None 填充
    #    這樣後續的類型轉換可以統一處理這些標準欄位名
    for col in expected_curated_columns:
        if col not in df.columns:
            df[col] = None # 使用 None 作為初始填充，後續類型轉換時會參考 default_values
            logger.debug(f"添加缺失的目標欄位 '{col}' 並填充為 None。")

    # 3. 選擇 DataFrame 中只包含 expected_curated_columns 的欄位
    #    這樣 df 就只包含我們關心的目標欄位了
    df = df[expected_curated_columns].copy() # 使用 .copy() 避免 SettingWithCopyWarning

    quarantined_rows_list: List[Dict[str, Any]] = []
    # 用於存儲有效行的索引，最後用 .loc[valid_indices] 提取有效行，避免逐行構建 DataFrame
    valid_indices = []

    for index, row_series in df.iterrows():
        original_row_for_quarantine = df_raw.loc[index].to_dict() # 記錄原始未重命名的行數據
        current_curated_row = row_series.copy() # 當前處理的行，欄位名已是 curated 標準名
        errors_in_row = []

        # 'trade_date' 轉換
        if 'trade_date' in df.columns:
            raw_date_val = current_curated_row.get('trade_date')
            try:
                # 如果在重命名前 trade_date 就已经是 pd.Timestamp 或 date 对象，这里会保持
                if pd.isna(raw_date_val):
                    current_curated_row['trade_date'] = data_type_defaults.get('trade_date')
                elif not isinstance(raw_date_val, (pd.Timestamp, pd.datetime, pd.np.datetime64)): # pd.np.datetime64 for older pandas
                    current_curated_row['trade_date'] = pd.to_datetime(raw_date_val).date()
                elif isinstance(raw_date_val, pd.Timestamp): # 如果已经是 Timestamp, 取 date 部分
                     current_curated_row['trade_date'] = raw_date_val.date()
                # else: 保持原樣 (已经是 date 对象)
            except Exception as e:
                errors_in_row.append(f"trade_date '{raw_date_val}' 轉換失敗: {e}")
                current_curated_row['trade_date'] = data_type_defaults.get('trade_date')

        # 數值類型轉換
        numerical_fields_type_map = {
            "close_price": "float", "open_price": "float", "high_price": "float", "low_price": "float",
            "settlement_price": "float", "strike_price": "float",
            "volume": "int", "open_interest": "int",
            "best_bid_price": "float", "best_ask_price": "float",
            "implied_volatility": "float"
        }
        # 允許千分位的欄位 (這些通常是價格或數量)
        allow_thousands_for = ["close_price", "open_price", "high_price", "low_price",
                               "settlement_price", "volume", "open_interest",
                               "best_bid_price", "best_ask_price"]

        for field, target_type in numerical_fields_type_map.items():
            if field not in df.columns: continue # 如果該欄位最終不在 expected_curated_columns 中，跳過

            raw_val_series = current_curated_row.get(field)
            val_to_process = None

            if pd.isna(raw_val_series) or str(raw_val_series).strip() == "" or \
               str(raw_val_series).strip().upper() == "N/A" or str(raw_val_series).strip() == "-":
                val_to_process = data_type_defaults.get(field)
            else:
                val_to_process = str(raw_val_series)
                if field in allow_thousands_for:
                    val_to_process = val_to_process.replace(',', '')

                if '%' in val_to_process and field == "implied_volatility": # 特殊處理 IV
                    try:
                        val_to_process = float(val_to_process.replace('%', '')) / 100.0
                    except ValueError:
                        errors_in_row.append(f"欄位 '{field}' 值 '{raw_val_series}' (含%) 轉換為浮點數失敗。")
                        val_to_process = data_type_defaults.get(field)

            if val_to_process is not None:
                try:
                    if target_type == "float":
                        current_curated_row[field] = float(val_to_process)
                    elif target_type == "int":
                        current_curated_row[field] = int(float(val_to_process))
                except (ValueError, TypeError) as e:
                    errors_in_row.append(f"欄位 '{field}' 值 '{raw_val_series}' (處理後為 '{val_to_process}') 轉換為 {target_type} 失敗: {e}")
                    current_curated_row[field] = data_type_defaults.get(field)
            else: # val_to_process is None (來自預設值)
                 current_curated_row[field] = None # 確保是 Python None

        # 字串類型欄位 (例如: contract_symbol, option_type, expiry_period)
        string_fields = ["contract_symbol", "option_type", "expiry_period"]
        for field in string_fields:
            if field not in df.columns: continue

            raw_val_series = current_curated_row.get(field)
            if pd.notna(raw_val_series):
                current_curated_row[field] = str(raw_val_series).strip()
            else:
                current_curated_row[field] = data_type_defaults.get(field)

        # 更新回 df 中，或收集到新的 list of dicts
        if errors_in_row:
            logger.warning(f"處理索引 {index} (原始檔名: {df_raw.loc[index].get('raw_source_file', 'N/A') if 'raw_source_file' in df_raw.columns else 'N/A'}) 的數據時發現問題。原始數據: {original_row_for_quarantine}, 錯誤: {'; '.join(errors_in_row)}")
            quarantined_rows_list.append({
                "original_row_data_json": original_row_for_quarantine, # 存儲原始行數據
                "error_message": "; ".join(errors_in_row),
                "recipe_description": recipe.get('description'),
                "source_file_fingerprint": recipe.get('_fingerprint_during_service_call', 'N/A')
            })
        else:
            # 如果沒有錯誤，將處理後的行更新回 df (如果選擇原地修改)
            # 或者將 current_curated_row 添加到一個新的列表中，最後從列表創建 DataFrame
            df.loc[index] = current_curated_row # 更新 df 中的行
            valid_indices.append(index)

    # 根據有效索引提取乾淨的行
    if valid_indices:
        cleaned_df = df.loc[valid_indices].copy()
        # 再次確保最終欄位和順序
        cleaned_df = cleaned_df[expected_curated_columns]
    else: # 如果所有行都被隔離了
        cleaned_df = pd.DataFrame(columns=expected_curated_columns)


    logger.info(f"選擇權每日數據清洗完成。共 {len(cleaned_df)} 行乾淨數據，{len(quarantined_rows_list)} 行隔離數據。")
    return cleaned_df, quarantined_rows


def clean_institutional_trades_data(df_raw: pd.DataFrame, recipe: Dict[str, Any], logger: logging.Logger) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    清洗「三大法人交易資訊」數據。
    (此為佔位符，需要根據實際數據格式和需求實現)

    Args:
        df_raw (pd.DataFrame): 從 raw_lake 讀取的原始 DataFrame。
        recipe (Dict[str, Any]): 此數據類型的處理配方。
        logger (logging.Logger): 日誌記錄器實例。

    Returns:
        Tuple[pd.DataFrame, List[Dict[str, Any]]]:
            - cleaned_df (pd.DataFrame): 清洗後的 DataFrame。
            - quarantined_rows (List[Dict[str, Any]]): 壞數據行列表。
    """
    logger.info(f"開始清洗三大法人交易數據 (佔位符實現)，共 {len(df_raw)} 行。配方: {recipe.get('description')}")
    # 實際的清洗邏輯將與 clean_options_daily_data 類似:
    # 1. 複製 DataFrame
    # 2. 獲取欄位映射、預設值等配置
    # 3. 欄位選擇和重命名
    # 4. 逐行或列式處理數據類型轉換、缺失值、特定業務邏輯
    # 5. 收集錯誤行到 quarantined_rows
    # 6. 返回 cleaned_df 和 quarantined_rows

    # 暫時直接返回原始數據和空的隔離列表作為佔位
    logger.warning("clean_institutional_trades_data 的清洗邏輯尚未完全實現，目前返回原始數據。")

    # 即使是佔位符，也應該嘗試進行基本的欄位重命名 (如果配方中有定義)
    df = df_raw.copy()
    column_mapping_curated = recipe.get('column_mapping_curated', {})
    if column_mapping_curated:
        df.rename(columns=column_mapping_curated, inplace=True)
        logger.debug(f"欄位重命名後 (佔位符): {df.columns.tolist()}")

        # 確保所有在 mapping 值中出現的欄位都存在
        expected_curated_columns = list(column_mapping_curated.values())
        for col in expected_curated_columns:
            if col not in df.columns:
                df[col] = None # 添加缺失的目標欄位
        df = df[expected_curated_columns] # 只保留目標欄位

    return df, []


# 可以在此處添加更多針對不同 TAIFEX 檔案類型的清洗函數
# 例如: clean_futures_daily_quotes, clean_large_trader_positions 等

if __name__ == '__main__':
    # 簡易測試 (需要構建模擬的 df_raw, recipe, logger)

    # 模擬 Logger
    test_logger = logging.getLogger("CleanerTest")
    test_logger.setLevel(logging.DEBUG)
    if not test_logger.hasHandlers():
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        test_logger.addHandler(ch)

    test_logger.info("--- 開始測試 taifex_cleaners.py ---")

    # 測試 clean_options_daily_data
    mock_options_data_raw = {
        # 這些是 recipe['column_mapping_raw'] 執行後的欄位名
        'trade_date_raw': ['2023/12/20', '2023/12/21', 'BadDate', '2023/12/22', '2023/12/23'],
        'contract_raw': ['TXO', 'TXO', 'TXO', 'TEO', 'TXO'],
        'expiry_raw': ['202312W4', '202312W4', '202312W4', '202401', '202312W5'],
        'strike_raw': ['17000', '17050.0', '17100', '16900', 'N/A'],
        'type_raw': ['買權', '賣權', '買權', '賣權', '買權'],
        'close_raw': ['500.5', '25,123.0', 'N/A', '10.0', 'BadValue'], # 含千分位, N/A, 壞值
        'volume_raw': ['1,234', '567', '300', '-', '100.5'], # 含千分位, -, 浮點數給整數型
        'iv_raw': ['20.5%', '18.0', 'N/A', '22.0%', 'BadIV%'] # 含百分號
    }
    df_options_raw = pd.DataFrame(mock_options_data_raw)

    mock_options_recipe = {
        "description": "測試每日選擇權行情",
        # 鍵是 df_options_raw 中的欄位名，值是 curated_mart 中的標準名
        "column_mapping_curated": {
            'trade_date_raw': 'trade_date',
            'contract_raw': 'contract_symbol',
            'expiry_raw': 'expiry_period',
            'strike_raw': 'strike_price',
            'type_raw': 'option_type',
            'close_raw': 'close_price',
            'volume_raw': 'volume',
            'iv_raw': 'implied_volatility',
            # 'open_interest_raw': 'open_interest' # 假設這個欄位在原始數據中不存在，但我們希望它出現在 curated 中
        },
        "data_type_defaults": {
            "volume": 0, # 成交量預設為 0
            "close_price": None, # 收盤價預設為 None
            "strike_price": None,
            "implied_volatility": None,
            "open_interest": 0 # 假設 oi 預設為0
        },
        "schema_curated_ref": "fact_options_daily_quotes" # 指向 database_schemas.json 中的定義
                                                        # 清洗函數內部目前沒有直接用這個，而是依賴 numerical_fields_map
    }
    # 手動添加一個期望在 curated 中的欄位，但在 raw 中不存在，以測試預設值填充
    mock_options_recipe["column_mapping_curated"]['open_interest_raw_placeholder'] = 'open_interest'


    test_logger.info("\n--- 測試 clean_options_daily_data ---")
    cleaned_options_df, quarantined_options_rows = clean_options_daily_data(df_options_raw, mock_options_recipe, test_logger)

    test_logger.info(f"\n清洗後的 DataFrame (clean_options_daily_data):\n{cleaned_options_df}")
    test_logger.info(f"\n隔離的行 (clean_options_daily_data): 共 {len(quarantined_options_rows)} 行")
    for i, row_info in enumerate(quarantined_options_rows):
        test_logger.info(f"  隔離行 {i+1}: 錯誤='{row_info['error_message']}', 原始數據='{row_info['original_row_data']}'")

    # 簡單驗證
    assert 'trade_date' in cleaned_options_df.columns
    assert 'open_interest' in cleaned_options_df.columns # 應被添加並使用預設值
    assert cleaned_options_df['open_interest'].iloc[0] == mock_options_recipe['data_type_defaults']['open_interest']

    # 預期第一行應成功，第二行 volume 應為 567，close_price 應為 25123.0
    # 第三行 trade_date, close_price 轉換失敗，應被隔離
    # 第四行 strike_price (N/A) 應為 None, volume (-) 應為0
    # 第五行 close_price (BadValue), iv_raw (BadIV%) 應被隔離
    if not cleaned_options_df.empty:
        assert cleaned_options_df.iloc[0]['volume'] == 1234
        assert cleaned_options_df.iloc[0]['close_price'] == 500.5
        assert cleaned_options_df.iloc[0]['implied_volatility'] == 0.205
        if len(cleaned_options_df) > 1: # 如果第二行也被清理了
            assert cleaned_options_df.iloc[1]['volume'] == 0 # 因為 strike_price 是 N/A, 會導致整行隔離
            assert cleaned_options_df.iloc[1]['strike_price'] is None

    assert len(quarantined_options_rows) == 3 # 預期第0,2,4行(索引)的原始數據會產生隔離行

    test_logger.info("--- taifex_cleaners.py 測試完畢 ---")
