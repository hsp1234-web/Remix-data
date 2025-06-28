# -*- coding: utf-8 -*-
"""
範例數據清洗函式模組 (Example Cleaners)

包含遵循標準化清洗函式介面的範例清洗函式。
每個清洗函式應接收一個 pandas DataFrame，並返回一個清洗後的新的 DataFrame。
函式名稱將在 format_catalog.json 中被引用。
"""
import pandas as pd
import numpy as np # Numpy 通常與 Pandas 一起使用，用於數值操作或 np.nan

from taifex_pipeline.core.logger_setup import get_logger

logger = get_logger(__name__)

# --- 清洗函式介面約定 ---
# def specific_cleaner_function(df: pd.DataFrame) -> pd.DataFrame:
#     # ... 清洗邏輯 ...
#     return cleaned_df
# --- ------------------- ---

def clean_daily_ohlcv_example_v1(df: pd.DataFrame) -> pd.DataFrame:
    """
    一個範例清洗函式，假設用於處理一個包含 OHLCV (開高低收量) 的每日行情數據。

    預期輸入 DataFrame 的欄位可能包含 (欄位名可能不一，取決於 parser_config):
    - '交易日期' (例如 "2023/01/01" 或 "20230101" 或 "112/01/01")
    - '商品代號'
    - '開盤價'
    - '最高價'
    - '最低價'
    - '收盤價'
    - '成交量'
    - '結算價' (可選)
    - '未平倉合約數' (可選)
    - 其他可能存在的欄位...

    清洗操作:
    1. 複製 DataFrame 以避免修改原始數據。
    2. 標準化欄位名稱 (例如，統一為英文小寫下劃線風格)。
    3. 處理「交易日期」：
        - 轉換為 pandas datetime 物件。
        - 處理可能的民國年或不同日期格式。
    4. 處理數值型欄位 (開/高/低/收/量/結算價/未平倉)：
        - 移除可能存在的千分位逗號。
        - 轉換為數值類型 (float 或 int)。
        - 將無法轉換的異常值 (例如 '-') 設為 NaN。
    5. 移除完全重複的行 (如果需要)。
    6. 根據需求選擇或重新排序欄位。
    """
    logger.info(f"開始執行清洗函式 'clean_daily_ohlcv_example_v1'，輸入 DataFrame 行數: {len(df)}")
    if df.empty:
        logger.warning("輸入 DataFrame 為空，直接返回。")
        return df

    # 1. 複製 DataFrame
    cleaned_df = df.copy()

    # 2. 標準化欄位名稱 (假設原始欄位名是中文，轉換為英文)
    # 這部分通常更建議在 parser_config 的 'names' 參數中處理，
    # 或者在 cleaner 中針對已知的幾種原始命名方式做映射。
    # 此處為簡化範例，假設欄位名已接近目標，只做簡單處理。

    column_rename_map = {
        # 中文名 (可能來自檔案) : 標準英文名
        "交易日期": "trade_date",
        "商品代號": "product_id",
        "契約": "product_id", # 有些檔案可能用 '契約'
        "開盤價": "open",
        "最高價": "high",
        "最低價": "low",
        "收盤價": "close",
        "成交量": "volume",
        "成交筆數": "ticks", # 假設有此欄位
        "結算價": "settlement_price",
        "未平倉合約數": "open_interest",
        "未平倉量": "open_interest",
        # ... 其他可能的欄位名映射 ...
    }

    # 只重命名存在的欄位
    actual_rename_map = {k: v for k, v in column_rename_map.items() if k in cleaned_df.columns}
    if actual_rename_map:
        cleaned_df.rename(columns=actual_rename_map, inplace=True)
        logger.debug(f"欄位重命名完成。映射: {actual_rename_map}")

    # 3. 處理 'trade_date'
    if 'trade_date' in cleaned_df.columns:
        try:
            # 嘗試多種日期格式，包括民國年轉換
            # 先替換掉可能的 '/' 和 '-' 為空，方便處理 'YYYYMMDD' 和 'YYYMMDD' (民國)
            date_series = cleaned_df['trade_date'].astype(str).str.replace(r'[/|-]', '', regex=True)

            # 判斷是否為民國年 (長度為 6 或 7，例如 1120101 或 980101)
            is_roc_year = date_series.str.len().isin([6, 7]) & date_series.str.match(r'^\d{6,7}$')

            roc_dates = date_series[is_roc_year]
            gregorian_dates_from_roc = pd.Series(index=roc_dates.index, dtype='datetime64[ns]')
            if not roc_dates.empty:
                year_offset = 1911
                # 分別處理 YYYMMDD 和 YYMMDD (民國百年以上和百年以下)
                # 民國100年 = 2011年. 7位數: 1000101 -> 100+1911 = 2011
                # 6位數: 980101 -> 98+1911 = 2009
                def convert_roc(roc_str):
                    if len(roc_str) == 7: # YYYMMDD
                        year = int(roc_str[:3]) + year_offset
                        month = int(roc_str[3:5])
                        day = int(roc_str[5:7])
                    elif len(roc_str) == 6: # YYMMDD
                        year = int(roc_str[:2]) + year_offset
                        month = int(roc_str[2:4])
                        day = int(roc_str[4:6])
                    else:
                        return pd.NaT
                    try:
                        return pd.Timestamp(year=year, month=month, day=day)
                    except ValueError:
                        return pd.NaT

                gregorian_dates_from_roc = roc_dates.apply(convert_roc)

            # 處理標準西元年 YYYYMMDD
            gregorian_dates_direct = pd.to_datetime(date_series[~is_roc_year], format='%Y%m%d', errors='coerce')

            # 合併結果
            cleaned_df['trade_date'] = gregorian_dates_direct.combine_first(gregorian_dates_from_roc)

            # 如果還有 NaT，嘗試其他通用格式 (例如 YYYY-MM-DD, YYYY/MM/DD)
            if cleaned_df['trade_date'].isna().any():
                remaining_nat = cleaned_df['trade_date'].isna()
                # 使用原始的 cleaned_df['trade_date'] 因為上面 date_series 移除了分隔符
                cleaned_df.loc[remaining_nat, 'trade_date'] = pd.to_datetime(
                    df.loc[remaining_nat, 'trade_date'], errors='coerce' # type: ignore
                )

            logger.info("已轉換 'trade_date' 欄位為 datetime 物件。")
        except Exception as e:
            logger.warning(f"轉換 'trade_date' 欄位失敗: {e}. 該欄位可能保留原始格式或充滿 NaT。", exc_info=True)
    else:
        logger.warning("'trade_date' 欄位不存在，跳過日期處理。")

    # 4. 處理數值型欄位
    numeric_cols_candidate = ['open', 'high', 'low', 'close', 'volume', 'settlement_price', 'open_interest', 'ticks']
    actual_numeric_cols = [col for col in numeric_cols_candidate if col in cleaned_df.columns]

    for col in actual_numeric_cols:
        try:
            # 移除千分位逗號 (如果存在)
            if cleaned_df[col].dtype == 'object': # 只對字串類型操作
                cleaned_df[col] = cleaned_df[col].astype(str).str.replace(',', '', regex=False)

            # 轉換為數值，無法轉換的設為 NaN
            # 对于 'volume', 'open_interest', 'ticks' 通常是整數
            if col in ['volume', 'open_interest', 'ticks']:
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').astype('Int64') # 使用可空整數 Int64
            else: # 其他價格欄位為 float
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').astype(float)
            logger.debug(f"已轉換欄位 '{col}' 為數值類型。")
        except Exception as e:
            logger.warning(f"轉換欄位 '{col}' 為數值類型失敗: {e}. 該欄位可能包含非數值數據。", exc_info=True)
            # 保險起見，如果轉換出錯，也將其設為 NaN 或保持原樣（取決於策略）
            # cleaned_df[col] = np.nan # 或 pd.NA

    # 5. 移除完全重複的行 (可選，根據業務需求)
    # initial_row_count = len(cleaned_df)
    # cleaned_df.drop_duplicates(inplace=True)
    # if len(cleaned_df) < initial_row_count:
    #     logger.info(f"移除了 {initial_row_count - len(cleaned_df)} 個重複行。")

    # 6. 根據需求選擇或重新排序欄位 (可選)
    # desired_columns_order = ['trade_date', 'product_id', 'open', 'high', 'low', 'close', 'volume',
    #                          'settlement_price', 'open_interest', 'ticks']
    # final_columns = [col for col in desired_columns_order if col in cleaned_df.columns]
    # if final_columns:
    #     cleaned_df = cleaned_df[final_columns]
    #     logger.debug(f"欄位已按預期順序排列: {final_columns}")

    logger.info(f"清洗函式 'clean_daily_ohlcv_example_v1' 執行完畢。輸出 DataFrame 行數: {len(cleaned_df)}")
    return cleaned_df


def another_cleaner_example(df: pd.DataFrame) -> pd.DataFrame:
    """
    另一個範例清洗函式，可能用於不同結構的數據。
    """
    logger.info(f"開始執行清洗函式 'another_cleaner_example'，輸入 DataFrame 行數: {len(df)}")
    cleaned_df = df.copy()

    # 假設這個清洗器需要 '契約月份' 欄位，並將其轉換為到期日
    if '契約月份' in cleaned_df.columns:
        # 這裡的邏輯會非常依賴 '契約月份' 的具體格式 (例如 "202303", "2023/3", "11203")
        # 並需要將其轉換為該月份的某個特定日期（如最後一個交易日）
        # 這通常需要一個日曆輔助或更複雜的邏輯
        logger.info("偵測到 '契約月份' 欄位，此處應有轉換為到期日的邏輯 (未實現)。")
        cleaned_df['expiry_date'] = pd.NaT # 示意

    if '成交時間' in cleaned_df.columns:
        try:
            # 假設時間格式為 HHMMSS 或 HH:MM:SS
            cleaned_df['trade_time'] = pd.to_datetime(cleaned_df['成交時間'], format='%H%M%S', errors='coerce').dt.time
            # 如果還有 NaT，嘗試另一種格式
            if cleaned_df['trade_time'].isna().any():
                 mask = cleaned_df['trade_time'].isna()
                 cleaned_df.loc[mask, 'trade_time'] = pd.to_datetime(cleaned_df.loc[mask, '成交時間'], format='%H:%M:%S', errors='coerce').dt.time
            logger.info("已轉換 '成交時間' 欄位。")
        except Exception as e:
            logger.warning(f"轉換 '成交時間' 欄位失敗: {e}", exc_info=True)


    logger.info(f"清洗函式 'another_cleaner_example' 執行完畢。輸出 DataFrame 行數: {len(cleaned_df)}")
    return cleaned_df

# 可以在此處添加更多針對不同數據格式的清洗函式

# --- 範例使用 ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # setup_global_logger(log_level_console=logging.DEBUG)
    logger.info("開始執行 example_cleaners.py 範例...")

    # 測試 clean_daily_ohlcv_example_v1
    sample_data_ohlcv = {
        '交易日期': ["112/03/15", "2023/03/16", "20230317", "100/1/1"],
        '商品代號': ["TXF", "MXF", "TXF", "TEF"],
        '開盤價': ["15,000", "14,900.5", "15100", "7000.0"],
        '最高價': ["15,100", "-", "15150", "7050"], # 包含異常值 '-'
        '最低價': ["14,950", "14,880", "15050", "6990"],
        '收盤價': ["15,050.00", "14,950", "15120", "7020"],
        '成交量': ["120,000", "80000", "150000", "500"],
        '結算價': ["15,055", "14,955", np.nan, "7025"], # 包含 np.nan
        '未平倉合約數': ["80,000", "40000", "85000", "2000"],
        '亂七八糟的欄位': [1,2,3,4]
    }
    df_ohlcv_raw = pd.DataFrame(sample_data_ohlcv)
    logger.info(f"\n--- 測試 clean_daily_ohlcv_example_v1 ---")
    logger.info(f"原始 DataFrame:\n{df_ohlcv_raw}\n原始 Dtypes:\n{df_ohlcv_raw.dtypes}")

    cleaned_df_ohlcv = clean_daily_ohlcv_example_v1(df_ohlcv_raw)
    logger.info(f"清洗後 DataFrame:\n{cleaned_df_ohlcv}\n清洗後 Dtypes:\n{cleaned_df_ohlcv.dtypes}")

    # 簡單斷言
    assert 'trade_date' in cleaned_df_ohlcv.columns
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df_ohlcv['trade_date'])
    assert cleaned_df_ohlcv['trade_date'].iloc[0] == pd.Timestamp(2023, 3, 15)
    assert cleaned_df_ohlcv['trade_date'].iloc[3] == pd.Timestamp(2011, 1, 1)


    assert 'open' in cleaned_df_ohlcv.columns and cleaned_df_ohlcv['open'].dtype == float
    assert cleaned_df_ohlcv['open'].iloc[0] == 15000.0

    assert 'high' in cleaned_df_ohlcv.columns and cleaned_df_ohlcv['high'].dtype == float
    assert pd.isna(cleaned_df_ohlcv['high'].iloc[1]) # 應為 NaN

    assert 'volume' in cleaned_df_ohlcv.columns and cleaned_df_ohlcv['volume'].dtype == 'Int64'
    assert cleaned_df_ohlcv['volume'].iloc[0] == 120000

    assert '亂七八糟的欄位' in cleaned_df_ohlcv.columns # 未被移除，除非在排序和選擇欄位步驟中明確排除

    logger.info("clean_daily_ohlcv_example_v1 範例測試通過。")

    logger.info("example_cleaners.py 範例執行完畢。")
