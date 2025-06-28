from datetime import datetime, timezone
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger
import json # 用於處理可能存儲為 JSON 字符串的元數據或複雜字段

logger = get_logger(__name__)

# --- 通用清洗輔助函數 ---

def clean_whitespace(value):
    """移除字符串前後的空白字符。如果值不是字符串，則原樣返回。"""
    if isinstance(value, str):
        return value.strip()
    return value

def to_float_or_none(value, default_on_error=None):
    """嘗試將值轉換為 float。如果失敗，返回 None 或指定的默認值。"""
    if value is None:
        return default_on_error
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.debug(f"無法將值 '{value}' (類型: {type(value)}) 轉換為 float。返回默認值。")
        return default_on_error

def to_int_or_none(value, default_on_error=None):
    """嘗試將值轉換為 int。如果失敗，返回 None 或指定的默認值。"""
    if value is None:
        return default_on_error
    try:
        # 先嘗試轉 float 再轉 int，可以處理 "123.0" 這樣的情況
        return int(float(value))
    except (ValueError, TypeError):
        logger.debug(f"無法將值 '{value}' (類型: {type(value)}) 轉換為 int。返回默認值。")
        return default_on_error

def standardize_date_str_to_iso(date_str, input_format="%Y-%m-%d"):
    """
    將給定格式的日期字符串標準化為 ISO 8601 日期字符串 (YYYY-MM-DD)。
    如果原始數據包含時間且需要保留，則應使用更複雜的函數轉換為完整的 ISO 8601 時間戳。
    這裡主要處理純日期。
    """
    if not isinstance(date_str, str):
        logger.debug(f"日期值 '{date_str}' 不是字符串，無法標準化。")
        return None
    try:
        dt_obj = datetime.strptime(date_str, input_format)
        return dt_obj.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"日期字符串 '{date_str}' 與期望格式 '{input_format}' 不匹配。")
        # 可以嘗試一些常見的其他格式
        common_formats = ["%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"]
        if input_format not in common_formats: #避免重複嘗試input_format
            common_formats.insert(0, input_format)

        for fmt in common_formats:
            try:
                dt_obj = datetime.strptime(date_str, fmt)
                return dt_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue
        logger.error(f"無法將日期字符串 '{date_str}' 解析為標準格式。")
        return None

def standardize_datetime_str_to_iso_utc(datetime_str, input_format=None, assume_utc_if_no_tz=True):
    """
    將日期時間字符串標準化為 ISO 8601 UTC 時間戳字符串 (YYYY-MM-DDTHH:MM:SSZ 或帶毫秒)。
    Args:
        datetime_str (str): 輸入的日期時間字符串。
        input_format (str, optional): 輸入字符串的格式。如果為 None，會嘗試幾種常見格式。
        assume_utc_if_no_tz (bool): 如果輸入字符串沒有時區信息，是否假設其為 UTC。
    Returns:
        str or None: ISO 8601 UTC 格式的字符串，或在失敗時返回 None。
    """
    if not isinstance(datetime_str, str):
        logger.debug(f"日期時間值 '{datetime_str}' 不是字符串，無法標準化。")
        return None

    parsed_dt = None
    if input_format:
        try:
            parsed_dt = datetime.strptime(datetime_str, input_format)
        except ValueError:
            logger.warning(f"日期時間字符串 '{datetime_str}' 與指定格式 '{input_format}' 不匹配。")
            # 仍然嘗試通用格式

    if not parsed_dt:
        # 嘗試一些常見的 ISO 相關格式
        # 注意：datetime.fromisoformat() 在 Python 3.7+ 可用，能處理更多 ISO 格式
        # 但為了更廣泛的兼容性或更細緻的控制，有時 strptime 更好
        common_dt_formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            # 如果數據可能包含時區偏移，例如 +08:00
            # "%Y-%m-%dT%H:%M:%S%z" (注意 %z 的處理，strptime 可能需要特定 Python 版本或額外處理)
        ]
        for fmt in common_dt_formats:
            try:
                parsed_dt = datetime.strptime(datetime_str, fmt)
                break
            except ValueError:
                continue

    if not parsed_dt: # 如果所有嘗試都失敗
        # 最後嘗試 datetime.fromisoformat() (Python 3.7+)
        if hasattr(datetime, 'fromisoformat'):
            try:
                # fromisoformat 對於 Z 後綴的處理可能需要 Python 3.11+
                # 或者需要手動替換 'Z' 為 '+00:00'
                temp_str = datetime_str
                if temp_str.endswith('Z'):
                    if '.' in temp_str: # 包含毫秒
                         # strptime 不支持 %f 後直接跟 Z，所以 fromisoformat 可能是唯一選擇
                         # fromisoformat 在 3.11 前對 Z 的支持也有限
                         # 為了簡化，這裡我們假設如果帶 Z，則它是 UTC
                         # 如果 fromisoformat 失敗，我們會在下面處理時區
                        pass # 讓 fromisoformat 嘗試
                    else: # 不包含毫秒
                        pass

                # fromisoformat 在處理某些無 tzinfo 的情況時可能不符合預期
                # 它也可能無法處理所有上面列出的 strptime 格式
                parsed_dt = datetime.fromisoformat(temp_str.replace('Z', '+00:00') if temp_str.endswith('Z') else temp_str)

            except ValueError:
                logger.error(f"無法將日期時間字符串 '{datetime_str}' 解析為任何已知格式。")
                return None
        else: # Python < 3.7
             logger.error(f"無法將日期時間字符串 '{datetime_str}' 解析為任何已知格式 (datetime.fromisoformat 不可用)。")
             return None


    if not parsed_dt: # 再次檢查，以防 fromisoformat 失敗但未拋錯而是返回 None (不太可能)
        logger.error(f"最終無法解析日期時間字符串 '{datetime_str}'。")
        return None

    # 處理時區
    if parsed_dt.tzinfo is None or parsed_dt.tzinfo.utcoffset(parsed_dt) is None:
        if assume_utc_if_no_tz:
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            logger.debug(f"日期時間 '{datetime_str}' 無時區信息，已假設為 UTC。")
        else:
            logger.warning(f"日期時間 '{datetime_str}' 無時區信息，且未配置為假設 UTC。返回 naive datetime。")
            # 或者根據策略返回 None 或錯誤
            # 為了統一輸出為 UTC ISO 字符串，naive datetime 必須被處理
            logger.error(f"無法處理無時區信息的日期時間 '{datetime_str}' 且無法假設 UTC。")
            return None # 或者拋出異常
    else:
        # 如果有時區信息，轉換為 UTC
        parsed_dt = parsed_dt.astimezone(timezone.utc)

    # 格式化為 ISO 8601 UTC (帶 Z)
    # SQLite 通常期望 'YYYY-MM-DD HH:MM:SS.SSS'，不帶 'Z' 或 'T'，但 ISO 8601 更標準
    # 包含毫秒，並以 'Z' 結尾表示 UTC
    return parsed_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clean_dict_values(data_dict: dict, cleaning_rules: dict):
    """
    對字典中的值根據提供的規則進行清洗。
    Args:
        data_dict (dict): 要清洗的字典。
        cleaning_rules (dict): 一個字典，key 是 data_dict 中的鍵，value 是要應用的清洗函數列表或單個函數。
                               例如: {"price": [clean_whitespace, to_float_or_none], "volume": [to_int_or_none]}
    Returns:
        dict: 清洗後的字典。
    """
    if not isinstance(data_dict, dict):
        logger.warning("clean_dict_values 接收到的輸入不是字典。")
        return data_dict

    cleaned_dict = {}
    for key, value in data_dict.items():
        cleaned_value = value
        if key in cleaning_rules:
            rules = cleaning_rules[key]
            if not isinstance(rules, list):
                rules = [rules] # 確保是列表

            for func in rules:
                if callable(func):
                    try:
                        cleaned_value = func(cleaned_value)
                    except Exception as e:
                        logger.error(f"對鍵 '{key}' 的值 '{value}' 應用清洗函數 {func.__name__} 時出錯: {e}", exc_info=True)
                        # 根據策略，可以選擇保留原值、設為None或跳過
                        # 這裡選擇保留上一步清洗成功的值（或原始值）
                        break # 停止對此字段的後續清洗
                else:
                    logger.warning(f"提供給鍵 '{key}' 的規則 {func} 不是可調用函數。")
        cleaned_dict[key] = cleaned_value
    return cleaned_dict

# --- 針對特定數據源的清洗函數 (示例) ---

def clean_yfinance_csv_row(row_dict: dict):
    """
    清洗從 Yahoo Finance CSV 獲取的一行數據 (已轉換為字典)。
    Yahoo CSV 列: Date,Open,High,Low,Close,Adj Close,Volume
    """
    if not isinstance(row_dict, dict):
        logger.warning(f"clean_yfinance_csv_row: 輸入不是字典: {row_dict}")
        return None # 或者返回空字典，取決於後續處理

    rules = {
        "Date": [clean_whitespace, lambda x: standardize_date_str_to_iso_utc(x, input_format="%Y-%m-%d")], # YF CSV 日期是 YYYY-MM-DD
        "Open": [to_float_or_none],
        "High": [to_float_or_none],
        "Low": [to_float_or_none],
        "Close": [to_float_or_none],
        "Adj Close": [to_float_or_none], # 有時列名是 "Adj Close" 或 "Adj. Close"
        "Volume": [to_int_or_none]
    }

    # Yahoo Finance CSV 可能有 "null" 字符串表示缺失值
    processed_row = {}
    for key, value in row_dict.items():
        # 處理列名中可能的點，替換為下劃線，並與 rules 中的鍵匹配
        normalized_key = key.replace('.', '').replace(' ', '_') # 例如 "Adj Close" -> "Adj_Close"

        # 處理 "null" 字符串
        if isinstance(value, str) and value.lower() == "null":
            current_value = None
        else:
            current_value = value

        # 應用清洗規則 (這裡我們直接查找原始 key，假設 CSV 解析時列名已處理好)
        # 如果 CSV 解析的列名與 rules 中的 key 不完全匹配，需要一個映射或更靈活的匹配
        # 為了簡單，假設 key 匹配
        rule_funcs = []
        if key in rules:
            rule_funcs = rules[key]
        elif normalized_key in rules: # 嘗試匹配標準化後的 key
             rule_funcs = rules[normalized_key]


        if rule_funcs:
            if not isinstance(rule_funcs, list): rule_funcs = [rule_funcs]
            for func in rule_funcs:
                current_value = func(current_value)

        # 使用標準化後的鍵名存儲 (或者在 rules 中使用標準化鍵名)
        # 這裡我們用原始 key 存儲，假設後續處理知道原始列名
        processed_row[key] = current_value

    # 重命名 "Adj Close" (如果存在)
    if "Adj Close" in processed_row:
        processed_row["adj_close"] = processed_row.pop("Adj Close")
    elif "Adj. Close" in processed_row: # 另一種可能的列名
        processed_row["adj_close"] = processed_row.pop("Adj. Close")

    return processed_row


def clean_fred_observation(observation_dict: dict):
    """
    清洗從 FRED API 獲取的一條觀測數據。
    FRED JSON 觀測格式: {"realtime_start": "...", "realtime_end": "...", "date": "YYYY-MM-DD", "value": "123.456"}
    """
    rules = {
        "date": [clean_whitespace, lambda x: standardize_date_str_to_iso_utc(x, input_format="%Y-%m-%d")],
        "value": [lambda x: to_float_or_none(x, default_on_error=None) if isinstance(x, str) and x.strip() != "." else None]
        # FRED 的 value 有時是 "." 表示無數據，需要特殊處理
    }
    # 只保留我們關心的字段
    cleaned = clean_dict_values({
        "date": observation_dict.get("date"),
        "value": observation_dict.get("value")
    }, rules)
    return cleaned if cleaned.get("date") and cleaned.get("value") is not None else None


def clean_finmind_data_item(item_dict: dict, dataset_name: str):
    """
    清洗從 FinMind API 獲取的一條數據。
    需要根據不同的 dataset_name 應用不同的清洗規則。
    這是一個通用框架，具體規則需要根據 API 文檔確定。

    Args:
        item_dict (dict): FinMind API 返回的 data 列表中的一個元素。
        dataset_name (str): 該數據來自的 FinMind dataset 名稱。
    """
    if not isinstance(item_dict, dict): return None

    cleaned_item = item_dict.copy() # 先複製一份，避免修改原始數據

    # 通用清洗：移除所有值的首尾空格
    for key, value in cleaned_item.items():
        if isinstance(value, str):
            cleaned_item[key] = value.strip()

    # 特定 dataset 的清洗規則 (示例)
    if dataset_name == "InstitutionalInvestorsBuySell":
        # 假設字段: date, stock_id, buy, sell, net_buy, name (法人名稱)
        rules = {
            "date": [lambda x: standardize_date_str_to_iso_utc(x, input_format="%Y-%m-%d")],
            "buy": [to_int_or_none],
            "sell": [to_int_or_none],
            "net_buy": [to_int_or_none] # "net_buy" 可能是 "buy" - "sell"
            # "stock_id" 和 "name" 通常是字符串，已做過 strip
        }
        cleaned_item = clean_dict_values(cleaned_item, rules)
        # 可能需要重命名字段以符合內部標準，例如 "stock_id" -> "symbol"
        if "stock_id" in cleaned_item:
            cleaned_item["symbol"] = cleaned_item.pop("stock_id")

    elif dataset_name == "FinancialStatements":
        # 財報數據結構複雜，可能包含 type (報表類型), date (財報日期), origin_name (會計科目), value
        # 這裡的清洗可能更側重於 value 的類型轉換，以及 date 的標準化
        rules = {
            "date": [lambda x: standardize_date_str_to_iso_utc(x, input_format="%Y-%m-%d")],
            "value": [to_float_or_none] # 假設財報數值都是 float
            # "type", "origin_name" 通常是字符串
        }
        cleaned_item = clean_dict_values(cleaned_item, rules)
        if "stock_id" in cleaned_item: # FinancialStatements 通常也有 stock_id
            cleaned_item["symbol"] = cleaned_item.pop("stock_id")

    elif dataset_name == "TaiwanStockPrice": # 假設有這個 dataset
        # 類似 yfinance: date, stock_id, Open, Max, Min, Close, Volume (FinMind 的列名可能是中文或英文)
        # 假設 FinMind 返回的列名是英文且首字母大寫
        rules = {
            "date": [lambda x: standardize_date_str_to_iso_utc(x, input_format="%Y-%m-%d")],
            "Open": [to_float_or_none],
            "Max": [to_float_or_none], # 對應 High
            "Min": [to_float_or_none], # 對應 Low
            "Close": [to_float_or_none],
            "Volume": [to_int_or_none] # 成交股數，可能是很大的整數
            # FinMind 的 "Trading_Volume" 可能是成交金額
        }
        cleaned_item = clean_dict_values(cleaned_item, rules)
        if "stock_id" in cleaned_item: cleaned_item["symbol"] = cleaned_item.pop("stock_id")
        if "Max" in cleaned_item: cleaned_item["high"] = cleaned_item.pop("Max")
        if "Min" in cleaned_item: cleaned_item["low"] = cleaned_item.pop("Min")
        # 將 Open, Close, Volume 等標準化為小寫 (如果需要)
        for key in ["Open", "Close", "Volume"]:
            if key in cleaned_item:
                cleaned_item[key.lower()] = cleaned_item.pop(key)

    # ...可以為更多 dataset 添加規則...

    return cleaned_item


if __name__ == "__main__":
    import logging
    setup_logger(PROJECT_LOGGER_NAME, level=logging.DEBUG) # 確保能看到 debug 日誌

    logger.info("--- Cleaners (__main__) 測試開始 ---")

    # 測試 to_float_or_none
    logger.info(f"to_float_or_none('123.45'): {to_float_or_none('123.45')}")
    logger.info(f"to_float_or_none('abc'): {to_float_or_none('abc')}")
    logger.info(f"to_float_or_none(None): {to_float_or_none(None)}")
    logger.info(f"to_float_or_none(' . ', default_on_error=0.0): {to_float_or_none(' . ', default_on_error=0.0)}") # FRED 的 "."

    # 測試 standardize_date_str_to_iso_utc
    logger.info(f"standardize_date_str_to_iso_utc('2023-05-15'): {standardize_date_str_to_iso_utc('2023-05-15', input_format='%Y-%m-%d')}")
    logger.info(f"standardize_date_str_to_iso_utc('2023/05/15'): {standardize_date_str_to_iso_utc('2023/05/15', input_format='%Y/%m/%d')}")
    logger.info(f"standardize_date_str_to_iso_utc('20231010'): {standardize_date_str_to_iso_utc('20231010', input_format='%Y%m%d')}")

    # 測試 standardize_datetime_str_to_iso_utc
    logger.info(f"standardize_datetime_str_to_iso_utc('2023-10-26T10:20:30Z'): {standardize_datetime_str_to_iso_utc('2023-10-26T10:20:30Z')}")
    logger.info(f"standardize_datetime_str_to_iso_utc('2023-10-26 10:20:30.123456'): {standardize_datetime_str_to_iso_utc('2023-10-26 10:20:30.123456')}")
    logger.info(f"standardize_datetime_str_to_iso_utc('2023-10-26 10:20:30', assume_utc_if_no_tz=True): {standardize_datetime_str_to_iso_utc('2023-10-26 10:20:30', assume_utc_if_no_tz=True)}")
    logger.info(f"standardize_datetime_str_to_iso_utc('invalid date string'): {standardize_datetime_str_to_iso_utc('invalid date string')}")


    # 測試 clean_yfinance_csv_row
    yf_row = {"Date": "2023-01-03", "Open": "130.279", "High": "130.89", "Low": "124.17",
              "Close": "125.07", "Adj Close": "124.87", "Volume": "112117500"}
    cleaned_yf_row = clean_yfinance_csv_row(yf_row)
    logger.info(f"Cleaned YFinance row: {cleaned_yf_row}")
    assert isinstance(cleaned_yf_row["Open"], float)
    assert isinstance(cleaned_yf_row["Volume"], int)
    assert cleaned_yf_row["Date"].endswith("Z") # 應為 UTC ISO 格式

    yf_row_null = {"Date": "2023-01-04", "Open": "null", "Volume": "null"}
    cleaned_yf_row_null = clean_yfinance_csv_row(yf_row_null)
    logger.info(f"Cleaned YFinance row with 'null': {cleaned_yf_row_null}")
    assert cleaned_yf_row_null["Open"] is None
    assert cleaned_yf_row_null["Volume"] is None

    # 測試 clean_fred_observation
    fred_obs = {"date": "2023-11-01", "value": "307.619"}
    cleaned_fred_obs = clean_fred_observation(fred_obs)
    logger.info(f"Cleaned FRED observation: {cleaned_fred_obs}")
    assert isinstance(cleaned_fred_obs["value"], float)
    assert cleaned_fred_obs["date"].endswith("Z")

    fred_obs_dot = {"date": "2023-12-01", "value": "."}
    cleaned_fred_obs_dot = clean_fred_observation(fred_obs_dot)
    logger.info(f"Cleaned FRED observation with '.': {cleaned_fred_obs_dot}")
    assert cleaned_fred_obs_dot is None # 因為 value 是 ".", to_float_or_none 返回 None, 導致整個記錄被視為無效

    # 測試 clean_finmind_data_item
    fm_chip_item = {"date":"2023-12-08","stock_id":"2330","name":"Foreign_Investor","buy":6900000,"sell":16000000,"net_buy":-9100000}
    cleaned_fm_chip = clean_finmind_data_item(fm_chip_item, "InstitutionalInvestorsBuySell")
    logger.info(f"Cleaned FinMind chip data: {cleaned_fm_chip}")
    assert cleaned_fm_chip["symbol"] == "2330"
    assert isinstance(cleaned_fm_chip["buy"], int)
    assert cleaned_fm_chip["date"].endswith("Z")

    fm_price_item = {"date":"2024-06-20", "stock_id":"0050", "Trading_Volume":6497000,"Trading_money":1168610200,
                     "Open":180.0,"Max":180.05,"Min":179.5,"Close":179.85,"spread":-0.25,
                     "Trading_turnover":16304000} # 假設 Trading_Volume 是股數，Open/Max/Min/Close 是價格
    cleaned_fm_price = clean_finmind_data_item(fm_price_item, "TaiwanStockPrice") # 假設 dataset 名稱
    logger.info(f"Cleaned FinMind price data: {cleaned_fm_price}")
    assert cleaned_fm_price["symbol"] == "0050"
    assert isinstance(cleaned_fm_price["open"], float)
    assert isinstance(cleaned_fm_price["volume"], int) # 應為小寫 volume
    assert cleaned_fm_price["high"] == 180.05
    assert cleaned_fm_price["date"].endswith("Z")


    logger.info("--- Cleaners (__main__) 測試結束 ---")
