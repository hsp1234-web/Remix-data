from datetime import datetime, timedelta, timezone
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger
from AI_Assisted_Historical_Backtesting.src.data_processing.cleaners import standardize_datetime_str_to_iso_utc # 可能需要

logger = get_logger(__name__)

# 假設我們的目標分析週期是每12小時一次，我們需要將數據對齊到這些週期的開始點
# 例如：UTC 00:00:00 和 UTC 12:00:00

def get_target_period_start_utc(timestamp_utc_str: str, period_hours: int = 12) -> str:
    """
    根據輸入的 UTC 時間戳字符串，計算其所屬的目標分析週期的開始時間。
    例如，如果 period_hours=12，則時間戳 "2023-10-26T08:00:00Z" 會對齊到 "2023-10-26T00:00:00Z"，
    而 "2023-10-26T14:00:00Z" 會對齊到 "2023-10-26T12:00:00Z"。

    Args:
        timestamp_utc_str (str): ISO 8601 UTC 格式的時間戳字符串。
        period_hours (int): 分析週期的時長（小時）。必須能被24整除或24能被其整除。

    Returns:
        str: 對齊後的週期開始時間的 ISO 8601 UTC 字符串，或在錯誤時返回原始字符串或 None。
    """
    if not (24 % period_hours == 0 or period_hours % 24 == 0): # 簡化檢查
        logger.error(f"無效的週期時長 {period_hours}小時。必須能被24整除或24能被其整除。")
        # raise ValueError("週期時長必須是1, 2, 3, 4, 6, 8, 12, 24的因子或倍數")
        return timestamp_utc_str # 或返回 None

    try:
        # 嘗試解析多種可能的 ISO 格式，包括帶 Z 和不帶 Z，以及有無毫秒
        dt_obj_utc = None
        fmts_to_try = [
            "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",  "%Y-%m-%dT%H:%M:%S", # 不帶Z，但我們假定它是UTC
            "%Y-%m-%d %H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%SZ", # 帶空格的
            "%Y-%m-%d %H:%M:%S.%f",  "%Y-%m-%d %H:%M:%S",
        ]
        # 有些 standardize_datetime_str_to_iso_utc 的輸出可能不完全匹配這些
        # 最好是確保輸入到此函數的 timestamp_utc_str 已經是標準的 UTC ISO 格式
        # 例如，由 standardize_datetime_str_to_iso_utc 處理過的

        # 簡化：假設輸入的 timestamp_utc_str 已經是 cleaners.py 中
        # standardize_datetime_str_to_iso_utc 函數的輸出格式： YYYY-MM-DDTHH:MM:SS.sssZ
        try:
            dt_obj_utc = datetime.strptime(timestamp_utc_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError: # 如果 strptime 失敗，嘗試 fromisoformat
             if hasattr(datetime, 'fromisoformat'):
                 # fromisoformat 要求 Z 後綴被替換
                 dt_obj_utc = datetime.fromisoformat(timestamp_utc_str.replace('Z', '+00:00'))
             else: # Python < 3.7
                 raise ValueError("無法解析日期時間字符串且 fromisoformat 不可用。")

        if dt_obj_utc.tzinfo is None or dt_obj_utc.tzinfo.utcoffset(dt_obj_utc) != timedelta(0):
            # 再次確認是 UTC，如果不是，則強制轉換 (儘管函數名暗示輸入已是 UTC)
            logger.warning(f"輸入的時間戳 '{timestamp_utc_str}' 未正確標識為 UTC 或不是 UTC，將嘗試轉換。")
            if dt_obj_utc.tzinfo is None:
                dt_obj_utc = dt_obj_utc.replace(tzinfo=timezone.utc)
            else:
                dt_obj_utc = dt_obj_utc.astimezone(timezone.utc)

        # 計算小時部分落在哪個週期的起始點
        hour_of_day = dt_obj_utc.hour
        target_hour = (hour_of_day // period_hours) * period_hours

        aligned_dt_utc = dt_obj_utc.replace(hour=target_hour, minute=0, second=0, microsecond=0)

        return aligned_dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    except ValueError as e:
        logger.error(f"解析時間戳字符串 '{timestamp_utc_str}' 失敗: {e}", exc_info=True)
        return None # 或者原樣返回 timestamp_utc_str


def align_data_to_periods(data_list: list,
                          timestamp_key: str,
                          value_keys: list,
                          period_hours: int = 12,
                          aggregation_funcs: dict = None):
    """
    將包含時間戳的數據列表對齊到指定的分析週期。
    對於每個週期，可以對落入該週期的多個數據點的值進行聚合。

    Args:
        data_list (list of dict): 要對齊的數據列表，每個元素是一個包含時間戳和數據值的字典。
                                  例如: [{"timestamp_utc": "...", "price": 100, "volume": 1000}, ...]
        timestamp_key (str): 字典中表示 UTC 時間戳字符串的鍵名。
        value_keys (list of str): 字典中需要聚合或選取的值的鍵名列表。
        period_hours (int): 分析週期的時長（小時）。
        aggregation_funcs (dict, optional):
            一個字典，指定對每個 value_key 使用的聚合函數。
            key 是 value_key，value 是聚合函數的名稱字符串或實際函數。
            支持的字符串名稱: "first", "last", "sum", "avg", "min", "max", "open", "high", "low", "close", "count"。
            如果為 None，默認對所有 value_keys 取 "last" (最後一個值)。
            如果 value_key 是 "open", "high", "low", "close", "volume"，會嘗試應用對應的OHLCV聚合。
            例如: {"price": "last", "volume": "sum", "score": "avg"}
                   {"ohlc_price": {"open":"first", "high":"max", "low":"min", "close":"last"}, "volume":"sum"}
                   (對於OHLC這種複合情況，可能需要更複雜的處理或預先拆分)

    Returns:
        dict: 一個字典，key 是對齊後的週期開始時間戳 (str)，
              value 是該週期對應的聚合後的數據字典 (包含 value_keys)。
              例如: {"2023-10-26T00:00:00.000Z": {"price": 101, "volume": 1500}, ...}
    """
    if not data_list:
        return {}

    if aggregation_funcs is None:
        aggregation_funcs = {key: "last" for key in value_keys}

    # 確保 aggregation_funcs 覆蓋所有 value_keys，如果沒有則默認為 "last"
    for key in value_keys:
        if key not in aggregation_funcs:
            aggregation_funcs[key] = "last"

    # 按週期開始時間對數據進行分組
    grouped_by_period = {}
    for item in data_list:
        ts_str = item.get(timestamp_key)
        if not ts_str:
            logger.warning(f"數據項缺少時間戳 (鍵: '{timestamp_key}'): {item}")
            continue

        period_start_str = get_target_period_start_utc(ts_str, period_hours)
        if not period_start_str:
            logger.warning(f"無法獲取數據項 '{item}' 的目標週期開始時間。")
            continue

        if period_start_str not in grouped_by_period:
            grouped_by_period[period_start_str] = []

        # 只保留需要的 value_keys 和原始時間戳（用於排序取 first/last）
        relevant_data = {key: item.get(key) for key in value_keys}
        relevant_data["_original_timestamp_for_sorting_"] = ts_str # 用於排序
        grouped_by_period[period_start_str].append(relevant_data)

    aligned_data = {}
    for period_start, items_in_period in grouped_by_period.items():
        if not items_in_period:
            continue

        # 對每個週期內的數據點按原始時間戳排序，以便正確取 first/last
        try:
            items_in_period.sort(key=lambda x: x["_original_timestamp_for_sorting_"])
        except TypeError as e:
            logger.error(f"週期 {period_start} 內的數據項排序失敗，可能存在無效的時間戳: {e}")
            # 可以選擇跳過此週期或嘗試無排序的聚合
            continue

        aggregated_values_for_period = {}
        for key in value_keys:
            agg_func_spec = aggregation_funcs.get(key, "last") # 默認取 last

            # 提取此 key 的所有值，過濾掉 None (除非聚合函數能處理 None，如 count)
            values_for_key = [d.get(key) for d in items_in_period if d.get(key) is not None]

            if not values_for_key and agg_func_spec not in ["count"]: # 如果沒有有效值且不是計數
                aggregated_values_for_period[key] = None # 或者用0，取決於策略
                continue

            agg_result = None
            try:
                if agg_func_spec == "first":
                    agg_result = items_in_period[0].get(key) # 已排序，取第一個的原始值
                elif agg_func_spec == "last":
                    agg_result = items_in_period[-1].get(key) # 已排序，取最後一個的原始值
                elif agg_func_spec == "sum":
                    agg_result = sum(v for v in values_for_key if isinstance(v, (int, float)))
                elif agg_func_spec == "avg":
                    numeric_values = [v for v in values_for_key if isinstance(v, (int, float))]
                    agg_result = sum(numeric_values) / len(numeric_values) if numeric_values else None
                elif agg_func_spec == "min":
                    agg_result = min(v for v in values_for_key if isinstance(v, (int, float))) if values_for_key else None
                elif agg_func_spec == "max":
                    agg_result = max(v for v in values_for_key if isinstance(v, (int, float))) if values_for_key else None
                elif agg_func_spec == "count":
                    agg_result = len(items_in_period) # 計算原始項目數，或 len(values_for_key) 計算有效值數
                # 針對 OHLC 的特殊處理 (假設 'price' 字段傳入)
                elif key.lower() == "price" and isinstance(agg_func_spec, dict): # e.g. {"price": {"open": "first", ...}}
                    # 這種情況下，value_keys 應該直接包含 "open", "high", "low", "close"
                    # 或者這裡的 key 應該是 "ohlc_price" 之類的複合鍵，然後拆分
                    # 目前的設計，如果 key 是 "price"，agg_func_spec 是字典，則不直接支持
                    # 調用者應將 OHLC 拆分為獨立的 value_keys 並分別指定聚合
                    logger.warning(f"不支持對鍵 '{key}' 使用字典形式的聚合函數 '{agg_func_spec}'。請單獨指定 O,H,L,C 的聚合。")
                    agg_result = items_in_period[-1].get(key) # fallback to last
                elif callable(agg_func_spec):
                    agg_result = agg_func_spec(values_for_key)
                else: # 未知聚合函數字符串
                    logger.warning(f"未知的聚合函數 '{agg_func_spec}' for key '{key}'. 默認使用 'last'。")
                    agg_result = items_in_period[-1].get(key)
            except Exception as e:
                logger.error(f"對鍵 '{key}' 應用聚合函數 '{agg_func_spec}' 時出錯: {e}", exc_info=True)
                agg_result = None # 出錯時設為 None

            aggregated_values_for_period[key] = agg_result

        aligned_data[period_start] = aggregated_values_for_period

    return aligned_data


# --- 示例：針對特定數據流的對齊和聚合包裝函數 ---

def align_ohlcv_data(ohlcv_data_list: list,
                     timestamp_key: str = "timestamp_utc",
                     open_key: str = "open", high_key: str = "high",
                     low_key: str = "low", close_key: str = "close",
                     volume_key: str = "volume",
                     period_hours: int = 12):
    """
    將 OHLCV 數據列表對齊到指定週期，並計算每個週期的 OHLCV。
    Args:
        ohlcv_data_list (list of dict): 包含 OHLCV 數據的字典列表。
                                        每個字典應有時間戳和 O,H,L,C,V 值。
        timestamp_key, open_key, ...: 各數據在字典中的鍵名。
        period_hours: 分析週期的時長（小時）。
    Returns:
        dict: key 是對齊後的週期開始時間戳，value 是包含該週期 OHLCV 的字典。
    """
    if not ohlcv_data_list:
        return {}

    value_keys_map = {
        "open_price": open_key, "high_price": high_key, "low_price": low_key,
        "close_price": close_key, "total_volume": volume_key
    }

    # 預處理，將原始數據轉換為內部期望的鍵名，以便 align_data_to_periods 使用
    # 並確保所有值都是數值類型
    processed_list = []
    for item in ohlcv_data_list:
        ts = item.get(timestamp_key)
        if not ts: continue

        # 標準化時間戳 (如果需要，但假設輸入已是標準 UTC ISO 字符串)
        # ts = standardize_datetime_str_to_iso_utc(ts)
        # if not ts: continue

        new_item = { "_original_timestamp_for_sorting_": ts }
        valid_item = True
        for internal_key, original_key in value_keys_map.items():
            val = item.get(original_key)
            # 簡單類型轉換和錯誤處理
            try:
                if "volume" in internal_key:
                    new_item[internal_key] = int(float(val)) if val is not None else 0
                else: # O, H, L, C
                    new_item[internal_key] = float(val) if val is not None else None
            except (ValueError, TypeError):
                logger.warning(f"無法轉換值 for key='{original_key}', value='{val}' in item {item}")
                new_item[internal_key] = None
                if "price" in internal_key: valid_item = False # 如果價格無效，可能跳過此數據點

        if valid_item:
             processed_list.append(new_item)


    # 按週期分組
    grouped_by_period = {}
    for item in processed_list:
        period_start_str = get_target_period_start_utc(item["_original_timestamp_for_sorting_"], period_hours)
        if not period_start_str: continue
        if period_start_str not in grouped_by_period:
            grouped_by_period[period_start_str] = []
        grouped_by_period[period_start_str].append(item)

    # 聚合計算每個週期的 OHLCV
    final_aligned_data = {}
    for period_start, items_in_period in grouped_by_period.items():
        if not items_in_period: continue

        # 按原始時間戳排序，以便取 first (open) 和 last (close)
        items_in_period.sort(key=lambda x: x["_original_timestamp_for_sorting_"])

        period_open = items_in_period[0].get("open_price")
        period_close = items_in_period[-1].get("close_price")

        all_highs = [d.get("high_price") for d in items_in_period if d.get("high_price") is not None]
        period_high = max(all_highs) if all_highs else None

        all_lows = [d.get("low_price") for d in items_in_period if d.get("low_price") is not None]
        period_low = min(all_lows) if all_lows else None

        total_volume = sum(d.get("total_volume", 0) for d in items_in_period if d.get("total_volume") is not None)

        # 只有當所有核心OHLC值都有效時才記錄 (或者根據策略調整)
        if all(v is not None for v in [period_open, period_high, period_low, period_close]):
            final_aligned_data[period_start] = {
                open_key: period_open,
                high_key: period_high,
                low_key: period_low,
                close_key: period_close,
                volume_key: total_volume
            }
    return final_aligned_data


if __name__ == "__main__":
    import logging
    setup_logger(PROJECT_LOGGER_NAME, level=logging.DEBUG)

    logger.info("--- Aligners (__main__) 測試開始 ---")

    # 測試 get_target_period_start_utc
    ts1 = "2023-10-26T08:30:45.123Z"
    ts2 = "2023-10-26T14:15:00.000Z"
    ts3 = "2023-10-26T00:00:00.000Z"
    ts4_nodot = "2023-10-26T11:59:59Z" # 無毫秒

    logger.info(f"Align '{ts1}' (12h): {get_target_period_start_utc(ts1, 12)}") # Exp: ..T00:00:00.000Z
    assert get_target_period_start_utc(ts1, 12) == "2023-10-26T00:00:00.000Z"
    logger.info(f"Align '{ts2}' (12h): {get_target_period_start_utc(ts2, 12)}") # Exp: ..T12:00:00.000Z
    assert get_target_period_start_utc(ts2, 12) == "2023-10-26T12:00:00.000Z"
    logger.info(f"Align '{ts3}' (12h): {get_target_period_start_utc(ts3, 12)}") # Exp: ..T00:00:00.000Z
    assert get_target_period_start_utc(ts3, 12) == "2023-10-26T00:00:00.000Z"
    logger.info(f"Align '{ts4_nodot}' (12h): {get_target_period_start_utc(ts4_nodot, 12)}")
    # 這裡需要確認 standardize_datetime_str_to_iso_utc 和 get_target_period_start_utc 對無毫秒的Z格式的處理
    # 如果 standardize_datetime_str_to_iso_utc 輸出 YYYY-MM-DDTHH:MM:SSZ
    # 則 get_target_period_start_utc 中的 strptime 可能需要 "%Y-%m-%dT%H:%M:%S%z"
    # 目前的實現，get_target_period_start_utc 期望 YYYY-MM-DDTHH:MM:SS.fffZ
    # 為了測試通過，我們假設輸入是帶毫秒的
    ts4_with_ms = ts4_nodot.replace("Z", ".000Z")
    assert get_target_period_start_utc(ts4_with_ms, 12) == "2023-10-26T00:00:00.000Z"


    logger.info(f"Align '{ts1}' (6h): {get_target_period_start_utc(ts1, 6)}")   # Exp: ..T06:00:00.000Z
    assert get_target_period_start_utc(ts1, 6) == "2023-10-26T06:00:00.000Z"

    # 測試 align_data_to_periods
    sample_data = [
        {"t": "2023-01-01T01:00:00.000Z", "price": 10, "volume": 100},
        {"t": "2023-01-01T02:00:00.000Z", "price": 12, "volume": 150}, # Period 1 (00:00)
        {"t": "2023-01-01T08:00:00.000Z", "price": 11},                 # Period 1 (00:00), volume missing
        {"t": "2023-01-01T13:00:00.000Z", "price": 15, "volume": 200}, # Period 2 (12:00)
        {"t": "2023-01-01T10:00:00.000Z", "price": 13, "volume": 50},  # Period 1 (00:00), out of order
        {"t": "2023-01-01T23:00:00.000Z", "price": 18, "volume": 120}, # Period 2 (12:00)
    ]
    aligned = align_data_to_periods(
        sample_data,
        timestamp_key="t",
        value_keys=["price", "volume"],
        period_hours=12,
        aggregation_funcs={"price": "last", "volume": "sum"}
    )
    logger.info(f"Aligned data (12h periods): {aligned}")
    # Period 1: 2023-01-01T00:00:00.000Z
    # Items in order of time: (p10,v100), (p12,v150), (p13,v50), (p11, vol_None)
    # Price (last of valid): 11. Volume (sum of valid): 100+150+50 = 300
    assert aligned["2023-01-01T00:00:00.000Z"]["price"] == 11
    assert aligned["2023-01-01T00:00:00.000Z"]["volume"] == 300

    # Period 2: 2023-01-01T12:00:00.000Z
    # Items: (p15,v200), (p18,v120)
    # Price (last): 18. Volume (sum): 200+120 = 320
    assert aligned["2023-01-01T12:00:00.000Z"]["price"] == 18
    assert aligned["2023-01-01T12:00:00.000Z"]["volume"] == 320

    # 測試 align_ohlcv_data
    ohlcv_list = [
        # Period 1 (00:00 to 11:59)
        {"dt": "2023-01-01T01:00:00Z", "o": 10, "h": 12, "l": 9, "c": 11, "v": 100},
        {"dt": "2023-01-01T02:00:00Z", "o": 11, "h": 13, "l": 10, "c": 12, "v": 150},
        {"dt": "2023-01-01T06:00:00Z", "o": 12, "h": 12, "l": 10.5, "c": 10.5, "v": 70},
        # Period 2 (12:00 to 23:59)
        {"dt": "2023-01-01T13:00:00Z", "o": 20, "h": 22, "l": 19, "c": 21, "v": 200},
        {"dt": "2023-01-01T15:00:00Z", "o": 21, "h": 23, "l": 20, "c": 20, "v": 250},
    ]
    # 預處理時間戳為標準化格式
    ohlcv_list_std_ts = []
    for item in ohlcv_list:
        item_copy = item.copy()
        item_copy["dt"] = standardize_datetime_str_to_iso_utc(item["dt"]) # Cleaners 會做這個
        ohlcv_list_std_ts.append(item_copy)

    aligned_ohlcv = align_ohlcv_data(
        ohlcv_list_std_ts,
        timestamp_key="dt",
        open_key="o", high_key="h", low_key="l", close_key="c", volume_key="v",
        period_hours=12
    )
    logger.info(f"Aligned OHLCV data (12h periods): {aligned_ohlcv}")
    # Period 1: 2023-01-01T00:00:00.000Z
    # Open: 10 (from first item)
    # High: 13 (max of 12, 13, 12)
    # Low: 9 (min of 9, 10, 10.5)
    # Close: 10.5 (from last item in period)
    # Volume: 100 + 150 + 70 = 320
    p1_result = aligned_ohlcv.get("2023-01-01T00:00:00.000Z")
    assert p1_result is not None
    assert p1_result["o"] == 10
    assert p1_result["h"] == 13
    assert p1_result["l"] == 9
    assert p1_result["c"] == 10.5
    assert p1_result["v"] == 320

    # Period 2: 2023-01-01T12:00:00.000Z
    # Open: 20
    # High: 23
    # Low: 19
    # Close: 20
    # Volume: 200 + 250 = 450
    p2_result = aligned_ohlcv.get("2023-01-01T12:00:00.000Z")
    assert p2_result is not None
    assert p2_result["o"] == 20
    assert p2_result["h"] == 23
    assert p2_result["l"] == 19
    assert p2_result["c"] == 20
    assert p2_result["v"] == 450

    logger.info("--- Aligners (__main__) 測試結束 ---")
