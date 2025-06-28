from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger
from collections import deque # 用於高效計算移動平均等

logger = get_logger(__name__)

# --- 輔助函數 ---
def _calculate_average(data_points: list) -> float | None:
    """計算列表中數值的平均值。如果列表為空或無有效數值，返回 None。"""
    if not data_points:
        return None
    numeric_points = [p for p in data_points if isinstance(p, (int, float))]
    if not numeric_points:
        return None
    return sum(numeric_points) / len(numeric_points)

# --- 技術指標計算函數 ---

def calculate_sma(data_series: list[float | None], window_size: int) -> list[float | None]:
    """
    計算簡單移動平均 (SMA)。

    Args:
        data_series (list of float or None): 輸入的數據序列 (例如收盤價列表)。
                                            列表中的 None 值會被忽略，但會影響窗口內的有效數據點數。
                                            更穩健的處理可能是要求 data_series 不含 None，或明確處理策略。
                                            這裡，我們假設 None 值不參與計算，且窗口必須有足夠數據。
        window_size (int): 移動平均的窗口大小。

    Returns:
        list of float or None: 計算出的 SMA 序列。列表長度與 data_series 相同。
                               在窗口大小不足以計算 SMA 的初期，值為 None。
    """
    if not data_series or window_size <= 0:
        return [None] * len(data_series if data_series else [])

    sma_values = [None] * len(data_series)

    # 使用 deque 來高效管理滑動窗口
    # 但為了純 Python 和簡單性，先用列表切片，性能對小窗口影響不大
    # 如果性能是大問題，可以考慮 deque 或更優化的滑動窗口求和

    for i in range(len(data_series)):
        if i < window_size - 1:
            sma_values[i] = None # 窗口未滿
        else:
            window = data_series[i - window_size + 1 : i + 1]
            valid_points_in_window = [p for p in window if p is not None]
            if len(valid_points_in_window) < window_size: # 或者設置一個最小數據點比例閾值
                # 如果窗口內的有效數據點太少，可以選擇返回 None
                # 這裡我們嚴格要求窗口滿（除非允許 None 參與平均計算，但這不常見）
                # 如果允許部分有效數據，則用 len(valid_points_in_window) 作為除數
                # 為了標準 SMA，通常窗口內所有點都應有效，或至少大部分有效
                # 簡單起見，如果窗口內有 None，且我們嚴格要求無 None，則此 SMA 無效
                # 或者，如果我們只計算 valid_points_in_window 的平均值：
                if not valid_points_in_window: # 如果窗口內全是 None
                    sma_values[i] = None
                else:
                    # 這種情況下，它不再是嚴格意義上固定窗口大小的 SMA
                    # sma_values[i] = sum(valid_points_in_window) / len(valid_points_in_window)
                    # 為了保持 SMA 的定義，如果窗口內任何值為 None，則結果為 None
                    if any(p is None for p in window):
                         sma_values[i] = None
                    else:
                         sma_values[i] = sum(valid_points_in_window) / window_size # type: ignore
            else: # 窗口已滿且無 None
                sma_values[i] = sum(valid_points_in_window) / window_size # type: ignore

    return sma_values


def calculate_ema(data_series: list[float | None], window_size: int, smoothing_factor: float = 2.0) -> list[float | None]:
    """
    計算指數移動平均 (EMA)。

    Args:
        data_series (list of float or None): 輸入的數據序列。
        window_size (int): EMA 的窗口大小 (用於計算 alpha)。
        smoothing_factor (float): 平滑因子，預設為 2.0 (標準 EMA)。

    Returns:
        list of float or None: 計算出的 EMA 序列。
    """
    if not data_series or window_size <= 0:
        return [None] * len(data_series if data_series else [])

    ema_values = [None] * len(data_series)
    alpha = smoothing_factor / (1 + window_size)

    # 尋找第一個有效的數據點作為初始 EMA (可以是第一個點，或前 N 個點的 SMA)
    # 這裡我們使用第一個有效數據點作為第一個 EMA 值，後面的值基於它計算
    # 或者，更常見的是，EMA 的第一個值是對應窗口的 SMA

    first_valid_index = -1
    for i, val in enumerate(data_series):
        if val is not None:
            if i < window_size -1 : # 如果在第一個SMA窗口形成之前
                 # 在能計算出第一個 SMA 之前，EMA 通常是 None
                 # 或者，有些實現會用第一個值直接作為第一個 EMA，然後迭代
                 # 我們選擇在第一個 SMA 出現時才開始計算 EMA
                 pass
            else: # 可以計算第一個SMA了
                first_sma_window = [p for p in data_series[i - window_size + 1 : i + 1] if p is not None]
                if len(first_sma_window) == window_size: # 確保窗口滿且無None
                    ema_values[i] = sum(first_sma_window) / window_size
                    first_valid_index = i
                    break # 找到了第一個 EMA 值 (基於 SMA)

    if first_valid_index == -1: # 如果整個序列都無法計算出第一個有效的 SMA
        logger.debug("無法計算初始 EMA (基於 SMA)，因數據不足或包含過多 None。")
        return ema_values # 返回全部為 None 的列表

    # 計算後續的 EMA 值
    for i in range(first_valid_index + 1, len(data_series)):
        current_value = data_series[i]
        prev_ema = ema_values[i-1]

        if current_value is None: # 如果當前值為 None，EMA 也應為 None (或保持前值，取決於策略)
            ema_values[i] = None # 或者 ema_values[i] = prev_ema
            logger.debug(f"EMA[{i}]: 當前值為 None，EMA 設為 None。")
            continue

        if prev_ema is None: # 如果前一個 EMA 是 None (例如因為之前的 current_value 是 None)
            # 我們需要重新初始化 EMA。可以嘗試用最近的 SMA，或者如果斷開太久就一直是 None
            # 簡單處理：如果前一個 EMA 是 None，則當前 EMA 也無法計算，設為 None
            # 直到遇到一個有效的 prev_ema
            # 為了避免這種情況，如果 current_value is None，我們可能應該讓 prev_ema 繼承
            # 修改：如果 current_value is None, ema_values[i] = prev_ema (如果 prev_ema 也不是 None)
            # 但這會導致 EMA 在數據缺失時保持不變，可能不是期望的。
            # 這裡我們堅持：如果 prev_ema 是 None (因為之前的數據斷裂)，則當前 EMA 也是 None
            # 除非我們能找到一個新的起始點 (例如，重新計算一個 SMA)
            # 在上面的 first_valid_index 循環後，prev_ema (即 ema_values[first_valid_index]) 不應為 None
            logger.warning(f"EMA[{i}]: 前一個 EMA 為 None，無法計算當前 EMA。將設為 None。")
            ema_values[i] = None
            continue

        ema_values[i] = (current_value * alpha) + (prev_ema * (1 - alpha))

    return ema_values


def calculate_rsi(data_series: list[float | None], window_size: int = 14) -> list[float | None]:
    """
    計算相對強弱指數 (RSI)。

    Args:
        data_series (list of float or None): 輸入的數據序列 (通常是收盤價)。
        window_size (int): RSI 的窗口大小，預設為 14。

    Returns:
        list of float or None: 計算出的 RSI 序列 (值在 0 到 100 之間)。
    """
    if not data_series or window_size <= 0 or len(data_series) < window_size:
        return [None] * len(data_series if data_series else [])

    rsi_values = [None] * len(data_series)
    price_changes = [data_series[i] - data_series[i-1]  # type: ignore
                     if data_series[i] is not None and data_series[i-1] is not None
                     else 0.0 # 或者 None，但 0.0 更利於後續計算 gain/loss
                     for i in range(1, len(data_series))]

    # 確保 price_changes 的長度與 rsi_values 對齊 (前面補一個0或None)
    # RSI 的計算從第二個數據點的變化開始
    # 為了使 rsi_values 與 data_series 等長，我們通常在最前面填充 None
    # price_changes[0] 對應 data_series[1] 和 data_series[0] 的變化

    if len(price_changes) < window_size: # 需要 window_size 個變化值，即 window_size+1 個價格點
        return rsi_values

    avg_gain = 0.0
    avg_loss = 0.0

    # 計算第一個窗口的平均增益和平均損失
    initial_gains = [max(0, pc) for pc in price_changes[:window_size]]
    initial_losses = [max(0, -pc) for pc in price_changes[:window_size]] # 損失取正值

    if not initial_gains or not initial_losses: # 數據不足或變化全為0
        return rsi_values

    avg_gain = sum(initial_gains) / window_size
    avg_loss = sum(initial_losses) / window_size

    # 計算第一個 RSI 值
    # RSI 的第一個值通常在第 window_size + 1 個數據點（即第 window_size 個變化）之後計算
    rsi_idx_start = window_size
    if avg_loss == 0:
        rsi_values[rsi_idx_start] = 100.0 if avg_gain > 0 else 50.0 # 避免除以零，如果全漲則100，無漲跌則50
    else:
        rs = avg_gain / avg_loss
        rsi_values[rsi_idx_start] = 100.0 - (100.0 / (1.0 + rs))

    # 計算後續的 RSI 值 (使用 Wilder's smoothing)
    for i in range(window_size, len(price_changes)):
        change = price_changes[i]
        gain = max(0, change)
        loss = max(0, -change) # 損失取正值

        avg_gain = (avg_gain * (window_size - 1) + gain) / window_size
        avg_loss = (avg_loss * (window_size - 1) + loss) / window_size

        if avg_loss == 0:
            rsi_values[i+1] = 100.0 if avg_gain > 0 else 50.0 # i+1 是因為 price_changes 比 data_series 短1
        else:
            rs = avg_gain / avg_loss
            rsi_values[i+1] = 100.0 - (100.0 / (1.0 + rs))

    return rsi_values


def calculate_atr(high_prices: list[float | None],
                  low_prices: list[float | None],
                  close_prices: list[float | None],
                  window_size: int = 14) -> list[float | None]:
    """
    計算平均真實波幅 (ATR)。

    Args:
        high_prices (list): 最高價序列。
        low_prices (list): 最低價序列。
        close_prices (list): 收盤價序列。
        window_size (int): ATR 的窗口大小，預設為 14。

    Returns:
        list of float or None: 計算出的 ATR 序列。
    """
    if not (high_prices and low_prices and close_prices) or \
       not (len(high_prices) == len(low_prices) == len(close_prices)) or \
       window_size <= 0:
        return [None] * len(high_prices if high_prices else [])

    num_periods = len(close_prices)
    if num_periods < 1: # 至少需要一個收盤價來計算 TR 的 prev_close
        return [None] * num_periods

    true_ranges = [None] * num_periods

    # 計算每個週期的 True Range (TR)
    # TR = Max(High - Low, abs(High - PreviousClose), abs(Low - PreviousClose))
    for i in range(num_periods):
        if high_prices[i] is None or low_prices[i] is None:
            true_ranges[i] = None # 如果當前高低價缺失，無法計算 TR
            continue

        tr1 = high_prices[i] - low_prices[i] # type: ignore

        if i == 0: # 第一個週期沒有前一收盤價
            # 有些實現會將第一個 TR 設為 High - Low
            # 或者，如果需要嚴格的 ATR，則第一個 TR 及其對應的 ATR 為 None
            true_ranges[i] = tr1 # 簡化處理，第一個 TR = H-L
            continue

        prev_close = close_prices[i-1]
        if prev_close is None:
            true_ranges[i] = tr1 # 如果前一收盤價缺失，也只能用 H-L
            continue

        tr2 = abs(high_prices[i] - prev_close) # type: ignore
        tr3 = abs(low_prices[i] - prev_close) # type: ignore

        true_ranges[i] = max(tr1, tr2, tr3)

    atr_values = [None] * num_periods
    if num_periods < window_size: # 數據不足以計算第一個 ATR
        return atr_values

    # 計算第一個 ATR (通常是前 N 個 TR 的簡單平均)
    initial_trs = [tr for tr in true_ranges[0:window_size] if tr is not None] # 從第一個 TR 開始取窗口
    if len(initial_trs) == window_size : # 確保有足夠的 TR 值
        current_atr = sum(initial_trs) / window_size
        atr_values[window_size -1] = current_atr # ATR 的第一個值對應第 N 個 TR (即第 N 個數據點)
                                                 # 或者有些定義是第 N+1 個數據點
                                                 # 這裡我們讓 atr_values[window_size-1] 是第一個 ATR
    else: # 數據不足或 TR 中有 None
        logger.debug(f"ATR: 初始 TR 窗口數據不足 {len(initial_trs)}/{window_size}")
        # 後續 ATR 也無法計算，保持為 None
        return atr_values


    # 計算後續的 ATR (通常使用類似 EMA 的平滑方法)
    # ATR_current = ((ATR_previous * (N-1)) + TR_current) / N
    for i in range(window_size, num_periods):
        if true_ranges[i] is None: # 如果當前 TR 無法計算
            atr_values[i] = None # 當前 ATR 也為 None (或繼承前值，取決於策略)
            current_atr = None # 標記 ATR 序列已斷開
            continue

        if current_atr is None: # 如果 ATR 序列已斷開
            # 嘗試重新初始化 ATR，例如用最近 N 個 TR 的 SMA
            # 這裡簡化：如果斷開，則後續 ATR 都為 None，直到有足夠數據重新計算 SMA
            # 或者，如果允許，我們可以從這裡開始一個新的 ATR 計算（即將此 TR 視為第一個 TR 的 SMA）
            # 但這會導致 ATR 不連續。
            # 為了保持一致性，如果 current_atr 變為 None，則後續都為 None。
            # 除非 true_ranges[i] 之後又有連續 N 個非 None 的 TR 可以重新計算 SMA。
            # 這裡的邏輯是，一旦 current_atr 為 None，它將保持 None。
            # 在更複雜的實現中，可以考慮重新計算初始 ATR。
            logger.warning(f"ATR[{i}]: 前一個 ATR 為 None，無法計算當前 ATR。")
            atr_values[i] = None
            continue

        current_atr = (current_atr * (window_size - 1) + true_ranges[i]) / window_size # type: ignore
        atr_values[i] = current_atr

    return atr_values


# --- 主特徵計算協調函數 (示例性) ---

def calculate_all_features(aligned_ohlcv_period_data: dict, historical_ohlcv_series: list[dict]) -> dict:
    """
    計算給定對齊後的一個週期的所有特徵。
    這是一個高層次的協調函數，會調用各種指標計算函數。

    Args:
        aligned_ohlcv_period_data (dict):
            單個週期的對齊後 OHLCV 數據。
            例如: {"open": 100, "high": 105, "low": 98, "close": 102, "volume": 10000}
            這些應該是 `align_ohlcv_data` 的輸出中的一個 value。

        historical_ohlcv_series (list of dict):
            包含當前週期及之前足夠多歷史週期的 OHLCV 數據列表 (已對齊和清洗)。
            列表中的每個元素與 aligned_ohlcv_period_data 結構相同。
            列表應按時間升序排列，最新的數據在列表末尾。
            長度應足以計算所需最長窗口的指標 (例如，SMA20 需要至少20個週期)。

    Returns:
        dict: 包含所有計算出的特徵的字典。鍵是特徵名，值是特徵值。
              例如: {"price_open": 100, ..., "sma_20": 101.5, "rsi_14": 60.2, ...}
    """
    features = {}

    # 1. 直接特徵 (來自當前週期的對齊數據)
    features["price_open"] = aligned_ohlcv_period_data.get("open")
    features["price_high"] = aligned_ohlcv_period_data.get("high")
    features["price_low"] = aligned_ohlcv_period_data.get("low")
    features["price_close"] = aligned_ohlcv_period_data.get("close")
    features["volume_total"] = aligned_ohlcv_period_data.get("volume")

    # 準備用於指標計算的歷史序列
    # 假設 historical_ohlcv_series 包含按時間排序的字典，每個字典有 "close", "high", "low" 等鍵
    # 並且最新的數據（對應 aligned_ohlcv_period_data）是列表的最後一個元素。

    close_prices = [p.get("close") for p in historical_ohlcv_series]
    high_prices = [p.get("high") for p in historical_ohlcv_series]
    low_prices = [p.get("low") for p in historical_ohlcv_series]

    # 2. 計算技術指標
    # SMA (例如 SMA20)
    sma_20_values = calculate_sma(close_prices, 20)
    features["sma_20"] = sma_20_values[-1] if sma_20_values and len(sma_20_values) > 0 else None

    # EMA (例如 EMA12)
    ema_12_values = calculate_ema(close_prices, 12)
    features["ema_12"] = ema_12_values[-1] if ema_12_values and len(ema_12_values) > 0 else None

    # RSI (例如 RSI14)
    rsi_14_values = calculate_rsi(close_prices, 14)
    features["rsi_14"] = rsi_14_values[-1] if rsi_14_values and len(rsi_14_values) > 0 else None

    # ATR (例如 ATR14)
    # ATR 需要 high, low, close 序列
    atr_14_values = calculate_atr(high_prices, low_prices, close_prices, 14)
    features["volatility_12hr_atr"] = atr_14_values[-1] if atr_14_values and len(atr_14_values) > 0 else None # 假設ATR對應設定的週期

    # MACD (示例 - 較複雜，可能需要單獨的函數)
    # ema_26 = calculate_ema(close_prices, 26)
    # if ema_12_values and ema_26_values and ema_12_values[-1] is not None and ema_26_values[-1] is not None:
    #     macd_line = ema_12_values[-1] - ema_26_values[-1]
    #     # features["macd_line"] = macd_line
    #     # macd_signal_values = calculate_ema([m for m in historical_macd_lines if m is not None], 9) # 需要歷史 MACD line
    #     # features["macd_signal"] = macd_signal_values[-1] if macd_signal_values else None
    # else:
    #     # features["macd_line"] = None
    #     # features["macd_signal"] = None
    logger.debug("MACD 和布林帶等更複雜指標的計算暫未完全實現。")

    # 布林帶 (示例 - 依賴 SMA 和標準差)
    # if sma_20_values and sma_20_values[-1] is not None and len(close_prices) >= 20:
    #     window_for_std = [p for p in close_prices[-20:] if p is not None]
    #     if len(window_for_std) == 20:
    #         mean = sma_20_values[-1]
    #         std_dev = (_calculate_average([(p - mean)**2 for p in window_for_std]))**0.5 if mean is not None else None
    #         if std_dev is not None and mean is not None:
    #             features["bollinger_upper"] = mean + 2 * std_dev
    #             features["bollinger_lower"] = mean - 2 * std_dev
    # else:
    #     features["bollinger_upper"] = None
    #     features["bollinger_lower"] = None

    # 3. 其他類型的特徵 (情緒、宏觀等)
    # 這些通常不是從 OHLCV 計算出來的，而是作為輸入傳遞給這個階段
    # 例如，如果 aligned_ohlcv_period_data 中包含了已對齊的新聞情緒分數：
    # features["news_sentiment_score"] = aligned_ohlcv_period_data.get("news_sentiment_score")
    # features["fred_interest_rate"] = aligned_ohlcv_period_data.get("fred_interest_rate")

    # 清理一下，確保所有預期在 processed_features_hourly 表中的字段都有值 (可以是 None)
    # 這裡只返回已計算的，schema.sql 中的所有字段應由上游組裝時填充

    logger.info(f"為週期數據計算了 {len(features)} 個特徵。")
    logger.debug(f"計算出的特徵: {features}")
    return features


if __name__ == "__main__":
    import logging
    setup_logger(PROJECT_LOGGER_NAME, level=logging.DEBUG)

    logger.info("--- FeatureCalculator (__main__) 測試開始 ---")

    # 測試 SMA
    prices1 = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    sma5 = calculate_sma(prices1, 5)
    logger.info(f"SMA(5) for {prices1}: {sma5}")
    # Expected: [N, N, N, N, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0]
    assert sma5 == [None, None, None, None, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0]

    prices2 = [10, None, 12, 13, None, 15, 16]
    sma3 = calculate_sma(prices2, 3)
    logger.info(f"SMA(3) for {prices2}: {sma3}")
    # Expected: [N, N, N (10,N,12), N (N,12,13), N (12,13,N), N (13,N,15), N (N,15,16)]
    # 根據目前的實現，如果窗口內有 None，結果是 None
    assert sma3 == [None, None, None, None, None, None, None]
    # 如果修改為只計算 valid_points:
    # [N,N, (10+12)/2=11, (12+13)/2=12.5, (12+13)/2=12.5, (13+15)/2=14, (15+16)/2=15.5] (如果窗口不滿也算)
    # 或者 [N,N,11,12.5,12.5,14,15.5] (如果sma_values[i] = sum(valid_points_in_window) / len(valid_points_in_window))


    # 測試 EMA
    ema5 = calculate_ema(prices1, 5) # window 5
    logger.info(f"EMA(5) for {prices1}: {ema5}")
    # EMA(5) alpha = 2/(5+1) = 1/3
    # SMA(5)[4] = 12.0 (first EMA)
    # EMA[5] = 15*(1/3) + 12*(2/3) = 5 + 8 = 13.0
    # EMA[6] = 16*(1/3) + 13*(2/3) = 5.333 + 8.666 = 13.999... (約 14.0)
    # EMA[7] = 17*(1/3) + 14*(2/3) = 5.666 + 9.333 = 14.999... (約 15.0)
    expected_ema5 = [None, None, None, None, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0] # 近似，因為alpha*value + (1-alpha)*prev_ema
    # 手動驗算更精確的：
    # prices1 = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    # ema_values = [N,N,N,N, 12.0, 0,0,0,0,0,0 ]
    # i=5: data=15, prev_ema=12. ema = 15*(1/3) + 12*(2/3) = 5+8=13.0
    # i=6: data=16, prev_ema=13. ema = 16/3 + 13*2/3 = (16+26)/3 = 42/3 = 14.0
    # i=7: data=17, prev_ema=14. ema = 17/3 + 14*2/3 = (17+28)/3 = 45/3 = 15.0
    # i=8: data=18, prev_ema=15. ema = 18/3 + 15*2/3 = (18+30)/3 = 48/3 = 16.0
    # i=9: data=19, prev_ema=16. ema = 19/3 + 16*2/3 = (19+32)/3 = 51/3 = 17.0
    # i=10:data=20, prev_ema=17. ema = 20/3 + 17*2/3 = (20+34)/3 = 54/3 = 18.0
    for i in range(len(prices1)):
        if ema5[i] is not None:
            assert abs(ema5[i] - expected_ema5[i]) < 0.001 # 比較浮點數

    ema_with_none = calculate_ema(prices2, 3)
    logger.info(f"EMA(3) for {prices2}: {ema_with_none}")
    # prices2 = [10, None, 12, 13, None, 15, 16] alpha = 2/4 = 0.5
    # SMA[2] (10,N,12) -> (10+12)/2 = 11. ema_values[2]=11. first_valid_index=2
    # i=3: data=13, prev_ema=11. ema = 13*0.5 + 11*0.5 = 6.5+5.5 = 12.0. ema_values[3]=12.0
    # i=4: data=None. ema_values[4]=None.
    # i=5: data=15, prev_ema=None. ema_values[5]=None.
    # i=6: data=16, prev_ema=None. ema_values[6]=None.
    assert ema_with_none == [None, None, 11.0, 12.0, None, None, None]


    # 測試 RSI
    rsi14 = calculate_rsi(prices1, 14) # 數據點不足14+1，應返回 Nones
    logger.info(f"RSI(14) for prices1 (len {len(prices1)}): {rsi14}")
    assert all(v is None for v in rsi14)

    # 構造一個更長的序列來測試 RSI
    prices_for_rsi = [44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28, 46.00, 46.03, 46.41, 46.22, 45.64, 46.21] # 21 個點
    rsi_output = calculate_rsi(prices_for_rsi, 14)
    logger.info(f"RSI(14) for longer series (len {len(prices_for_rsi)}): {rsi_output}")
    # 期望前14個為None (因為需要14個price_changes，即15個價格點來計算第一個avg_gain/loss，然後第15個RSI值)
    # RSI 的第一個值在 index 14 (第15個價格點)
    # 這裡rsi_values的index對應data_series的index，所以rsi_values[14]是第一個RSI
    assert all(rsi_output[i] is None for i in range(14))
    assert rsi_output[14] is not None # 第一個RSI值
    logger.info(f"First calculated RSI: {rsi_output[14]}") # 打印看看
    # (需要與已知計算器驗證準確性，這裡只驗證結構)


    # 測試 ATR
    highs = [10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25] # 16
    lows  = [9, 10,11,12,13,14,13,14,15,16,17,18,19,20,21,22]
    closes= [9.5,10.5,11.5,12.5,13.5,14.5,13.5,14.5,15.5,16.5,17.5,18.5,19.5,21.0,22.5,23.5] # 讓 prev_close 變化
    atr14 = calculate_atr(highs, lows, closes, 14)
    logger.info(f"ATR(14) for H/L/C series (len {len(highs)}): {atr14}")
    # ATR 的第一個值在 index 13 (第14個TR之後)
    assert all(atr14[i] is None for i in range(13))
    assert atr14[13] is not None
    logger.info(f"First calculated ATR: {atr14[13]}")


    # 測試 calculate_all_features (簡化版)
    # 需要歷史數據
    history_len = 30
    mock_historical_ohlcv = []
    for i in range(history_len):
        mock_historical_ohlcv.append({
            "open": 100 + i*0.1, "high": 101 + i*0.15,
            "low": 99 + i*0.05, "close": 100.5 + i*0.1, "volume": 10000 + i*100
        })

    current_period_data = mock_historical_ohlcv[-1] # 最後一個作為當前週期

    all_features = calculate_all_features(current_period_data, mock_historical_ohlcv)
    logger.info(f"Calculated all features for current period: {all_features}")
    assert features["price_close"] == current_period_data["close"]
    assert features["sma_20"] is not None # 假設數據足夠
    assert features["ema_12"] is not None
    assert features["rsi_14"] is not None
    assert features["volatility_12hr_atr"] is not None


    logger.info("--- FeatureCalculator (__main__) 測試結束 ---")
