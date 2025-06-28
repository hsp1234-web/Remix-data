import yfinance as yf
import requests_cache
import time
import random
import logging
import pandas as pd
from datetime import datetime, timedelta
import os

# --- 配置區域 ---
# 快取設定
CACHE_DIR = "CACHE_Market_Data"
CACHE_NAME = os.path.join(CACHE_DIR, "yfinance_cache")
# 確保快取資料夾存在
os.makedirs(CACHE_DIR, exist_ok=True)
# 設定 requests_cache，快取有效期預設為 1 天，可以根據需要調整
# yfinance 底層使用 requests，所以 requests_cache 可以為其快取
SESSION = requests_cache.CachedSession(
    CACHE_NAME,
    backend='sqlite',
    expire_after=timedelta(days=1),  # 快取有效期限
    allowable_codes=[200, 404],        # 也快取 404 錯誤，避免重複請求不存在的資源
    stale_if_error=True,             # 如果 API 錯誤，則使用過期的快取
)
# 讓 yfinance 使用我們配置了快取的 session
yf.set_session(SESSION)


# API 呼叫穩定性設定
MAX_RETRIES = 5
INITIAL_DELAY_SECONDS = 1  # 初始延遲時間（秒）
MAX_DELAY_SECONDS = 60     # 最大延遲時間（秒）
JITTER_FACTOR = 0.5        # 抖動因子 (0 到 1 之間)

# 日誌設定
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# --- 核心功能 ---

def fetch_yfinance_data_stable(ticker_symbol: str, 
                               start_date: str, 
                               end_date: str, 
                               interval: str = "1d",
                               attempt_hourly_first: bool = True) -> pd.DataFrame | None:
    """
    使用 yfinance 獲取指定股票代號的歷史數據，並加入錯誤處理、重試與快取機制。
    如果 attempt_hourly_first 為 True，會先嘗試獲取小時線數據，失敗則降級獲取日線數據。

    Args:
        ticker_symbol (str): 股票代號 (例如 "SPY", "^TWII")
        start_date (str): 開始日期 (YYYY-MM-DD)
        end_date (str): 結束日期 (YYYY-MM-DD)
        interval (str): 數據間隔，預設為 "1d" (日線)。
                        如果 attempt_hourly_first 為 True，此參數將被內部覆蓋。
        attempt_hourly_first (bool): 是否先嘗試獲取小時線數據。

    Returns:
        pd.DataFrame | None: 包含 OHLCV 數據的 DataFrame，或在無法獲取時回傳 None。
    """
    ticker = yf.Ticker(ticker_symbol)
    current_delay = INITIAL_DELAY_SECONDS
    
    intervals_to_try = []
    if attempt_hourly_first:
        intervals_to_try.append("1h")
        # Yahoo Finance 的小時數據通常有時間範圍限制 (例如最近 730 天)
        # 如果請求時間範圍過長，小時數據請求必然失敗，直接使用日線
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if (end_dt - start_dt).days > 700: # 大約兩年，保守估計
            logger.warning(f"{ticker_symbol}: 請求時間範圍超過700天 ({start_date} 至 {end_date})，"
                           f"小時線數據可能不可用或不完整，將直接嘗試日線數據。")
            intervals_to_try = ["1d"] if interval == "1d" or "h" in interval else [interval, "1d"]
        elif interval != "1d" and interval != "1h": # 如果使用者指定了其他 interval
             intervals_to_try = [interval, "1h", "1d"]

    if not intervals_to_try: # 如果不嘗試小時線，或已調整
        intervals_to_try.append(interval)
        if interval != "1d": # 確保日線是最終備選
            intervals_to_try.append("1d")
    
    # 去重並保持順序
    final_intervals_to_try = list(dict.fromkeys(intervals_to_try))

    for current_interval in final_intervals_to_try:
        logger.info(f"開始嘗試獲取 {ticker_symbol} 的 {current_interval} 數據，"
                    f"日期範圍：{start_date} 至 {end_date}")
        for attempt in range(MAX_RETRIES):
            try:
                # 檢查快取狀態 (僅為演示，實際 yfinance 會自動處理)
                cache_key = SESSION.cache.create_key(method='GET', url=f"https://query1.finance.yahoo.com/v7/finance/download/{ticker_symbol}")
                is_cached = SESSION.cache.contains(key=cache_key)
                logger.debug(f"{ticker_symbol} ({current_interval}) - 第 {attempt + 1}/{MAX_RETRIES} 次嘗試。"
                             f" 快取狀態: {'已快取' if is_cached else '未快取'}")

                data = ticker.history(start=start_date, end=end_date, interval=current_interval)

                if data is None or data.empty:
                    logger.warning(f"{ticker_symbol} ({current_interval}): "
                                   f"在 {start_date} 到 {end_date} 期間未找到數據。")
                    if current_interval == final_intervals_to_try[-1]: # 如果是最後一個嘗試的 interval
                        return None # 所有 interval 都失敗
                    break # 嘗試下一個 interval

                logger.info(f"成功獲取 {ticker_symbol} 的 {current_interval} 數據，共 {len(data)} 筆。")
                # 確保 DatetimeIndex 是 timezone-naive，以避免後續處理問題
                if isinstance(data.index, pd.DatetimeIndex) and data.index.tz is not None:
                    data.index = data.index.tz_localize(None)
                return data

            except requests_cache.exceptions.CacheError as ce:
                logger.error(f"{ticker_symbol} ({current_interval}): 快取錯誤 - {ce}")
                # 快取錯誤通常比較嚴重，可能需要檢查快取後端設定
                return None # 直接放棄
            except ConnectionError as e: # requests.exceptions.ConnectionError
                 logger.warning(f"{ticker_symbol} ({current_interval}): 第 {attempt + 1}/{MAX_RETRIES} 次嘗試 - 連線錯誤: {e}")
            except TimeoutError as e: # requests.exceptions.Timeout
                logger.warning(f"{ticker_symbol} ({current_interval}): 第 {attempt + 1}/{MAX_RETRIES} 次嘗試 - 請求超時: {e}")
            except Exception as e:
                # yfinance 可能會拋出各種錯誤，有些是非標準的
                # 例如，當數據不存在或 ticker 無效時，yfinance 可能直接印出錯誤到 stderr 且不拋出標準 Python 異常
                # 或者拋出 KeyError, IndexError 等
                error_type = type(e).__name__
                logger.warning(f"{ticker_symbol} ({current_interval}): 第 {attempt + 1}/{MAX_RETRIES} 次嘗試 - "
                               f"發生未預期錯誤 ({error_type}): {e}")
                # 有些 yfinance 錯誤 (例如 404) 可能已被 requests_cache 快取，這裡不一定會看到
                # 如果 ticker 真的有問題，history() 可能會回傳空 DataFrame 或引發其他錯誤
                # 檢查 ticker 是否有效 (一個簡單的方法是看 info 屬性，但這也會發起網路請求)
                try:
                    if not ticker.info and attempt == 0 and current_interval == final_intervals_to_try[0]: # 第一次嘗試就檢查
                         logger.error(f"{ticker_symbol}: 股票代號可能無效或沒有 info 屬性。")
                         # 如果 ticker 無效，後續所有嘗試可能都會失敗，可以考慮直接返回
                         # return None # 取決於策略，這裡我們先繼續重試
                except Exception as info_e:
                    logger.warning(f"{ticker_symbol}: 獲取 info 屬性時發生錯誤: {info_e}")


            if attempt < MAX_RETRIES - 1:
                sleep_time = current_delay + random.uniform(0, JITTER_FACTOR * current_delay)
                logger.info(f"{ticker_symbol} ({current_interval}): {sleep_time:.2f} 秒後重試...")
                time.sleep(sleep_time)
                current_delay = min(current_delay * 2, MAX_DELAY_SECONDS) # 指數退避
            else:
                logger.error(f"{ticker_symbol} ({current_interval}): "
                               f"經過 {MAX_RETRIES} 次嘗試後，仍無法獲取數據。")
                if current_interval == final_intervals_to_try[-1]:
                    return None # 所有 interval 都失敗
                # 否則，將在外層循環中嘗試下一個 interval
        
        # 如果一個 interval 嘗試完畢且未成功返回數據 (例如數據為空)，則繼續下一個 interval
        if final_intervals_to_try.index(current_interval) < len(final_intervals_to_try) - 1:
            logger.info(f"{ticker_symbol}: {current_interval} 數據獲取失敗或為空，嘗試下一個 interval...")
            current_delay = INITIAL_DELAY_SECONDS # 重置延遲以供下一個 interval 類型使用
        else:
            logger.error(f"{ticker_symbol}: 所有嘗試的 interval ({final_intervals_to_try}) 均無法獲取有效數據。")
            return None
            
    return None # 理論上不應該執行到這裡

# --- 測試範例 ---
if __name__ == "__main__":
    logger.info("開始 yfinance 數據獲取測試...")

    # 清理舊快取 (僅為測試，實際使用時可不清)
    # if os.path.exists(CACHE_NAME + ".sqlite"):
    #     logger.info(f"正在刪除舊快取檔案: {CACHE_NAME}.sqlite")
    #     os.remove(CACHE_NAME + ".sqlite")

    today = datetime.now()
    start_date_long = (today - timedelta(days=800)).strftime("%Y-%m-%d") # 超過730天，測試小時線降級
    start_date_short = (today - timedelta(days=60)).strftime("%Y-%m-%d") # 60天內，小時線應可用
    end_date = today.strftime("%Y-%m-%d")

    tickers_to_test = {
        "美股指數 S&P 500": "^GSPC",
        "台股加權指數": "^TWII",
        "美國20年期以上公債ETF": "TLT",
        "美國高收益公司債ETF": "HYG",
        "美國投資級公司債ETF": "LQD",
        "波動率指數 VIX": "^VIX",
        "新台幣兌美元": "TWD=X",
        "S&P 500 ETF": "SPY",
        "一個可能不存在的代號": "NONEXISTENTTICKERXYZ",
        "短期數據測試 (AAPL)": "AAPL" # 用於測試小時線
    }

    all_data = {}

    for name, symbol in tickers_to_test.items():
        logger.info(f"\n--- 測試獲取: {name} ({symbol}) ---")
        
        current_start = start_date_short if symbol == "AAPL" or "TWD=X" in symbol else start_date_long
        if symbol == "^VIX": # VIX 通常用日線
             data = fetch_yfinance_data_stable(symbol, current_start, end_date, interval="1d", attempt_hourly_first=False)
        elif symbol == "NONEXISTENTTICKERXYZ":
            data = fetch_yfinance_data_stable(symbol, start_date_short, end_date, attempt_hourly_first=True)
        else:
            data = fetch_yfinance_data_stable(symbol, current_start, end_date, attempt_hourly_first=True)
        
        if data is not None and not data.empty:
            logger.info(f"成功獲取 {name} ({symbol}) 的數據，前5筆：")
            print(data.head())
            logger.info(f"數據維度: {data.shape}")
            all_data[symbol] = data
        else:
            logger.warning(f"未能獲取 {name} ({symbol}) 的數據。")
        
        # 為了避免過於頻繁的請求，即使有快取，也稍微延遲一下
        time.sleep(random.uniform(0.5, 1.5)) 

    logger.info("\n--- yfinance 數據獲取測試完成 ---")

    # 簡單演示快取效果
    if "SPY" in all_data:
        logger.info("\n--- 測試快取：再次獲取 SPY 日線數據 ---")
        # 確保使用與之前不同的 start_date 或 end_date 以觸發新的 history 請求，
        # 但如果 interval 和 symbol 相同，且大部分數據已快取，速度應該很快。
        # 或者，如果請求完全相同的參數，則應直接從快取中讀取。
        spy_start = (today - timedelta(days=5)).strftime("%Y-%m-%d")
        spy_data_cached = fetch_yfinance_data_stable("SPY", spy_start, end_date, interval="1d", attempt_hourly_first=False)
        if spy_data_cached is not None:
            logger.info("第二次獲取 SPY 數據成功 (應主要來自快取或快速獲取)。")
            print(spy_data_cached.head())

    logger.info(f"快取檔案位於: {CACHE_NAME}.sqlite")
    logger.info("請檢查日誌輸出以了解詳細的獲取過程和錯誤處理情況。")

# --- 台指期貨 (TAIFEX) 相關功能 ---

def get_taifex_futures_last_trading_day(year: int, month: int) -> pd.Timestamp:
    """
    計算指定年份和月份的台指期貨最後交易日（該月的第三個星期三）。
    """
    # 從該月的第一天開始
    current_date = pd.Timestamp(year, month, 1)
    # 找到該月的第一個星期三
    while current_date.dayofweek != 2: # 0=Mon, 1=Tue, 2=Wed
        current_date += timedelta(days=1)
    # 第三個星期三 = 第一個星期三 + 14 天
    last_trading_day = current_date + timedelta(days=14)
    return last_trading_day

def get_taifex_futures_symbol(year: int, month: int) -> str:
    """
    生成 yfinance 使用的台指期貨合約代號。
    格式推測為 TXFYYYYMM.TW，例如 TXF202408.TW
    """
    return f"TXF{year}{month:02d}.TW"

def fetch_continuous_taifex_futures(start_year: int, 
                                    start_month: int, 
                                    end_year: int, 
                                    end_month: int,
                                    interval: str = "1d") -> pd.DataFrame | None:
    """
    獲取並拼接台指期貨連續近月合約數據，使用向後調整法。

    Args:
        start_year (int): 開始年份
        start_month (int): 開始月份
        end_year (int): 結束年份
        end_month (int): 結束月份
        interval (str): 數據間隔 (例如 "1d", "1h")，注意小時數據有其限制。

    Returns:
        pd.DataFrame | None: 包含連續合約 OHLCV 數據的 DataFrame，或在無法獲取時回傳 None。
                             索引為日期，欄位為 Open, High, Low, Close, Volume。
    """
    logger.info(f"開始獲取台指期貨連續合約數據：{start_year}-{start_month:02d} 至 {end_year}-{end_month:02d}")

    all_contracts_data = []
    # 生成需要獲取的合約月份列表
    current_y, current_m = start_year, start_month
    target_contracts = []
    while current_y < end_year or (current_y == end_year and current_m <= end_month):
        target_contracts.append((current_y, current_m))
        current_m += 1
        if current_m > 12:
            current_m = 1
            current_y += 1
    
    # 為了進行向後調整，我們還需要目標結束月份的下一個月合約數據 (如果它不是最後一個要分析的合約)
    # 但在拼接時，我們只取到 end_month 的數據。
    # 實際上，向後調整是從最新的合約往前回溯。
    # 我們需要從 end_year, end_month 開始，向前獲取到 start_year, start_month
    
    # 倒序生成合約列表，方便向後調整
    # 合約列表應從 (end_year, end_month) 的 *下一個月* 開始，一直到 (start_year, start_month)
    # 因為調整基準是 "未來" 的合約。
    
    # 修正：我們需要從 start_year, start_month 到 end_year, end_month 的所有合約
    # 再加上 end_year, end_month 之後的一個合約，作為計算最後一個價差的 "新合約"
    
    contracts_to_fetch_info = [] # (year, month, symbol, last_trading_day)
    
    # 生成從 (start_year, start_month) 到 (end_year, end_month + 1) 的合約資訊
    # (如果 end_month 是12月，則下一個月是明年1月)
    temp_y, temp_m = start_year, start_month
    
    # 確保我們至少獲取到 end_year, end_month 的數據，以及其後一個月（如果存在）用於價差計算
    final_fetch_y, final_fetch_m = end_year, end_month
    final_fetch_m +=1
    if final_fetch_m > 12:
        final_fetch_m = 1
        final_fetch_y += 1

    while temp_y < final_fetch_y or (temp_y == final_fetch_y and temp_m <= final_fetch_m):
        symbol = get_taifex_futures_symbol(temp_y, temp_m)
        try:
            # 最後交易日可能需要精確，但對於yfinance的日線數據，我們主要關心的是獲取到足夠的重疊
            # 我們假設合約數據至少能取到其月份的月底，或最後交易日之後幾天
            # 獲取數據時的 end_date 應略大於該合約月份的最後交易日
            # start_date 可以是上一個月的月中，確保有重疊
            
            # 簡化：先獲取每個合約從月初到月底（或下月初）的數據
            contract_start_date = datetime(temp_y, temp_m, 1).strftime("%Y-%m-%d")
            next_m, next_y = (temp_m % 12) + 1, temp_y + (temp_m // 12)
            if temp_m == 12: # 如果是12月，下個月是明年1月
                next_y = temp_y + 1
                next_m = 1
            contract_end_date = (datetime(next_y, next_m, 1) + timedelta(days=5)).strftime("%Y-%m-%d") # 多取幾天確保覆蓋
            
            # 特殊處理：對於最後一個要獲取的合約 (final_fetch_y, final_fetch_m)，其結束日期不應超過今天太多
            if temp_y == final_fetch_y and temp_m == final_fetch_m:
                 today_str = datetime.now().strftime("%Y-%m-%d")
                 if contract_end_date > today_str:
                     contract_end_date = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")


            logger.info(f"準備獲取合約 {symbol} ({temp_y}-{temp_m:02d})，"
                        f"預計數據範圍: {contract_start_date} 至 {contract_end_date}")
            
            # 使用已有的穩定獲取函數
            # 台指期貨的小時線數據在 yfinance 上可能非常稀疏或不存在，優先日線
            fut_interval = "1d" if "h" in interval else interval # 預設用日線，除非明確指定
            # attempt_hourly_first 應設為 False，因為台指期貨小時線很可能沒有
            contract_data = fetch_yfinance_data_stable(symbol, 
                                                       contract_start_date, 
                                                       contract_end_date, 
                                                       interval=fut_interval,
                                                       attempt_hourly_first=False) 
            time.sleep(random.uniform(1.0, 2.5)) # 尊重伺服器

            if contract_data is not None and not contract_data.empty:
                # 確定此合約的實際換月日 (即此合約月份的第三個星期三)
                # 注意：這是此合約 *成為過去* 的換月日，即下一個合約開始成為主力
                ltd = get_taifex_futures_last_trading_day(temp_y, temp_m)
                contracts_to_fetch_info.append({
                    "year": temp_y, "month": temp_m, "symbol": symbol, 
                    "data": contract_data, "last_trading_day": ltd
                })
                logger.info(f"成功獲取合約 {symbol} 數據，共 {len(contract_data)} 筆。")
            else:
                logger.warning(f"未能獲取合約 {symbol} ({temp_y}-{temp_m:02d}) 的數據。後續拼接可能受影響。")

        except Exception as e:
            logger.error(f"獲取合約 {symbol} ({temp_y}-{temp_m:02d}) 數據時發生錯誤: {e}")
        
        temp_m += 1
        if temp_m > 12:
            temp_m = 1
            temp_y += 1
            
    if not contracts_to_fetch_info or len(contracts_to_fetch_info) < 2:
        logger.error("未能獲取足夠的期貨合約數據來進行拼接。至少需要兩個連續合約。")
        return None

    # 按年份和月份排序合約 (雖然生成時應該有序，但保險起見)
    contracts_to_fetch_info.sort(key=lambda x: (x['year'], x['month']))

    # 開始向後拼接和調整
    # 從 `contracts_to_fetch_info` 的倒數第二個合約開始向前處理
    # `adjusted_futures_data` 將是最終結果，從最新的數據開始建立
    
    # 最新合約的數據 (即列表中的最後一個，如果它在 end_year, end_month 範圍內)
    # 或者說，我們處理到 end_year, end_month 所對應的那個合約為止
    
    combined_data = pd.DataFrame()
    
    # 我們從 (end_year, end_month) 的合約開始，用其 *下一個月* 的合約數據來計算第一個價差
    # 然後逐步往前調整
    
    # `contracts_to_fetch_info` 已包含到 final_fetch_y, final_fetch_m (即 end_month 的下個月)
    
    # 確保我們有 end_month 的合約以及其後一個月的合約
    idx_end_contract = -1
    idx_next_contract = -1

    for i, contract_info in enumerate(contracts_to_fetch_info):
        if contract_info['year'] == end_year and contract_info['month'] == end_month:
            idx_end_contract = i
        if contract_info['year'] == final_fetch_y and contract_info['month'] == final_fetch_m:
            idx_next_contract = i # 這其實就是列表的最後一個
            break # 找到了 end_month 的下一個月合約

    if idx_end_contract == -1:
        logger.error(f"未能找到 {end_year}-{end_month:02d} 的合約數據。")
        # 可能的情況是 end_year, end_month 就是列表最後一個，沒有 "下一個月" 的數據
        if contracts_to_fetch_info[-1]['year'] == end_year and \
           contracts_to_fetch_info[-1]['month'] == end_month:
            # 如果目標結束月就是獲取到的最後一個合約，那麼我們無法計算其價差來調整它之前的合約
            # 這種情況下，我們只能提供這個合約本身的數據，或者提示需要更遠期的數據
            logger.warning(f"目標結束月份 {end_year}-{end_month:02d} 是獲取到的最新合約，"
                           f"無法進行向後調整。將僅使用此合約到其最後交易日的數據。")
            last_contract_info = contracts_to_fetch_info[-1]
            ltd = last_contract_info['last_trading_day']
            # 只取到最後交易日 (包含當天)
            data_to_use = last_contract_info['data'][last_contract_info['data'].index <= ltd].copy()
            # 並且數據的起始日期不能早於 start_year, start_month 的第一個交易日
            # (這部分邏輯需要在外層拼接時處理)
            if not data_to_use.empty:
                 # 過濾掉不在請求範圍 (start_year, start_month 到 end_year, end_month) 內的數據
                min_date_allowed = pd.Timestamp(start_year, start_month, 1)
                max_date_allowed = get_taifex_futures_last_trading_day(end_year, end_month) # pd.Timestamp(end_year, end_month, 1) + pd.offsets.MonthEnd(0)

                data_to_use = data_to_use[(data_to_use.index >= min_date_allowed) & (data_to_use.index <= max_date_allowed)]
                return data_to_use[['Open', 'High', 'Low', 'Close', 'Volume']] if not data_to_use.empty else None
            return None
        else: # 其他情況，說明連目標結束月份的數據都沒取到
            logger.error(f"無法獲取 {end_year}-{end_month:02d} 的合約數據進行拼接。")
            return None

    # 如果有 end_month 的下一個月合約 (idx_next_contract 有效且 > idx_end_contract)
    # 或者 idx_next_contract 就是 idx_end_contract + 1
    if not (idx_next_contract > idx_end_contract and idx_next_contract == idx_end_contract + 1):
         # 如果 end_month 剛好是列表最後一個，那麼 idx_next_contract 會是 -1 或其他值
         # 這種情況已在上面處理。這裡處理的是中間有缺失合約的情況。
         if contracts_to_fetch_info[idx_end_contract+1]['year'] == final_fetch_y and \
            contracts_to_fetch_info[idx_end_contract+1]['month'] == final_fetch_m:
            pass # 正常情況
         else:
            logger.error(f"缺少 {end_year}-{end_month:02d} 之後的連續合約數據，無法進行完整的向後調整。")
            # 可以考慮只處理到有數據的部分，或者返回 None
            # 為簡單起見，這裡返回 None，或提示只處理到某個月份
            # 此處選擇嘗試處理到 end_contract (不調整)
            logger.warning(f"將嘗試僅使用到 {end_year}-{end_month:02d} 合約的數據，不進行調整。")
            end_contract_info = contracts_to_fetch_info[idx_end_contract]
            ltd_end = end_contract_info['last_trading_day']
            data_to_use = end_contract_info['data'][end_contract_info['data'].index <= ltd_end].copy()
            min_date_allowed = pd.Timestamp(start_year, start_month, 1)
            max_date_allowed = get_taifex_futures_last_trading_day(end_year, end_month)
            data_to_use = data_to_use[(data_to_use.index >= min_date_allowed) & (data_to_use.index <= max_date_allowed)]
            return data_to_use[['Open', 'High', 'Low', 'Close', 'Volume']] if not data_to_use.empty else None


    # `current_adjusted_data` 從最新的合約 (即 end_month 合約) 開始
    # 但它的價格需要被它之後的合約 (next_contract) 調整
    
    # 從 `idx_end_contract` 開始，向前迭代到 `contracts_to_fetch_info` 的開頭
    # `next_contract_data` 是 "新" 合約，`current_contract_data` 是 "舊" 合約
    
    # 初始化：最後一段數據來自 end_month 合約，但只取到其最後交易日
    # 這段數據的價格將被其後一個月 (next_contract) 的價格調整
    
    # `running_adjustment` 初始為 0
    adjustment_factor = 0.0
    
    # 從 `idx_end_contract` 向前迭代到列表開頭 (或到 start_year, start_month)
    # `i` 代表當前要處理的合約 (舊合約)
    # `i+1` 代表下一個合約 (新合約)
    
    # 迭代 contracts_to_fetch_info 直到 start_year, start_month
    # `i` 從 `idx_end_contract` (代表 end_year, end_month 的合約) 開始遞減
    for i in range(idx_end_contract, -1, -1):
        current_contract_info = contracts_to_fetch_info[i]
        current_contract_data = current_contract_info['data'].copy()
        current_ltd = current_contract_info['last_trading_day'] # 這是 current_contract 的最後交易日

        # 檢查此合約是否早於請求的 start_year, start_month，如果是則停止
        if current_contract_info['year'] < start_year or \
           (current_contract_info['year'] == start_year and current_contract_info['month'] < start_month):
            break

        # 調整價格 (Open, High, Low, Close)
        price_cols = ['Open', 'High', 'Low', 'Close']
        for col in price_cols:
            if col in current_contract_data.columns:
                current_contract_data[col] = current_contract_data[col] + adjustment_factor
        
        # 確定此合約的數據段
        # 對於 current_contract，我們取從上一個合約換月日之後 到 current_contract 換月日 (含) 的數據
        
        # `segment_start_date` 是上一個合約的最後交易日之後一天
        # `segment_end_date` 是當前合約的最後交易日 (current_ltd)
        
        if i > 0: # 如果不是最早的合約
            prev_contract_ltd = contracts_to_fetch_info[i-1]['last_trading_day']
            segment_start_date = prev_contract_ltd + timedelta(days=1)
        else: # 如果是列表中的第一個合約 (即我們能獲取到的最早的合約)
              # 它的起始日期應該是該合約數據的第一天，但不能早於請求的 start_year, start_month 的第一天
            segment_start_date = max(current_contract_data.index.min(), 
                                     pd.Timestamp(start_year, start_month, 1))

        segment_end_date = current_ltd
        
        # 特殊處理：對於請求的 start_year, start_month 合約，其 segment_start_date 不能早於該月第一天
        if current_contract_info['year'] == start_year and current_contract_info['month'] == start_month:
            segment_start_date = max(segment_start_date, pd.Timestamp(start_year, start_month, 1))

        # 特殊處理：對於請求的 end_year, end_month 合約，其 segment_end_date 就是 current_ltd
        # 但如果 current_ltd 晚於請求的 end_date (例如請求只到月中)，則應以請求的 end_date 為準
        # 但我們的請求是以月份為單位，所以 end_month 的最後交易日是合理的結束點
        if current_contract_info['year'] == end_year and current_contract_info['month'] == end_month:
             # end_month 的數據只取到其最後交易日
             pass # segment_end_date 已設為 current_ltd

        # 截取數據段
        segment = current_contract_data[
            (current_contract_data.index >= segment_start_date) &
            (current_contract_data.index <= segment_end_date)
        ].copy()
        
        # 如果這是請求範圍內的第一個合約 (start_year, start_month)，則其起始日期不能早於該月第一天
        if current_contract_info['year'] == start_year and current_contract_info['month'] == start_month:
            min_req_date = pd.Timestamp(start_year, start_month, 1)
            segment = segment[segment.index >= min_req_date]

        if not segment.empty:
            all_contracts_data.append(segment)
        
        # 計算下一個 adjustment_factor (為下一次迭代，即更早的合約做準備)
        # 需要用到 current_contract (現在是舊合約) 和它之前一個 (prev_contract, 如果存在)
        # 不對，是 current_contract (作為舊) 和 next_contract_info (作為新)
        # `adjustment_factor` 是 `new_close - old_close` on `old_ltd`
        
        # `next_contract_info` 應該是 `contracts_to_fetch_info[i+1]`
        # 我們是在 `current_ltd` (舊合約的最後交易日) 進行換月
        # 新合約是 `contracts_to_fetch_info[i+1]`
        # 舊合約是 `contracts_to_fetch_info[i]` (即 `current_contract_info`)
        
        if i < idx_end_contract + 1 : # 確保 i+1 不會超出我們擁有的 "未來" 合約範圍
                                     # idx_end_contract + 1 是 end_month 的下一個月合約索引
            if (i + 1) < len(contracts_to_fetch_info): # 檢查 i+1 是否在列表範圍內
                next_contract_info_for_adj = contracts_to_fetch_info[i+1]
                next_contract_data_for_adj = next_contract_info_for_adj['data'] # 未調整的原始數據
                
                # 在 current_ltd (舊合約的最後交易日) 找到兩個合約的收盤價
                if current_ltd in current_contract_data.index and \
                   current_ltd in next_contract_data_for_adj.index:
                    
                    old_close_on_ltd = current_contract_data.loc[current_ltd, 'Close'] # 已調整過的
                    new_close_on_ltd = next_contract_data_for_adj.loc[current_ltd, 'Close'] # 原始的
                    
                    # 注意：這裡的 old_close_on_ltd 已經被之前的 adjustment_factor 調整過了。
                    # 我們需要的是原始的 old_close。
                    # 或者，更簡單：adjustment_factor 應該是累加的。
                    # 新的 adjustment = (舊的 adjustment) + (新合約在換月日的價格 - 舊合約在換月日的價格)
                    # 這裡的 "舊合約在換月日的價格" 應該是原始價格。
                    
                    # 重新思考：
                    # adjustment_factor 是從最新的價差開始，向前累加。
                    # 第一次 (i = idx_end_contract):
                    #   next_contract = contracts_to_fetch_info[idx_end_contract + 1] (end_month 的下個月)
                    #   current_contract = contracts_to_fetch_info[idx_end_contract] (end_month)
                    #   rollover_date = current_contract_ltd
                    #   diff = next_contract.Close[rollover_date] - current_contract.OriginalClose[rollover_date]
                    #   adjustment_factor = diff
                    #   然後 current_contract 的價格被調整 (price + adjustment_factor)
                    #   然後這個調整後的 current_contract 的數據段被加入總數據
                    #
                    # 第二次 (i = idx_end_contract - 1):
                    #   prev_adjustment_factor = adjustment_factor (來自上一步)
                    #   next_contract = contracts_to_fetch_info[idx_end_contract] (現在是 "新" 合約)
                    #   current_contract = contracts_to_fetch_info[idx_end_contract - 1] (現在是 "舊" 合約)
                    #   rollover_date = current_contract_ltd
                    #   diff = next_contract.OriginalClose[rollover_date] - current_contract.OriginalClose[rollover_date]
                    #   adjustment_factor = prev_adjustment_factor + diff
                    #   然後 current_contract 的價格被調整 (price + adjustment_factor)
                    
                    # 所以，在循環開始前，adjustment_factor 應為 0.
                    # 在每次迭代 *結束* 時，為 *下一次* (更早的合約) 計算新的 adjustment_factor。
                    
                    # 獲取 current_contract 的原始收盤價
                    original_current_close_on_ltd = contracts_to_fetch_info[i]['data'].loc[current_ltd, 'Close']
                    
                    adjustment_factor += (new_close_on_ltd - original_current_close_on_ltd)
                    logger.debug(f"合約 {contracts_to_fetch_info[i]['symbol']} 在 {current_ltd.strftime('%Y-%m-%d')} 換月 (與 {next_contract_info_for_adj['symbol']}):\n"
                                 f"  舊合約原始收盤: {original_current_close_on_ltd:.2f}\n"
                                 f"  新合約原始收盤: {new_close_on_ltd:.2f}\n"
                                 f"  本次價差: {(new_close_on_ltd - original_current_close_on_ltd):.2f}\n"
                                 f"  累計調整因子更新為: {adjustment_factor:.2f}")
                else:
                    logger.warning(f"在 {current_ltd.strftime('%Y-%m-%d')} 換月時，"
                                   f"{contracts_to_fetch_info[i]['symbol']} 或 {next_contract_info_for_adj['symbol']} 缺少數據，"
                                   f"無法計算價差。調整因子將保持不變: {adjustment_factor:.2f}")
            else:
                # 這是最早的合約了，沒有更 "新" 的合約來計算價差給它之前的合約 (也不需要了)
                pass


    if not all_contracts_data:
        logger.error("未能生成任何連續期貨數據。")
        return None

    # 合併所有數據段 (它們是從新到舊的順序加入 list 的)
    final_data = pd.concat(reversed(all_contracts_data)) # 反轉列表使最早的在前，然後合併
    final_data.sort_index(inplace=True) # 確保索引嚴格遞增

    # 再次過濾確保只包含請求的 start_year, start_month 到 end_year, end_month 的數據
    min_date_final = pd.Timestamp(start_year, start_month, 1)
    # 最後日期是 end_month 的最後交易日
    max_date_final = get_taifex_futures_last_trading_day(end_year, end_month) 
    
    final_data = final_data[
        (final_data.index >= min_date_final) &
        (final_data.index <= max_date_final)
    ]

    # 移除重複的索引 (理論上不應該有，除非換月日處理有微小問題)
    final_data = final_data[~final_data.index.duplicated(keep='first')]

    if final_data.empty:
        logger.error(f"最終拼接的台指期貨數據為空，範圍 {start_year}-{start_month:02d} 至 {end_year}-{end_month:02d}。")
        return None
        
    logger.info(f"成功生成台指期貨連續合約數據，共 {len(final_data)} 筆。")
    return final_data[['Open', 'High', 'Low', 'Close', 'Volume']]


"""
一些關於 yfinance 和數據獲取的注意事項：
1.  **小時線數據限制**：Yahoo Finance 通常只提供最近約 730 天的小時線 (interval='1h') 數據。
    如果請求的時間範圍超出此限制，`ticker.history()` 可能會返回空的 DataFrame 或引發錯誤。
    程式碼中已加入對此的初步處理，如果請求時間過長，會警告並優先嘗試日線。
2.  **數據延遲**：免費的 Yahoo Finance 數據可能有延遲，不同市場和商品的延遲情況不同。
3.  **股票代號 (Ticker Symbols)**：
    *   美股指數：^GSPC (S&P 500), ^DJI (Dow Jones), ^IXIC (NASDAQ Composite)
    *   台股指數：^TWII (台灣加權指數)
    *   外匯：TWD=X (新台幣兌美元), EURUSD=X (歐元兌美元)
    *   期貨：需要特定月份的合約代號，例如 CL=F (原油期貨近月)，但 yfinance 對期貨的支援可能不如此穩定。
              台指期 TXF 通常需要更特定的代號，例如 TXF202407.TW 或 TXF07.TW (寫法可能隨時間改變)。
              這部分會在後續台指期貨處理步驟中詳細研究。
4.  **錯誤處理的複雜性**：`yfinance` 不是官方 API，其行為可能隨 Yahoo Finance 網站的變動而改變。
    錯誤類型多樣，有時甚至不拋出標準 Python 異常，而是直接打印到 stderr。
    因此，錯誤處理需要比較有彈性，並依賴日誌來追蹤問題。
5.  **速率限制**：即使有快取和延遲重試，過於頻繁或大量的請求仍可能觸發 Yahoo Finance 的速率限制。
    在大型專案中，如果需要大量、高頻的數據，考慮使用付費的專業數據提供商是更穩健的選擇。
6.  **`yf.set_session(SESSION)`**: 這是讓 `yfinance` 全局使用我們配置了 `requests-cache` 的 `requests.Session` 對象的關鍵。
    這樣 `yfinance` 內部發出的所有 HTTP 請求都會經過快取層。
7.  **台指期貨連續合約的複雜性**:
    *   `yfinance` 本身不直接提供經過學術標準方法調整的連續期貨數據。
    *   自行拼接和調整（如向後調整法）是必要的，但過程複雜，涉及正確識別換月日、處理價差、避免前後看偏差等。
    *   本模組中的 `fetch_continuous_taifex_futures` 嘗試實現這一點，但仍需仔細測試和驗證其準確性。
    *   特別是換月日的價差計算，以及數據段的選取，需要非常小心。
    *   成交量數據通常是直接拼接，不進行調整。未平倉量 (Open Interest) 的處理則更為複雜，此處未包含。
"""

# --- 台指選擇權 (TXO) 相關功能 ---

def check_taifex_options_support_yfinance(underlying_symbol: str = "^TWII") -> None:
    """
    檢查並記錄 yfinance 對於台指選擇權 (或指定標的物的選擇權) 的數據支持情況。
    主要目的是驗證是否能獲取歷史選擇權 OCHLV 或希臘字母。
    """
    logger.info(f"開始檢查 yfinance 對於標的 {underlying_symbol} 的選擇權數據支持情況...")
    
    ticker = yf.Ticker(underlying_symbol)
    
    try:
        # 1. 獲取可用的選擇權到期日
        expirations = ticker.options
        if not expirations:
            logger.warning(f"yfinance 未能找到標的 {underlying_symbol} 的任何選擇權到期日。")
            logger.warning("這可能表示 yfinance 不直接支持該標的物的選擇權，或者當前市場無可用選擇權。")
            logger.warning("對於台指選擇權 (TXO)，yfinance 的支持非常有限，通常無法獲取歷史數據或希臘字母。")
            logger.warning("建議從 TAIFEX 官網或專業數據提供商獲取詳細的台指選擇權歷史數據。")
            return

        logger.info(f"找到標的 {underlying_symbol} 的可用選擇權到期日: {expirations}")
        
        # 2. 嘗試獲取第一個到期日的選擇權鏈 (通常是當前或近期的快照)
        # 選擇一個近期的到期日，避免選擇太遠的可能數據不完整
        target_expiration = expirations[0]
        logger.info(f"嘗試獲取到期日為 {target_expiration} 的選擇權鏈...")
        
        opt_chain = ticker.option_chain(target_expiration)
        
        if opt_chain.calls.empty and opt_chain.puts.empty:
            logger.warning(f"對於到期日 {target_expiration}，yfinance 返回了空的買權和賣權數據。")
        else:
            logger.info(f"成功獲取到期日 {target_expiration} 的選擇權鏈數據。")
            if not opt_chain.calls.empty:
                logger.info(f"買權 (Calls) 數據欄位: {opt_chain.calls.columns.tolist()}")
                # print(f"部分買權數據:\n{opt_chain.calls.head()}")
            if not opt_chain.puts.empty:
                logger.info(f"賣權 (Puts) 數據欄位: {opt_chain.puts.columns.tolist()}")
                # print(f"部分賣權數據:\n{opt_chain.puts.head()}")

        # 檢查欄位是否包含 OCHLV 或希臘字母
        # yfinance 返回的選擇權鏈通常包含:
        # contractSymbol, lastTradeDate, strike, lastPrice, bid, ask, change, percentChange, 
        # volume, openInterest, impliedVolatility, inTheMoney, contractSize, currency
        # 歷史 OCHLV (每日的開高低收) 和 希臘字母 (delta, gamma, theta, vega, rho) 通常不包含在內。
        
        required_cols_ochlv = ['open', 'high', 'low', 'close'] # 這些通常不會是 yf.option_chain 的欄位
        greeks_cols = ['delta', 'gamma', 'theta', 'vega', 'rho'] # 這些通常不會是 yf.option_chain 的欄位

        has_ochlv = False
        if not opt_chain.calls.empty:
            call_cols = [col.lower() for col in opt_chain.calls.columns]
            if all(col in call_cols for col in required_cols_ochlv):
                has_ochlv = True
        elif not opt_chain.puts.empty: # 如果calls為空，檢查puts
            put_cols = [col.lower() for col in opt_chain.puts.columns]
            if all(col in put_cols for col in required_cols_ochlv):
                has_ochlv = True
        
        if has_ochlv:
             logger.info("選擇權鏈數據中似乎包含 OCHLV 欄位 (這比較罕見於 yfinance 的 option_chain)。")
        else:
             logger.warning("yfinance 返回的選擇權鏈數據通常不包含歷史每日 OCHLV (開高低收) 數據。")
             logger.warning("它主要提供的是選擇權的當前市場快照 (最後價格, 買賣價, 成交量, 未平倉量, 隱含波動率)。")

        has_greeks = False
        if not opt_chain.calls.empty:
            call_cols = [col.lower() for col in opt_chain.calls.columns]
            if any(greek in call_cols for greek in greeks_cols):
                has_greeks = True
        elif not opt_chain.puts.empty:
            put_cols = [col.lower() for col in opt_chain.puts.columns]
            if any(greek in put_cols for greek in greeks_cols):
                has_greeks = True
        
        if has_greeks:
            logger.info("選擇權鏈數據中似乎包含部分希臘字母欄位 (這也比較罕見於 yfinance 的 option_chain)。")
        else:
            logger.warning("yfinance 返回的選擇權鏈數據通常不包含計算好的希臘字母 (Delta, Gamma, Theta, Vega)。")

        # 3. 嘗試獲取一個已過去的日期的選擇權鏈 (這通常會失敗或返回空)
        # 選擇一個幾天前的日期，假設市場有開市
        # 注意：yfinance 的 .option_chain(date=YYYY-MM-DD) 是獲取在 YYYY-MM-DD 那天 *可交易的* 選擇權鏈
        # 而不是獲取某個特定選擇權合約在 YYYY-MM-DD 的歷史價格。
        
        # 這一步驟在 yfinance 中意義不大，因為 option_chain 本身不是設計來取歷史 OCHLV 的。
        # 它獲取的是某個特定到期日 (expirations) 的所有選擇權合約在 *當前* (或指定 date) 的市場狀態。
        # 要獲取 TXO202408W3 17000 Call 在 2023-07-15 的價格，yfinance 通常做不到。
        
        logger.info("-" * 50)
        logger.warning("結論：yfinance 主要用於獲取股票、ETF 等的歷史價格數據，以及選擇權的當前市場快照 (選擇權鏈)。")
        logger.warning("對於台指選擇權 (TXO) 或其他選擇權的【歷史每日 OCHLV 數據】或【歷史每日希臘字母】，")
        logger.warning("yfinance 通常無法提供。這些數據通常需要從交易所官方 (如 TAIFEX 網站下載每日結算數據) 或專業的金融數據提供商獲取。")
        logger.warning("TAIFEX 網站通常會提供每日選擇權的結算價、成交量等，但可能不直接提供盤中 OCHL 或計算好的希臘字母。")
        logger.info("因此，任何需要詳細選擇權歷史數據進行 AI 研究的任務，都應尋求 yfinance 以外的數據源。")
        logger.info("-" * 50)

    except Exception as e:
        logger.error(f"在檢查 {underlying_symbol} 選擇權支持時發生錯誤: {e}")
        logger.warning("這進一步表明 yfinance 對於此標的選擇權的直接支持可能有限或存在問題。")
        logger.warning("強烈建議尋求 TAIFEX 官方數據或專業數據商獲取台指選擇權歷史數據。")

if __name__ == "__main__":
    # ... (原有的 __main__ 內容) ...

    # 新增 TXO 檢查到測試案例中
    logger.info("\n--- 測試台指選擇權 (TXO) 支持情況 ---")
    # 台指選擇權的標的物是台灣加權指數 (^TWII)
    # 或者，有時可能是用期貨作為標的，但 yfinance 更可能連結到指數
    check_taifex_options_support_yfinance(underlying_symbol="^TWII") 
    
    # 也可以嘗試用一個美股代號看看差異 (例如 AAPL)
    # logger.info("\n--- 測試 AAPL 選擇權支持情況 (對比) ---")
    # check_taifex_options_support_yfinance(underlying_symbol="AAPL")
    
    logger.info("\n--- yfinance 數據獲取測試完成 (包含期貨和選擇權檢查) ---")
