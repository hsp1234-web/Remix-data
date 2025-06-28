import requests
import pandas as pd
from io import StringIO
import time
import random
import logging
import os
from datetime import datetime, timedelta

# --- 配置區域 ---
# requests_cache 會被 market_data_yfinance.py 中的 SESSION 全局設定影響
# 但如果此模組獨立運行或 yfinance 的 SESSION 未被導入，則這裡的請求不會自動快取。
# 為了此模組的獨立性和明確性，我們可以為 FRED 請求建立一個獨立的快取會話。
import requests_cache
FRED_CACHE_DIR = "CACHE_Market_Data"
FRED_CACHE_NAME = os.path.join(FRED_CACHE_DIR, "fred_data_cache")
os.makedirs(FRED_CACHE_DIR, exist_ok=True)

# 為 FRED 請求建立獨立的快取會話
# 注意：如果 market_data_yfinance.py 中的 SESSION 已經被 yf.set_session() 設定，
# 且此處直接使用 requests.get()，它可能會也可能不會使用那個全局 session，取決於 yfinance 內部實現。
# 為了清晰和隔離，這裡我們明確使用一個新的 CachedSession。
fred_session = requests_cache.CachedSession(
    FRED_CACHE_NAME,
    backend='sqlite',
    expire_after=timedelta(days=1),  # FRED數據通常每日更新，快取一天是合理的
    allowable_codes=[200, 400, 404], # 也快取客戶端錯誤，避免重複請求無效序列
    stale_if_error=True,
)

# API 呼叫穩定性設定 (與 yfinance 類似，但可能需要根據 FRED 的特性調整)
MAX_RETRIES_FRED = 3
INITIAL_DELAY_SECONDS_FRED = 1
MAX_DELAY_SECONDS_FRED = 30
JITTER_FACTOR_FRED = 0.5

# 日誌設定 (與 yfinance 模組共用相同的 logger 設定方式，如果它們在同一個應用程式中運行)
# 如果獨立運行，需要重新配置 logging
log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format) # 確保基礎配置被呼叫
logger = logging.getLogger(__name__) # 為此模組獲取 logger

# FRED 基本 URL
FRED_DOWNLOAD_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# --- 核心功能 ---

def fetch_fred_series_no_key(series_id: str, 
                             start_date: str, # YYYY-MM-DD
                             end_date: str    # YYYY-MM-DD
                            ) -> pd.DataFrame | None:
    """
    透過直接構造下載 URL 的方式，從 FRED 獲取指定經濟數據序列 (無須 API 金鑰)。

    Args:
        series_id (str): FRED 數據序列的 ID (例如 "DGS10", "VIXCLS", "FEDFUNDS").
        start_date (str): 開始日期 (YYYY-MM-DD).
        end_date (str): 結束日期 (YYYY-MM-DD).

    Returns:
        pd.DataFrame | None: 包含日期和數值的 DataFrame，索引為日期，欄位名為 series_id。
                             若無法獲取則回傳 None。
    """
    params = {
        "series_id": series_id,
        "cosd": start_date,
        "coed": end_date,
        # "fq": "Daily", # 可以嘗試指定頻率，但通常 series_id 已隱含頻率
        # "transform": "lin", # 預設是 Level (lin)
        # "vintage_date": end_date, # 通常不需要指定 vintage dates
    }
    current_delay = INITIAL_DELAY_SECONDS_FRED

    logger.info(f"開始嘗試獲取 FRED 數據序列: {series_id}，日期範圍：{start_date} 至 {end_date}")

    for attempt in range(MAX_RETRIES_FRED):
        try:
            logger.debug(f"{series_id} - 第 {attempt + 1}/{MAX_RETRIES_FRED} 次嘗試。")
            # 使用我們為 FRED 配置的 fred_session
            response = fred_session.get(FRED_DOWNLOAD_URL, params=params, timeout=20) # 設定超時

            # 檢查 HTTP 狀態碼
            if response.status_code == 200:
                csv_data = response.text
                # FRED CSV 的第一行是標題，第二行是欄位名，之後是數據
                # 有時數據為空或只有標頭，例如 "Series is not available."
                if "Series is not available." in csv_data or "No data available" in csv_data:
                    logger.warning(f"{series_id}: FRED 回應數據不可用或無數據。")
                    return None

                df = pd.read_csv(StringIO(csv_data))
                
                if df.empty:
                    logger.warning(f"{series_id}: FRED 回應的 CSV 數據為空。")
                    return None
                
                # FRED CSV 通常有兩欄：'DATE' 和序列ID本身 (例如 'DGS10')
                # 有時序列ID欄位可能包含非數值 (例如 '.') 表示缺失，需妥善處理
                if series_id not in df.columns and len(df.columns) == 2:
                    # 如果第二欄的名稱不是 series_id，但只有兩欄，我們假設它是我們要的數據
                    df.rename(columns={df.columns[1]: series_id}, inplace=True)

                if 'DATE' not in df.columns or series_id not in df.columns:
                    logger.error(f"{series_id}: FRED CSV 格式不符合預期。欄位: {df.columns.tolist()}")
                    return None

                df['DATE'] = pd.to_datetime(df['DATE'])
                df.set_index('DATE', inplace=True)
                
                # 將數據欄位轉換為數值，無法轉換的設為 NaN
                df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
                
                # 移除所有值都是 NaT/NaN 的列 (通常是轉換錯誤或原始數據就是 '.')
                df.dropna(subset=[series_id], how='all', inplace=True)

                if df.empty:
                    logger.warning(f"{series_id}: 處理後數據為空 (可能所有值都是非數值)。")
                    return None

                logger.info(f"成功獲取並解析 FRED 數據序列: {series_id}，共 {len(df)} 筆。")
                return df[[series_id]] # 只回傳包含該序列ID數據的欄位

            elif response.status_code == 400: # Bad Request，通常是 series_id 有問題或日期格式不對
                logger.error(f"{series_id}: FRED 請求錯誤 (400 - Bad Request)。"
                               f"請檢查 series_id 和日期格式。回應: {response.text[:200]}")
                return None # 通常這種錯誤重試無效
            elif response.status_code == 404: # Not Found
                logger.error(f"{series_id}: FRED 找不到請求的資源 (404 - Not Found)。"
                               f"回應: {response.text[:200]}")
                return None
            else:
                logger.warning(f"{series_id}: FRED 請求失敗，狀態碼: {response.status_code}。"
                               f"回應: {response.text[:200]}")

        except requests.exceptions.Timeout:
            logger.warning(f"{series_id}: 第 {attempt + 1}/{MAX_RETRIES_FRED} 次嘗試 - FRED 請求超時。")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"{series_id}: 第 {attempt + 1}/{MAX_RETRIES_FRED} 次嘗試 - FRED 連線錯誤: {e}")
        except pd.errors.EmptyDataError:
            logger.warning(f"{series_id}: FRED 回應的 CSV 內容為空或無法解析為 DataFrame。")
        except Exception as e:
            error_type = type(e).__name__
            logger.error(f"{series_id}: 第 {attempt + 1}/{MAX_RETRIES_FRED} 次嘗試 - "
                           f"處理 FRED 數據時發生未預期錯誤 ({error_type}): {e}")

        if attempt < MAX_RETRIES_FRED - 1:
            sleep_time = current_delay + random.uniform(0, JITTER_FACTOR_FRED * current_delay)
            logger.info(f"{series_id}: {sleep_time:.2f} 秒後重試...")
            time.sleep(sleep_time)
            current_delay = min(current_delay * 2, MAX_DELAY_SECONDS_FRED)
        else:
            logger.error(f"{series_id}: 經過 {MAX_RETRIES_FRED} 次嘗試後，仍無法獲取 FRED 數據。")
            return None
            
    return None

# --- 測試範例 ---
if __name__ == "__main__":
    logger.info("開始 FRED (無金鑰) 數據獲取測試...")

    # 清理舊快取 (僅為測試)
    # cache_file_path = FRED_CACHE_NAME + ".sqlite"
    # if os.path.exists(cache_file_path):
    #     logger.info(f"正在刪除舊 FRED 快取檔案: {cache_file_path}")
    #     os.remove(cache_file_path)
        
    today = datetime.now()
    start_date_default = (today - timedelta(days=5*365)).strftime("%Y-%m-%d") # 預設獲取近5年數據
    end_date_default = today.strftime("%Y-%m-%d")

    series_to_test = {
        "10年期美國公債殖利率": "DGS10",
        "VIX 收盤價 (FRED)": "VIXCLS", # CBOE Volatility Index: VIX Close
        "聯邦基金有效利率": "FEDFUNDS",
        "一個可能不存在的序列": "NONEXISTENTSERIESXYZ",
        "短期測試 (CPI)": "CPIAUCSL" # 消費者物價指數 (月數據)
    }

    all_fred_data = {}

    for name, series_id_val in series_to_test.items():
        logger.info(f"\n--- 測試獲取 FRED: {name} ({series_id_val}) ---")
        
        # CPI 是月數據，獲取太短的時間範圍可能沒數據
        current_start = start_date_default
        current_end = end_date_default
        if series_id_val == "CPIAUCSL":
             current_start = (today - timedelta(days=10*365)).strftime("%Y-%m-%d") # 獲取10年CPI

        data = fetch_fred_series_no_key(series_id_val, current_start, current_end)
        
        if data is not None and not data.empty:
            logger.info(f"成功獲取 FRED {name} ({series_id_val}) 的數據，前5筆：")
            print(data.head())
            logger.info(f"尾5筆：")
            print(data.tail())
            logger.info(f"數據維度: {data.shape}")
            all_fred_data[series_id_val] = data
        else:
            logger.warning(f"未能獲取 FRED {name} ({series_id_val}) 的數據。")
        
        time.sleep(random.uniform(1.0, 2.0)) # 尊重 FRED 伺服器

    logger.info("\n--- FRED (無金鑰) 數據獲取測試完成 ---")
    
    if "DGS10" in all_fred_data:
        logger.info("\n--- 測試 FRED 快取：再次獲取 DGS10 數據 ---")
        dgs10_start = (today - timedelta(days=30)).strftime("%Y-%m-%d") # 較短時間
        dgs10_data_cached = fetch_fred_series_no_key("DGS10", dgs10_start, end_date_default)
        if dgs10_data_cached is not None:
            logger.info("第二次獲取 DGS10 數據成功 (應主要來自快取或快速獲取)。")
            print(dgs10_data_cached.head())

    logger.info(f"FRED 快取檔案位於: {FRED_CACHE_NAME}.sqlite")
    logger.info("請檢查日誌輸出以了解詳細的獲取過程和錯誤處理情況。")

"""
FRED 無金鑰數據獲取注意事項：
1.  **數據頻率**：不同的 FRED 序列有不同的發布頻率（日、週、月、季、年）。
    直接下載 CSV 時，通常會獲取該序列的原始頻率。
    如果請求的日期範圍內沒有該頻率的數據點，可能會返回空或部分數據。
    例如，月度數據 FEDFUNDS，如果 start_date 和 end_date 在同一個月中，可能只會得到一個點或沒有。
2.  **數據值中的 '.'**：FRED CSV 中，缺失值或未發布的值常用 '.' 表示。
    `pd.to_numeric(errors='coerce')` 會將這些轉換為 `NaN`，這通常是期望的行為。
3.  **URL 穩定性**：雖然這種直接下載 CSV 的 URL 模式目前有效，但它並非官方支持的 API 端點。
    FRED 網站未來可能會更改 URL 結構或下載機制，導致此方法失效。
    這是在沒有 API 金鑰情況下的一種權宜之計。
4.  **速率限制**：即使是直接下載，過於頻繁的請求也可能被 FRED 伺服器限制。
    適當的延遲和快取非常重要。
5.  **錯誤訊息**：當序列不存在或日期範圍無效時，FRED 返回的 CSV 內容可能只是簡單的錯誤文字，
    例如 "Series is not available."。程式碼中已加入對此的檢查。
6.  **快取隔離**：為 FRED 數據建立了一個獨立的 `requests_cache.CachedSession` (`fred_session`)，
    使其快取行為與 `yfinance` 的快取分離，這樣更清晰且易於管理各自的快取策略。
"""
