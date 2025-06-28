import os
import time
import random
import logging
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 從專案模組導入 (如果需要，例如已有的數據獲取函式或快取配置)
# import market_data_yfinance
# import market_data_fred

# --- 環境與日誌設定 ---
load_dotenv() # 從 .env 檔案載入環境變數

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - [%(funcName)s] - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# --- API 金鑰獲取 ---
# 建議將金鑰名稱與 .env 檔案中使用的名稱保持一致
FRED_API_KEY = os.getenv("FRED_API_KEY")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY") # 或者 FMP_API_KEY_1, FMP_API_KEY_2
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 檢查金鑰是否成功載入 (可選，用於調試)
logger.info(f"FRED_API_KEY loaded: {'Yes' if FRED_API_KEY else 'No'}")
logger.info(f"ALPHA_VANTAGE_API_KEY loaded: {'Yes' if ALPHA_VANTAGE_API_KEY else 'No'}")
logger.info(f"FINNHUB_API_KEY loaded: {'Yes' if FINNHUB_API_KEY else 'No'}")
logger.info(f"NEWS_API_KEY loaded: {'Yes' if NEWS_API_KEY else 'No'}")
logger.info(f"FMP_API_KEY loaded: {'Yes' if FMP_API_KEY else 'No'}")
logger.info(f"POLYGON_API_KEY loaded: {'Yes' if POLYGON_API_KEY else 'No'}")


# --- 通用輔助函式 ---
def respectful_sleep(min_seconds=1, max_seconds=3):
    """在請求之間加入隨機延遲，以尊重 API 服務器。"""
    time.sleep(random.uniform(min_seconds, max_seconds))

# --- 各 API 測試函式框架 ---

def test_fred_api():
    logger.info("--- 開始測試 FRED API ---")
    if not FRED_API_KEY:
        logger.warning("FRED_API_KEY 未設定，跳過 FRED API 測試。")
        return
    logger.info("--- 開始測試 FRED API ---")
    if not FRED_API_KEY:
        logger.warning("FRED_API_KEY 未設定，跳過 FRED API 測試。")
        return
    
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
        # 測試獲取一個常見的、更新頻繁的序列，例如 'UNRATE' (失業率)
        # 只獲取最近的幾個值以減少數據量
        data = fred.get_series('UNRATE', observation_start='2023-01-01') 
        if data is not None and not data.empty:
            logger.info(f"成功從 FRED 獲取 'UNRATE' 序列。最新值 ({data.index[-1].strftime('%Y-%m-%d')}): {data.iloc[-1]}")
            logger.info(f"FRED API 連通性測試成功。")
        elif data is not None and data.empty:
            logger.warning("FRED API 連通性測試成功，但 'UNRATE' 序列在指定日期後無數據。")
        else:
            logger.error("FRED API 連通性測試失敗：無法獲取 'UNRATE' 序列 (返回 None 或意外空)。")
        
        # 探索：嘗試搜索系列
        logger.info("FRED API 探索：嘗試搜索 'inflation' 相關系列...")
        search_results = fred.search('inflation', limit=5) # 限制結果數量
        if search_results is not None and not search_results.empty:
            logger.info(f"找到 {len(search_results)} 個關於 'inflation' 的系列 (顯示前5個):")
            for series_id, row in search_results.iterrows():
                logger.info(f"  ID: {series_id}, Title: {row.get('title', 'N/A')}")
        else:
            logger.warning("FRED API 搜索 'inflation' 未返回結果或結果為空。")

        # 探索：嘗試獲取某個分類下的系列 (例如，分類ID 18 是 'Unemployment')
        # 分類ID可以從 FRED 網站或 API (fred/category/children) 獲取
        # 這裡用一個已知的分類ID
        logger.info("FRED API 探索：嘗試獲取分類 ID 18 (Unemployment) 下的系列...")
        try:
            category_series = fred.get_series_in_category(18, limit=5) # 限制結果數量
            if category_series is not None and not category_series.empty:
                logger.info(f"找到分類 ID 18 下的 {len(category_series)} 個系列 (顯示前5個):")
                for series_id, title in category_series.items():
                     logger.info(f"  ID: {series_id}, Title: {title}")
            else:
                logger.warning("FRED API 獲取分類 ID 18 下的系列未返回結果或結果為空。")
        except Exception as cat_e: # get_series_in_category 可能因 fredapi 版本或底層 API 變化拋錯
            logger.error(f"FRED API 獲取分類系列時出錯: {cat_e}")

        # 探索：時間週期和歷史長度
        logger.info("FRED API 探索：時間週期與歷史數據長度...")
        series_to_test_history = {
            "DGS10": "10-Year Treasury Constant Maturity Rate (Daily)",
            "UNRATE": "Civilian Unemployment Rate (Monthly)",
            "GDP": "Gross Domestic Product (Quarterly)"
        }
        for series_id, description in series_to_test_history.items():
            try:
                logger.info(f"  測試序列: {series_id} ({description})")
                # 獲取序列的全部歷史數據信息 (但不下載全部數據，先獲取info)
                series_info = fred.get_series_info(series_id)
                if series_info is not None and not series_info.empty:
                    freq = series_info.get('frequency_short', 'N/A')
                    obs_start = series_info.get('observation_start', 'N/A')
                    obs_end = series_info.get('observation_end', 'N/A')
                    units = series_info.get('units_short', 'N/A')
                    logger.info(f"    頻率: {freq}, 單位: {units}")
                    logger.info(f"    歷史數據始於: {obs_start}, 終於: {obs_end}")
                    
                    # 嘗試獲取最早的幾個數據點和最新的幾個數據點
                    if obs_start != 'N/A':
                        # 獲取最早的3個數據點
                        first_data = fred.get_series(series_id, observation_start=obs_start, limit=3)
                        if first_data is not None and not first_data.empty:
                            logger.info(f"    最早的數據點示例: {first_data.to_dict()}")
                        
                        # 獲取最新的3個數據點 (如果用 limit，需要從後往前數，或者取最近日期)
                        # 簡便起見，我們已經在連通性測試中獲取了UNRATE的最新值
                        # if series_id == 'DGS10': # DGS10 更新較頻繁
                        #     recent_data = fred.get_series(series_id, observation_end=obs_end, limit=3, sort_order='desc')
                        #     if recent_data is not None and not recent_data.empty:
                        #          logger.info(f"    最新的數據點示例 (逆序): {recent_data.to_dict()}")
                else:
                    logger.warning(f"    未能獲取序列 {series_id} 的詳細信息。")
                respectful_sleep(0.5, 1)
            except Exception as hist_e:
                logger.error(f"    測試序列 {series_id} 歷史數據時出錯: {hist_e}")
        
        logger.info("FRED API 的時間週期由數據系列本身定義 (日、週、月、季、年等)。")
        logger.info("通常不提供分鐘或小時級別的宏觀經濟數據。歷史數據長度因系列而異，許多都很長。")

    except ImportError:
        logger.error("fredapi 套件未安裝。請執行 'pip install fredapi'。")
        report_lines.append("  錯誤: fredapi 套件未安裝。")
    except Exception as e:
        logger.error(f"FRED API 測試失敗: {e}", exc_info=True)
        report_lines.append(f"  測試期間發生意外錯誤: {e}")
    logger.info("--- FRED API 測試結束 ---")
    return report_lines

def test_alpha_vantage_api():
    logger.info("--- 開始測試 Alpha Vantage API ---")
    if not ALPHA_VANTAGE_API_KEY:
        logger.warning("ALPHA_VANTAGE_API_KEY 未設定，跳過 Alpha Vantage API 測試。")
        return
    
    try:
        from alpha_vantage.timeseries import TimeSeries
        # 也可以測試 ForeignExchange, Cryptocurrencies 等
        
        ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
        # 測試獲取 'IBM' 的日線數據，只獲取少量數據 (outputsize='compact' 返回最近100條)
        data, meta_data = ts.get_daily(symbol='IBM', outputsize='compact')
        
        if data is not None and not data.empty:
            # Alpha Vantage 的欄名通常是 '1. open', '2. high' 等
            latest_date = data.index[0] # pandas DataFrame 預設降序
            latest_close = data['4. close'].iloc[0]
            logger.info(f"成功從 Alpha Vantage 獲取 'IBM' 日線數據。最新收盤價 ({latest_date.strftime('%Y-%m-%d')}): {latest_close}")
            logger.info(f"Alpha Vantage API 連通性測試成功。")
        elif data is not None and data.empty: # API呼叫成功但無數據返回
            logger.warning("Alpha Vantage API 連通性測試成功，但 'IBM' 日線數據為空。")
        else: # data is None # data is None
            logger.error("Alpha Vantage API 連通性測試失敗：無法獲取 'IBM' 日線數據 (返回 None)。")
        
        # 探索：嘗試 SYMBOL_SEARCH 功能
        logger.info("Alpha Vantage API 探索：嘗試搜索關鍵詞 'Baosteel'...")
        try:
            # from alpha_vantage.fundamentaldata import FundamentalData # FundamentalData 可能不包含 search
            # Search 通常是直接的 API call 或在 TimeSeries/다른 모듈에 포함
            # alpha_vantage 庫的 search 功能可能在不同類別下，或需要特定方式調用
            # 根據文檔，SYMBOL_SEARCH 是一個獨立的 function
            # https://www.alphavantage.co/documentation/#symbolsearch
            # 我們可以直接構造請求，或看 alpha_vantage 庫是否有封裝
            # alpha_vantage 庫似乎沒有直接封裝 SYMBOL_SEARCH，我們手動請求一次
            
            search_url = (f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&"
                          f"keywords=Baosteel&apikey={ALPHA_VANTAGE_API_KEY}")
            import requests
            response = requests.get(search_url)
            response.raise_for_status()
            search_data = response.json()
            
            if search_data and "bestMatches" in search_data and search_data["bestMatches"]:
                logger.info(f"Alpha Vantage SYMBOL_SEARCH 找到 'Baosteel' 的匹配項 (顯示前3個):")
                for match in search_data["bestMatches"][:3]:
                    logger.info(f"  Symbol: {match.get('1. symbol')}, Name: {match.get('2. name')}, "
                                f"Type: {match.get('3. type')}, Region: {match.get('4. region')}")
            elif search_data and "Note" in search_data: # 可能是達到免費限制的提示
                 logger.warning(f"Alpha Vantage SYMBOL_SEARCH 返回提示: {search_data['Note']}")
                 logger.warning("免費方案的 SYMBOL_SEARCH 可能受限或不可用。")
            else:
                logger.warning(f"Alpha Vantage SYMBOL_SEARCH 未找到 'Baosteel' 的匹配項或返回非預期格式: {search_data}")

        except requests.exceptions.RequestException as req_e:
            logger.error(f"Alpha Vantage SYMBOL_SEARCH 請求失敗: {req_e}")
        except Exception as search_e:
            logger.error(f"Alpha Vantage SYMBOL_SEARCH 過程中發生錯誤: {search_e}")

        logger.info("Alpha Vantage 主要數據類型包括：股票時間序列 (日/週/月/內日), 外匯 (FX), "
                    "加密貨幣, 技術指標, 公司基本面 (Overview, Earnings, Balance Sheet等)。"
                    "通常需要提供明確的股票代號、貨幣對或加密貨幣代號進行查詢。")
        
        # 探索：時間週期和歷史長度 (以 IBM 為例)
        logger.info("Alpha Vantage API 探索：測試 IBM 的不同時間週期和歷史長度...")
        symbol_to_test = 'IBM'
        try:
            ts_hist = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
            
            # 測試日線 (outputsize='full' 嘗試獲取完整歷史)
            logger.info(f"  測試 {symbol_to_test} 日線 (outputsize=full)...")
            daily_data, _ = ts_hist.get_daily(symbol=symbol_to_test, outputsize='full')
            if daily_data is not None and not daily_data.empty:
                logger.info(f"    成功獲取 {symbol_to_test} 日線數據 {len(daily_data)} 條。"
                            f"最早日期: {daily_data.index[-1].strftime('%Y-%m-%d')}, "
                            f"最新日期: {daily_data.index[0].strftime('%Y-%m-%d')}")
            else:
                logger.warning(f"    未能獲取 {symbol_to_test} 的完整日線歷史數據。")
            respectful_sleep()

            # 測試內日數據 (例如 60 分鐘線，免費版通常只有最近幾天)
            # 可用的 interval: '1min', '5min', '15min', '30min', '60min'
            intraday_interval = '60min'
            logger.info(f"  測試 {symbol_to_test} 內日數據 ({intraday_interval}, outputsize=compact)...")
            # intraday_data, _ = ts_hist.get_intraday(symbol=symbol_to_test, interval=intraday_interval, outputsize='compact')
            # get_intraday 的 outputsize 參數可能是 extended (過去30天) 或 compact (最近100點)
            # 免費版可能只支持 compact 或非常有限的 extended
            # 改用 TIME_SERIES_INTRADAY_EXTENDED 嘗試獲取更多，但免費版可能不支持slice
            # https://www.alphavantage.co/documentation/#intraday-extended
            # 由於 alpha_vantage 庫對 extended history 的 slice 支持不明確，這裡用 compact
            intraday_data, _ = ts_hist.get_intraday(symbol=symbol_to_test, interval=intraday_interval, outputsize='compact')
            if intraday_data is not None and not intraday_data.empty:
                 logger.info(f"    成功獲取 {symbol_to_test} {intraday_interval} 數據 {len(intraday_data)} 條。"
                            f"最早時間: {intraday_data.index[-1]}, 最新時間: {intraday_data.index[0]}")
                 logger.info(f"    注意：免費版 Alpha Vantage 的內日歷史數據通常非常有限 (例如幾天或100個數據點)。")
            else:
                logger.warning(f"    未能獲取 {symbol_to_test} 的 {intraday_interval} 數據。")
            respectful_sleep()
            
            # 測試週線
            logger.info(f"  測試 {symbol_to_test} 週線...")
            weekly_data, _ = ts_hist.get_weekly(symbol=symbol_to_test)
            if weekly_data is not None and not weekly_data.empty:
                 logger.info(f"    成功獲取 {symbol_to_test} 週線數據 {len(weekly_data)} 條。"
                            f"最早日期: {weekly_data.index[-1].strftime('%Y-%m-%d')}, "
                            f"最新日期: {weekly_data.index[0].strftime('%Y-%m-%d')}")
            else:
                logger.warning(f"    未能獲取 {symbol_to_test} 的週線數據。")

        except ValueError as ve_hist: # 可能因為請求頻繁或金鑰問題
            logger.error(f"  Alpha Vantage 歷史數據測試中發生錯誤 (ValueError): {ve_hist}")
            if "call frequency" in str(ve_hist).lower():
                logger.warning("    錯誤提示已達到請求頻率限制。")
        except Exception as e_hist:
            logger.error(f"  Alpha Vantage 歷史數據測試中發生未知錯誤: {e_hist}")

        logger.warning("Alpha Vantage 免費方案對請求頻率 (例如每分鐘5次，每天25次) 和歷史數據深度/頻率有嚴格限制。")
        logger.warning("  - 日線/週線/月線: outputsize='full' 可嘗試獲取較長歷史 (約20年)，但仍可能受限。")
        logger.warning("  - 內日數據: 通常僅限最近幾天 (compact) 或一個月內 (extended，但免費版支持不穩定)。")
        logger.warning("  - 分鐘線數據: 限制更多，通常僅高級付費方案提供完整歷史。")

        # 嘗試觸發速率限制 (Alpha Vantage 免費版限制嚴格)
        report_lines.append("\n  Alpha Vantage 速率限制初步觀察:")
        logger.info("  Alpha Vantage: 嘗試連續請求以觀察速率限制 (免費版每日25次, 每分鐘約5次)...")
        # 選用一個輕量級請求，例如 GLOBAL_QUOTE
        from alpha_vantage.fundamentaldata import FundamentalData # Global Quote 在這裡
        # 或者用 ts.get_quote_endpoint(symbol='...')
        # 為了避免與上面的 TimeSeries 實例衝突，重新建立一個
        # 或者直接用 requests 構造請求
        
        # Alpha Vantage 的 Python 庫在達到限制時通常會拋出 ValueError
        # 例如 "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 25 calls per day."
        # "Please Ccontact us at support@alphavantage.co if you would like to have a higher API call frequency."
        
        symbols_for_rate_test = ["MSFT", "GOOG", "AMZN", "META", "NVDA", "TSLA"] # 6 個請求
        rate_limit_hit = False
        for i, sym_rt in enumerate(symbols_for_rate_test):
            try:
                logger.info(f"    速率測試請求 {i+1}/6: 獲取 {sym_rt} 的 Global Quote...")
                # quote_data, _ = ts_hist.get_quote_endpoint(symbol=sym_rt) # TimeSeries 也有 quote endpoint
                # 為了測試不同的端點，這裡用 FundamentalData (雖然 Global Quote 也在 TimeSeries)
                # fd = FundamentalData(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
                # company_overview = fd.get_company_overview(symbol=sym_rt)
                # 上述方式可能不適用於快速測試，因為 overview 數據量大。
                # 直接用 ts.get_quote_endpoint 更合適
                quote_data, _ = ts_hist.get_quote_endpoint(symbol=sym_rt)

                if quote_data is not None and not quote_data.empty:
                    logger.info(f"      成功獲取 {sym_rt} quote。Price: {quote_data.get('05. price', 'N/A').iloc[0]}")
                else:
                    logger.warning(f"      獲取 {sym_rt} quote 成功但返回空。")
                
                if i < len(symbols_for_rate_test) -1 : time.sleep(1) # 模擬快速請求，但又不要太快導致其他問題

            except ValueError as ve_rate:
                logger.warning(f"    在請求 {sym_rt} quote 時發生 ValueError: {ve_rate}")
                report_lines.append(f"    - 在請求 {sym_rt} quote (第 {i+1} 個) 時，可能觸發速率限制: {ve_rate}")
                if "call frequency" in str(ve_rate).lower() or "limit" in str(ve_rate).lower():
                    rate_limit_hit = True
                    break # 已觸發，停止進一步請求
            except Exception as e_rate:
                logger.error(f"    在請求 {sym_rt} quote 時發生其他錯誤: {e_rate}")
                report_lines.append(f"    - 在請求 {sym_rt} quote (第 {i+1} 個) 時發生錯誤: {e_rate}")
                # break # 發生其他錯誤也可能停止
        
        if rate_limit_hit:
            report_lines.append("    => Alpha Vantage 的速率限制 (如每分鐘5次) 較易觸發。")
            logger.info("    => Alpha Vantage 的速率限制 (如每分鐘5次) 被觸發或觀察到相關錯誤。")
        else:
            report_lines.append("    在此小批量測試中未明確觸發 Alpha Vantage 的速率限制錯誤 (可能已用完當日總量，或請求間隔仍不夠密集)。")
            logger.info("    在此小批量測試中未明確觸發 Alpha Vantage 的速率限制錯誤。")


    except ImportError:
        logger.error("alpha_vantage 套件未安裝。請執行 'pip install alpha_vantage'。")
        report_lines.append("  錯誤: alpha_vantage 套件未安裝。")
    except ValueError as ve: # alpha_vantage 庫在金鑰無效或請求頻繁時可能拋出 ValueError
        logger.error(f"Alpha Vantage API 測試失敗 (可能是金鑰問題或速率限制): {ve}", exc_info=True)
        report_lines.append(f"  測試期間發生Value錯誤 (可能是金鑰或速率限制): {ve}")
    except Exception as e:
        logger.error(f"Alpha Vantage API 測試失敗: {e}", exc_info=True)
        report_lines.append(f"  測試期間發生意外錯誤: {e}")
    logger.info("--- Alpha Vantage API 測試結束 ---")
    return report_lines

def test_finnhub_api():
    logger.info("--- 開始測試 Finnhub API ---")
    if not FINNHUB_API_KEY:
        logger.warning("FINNHUB_API_KEY 未設定，跳過 Finnhub API 測試。")
        return
    
    try:
        import finnhub
        finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
        
        # 測試獲取 Apple (AAPL) 的公司基本資料
        company_profile = finnhub_client.company_profile2(symbol='AAPL') # company_profile2 包含更多信息
        
        if company_profile and company_profile.get('name'):
            logger.info(f"成功從 Finnhub 獲取 'AAPL' 的公司名稱: {company_profile.get('name')}")
            logger.info(f"Finnhub API 連通性測試成功。")
        elif company_profile: # 返回了字典但可能內容不符合預期或部分為空
            logger.warning(f"Finnhub API 連通性測試可能部分成功，獲取到 AAPL profile 但關鍵信息缺失: {company_profile}")
        else: # company_profile is None or empty
            logger.error("Finnhub API 連通性測試失敗：無法獲取 'AAPL' 公司基本資料 (返回 None 或空)。")
        
        # 探索：嘗試獲取某交易所的股票代號列表
        logger.info("Finnhub API 探索：嘗試獲取美國 (US) 交易所的股票代號...")
        try:
            # Finnhub 免費版對 stock_symbols 的支持可能有限或有特定要求
            # 文檔: https://finnhub.io/docs/api/stock-symbols
            # exchange: LSE, US, HK, TW, etc.
            us_symbols = finnhub_client.stock_symbols('US', limit=10) # 限制返回數量
            if us_symbols and isinstance(us_symbols, list) and len(us_symbols) > 0:
                logger.info(f"Finnhub 成功獲取部分美國交易所股票代號 (顯示前 {len(us_symbols)} 個):")
                for symbol_info in us_symbols[:5]: # 只顯示前5個的詳細信息
                    logger.info(f"  Symbol: {symbol_info.get('symbol')}, Description: {symbol_info.get('description')}, "
                                f"DisplaySymbol: {symbol_info.get('displaySymbol')}, FIGI: {symbol_info.get('figi')}")
            elif isinstance(us_symbols, list) and len(us_symbols) == 0 :
                 logger.warning("Finnhub API 獲取美國交易所股票代號返回空列表。")
            else: # 可能返回錯誤信息或非預期格式
                logger.warning(f"Finnhub API 獲取美國交易所股票代號失敗或返回非預期格式: {us_symbols}")
                if isinstance(us_symbols, dict) and "error" in us_symbols:
                    logger.error(f"Finnhub API 返回錯誤: {us_symbols['error']}")
                    logger.warning("免費方案可能不完全支持此端點，或已達請求限制。")

        except finnhub.FinnhubAPIException as sym_api_e:
            logger.error(f"Finnhub stock_symbols 請求失敗 (API Exception): {sym_api_e}")
            logger.warning("免費方案可能不完全支持此端點，或已達請求限制。")
        except Exception as sym_e:
            logger.error(f"Finnhub stock_symbols 請求過程中發生錯誤: {sym_e}")

        logger.info("Finnhub API 提供數據類型包括：股票行情、公司基本面、財報、分析師評級、"
                    "新聞、經濟數據、加密貨幣、外匯等。")
        
        # 探索：時間週期和歷史長度 (以 AAPL 為例)
        logger.info("Finnhub API 探索：測試 AAPL 的不同時間週期和歷史長度...")
        symbol_to_test = 'AAPL'
        current_time_unix = int(time.time())
        one_year_ago_unix = int((datetime.now() - timedelta(days=365)).timestamp())
        five_years_ago_unix = int((datetime.now() - timedelta(days=5*365)).timestamp())

        resolutions_to_test = {
            "D": "日線",
            "W": "週線",
            "M": "月線",
            "60": "60分鐘線"
            # "1": "1分鐘線" # 分鐘線對免費方案限制可能更嚴格，謹慎測試
        }

        for res_code, res_name in resolutions_to_test.items():
            try:
                logger.info(f"  測試 {symbol_to_test} {res_name} (resolution={res_code})...")
                # 嘗試獲取過去一年的數據
                candles = finnhub_client.stock_candles(symbol_to_test, res_code, one_year_ago_unix, current_time_unix)
                if candles and candles.get('s') == 'ok' and candles.get('t'):
                    num_candles = len(candles['t'])
                    logger.info(f"    成功獲取 {symbol_to_test} {res_name} 數據 {num_candles} 條 (過去一年)。")
                    if num_candles > 0:
                        first_ts = datetime.fromtimestamp(candles['t'][0]).strftime('%Y-%m-%d %H:%M:%S')
                        last_ts = datetime.fromtimestamp(candles['t'][-1]).strftime('%Y-%m-%d %H:%M:%S')
                        logger.info(f"      數據時間範圍: {first_ts} 至 {last_ts}")
                    if res_code == "D": # 對日線嘗試更長歷史
                        logger.info(f"    嘗試獲取 {symbol_to_test} 日線更長歷史 (5年前至今)...")
                        long_candles = finnhub_client.stock_candles(symbol_to_test, "D", five_years_ago_unix, current_time_unix)
                        if long_candles and long_candles.get('s') == 'ok' and long_candles.get('t'):
                            logger.info(f"      成功獲取 {symbol_to_test} 日線數據 {len(long_candles['t'])} 條 (過去五年)。"
                                        f"最早時間戳: {datetime.fromtimestamp(long_candles['t'][0]).strftime('%Y-%m-%d')}")
                        else:
                            logger.warning(f"      未能獲取 {symbol_to_test} 日線的5年歷史數據。Status: {long_candles.get('s')}")
                elif candles and candles.get('s') != 'ok':
                    logger.warning(f"    未能獲取 {symbol_to_test} {res_name} 數據。API狀態: {candles.get('s')}")
                else:
                    logger.warning(f"    未能獲取 {symbol_to_test} {res_name} 數據或返回格式不符。")
                respectful_sleep()
            except finnhub.FinnhubAPIException as fh_api_e:
                logger.error(f"    測試 {symbol_to_test} {res_name} 時發生 API 錯誤: {fh_api_e}")
                if "rate limit" in str(fh_api_e).lower() or "429" in str(fh_api_e):
                    logger.warning("      已達 Finnhub 速率限制，後續測試可能受影響。")
                    break # 達到速率限制，可能需要停止對此 API 的進一步測試
            except Exception as e_hist:
                logger.error(f"    測試 {symbol_to_test} {res_name} 時發生未知錯誤: {e_hist}")
        
        logger.warning("Finnhub 免費方案對歷史數據的範圍和請求頻率 (每分鐘60次) 有限。")
        logger.warning("  - 分鐘級數據的歷史回溯通常較短。")
        logger.warning("  - 日線、週線、月線的歷史數據可能也有限制 (例如幾年)。")

        # 嘗試觸發速率限制 (Finnhub 免費版每分鐘60次)
        report_lines.append("\n  Finnhub API 速率限制初步觀察:")
        logger.info("  Finnhub: 嘗試連續請求以觀察速率限制 (免費版每分鐘約60次)...")
        # 選用一個輕量級請求，例如 company_profile2
        # 連續請求多個不同股票的 profile
        symbols_for_fin_rate_test = candidate_symbols[:10] # 取10個，避免過多
        fin_rate_limit_hit = False
        fin_success_count = 0
        
        # 確保 finnhub_client 實例存在
        if 'finnhub_client' not in locals() and FINNHUB_API_KEY:
             try:
                import finnhub
                finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
             except Exception:
                 logger.error("  無法初始化 finnhub_client 進行速率測試。")
                 finnhub_client = None
        
        if finnhub_client:
            for i, sym_rt in enumerate(symbols_for_fin_rate_test):
                try:
                    logger.info(f"    Finnhub 速率測試請求 {i+1}/{len(symbols_for_fin_rate_test)}: 獲取 {sym_rt} 的 profile...")
                    profile = finnhub_client.company_profile2(symbol=sym_rt)
                    if profile and profile.get('name'):
                        fin_success_count +=1
                        logger.debug(f"      成功獲取 {sym_rt} profile。")
                    else:
                        logger.warning(f"      獲取 {sym_rt} profile 成功但數據不完整。")
                    
                    if i < len(symbols_for_fin_rate_test) -1 : time.sleep(0.5) # 較短延遲以嘗試觸發限制

                except finnhub.FinnhubAPIException as fh_api_e_rate:
                    logger.warning(f"    在請求 {sym_rt} profile 時發生 FinnhubAPIException: {fh_api_e_rate}")
                    report_lines.append(f"    - 在請求 {sym_rt} profile (第 {i+1} 個) 時，可能觸發速率限制: {fh_api_e_rate}")
                    if fh_api_e_rate.status_code == 429 or "rate limit" in str(fh_api_e_rate).lower():
                        fin_rate_limit_hit = True
                        logger.error("      Finnhub 速率限制被觸發！")
                        break 
                except Exception as e_fin_rate:
                    logger.error(f"    在請求 {sym_rt} profile 時發生其他錯誤: {e_fin_rate}")
                    report_lines.append(f"    - 在請求 {sym_rt} profile (第 {i+1} 個) 時發生錯誤: {e_fin_rate}")
            
            report_lines.append(f"    Finnhub 速率測試完成 {fin_success_count}/{len(symbols_for_fin_rate_test)} 個請求成功。")
            if fin_rate_limit_hit:
                report_lines.append("    => Finnhub 的速率限制 (如每分鐘60次) 被觸發。")
                logger.info("    => Finnhub 的速率限制被觸發。")
            else:
                report_lines.append("    在此小批量測試中未明確觸發 Finnhub 的速率限制錯誤 (可能請求量不足或間隔仍可接受)。")
                logger.info("    在此小批量測試中未明確觸發 Finnhub 的速率限制錯誤。")
        else:
            report_lines.append("    Finnhub client 未能初始化，跳過速率限制觀察。")


    except ImportError:
        logger.error("finnhub 套件未安裝。請執行 'pip install finnhub-python'。")
        # report_lines.append("  錯誤: finnhub 套件未安裝。") # 假設 report_lines 在此函式頂部初始化
    except finnhub.FinnhubAPIException as api_e:
        logger.error(f"Finnhub API 測試失敗 (API Exception): {api_e}", exc_info=True)
        # report_lines.append(f"  測試期間發生API錯誤: {api_e}")
        if "401" in str(api_e) or "Invalid API key" in str(api_e):
            logger.error("錯誤訊息提示 API 金鑰無效。")
        elif "429" in str(api_e) or "rate limit" in str(api_e).lower():
            logger.error("錯誤訊息提示已達到速率限制。")
    except Exception as e:
        logger.error(f"Finnhub API 連通性測試失敗 (其他錯誤): {e}", exc_info=True)
        # report_lines.append(f"  測試期間發生意外錯誤: {e}")
    logger.info("--- Finnhub API 測試結束 ---")
    # return report_lines # 確保函式返回 report_lines

def test_news_api():
    logger.info("--- 開始測試 News API ---")
    if not NEWS_API_KEY:
        logger.warning("NEWS_API_KEY 未設定，跳過 News API 測試。")
        return
    
    try:
        from newsapi import NewsApiClient
        newsapi = NewsApiClient(api_key=NEWS_API_KEY)
        
        # 測試獲取關於 "市場" (market) 的頭條新聞，英文，取少量
        # 免費方案的新聞通常有延遲，且來源有限
        top_headlines = newsapi.get_top_headlines(q='market',
                                                  language='en',
                                                  page_size=5) # 只取5條以節省
        
        if top_headlines and top_headlines.get('status') == 'ok':
            if top_headlines.get('totalResults', 0) > 0 and top_headlines.get('articles'):
                logger.info(f"成功從 News API 獲取到 {top_headlines['totalResults']} 條關於 'market' 的新聞。"
                            f"第一條標題: {top_headlines['articles'][0]['title']}")
                logger.info(f"News API 連通性測試成功。")
            else:
                logger.warning(f"News API 連通性測試成功，但未找到關於 'market' 的新聞。")
        else: # API 呼叫本身可能有問題，或 status 不是 'ok'
            error_message = top_headlines.get('message', '未知錯誤') if isinstance(top_headlines, dict) else '回應非預期格式或為空'
            logger.error(f"News API 連通性測試失敗：API 回應狀態非 'ok' 或結果為空。錯誤: {error_message}")
            if isinstance(top_headlines, dict):
                if top_headlines.get('code') == 'apiKeyInvalid':
                     logger.error("News API 金鑰無效。")
                elif top_headlines.get('code') == 'rateLimited':
                     logger.error("News API 已達到速率限制。")

        # 探索：獲取可用新聞來源
        logger.info("News API 探索：嘗試獲取可用新聞來源 (部分)...")
        try:
            sources_response = newsapi.get_sources(language='en') # 可指定 category, country, language
            if sources_response and sources_response.get('status') == 'ok':
                if sources_response.get('sources'):
                    logger.info(f"News API 成功獲取 {len(sources_response['sources'])} 個新聞來源 (顯示前5個):")
                    for source in sources_response['sources'][:5]:
                        logger.info(f"  ID: {source.get('id')}, Name: {source.get('name')}, Category: {source.get('category')}")
                else:
                    logger.warning("News API 未返回任何新聞來源。")
            else:
                src_error_message = sources_response.get('message', '未知錯誤') if isinstance(sources_response, dict) else '回應非預期格式或為空'
                logger.error(f"News API 獲取新聞來源失敗: {src_error_message}")
        except Exception as src_e:
            logger.error(f"News API 獲取新聞來源時發生錯誤: {src_e}")
        
        logger.info("News API 主要通過關鍵詞(q)、新聞來源(sources)、國家(country)、"
                    "類別(category: business, entertainment, general, health, science, sports, technology) "
                    "和語言(language)進行查詢。")
        
        # 探索：時間週期和歷史長度
        logger.info("News API 探索：測試日期範圍查詢與歷史限制...")
        try:
            # 嘗試獲取兩天前到一天前的新聞 (免費版應該支持)
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S')
            one_day_ago = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
            
            logger.info(f"  測試獲取 'finance' 相關新聞，日期範圍: {two_days_ago} 至 {one_day_ago}")
            dated_news = newsapi.get_everything(q='finance',
                                                from_param=two_days_ago, # newsapi-python 用 from_param
                                                to=one_day_ago,
                                                language='en',
                                                sort_by='publishedAt',
                                                page_size=5)
            if dated_news and dated_news.get('status') == 'ok' and dated_news.get('articles'):
                logger.info(f"    成功獲取到 {len(dated_news['articles'])} 条指定日期範圍內的新聞。")
                for article in dated_news['articles']:
                    logger.info(f"      - [{article.get('publishedAt')}] {article.get('title')}")
            elif dated_news and dated_news.get('status') != 'ok':
                 logger.warning(f"    獲取指定日期新聞失敗，狀態: {dated_news.get('status')}, 訊息: {dated_news.get('message')}")
            else:
                logger.warning(f"    未找到指定日期範圍內關於 'finance' 的新聞。")
            
            # 嘗試獲取兩個月前的新聞 (免費版應該會失敗或返回空)
            two_months_ago_start = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%dT%H:%M:%S')
            two_months_ago_end = (datetime.now() - timedelta(days=58)).strftime('%Y-%m-%dT%H:%M:%S')
            logger.info(f"  測試獲取兩個月前的新聞 (預期免費版會失敗或返回空)...")
            old_news = newsapi.get_everything(q='finance',
                                              from_param=two_months_ago_start,
                                              to=two_months_ago_end,
                                              language='en',
                                              page_size=5)
            if old_news and old_news.get('status') == 'ok' and old_news.get('articles'):
                logger.warning(f"    意外地獲取到兩個月前的新聞 {len(old_news['articles'])} 條 (可能API政策變動或測試日期接近月底)。")
            elif old_news and old_news.get('status') == 'error' and old_news.get('code') == 'sourcesUnavailable':
                 logger.info(f"    獲取兩個月前新聞按預期失敗/無來源: {old_news.get('message')}")
            elif old_news and old_news.get('status') == 'ok' and not old_news.get('articles'):
                 logger.info(f"    獲取兩個月前新聞按預期返回空列表。")
            else:
                logger.warning(f"    獲取兩個月前新聞返回非預期: Status {old_news.get('status')}, Msg: {old_news.get('message')}")


        except Exception as date_e:
            logger.error(f"  News API 日期範圍測試中發生錯誤: {date_e}")

        logger.info("News API 不提供傳統意義上的時間週期數據 (如OHLC)。")
        logger.warning("免費方案 ('Developer') 的新聞數據僅限於【過去一個月】。更早的歷史數據需要付費方案。")
        logger.warning("查詢時可指定 `from` 和 `to` 日期參數 (ISO 8601格式) 來限定時間範圍。")


    except ImportError:
        logger.error("newsapi 套件未安裝。請執行 'pip install newsapi-python'。")
    except Exception as e: # NewsAPIClient 可能會拋出 requests.exceptions.HTTPError 等
        logger.error(f"News API 測試失敗 (其他錯誤): {e}", exc_info=True)
    logger.info("--- News API 測試結束 ---")

def test_fmp_api():
    logger.info("--- 開始測試 Financial Modeling Prep (FMP) API ---")
    if not FMP_API_KEY:
        logger.warning("FMP_API_KEY 未設定，跳過 FMP API 測試。")
        return
    
    try:
        import requests # FMP API 通常直接用 requests 調用
        
        # 測試獲取 Apple (AAPL) 的公司簡介 (profile)
        # FMP API v3 的端點
        symbol = "AAPL"
        # FMP 有多個端點，例如 /profile, /quote, /historical-price-full 等
        # 這裡使用 /profile
        api_url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={FMP_API_KEY}"
        
        response = requests.get(api_url, timeout=10) # 設定超時
        response.raise_for_status() # 如果狀態碼是 4xx 或 5xx，則拋出 HTTPError
        
        data = response.json()
        
        # FMP 的 profile 通常是一個列表，包含一個字典
        if data and isinstance(data, list) and len(data) > 0:
            company_info = data[0]
            if company_info.get('companyName'):
                logger.info(f"成功從 FMP 獲取 '{symbol}' 的公司名稱: {company_info.get('companyName')}")
                logger.info(f"FMP API 連通性測試成功。")
            else:
                logger.warning(f"FMP API 連通性測試成功，但獲取的 '{symbol}' 公司簡介中缺少 companyName。")
        elif isinstance(data, dict) and data.get("Error Message"): # FMP 返回錯誤的方式
            logger.error(f"FMP API 返回錯誤: {data.get('Error Message')}")
            if "limit" in data.get('Error Message', '').lower():
                logger.error("FMP API 錯誤可能與速率限制或方案限制有關。")
            elif "invalid" in data.get('Error Message', '').lower() and "api key" in data.get('Error Message', '').lower():
                logger.error("FMP API 金鑰可能無效。")
        else:
            logger.error(f"FMP API 連通性測試失敗：無法獲取 '{symbol}' 公司簡介，或返回數據格式非預期。回應: {data}")

        # 探索：嘗試獲取股票列表 (stock list)
        # https://financialmodelingprep.com/api/v3/stock/list?apikey=YOUR_API_KEY
        # 免費方案可能對此有限制或不提供完整列表
        logger.info("FMP API 探索：嘗試獲取股票列表 (部分)...")
        stock_list_url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_API_KEY}"
        try:
            stock_list_response = requests.get(stock_list_url, timeout=15) # 列表可能較大，增加超時
            stock_list_response.raise_for_status()
            stock_list_data = stock_list_response.json()

            if stock_list_data and isinstance(stock_list_data, list) and len(stock_list_data) > 0:
                logger.info(f"FMP API 成功獲取股票列表，總共約 {len(stock_list_data)} 條記錄 (免費版可能不完整)。顯示前5條:")
                for stock_item in stock_list_data[:5]:
                    logger.info(f"  Symbol: {stock_item.get('symbol')}, Name: {stock_item.get('name')}, "
                                f"Exchange: {stock_item.get('exchangeShortName')}")
            elif isinstance(stock_list_data, dict) and stock_list_data.get("Error Message"):
                 logger.error(f"FMP API 獲取股票列表返回錯誤: {stock_list_data.get('Error Message')}")
                 logger.warning("免費方案可能不完全支持此端點或已達限制。")
            else:
                logger.warning(f"FMP API 獲取股票列表未返回預期數據或列表為空: {stock_list_data[:100]}") # 顯示部分原始回應

        except requests.exceptions.HTTPError as sl_http_err:
            logger.error(f"FMP API 獲取股票列表失敗 (HTTP Error): {sl_http_err}")
            if sl_http_err.response.status_code == 401 or sl_http_err.response.status_code == 403:
                 logger.error("HTTP 401/403 錯誤，通常表示 API 金鑰問題或權限不足。")
        except requests.exceptions.RequestException as sl_req_err:
            logger.error(f"FMP API 獲取股票列表請求失敗: {sl_req_err}")
        except Exception as sl_e:
            logger.error(f"FMP API 獲取股票列表過程中發生錯誤: {sl_e}")

        logger.info("FMP API 提供股票、ETF、外匯、加密貨幣的歷史價格、公司基本面、財報、"
                    "市場指數、經濟指標等多種數據。通常通過指定股票代號或特定端點路徑查詢。")
        
        # 探索：時間週期和歷史長度 (以 AAPL 為例)
        logger.info("FMP API 探索：測試 AAPL 的不同時間週期和歷史長度...")
        symbol_to_test = 'AAPL'
        
        # 測試日線歷史長度 (免費版通常5年)
        # 端點: /api/v3/historical-price-full/AAPL?apikey=YOUR_API_KEY
        try:
            logger.info(f"  測試 {symbol_to_test} 日線歷史 (historical-price-full)...")
            daily_hist_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol_to_test}?apikey={FMP_API_KEY}"
            # 可以添加 from 和 to 參數來限制日期範圍，例如 ?from=YYYY-MM-DD&to=YYYY-MM-DD
            # 或者用 timeseries={days} 參數
            # 為了測試最大長度，先不加日期限制，但免費版可能只返回部分
            # 改用 timeseries 參數獲取約5年數據 (5 * 252 交易日)
            daily_hist_url_limited = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol_to_test}?timeseries={5*252}&apikey={FMP_API_KEY}"

            response_daily = requests.get(daily_hist_url_limited, timeout=20)
            response_daily.raise_for_status()
            daily_data_fmp = response_daily.json()

            if daily_data_fmp and daily_data_fmp.get('symbol') and daily_data_fmp.get('historical'):
                historical_points = daily_data_fmp['historical']
                logger.info(f"    成功獲取 {symbol_to_test} 日線數據 {len(historical_points)} 條。")
                if historical_points:
                    logger.info(f"      最早日期: {historical_points[-1].get('date')}, 最新日期: {historical_points[0].get('date')}")
                    logger.info(f"      (FMP免費版通常提供約5年日線歷史)")
            elif isinstance(daily_data_fmp, dict) and daily_data_fmp.get("Error Message"):
                 logger.error(f"    FMP API 獲取日線歷史返回錯誤: {daily_data_fmp.get('Error Message')}")
            else:
                logger.warning(f"    未能獲取 {symbol_to_test} 的日線歷史數據，或返回格式非預期。")
            respectful_sleep()
        except requests.exceptions.RequestException as req_e:
            logger.error(f"    FMP 日線歷史請求失敗: {req_e}")
        except Exception as e_daily:
            logger.error(f"    FMP 日線歷史測試中發生錯誤: {e_daily}")

        # 測試內日數據 (例如 1hour, 免費版歷史通常很短)
        # 端點: /api/v3/historical-chart/{interval}/{ticker}?apikey=YOUR_API_KEY
        # interval: 1min, 5min, 15min, 30min, 1hour
        intraday_intervals_fmp = {"1hour": "1小時線", "15min": "15分鐘線"}
        for interval_code, interval_name in intraday_intervals_fmp.items():
            try:
                logger.info(f"  測試 {symbol_to_test} {interval_name} (interval={interval_code})...")
                # 嘗試獲取最近幾天的數據 (例如，通過 from/to 或 timeseries 參數)
                # FMP 內日數據的 from/to 參數可能與日線不同，或免費版限制嚴格
                # 這裡不加日期限制，看它預設返回多少 (通常是最近的)
                intraday_url = f"https://financialmodelingprep.com/api/v3/historical-chart/{interval_code}/{symbol_to_test}?apikey={FMP_API_KEY}"
                # 獲取最近幾天的數據 (例如，5天前到現在)
                date_to_param = datetime.now().strftime('%Y-%m-%d')
                date_from_param = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
                intraday_url_dated = f"{intraday_url}&from={date_from_param}&to={date_to_param}"

                response_intraday = requests.get(intraday_url_dated, timeout=20)
                response_intraday.raise_for_status()
                intraday_data_fmp = response_intraday.json()

                if intraday_data_fmp and isinstance(intraday_data_fmp, list) and len(intraday_data_fmp) > 0:
                    logger.info(f"    成功獲取 {symbol_to_test} {interval_name} 數據 {len(intraday_data_fmp)} 條。")
                    logger.info(f"      最早時間: {intraday_data_fmp[-1].get('date')}, 最新時間: {intraday_data_fmp[0].get('date')}")
                    logger.info(f"      (FMP免費版內日數據歷史通常很短，例如幾天或當日)")
                elif isinstance(intraday_data_fmp, dict) and intraday_data_fmp.get("Error Message"):
                     logger.error(f"    FMP API 獲取 {interval_name} 返回錯誤: {intraday_data_fmp.get('Error Message')}")
                else:
                    logger.warning(f"    未能獲取 {symbol_to_test} 的 {interval_name} 數據，或返回空列表/非預期格式。")
                respectful_sleep()
            except requests.exceptions.RequestException as req_e_intra:
                logger.error(f"    FMP {interval_name} 請求失敗: {req_e_intra}")
            except Exception as e_intra:
                 logger.error(f"    FMP {interval_name} 測試中發生錯誤: {e_intra}")
        
        logger.warning("FMP API 免費方案 ('Basic'):")
        logger.warning("  - 日線歷史: 通常約5年。")
        logger.warning("  - 內日 (分鐘/小時) 歷史: 通常非常有限，可能只有幾天或當日數據。")
        logger.warning("  - 請求限制: 每天約250次 API 呼叫。")


    except requests.exceptions.HTTPError as http_err:
        logger.error(f"FMP API 測試失敗 (HTTP Error): {http_err}", exc_info=True)
        if http_err.response.status_code == 401 or http_err.response.status_code == 403:
            logger.error("HTTP 401/403 錯誤，通常表示 API 金鑰問題或權限不足。")
        elif http_err.response.status_code == 429:
            logger.error("HTTP 429 錯誤，表示已達到速率限制。")
    except requests.exceptions.RequestException as req_err: # 包括 ConnectionError, Timeout 等
        logger.error(f"FMP API 測試失敗 (Request Exception): {req_err}", exc_info=True)
    except Exception as e:
        logger.error(f"FMP API 連通性測試失敗 (其他錯誤): {e}", exc_info=True)
    logger.info("--- FMP API 測試結束 ---")

def test_polygon_api():
    logger.info("--- 開始測試 Polygon.io API ---")
    if not POLYGON_API_KEY:
        logger.warning("POLYGON_API_KEY 未設定，跳過 Polygon.io API 測試。")
        return
    
    try:
        from polygon import RESTClient
        # from polygon.exceptions import BadResponse # 用於更細緻的錯誤處理

        # client = RESTClient(api_key=POLYGON_API_KEY, read_timeout=10) # 舊版初始化
        client = RESTClient(api_key=POLYGON_API_KEY) # 新版初始化，timeout 可在請求時指定

        # 測試獲取 Apple (AAPL) 的前一日收盤價
        # Polygon 的免費方案通常對此類請求有限制 (例如延遲數據)
        ticker_symbol = "AAPL"
        # 獲取前一個交易日的日期 (假設今天是週一，則獲取上週五)
        # 為了簡化，我們先嘗試獲取一個固定較近的日期，例如 2023-10-05，確保該日有數據
        # 或者，更動態地獲取 "previous close"
        
        # 使用 "Previous Close" 端點
        # aggs = client.get_previous_close_agg(ticker=ticker_symbol, read_timeout=10)
        # Polygon API client v1.10+ uses get_previous_close
        aggs = client.get_previous_close(ticker=ticker_symbol, params={'adjusted': 'true'}, read_timeout_sec=10)


        if aggs and hasattr(aggs, 'results') and aggs.results and len(aggs.results) > 0:
            prev_close_info = aggs.results[0]
            logger.info(f"成功從 Polygon.io 獲取 '{ticker_symbol}' 的前一日收盤價資訊。")
            logger.info(f"  Ticker: {prev_close_info.get('T')}, "
                        f"Close: {prev_close_info.get('c')}, "
                        f"Volume: {prev_close_info.get('v')}, "
                        f"Timestamp: {datetime.fromtimestamp(prev_close_info.get('t') / 1000) if prev_close_info.get('t') else 'N/A'}")
            logger.info(f"Polygon.io API 連通性測試成功。")
        elif hasattr(aggs, 'status') and aggs.status == 'DELAYED': # 免費方案通常是延遲數據
             logger.warning(f"Polygon.io API 連通性測試成功，但 '{ticker_symbol}' 的數據是延遲的 (免費方案限制)。")
             if aggs.results and len(aggs.results) > 0: # 即使延遲，也可能有結果
                prev_close_info = aggs.results[0]
                logger.info(f"  延遲數據 - Ticker: {prev_close_info.get('T')}, Close: {prev_close_info.get('c')}")
             else: # 延遲但無結果
                logger.warning("延遲數據中未找到結果。")
        elif hasattr(aggs, 'message') and aggs.message: # API 返回了帶有 message 的錯誤結構
            logger.error(f"Polygon.io API 返回訊息: {aggs.message}")
            if hasattr(aggs, 'status') and aggs.status == 'ERROR':
                 logger.error(f"Polygon.io API 錯誤狀態: {aggs.status}, Request ID: {aggs.request_id if hasattr(aggs, 'request_id') else 'N/A'}")
            if "key" in aggs.message.lower() or (hasattr(aggs, 'status') and aggs.status == 'AUTH_ERROR'):
                logger.error("Polygon.io API 金鑰可能無效或未授權。")
            elif "limit" in aggs.message.lower() or (hasattr(aggs, 'status') and "limit" in aggs.status.lower()):
                 logger.error("Polygon.io API 錯誤可能與速率限制有關。")
        else: # 其他未知錯誤或非預期格式
            logger.error(f"Polygon.io API 連通性測試失敗：無法獲取 '{ticker_symbol}' 前一日收盤價，或返回數據格式非預期。回應類型: {type(aggs)}, 回應內容: {str(aggs)[:200]}")

        # 探索：嘗試獲取 Tickers 列表 (帶搜索條件)
        # https://polygon.io/docs/stocks/get_v3_reference_tickers
        logger.info("Polygon.io API 探索：嘗試搜索 'Microsoft' 相關的 Tickers...")
        try:
            # client.list_tickers() # 舊版
            # client.list_tickers(market='stocks', exchange='XNAS', active=True, limit=5, search='Micro') # 帶參數的範例
            # 新版 client.list_tickers(search="Microsoft", active=True, limit=5)
            tickers_response = client.list_tickers(search="Microsoft", market="stocks", active=True, limit=5, order="asc", sort="ticker")

            # list_tickers 返回的是一個 generator，需要迭代
            ticker_results = []
            for t in tickers_response:
                ticker_results.append(t)
            
            if ticker_results:
                logger.info(f"Polygon.io Tickers 搜索 'Microsoft' 找到 {len(ticker_results)} 個結果 (顯示部分):")
                for t_info in ticker_results[:3]: # 顯示前3個
                    logger.info(f"  Ticker: {t_info.ticker}, Name: {t_info.name}, Market: {t_info.market}, Locale: {t_info.locale}")
            else:
                logger.warning("Polygon.io Tickers 搜索 'Microsoft' 未返回任何結果。")
        
        except Exception as tick_e: # Polygon client 可能拋出各種異常，包括 HTTPError 等
            logger.error(f"Polygon.io Tickers 列表請求失敗: {tick_e}", exc_info=True)
            if hasattr(tick_e, 'response') and hasattr(tick_e.response, 'status_code'):
                status_code = tick_e.response.status_code
                if status_code == 401 or status_code == 403:
                    logger.error("HTTP 401/403 錯誤，通常表示 API 金鑰問題或權限不足。")
                elif status_code == 429: # Too Many Requests
                    logger.error("HTTP 429 錯誤，表示已達到速率限制。")
            elif "Forbidden" in str(tick_e) or "auth" in str(tick_e).lower(): # 有些 client 錯誤訊息可能不同
                 logger.error("錯誤訊息提示 API 金鑰無效或權限問題。")


        logger.info("Polygon.io API 提供股票、期權、指數、外匯、加密貨幣等數據。"
                    "可通過 Tickers 端點搜索，或直接查詢已知代號的行情、聚合數據等。")
        
        # 探索：時間週期和歷史長度 (以 AAPL 為例)
        logger.info("Polygon.io API 探索：測試 AAPL 的不同時間週期和歷史長度...")
        symbol_to_test = 'AAPL'
        # 日期格式: YYYY-MM-DD
        today_str = datetime.now().strftime('%Y-%m-%d')
        one_year_ago_str = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        three_years_ago_str = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')

        # 測試日線歷史 (免費版通常2年)
        try:
            logger.info(f"  測試 {symbol_to_test} 日線歷史 (3年前至今)...")
            # client.get_aggs(ticker, multiplier, timespan, from_date, to_date)
            # aggs_daily = client.get_aggs(symbol_to_test, 1, "day", three_years_ago_str, today_str, limit=50000) # limit 最大 50000
            # v1.10+ uses get_aggregates
            aggs_daily_response = client.get_aggregates(symbol_to_test, 1, "day", three_years_ago_str, today_str, limit=50000, read_timeout_sec=30)

            if aggs_daily_response and hasattr(aggs_daily_response, 'results') and aggs_daily_response.results:
                daily_bars = aggs_daily_response.results
                logger.info(f"    成功獲取 {symbol_to_test} 日線數據 {len(daily_bars)} 條。")
                if daily_bars:
                    first_bar_ts = datetime.fromtimestamp(daily_bars[0].get('t') / 1000).strftime('%Y-%m-%d')
                    last_bar_ts = datetime.fromtimestamp(daily_bars[-1].get('t') / 1000).strftime('%Y-%m-%d')
                    logger.info(f"      數據時間範圍: {first_bar_ts} 至 {last_bar_ts}")
                    logger.info(f"      (Polygon免費版日線歷史通常約2年，請求更長可能只返回部分)")
            elif hasattr(aggs_daily_response, 'status') and aggs_daily_response.status != "OK":
                 logger.warning(f"    未能獲取 {symbol_to_test} 日線歷史數據。Status: {aggs_daily_response.status}, "
                                f"Message: {aggs_daily_response.message if hasattr(aggs_daily_response, 'message') else 'N/A'}")
            else:
                logger.warning(f"    未能獲取 {symbol_to_test} 的日線歷史數據，或返回空結果。")
            respectful_sleep()
        except Exception as e_daily_poly:
            logger.error(f"    Polygon.io 日線歷史測試中發生錯誤: {e_daily_poly}", exc_info=True)
            if "Forbidden" in str(e_daily_poly) or "auth" in str(e_daily_poly).lower():
                logger.error("    錯誤可能與 API 金鑰權限或方案限制有關。")


        # 測試小時線 (免費版歷史通常較短)
        # timespan: minute, hour, day, week, month, quarter, year
        try:
            logger.info(f"  測試 {symbol_to_test} 小時線歷史 (過去1年)...")
            # aggs_hourly = client.get_aggs(symbol_to_test, 1, "hour", one_year_ago_str, today_str, limit=5000)
            aggs_hourly_response = client.get_aggregates(symbol_to_test, 1, "hour", one_year_ago_str, today_str, limit=5000, read_timeout_sec=30)

            if aggs_hourly_response and hasattr(aggs_hourly_response, 'results') and aggs_hourly_response.results:
                hourly_bars = aggs_hourly_response.results
                logger.info(f"    成功獲取 {symbol_to_test} 小時線數據 {len(hourly_bars)} 條。")
                if hourly_bars:
                    first_bar_ts = datetime.fromtimestamp(hourly_bars[0].get('t') / 1000).strftime('%Y-%m-%d %H:%M')
                    last_bar_ts = datetime.fromtimestamp(hourly_bars[-1].get('t') / 1000).strftime('%Y-%m-%d %H:%M')
                    logger.info(f"      數據時間範圍: {first_bar_ts} 至 {last_bar_ts}")
                    logger.info(f"      (Polygon免費版小時線歷史通常比日線短)")
            elif hasattr(aggs_hourly_response, 'status') and aggs_hourly_response.status != "OK":
                 logger.warning(f"    未能獲取 {symbol_to_test} 小時線數據。Status: {aggs_hourly_response.status}, "
                                f"Message: {aggs_hourly_response.message if hasattr(aggs_hourly_response, 'message') else 'N/A'}")
            else:
                logger.warning(f"    未能獲取 {symbol_to_test} 的小時線數據，或返回空結果。")
        except Exception as e_hourly_poly:
            logger.error(f"    Polygon.io 小時線歷史測試中發生錯誤: {e_hourly_poly}", exc_info=True)

        logger.warning("Polygon.io API 免費方案:")
        logger.warning("  - 歷史數據: 通常股票/ETF日線約2年，其他頻率 (如小時/分鐘) 和資產類別 (如期權) 的歷史可能更短或不提供。")
        logger.warning("  - 數據延遲: 免費方案數據通常是延遲的。")
        logger.warning("  - 請求限制: 每分鐘約5次 API 呼叫。")


    except ImportError:
        logger.error("polygon-api-client 套件未安裝。請執行 'pip install polygon-api-client'。")
    # except BadResponse as br: # 特定於 polygon 客戶端庫的異常 (舊版?)
    #     logger.error(f"Polygon.io API 測試失敗 (BadResponse): {br}", exc_info=True)
    #     if br.status_code == 401 or br.status_code == 403:
    #         logger.error("HTTP 401/403 錯誤，通常表示 API 金鑰問題或權限不足。")
    #     elif br.status_code == 429:
    #         logger.error("HTTP 429 錯誤，表示已達到速率限制。")
    except Exception as e: # 捕獲更通用的錯誤，例如 requests.exceptions.HTTPError (如果底層使用)
        logger.error(f"Polygon.io API 連通性測試失敗 (其他錯誤): {e}", exc_info=True)
        # 檢查錯誤類型或訊息中是否有關鍵字
        if hasattr(e, 'status_code'): # 如果是 requests.HTTPError
            if e.status_code == 401 or e.status_code == 403:
                 logger.error("HTTP 401/403 錯誤，通常表示 API 金鑰問題或權限不足。")
            elif e.status_code == 429:
                 logger.error("HTTP 429 錯誤，表示已達到速率限制。")

    logger.info("--- Polygon.io API 測試結束 ---")

# --- 免費/公開 API 測試函式框架 ---
def test_ecb_api():
    logger.info("--- 開始測試 European Central Bank (ECB) API ---")
    try:
        import pandasdmx as sdmx
        # ECB SDMX v2.1 API endpoint
        # 文件: https://sdw-wsrest.ecb.europa.eu/service/datastructure/ECB/ECB_CONSOLIDATED/latest?references=children
        # 和 https://data.ecb.europa.eu/help/api/data
        
        # 建立 ECB 數據源的請求物件
        # ECB 的 REST API 端點是 https://data-api.ecb.europa.eu/service/
        # 或者舊的 Statistical Data Warehouse (SDW) https://sdw-wsrest.ecb.europa.eu/service/
        # 新的門戶 API (data-api.ecb.europa.eu) 是推薦的
        ecb = sdmx.Request('ECB', sdmx_version='2.1') # 指定源為 ECB
        
        # 探索：獲取可用的數據流 (dataflows) 列表
        # 這是一個比較大的請求，可能需要較長時間，並且返回很多數據流
        # 我們只嘗試獲取數據流列表的元數據，而不是所有數據流的完整結構
        logger.info("ECB API 探索：嘗試獲取數據流列表 (可能耗時)...")
        
        # dataflows_response = ecb.dataflow() # 這會獲取所有 dataflow 的詳細定義
        # 改為獲取 dataflow 列表的簡要信息 (如果 API 支持，或者限制返回數量)
        # 根據 ECB Data Portal API 文檔，獲取所有數據流的端點是 /dataflow/{agencyID}/{resourceID}/{version}
        # 例如 /dataflow/ECB/all/latest
        # pandasdmx 可能沒有直接的方法只列出 dataflow ID 和名稱，它通常下載整個結構定義
        # 這裡我們嘗試獲取一個已知的、較小的數據流的結構，以驗證連通性
        
        # 嘗試獲取一個已知的 dataflow ID, 例如 'EXR' (Exchange Rates)
        flow_id = 'EXR'
        logger.info(f"ECB API 探索：嘗試獲取數據流 '{flow_id}' 的結構定義...")
        # dataflow_msg = ecb.dataflow(flow_id) # 獲取特定數據流的定義
        # 改為直接獲取該數據流的數據，例如歐元對美元的日匯率
        # key for exchange rates: D.USD.EUR.SP00.A (Daily, USD per EUR, Spot, Average)
        # 根據新 API 文檔: https://data.ecb.europa.eu/data/datasets/EXR
        # 數據集代碼: EXR, key: D.USD.EUR.SP00.A
        # pandasdmx 構造 key: 'D.USD.EUR.SP00.A'
        # resource_id = 'EXR'
        # key = {'CURRENCY': ['USD', 'JPY'], 'CURRENCY_DENOM': 'EUR', 'EXR_TYPE': 'SP00', 'FREQ': 'D'}
        # 上述 key 的構造方式更為 SDMX 標準
        
        # 簡化：嘗試獲取歐元對美元日匯率數據
        # Key for daily EUR-USD exchange rate: EXR.D.USD.EUR.SP00.A
        # pandasdmx Key: D.USD.EUR.SP00.A (resource_id is EXR)
        # params = {'startPeriod': '2023-01-01', 'endPeriod': '2023-01-07'}
        
        # 使用 pandasdmx 0.9+ 的新語法
        # exr_flow = ecb.dataflow('EXR')
        # ds_key = '.'.join(['D', 'USD', 'EUR', 'SP00', 'A']) # Daily, USD, EUR, Spot, Average
        # resp = ecb.data(resource_id='EXR', key=ds_key, params={'startPeriod': '2023'})
        
        # 根據 ECB Data Portal API (data-api.ecb.europa.eu) 的直接 GET 請求格式
        # https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod=2023-01-01&endPeriod=2023-01-07&format=jsondata
        # 我們可以用 requests 直接調用，因為 pandasdmx 對這個新 API 的適配可能還不完美或文檔較少
        import requests
        ecb_data_api_url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
        params = {
            "startPeriod": (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            "endPeriod": datetime.now().strftime('%Y-%m-%d'),
            "format": "jsondata" # 或者 'csvdata'
        }
        logger.info(f"ECB API 探索：直接請求 {ecb_data_api_url} 獲取歐元兌美元匯率...")
        response = requests.get(ecb_data_api_url, params=params, timeout=20)
        response.raise_for_status()
        json_data = response.json()

        if json_data and 'dataSets' in json_data and json_data['dataSets']:
            dataset = json_data['dataSets'][0]
            if 'series' in dataset and dataset['series']:
                series_key = list(dataset['series'].keys())[0]
                observations = dataset['series'][series_key]['observations']
                num_observations = len(observations)
                logger.info(f"成功從 ECB Data Portal API 獲取歐元兌美元匯率數據，共 {num_observations} 個觀察值。")
                # 顯示最後一個觀察值
                last_obs_key = list(observations.keys())[-1] # 時間維度索引
                last_obs_value = observations[last_obs_key][0]
                # 時間維度通常在 structure/dimensions/observation 下的 id='TIME_PERIOD'
                # 在 jsondata 格式中，觀察值的 key 就是日期或時間戳
                # 需要找到時間維度在 structure 中的位置來獲取其名稱
                time_dimension_name = "UnknownTimeDimension"
                if 'structure' in json_data and 'dimensions' in json_data['structure'] and 'observation' in json_data['structure']['dimensions']:
                    for dim in json_data['structure']['dimensions']['observation']:
                        if dim.get('role') == 'time' or dim.get('id') == 'TIME_PERIOD':
                             time_dimension_name = dim.get('name', dim.get('id'))
                             #  jsondata 中 observation 的 key 就是時間維度第0個 value 的 name
                             #  例如 "2023-01-02"
                             #  所以 last_obs_key 通常就是日期字串
                             break
                
                logger.info(f"  最新觀察值 ({last_obs_key}): {last_obs_value}")
                logger.info("ECB Data Portal API 連通性及基本數據獲取成功。")
            else:
                logger.warning("ECB Data Portal API 返回數據集中無 series 數據。")
        else:
            logger.error(f"ECB Data Portal API 返回數據格式非預期或無數據集: {json_data}")

        logger.info("ECB API (新數據門戶) 主要通過 RESTful GET 請求訪問，支持 JSON 和 CSV 格式。"
                    "數據組織為數據集 (datasets)，每個數據集有其維度 (dimensions) 和觀察值 (observations)。"
                    "查詢特定數據通常需要知道數據集ID和構成Key的維度值。"
                    "官方文檔和數據瀏覽器是探索可用數據的關鍵。")

    except ImportError:
        logger.error("pandasdmx 或 requests 套件未安裝。")
    except requests.exceptions.RequestException as req_e:
        logger.error(f"ECB API 請求失敗: {req_e}", exc_info=True)
    except Exception as e:
        logger.error(f"ECB API 測試失敗: {e}", exc_info=True)
    logger.info("--- ECB API 測試結束 ---")

def test_world_bank_api():
    logger.info("--- 開始測試 World Bank API ---")
    try:
        import requests
        # 世界銀行 API v2, 無需金鑰
        # 基礎 URL: http://api.worldbank.org/v2/
        # 獲取指標列表 (indicators) - 可能非常多，需要分頁或篩選
        # 例如，獲取關於 'GDP per capita' 的指標
        
        indicator_search_url = "http://api.worldbank.org/v2/indicator"
        params = {
            "source": "2", # World Development Indicators (WDI) 的 source ID 通常是 2
            "search": "GDP per capita", # 搜索關鍵詞
            "format": "json",
            "per_page": "5" # 只取少量結果作為演示
        }
        logger.info(f"World Bank API 探索：搜索 '{params['search']}' 相關指標...")
        response = requests.get(indicator_search_url, params=params, timeout=20)
        response.raise_for_status()
        json_data = response.json()

        # json_data[0] 包含分頁信息，json_data[1] 包含指標列表
        if json_data and len(json_data) > 1 and json_data[1]:
            indicators = json_data[1]
            logger.info(f"成功從 World Bank API 找到 {len(indicators)} 個關於 '{params['search']}' 的指標 (顯示部分):")
            for indicator in indicators:
                logger.info(f"  ID: {indicator.get('id')}, Name: {indicator.get('name')}, "
                            f"Source: {indicator.get('source', {}).get('value')}")
            logger.info("World Bank API 指標搜索測試成功。")
            
            # 嘗試獲取其中一個指標的數據 (例如，第一個找到的指標)
            if indicators:
                first_indicator_id = indicators[0].get('id')
                country_code = 'US' # 以美國為例
                logger.info(f"World Bank API 探索：獲取指標 '{first_indicator_id}' (國家 '{country_code}') 的歷史數據...")
                
                # 獲取儘可能長的歷史數據 (World Bank 通常是年度數據)
                indicator_data_url_full = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{first_indicator_id}"
                data_params_full = {"format": "json", "per_page": "1000"} # 嘗試獲取多些數據點
                
                data_response_full = requests.get(indicator_data_url_full, params=data_params_full, timeout=30)
                data_response_full.raise_for_status()
                indicator_json_data_full = data_response_full.json()

                if indicator_json_data_full and len(indicator_json_data_full) > 1 and indicator_json_data_full[1]:
                    observations_full = indicator_json_data_full[1]
                    logger.info(f"  成功獲取指標 '{first_indicator_id}' 的數據，共 {len(observations_full)} 條記錄。")
                    if observations_full:
                        # 世界銀行數據通常按年份降序排列
                        first_obs_year = observations_full[-1].get('date') # 最早的
                        last_obs_year = observations_full[0].get('date') # 最新的
                        logger.info(f"    歷史數據範圍: {first_obs_year} 至 {last_obs_year}")
                        logger.info(f"    最新值 ({last_obs_year}): {observations_full[0].get('value')}")
                else:
                    logger.warning(f"  未能獲取指標 '{first_indicator_id}' 的完整歷史數據，或數據為空。")
        else:
            logger.error(f"World Bank API 指標搜索失敗或未返回預期格式: {json_data}")

        logger.info("World Bank API 主要通過 RESTful GET 請求訪問，支持 JSON 和 XML 格式。"
                    "數據按國家 (country) 和指標 (indicator) 組織。可按主題 (topic)、來源 (source) 瀏覽。"
                    "無需 API 金鑰，使用相對寬鬆。時間週期主要是【年度】，部分指標可能有月度或季度。")

    except requests.exceptions.RequestException as req_e:
        logger.error(f"World Bank API 請求失敗: {req_e}", exc_info=True)
    except Exception as e:
        logger.error(f"World Bank API 測試失敗: {e}", exc_info=True)
    logger.info("--- World Bank API 測試結束 ---")

def test_oecd_api():
    logger.info("--- 開始測試 OECD API ---")
    # OECD API 基於 SDMX，類似 ECB，但其速率限制更明確 (每分鐘20次查詢)
    # 無需金鑰
    # 基礎 URL: https://sdmx.oecd.org/public/rest/
    try:
        # import pandasdmx as sdmx # 前面已導入
        # oecd = sdmx.Request("OECD", sdmx_version='2.1')
        logger.info("OECD API 探索：時間週期與歷史數據長度（概念性）。")
        logger.info("  OECD 數據的頻率 (年/季/月等) 和歷史長度由具體數據集 (Dataflow) 和維度鍵決定。")
        logger.info("  例如，'MEI' (Main Economic Indicators) 或 'QNA' (Quarterly National Accounts) 中的系列。")
        logger.info("  查詢特定序列時，可使用 'startPeriod' 和 'endPeriod' 參數限定範圍。")
        logger.info("  由於直接枚舉所有序列或測試每個序列的最大歷史不切實際（且受限於每分鐘20次查詢），")
        logger.info("  此處僅作說明。實際使用時需參考 OECD Data Explorer 確定目標序列並測試其範圍。")
        logger.info("  例如，月度失業率數據 (如 LFS_SEXAGE_I_M.LRHURTTT.AUS.ST.M) 可能有數十年歷史。")
        logger.info("OECD API 初步探索說明完成。")
    except Exception as e:
        logger.error(f"OECD API 測試失敗: {e}", exc_info=True)
    logger.info("--- OECD API 測試結束 ---")

def test_fiscal_data_api():
    logger.info("--- 開始測試 US Treasury FiscalData API ---")
    try:
        import requests # 前面已導入
        # ... (dataset list 探索代碼保持不變) ...
        datasets_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/datasets"
        logger.info(f"FiscalData API 探索：獲取數據集列表 (部分)...") # 已在之前步驟完成，此處簡略或跳過
        # response = requests.get(datasets_url, params={"page[size]": 2, "page[number]": 1}, timeout=15) ...

        # 探索時間週期和歷史長度：以 "Debt to the Penny" 為例
        # 端點: /v2/accounting/od/debt_to_penny
        # 此數據通常是日更的 (每個聯邦政府工作日)
        debt_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
        logger.info(f"FiscalData API 探索 ('Debt to the Penny'): 時間週期與歷史長度...")
        
        # 嘗試獲取最早的一筆數據以確定歷史起點
        # sort=+record_date (升序)
        params_first = {"sort": "+record_date", "page[size]": "1", "page[number]": "1"}
        response_first = requests.get(debt_url, params=params_first, timeout=20)
        response_first.raise_for_status()
        json_first = response_first.json()
        if json_first and "data" in json_first and json_first["data"]:
            first_record = json_first["data"][0]
            logger.info(f"  'Debt to the Penny' 最早記錄日期: {first_record.get('record_date')}")
        else:
            logger.warning("  未能獲取 'Debt to the Penny' 的最早記錄。")
        
        # 獲取總記錄數以了解數據量
        meta_params = {"page[size]": "1", "meta": "true"} # 只獲取元數據
        response_meta = requests.get(debt_url, params=meta_params, timeout=15)
        response_meta.raise_for_status()
        json_meta = response_meta.json()
        total_count = json_meta.get("meta", {}).get("total-count", "未知")
        logger.info(f"  'Debt to the Penny' 總記錄數: {total_count}")
        
        logger.info("FiscalData API 的數據頻率（日/月/年等）和歷史長度因具體數據集而異。")
        logger.info("  例如 'Debt to the Penny' 是日度數據，歷史悠久。")
        logger.info("  API 支持分頁和排序，可用於探索數據範圍。")

    except requests.exceptions.RequestException as req_e:
        logger.error(f"FiscalData API 請求失敗: {req_e}", exc_info=True)
    except Exception as e:
        logger.error(f"FiscalData API 測試失敗: {e}", exc_info=True)
    logger.info("--- US Treasury FiscalData API 測試結束 ---")

def test_bea_api():
    logger.info("--- 開始測試 US Bureau of Economic Analysis (BEA) API ---")
    BEA_USER_ID = os.getenv("BEA_API_KEY")
    if not BEA_USER_ID:
        logger.warning("BEA_API_KEY 未設定，大部分詳細數據探索將受限。")
    
    try:
        import requests # 前面已導入
        # ... (dataset list 探索代碼保持不變) ...
        # params = {"method": "GETDATASETLIST", "ResultFormat": "JSON"} ...
        
        logger.info("BEA API 探索：時間週期與歷史數據長度（概念性）。")
        logger.info("  BEA 數據（如GDP、個人收入）通常按年(A)、季度(Q)、月度(M)發布。")
        logger.info("  查詢特定數據時，需指定 'DatasetName', 'TableName', 'Frequency', 'Year' 等參數。")
        logger.info("  歷史長度因數據集和表格而異，許多核心數據有數十年歷史。")
        logger.info("  例如，NIPA Table 1.1.5 (Gross Domestic Product) 可查詢年度和季度數據。")
        logger.info("  由於需要 UserID 和詳細參數構造，此處僅作說明。請參考 BEA API 文檔。")
        if BEA_USER_ID:
            logger.info("  (有 UserID 時，可嘗試獲取一個已知表格的少量歷史數據作為示例)")
            # 示例：嘗試獲取 NIPA Table 1.1.5 的部分年度數據
            # params_data = {
            #     "UserID": BEA_USER_ID, "method": "GetData", "DatasetName": "NIPA",
            #     "TableName": "T10105", "Frequency": "A", "Year": "2020,2021,2022", 
            #     "ResultFormat": "JSON"
            # }
            # response_data = requests.get("https://apps.bea.gov/api/data/", params=params_data, timeout=20) ...
            pass 

    except Exception as e:
        logger.error(f"BEA API 測試失敗: {e}", exc_info=True)
    logger.info("--- BEA API 測試結束 ---")

def test_bls_api():
    logger.info("--- 開始測試 US Bureau of Labor Statistics (BLS) API ---")
    try:
        import requests # 前面已導入
        import json # 前面已導入
        # ... (獲取 CPI 和失業率數據的代碼保持不變) ...
        
        # 探索時間週期和歷史長度
        # BLS 數據的頻率（月度、季度、年度）由序列ID本身定義。
        # 歷史長度可通過不指定 startyear/endyear 或指定一個很早的 startyear 來測試。
        # 但為避免請求過多數據，我們先獲取序列信息（如果API支持）或查閱文檔。
        
        logger.info("BLS API 探索：時間週期與歷史數據長度。")
        logger.info("  BLS 數據的頻率由序列ID隱含（例如 M-月度, Q-季度, A-年度）。")
        logger.info("  API v2 允許請求長達20年的數據 (如果序列本身有那麼長)。")
        
        # 嘗試獲取一個序列的較長歷史 (例如 CUUR0000SA0 - CPI)
        series_to_test_long = "CUUR0000SA0"
        current_year = datetime.now().year
        twenty_years_ago = str(current_year - 20)
        
        payload_long = {
            "seriesid": [series_to_test_long],
            "startyear": twenty_years_ago, # 嘗試20年
            "endyear": str(current_year)
        }
        logger.info(f"  嘗試獲取序列 {series_to_test_long} 過去20年的數據...")
        response_long = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', 
                                 data=json.dumps(payload_long), headers={'Content-type': 'application/json'}, timeout=30)
        response_long.raise_for_status()
        json_long_data = response_long.json()

        if json_long_data.get('status') == 'REQUEST_SUCCEEDED':
            series_data_long = json_long_data.get('Results', {}).get('series', [])
            if series_data_long and series_data_long[0].get('data'):
                num_points = len(series_data_long[0]['data'])
                first_year = series_data_long[0]['data'][-1].get('year') # 數據通常倒序
                last_year = series_data_long[0]['data'][0].get('year')
                logger.info(f"    成功獲取序列 {series_to_test_long} {num_points} 個數據點，年份範圍: {first_year}-{last_year}")
            else:
                logger.warning(f"    未能獲取序列 {series_to_test_long} 的20年歷史數據，或數據為空。")
        else:
            logger.error(f"    獲取序列 {series_to_test_long} 長期歷史失敗: {json_long_data.get('status')}")

        logger.info("  用戶可通過 BLS 網站的序列查找工具確定序列ID及其可用歷史。")

    except requests.exceptions.RequestException as req_e:
        logger.error(f"BLS API 請求失敗: {req_e}", exc_info=True)
    except Exception as e:
        logger.error(f"BLS API 測試失敗: {e}", exc_info=True)
    logger.info("--- BLS API 測試結束 ---")

def test_twse_api():
    logger.info("--- 開始測試 Taiwan Stock Exchange (TWSE) OpenAPI ---")
    try:
        import requests # 前面已導入
        headers = {'User-Agent': 'Mozilla/5.0 ...'} # (保持不變)
        # ... (MI_INDEX 測試代碼保持不變) ...
        
        logger.info("TWSE OpenAPI 探索：時間週期與歷史數據長度。")
        logger.info("  TWSE OpenAPI 主要提供【日度】數據。")
        logger.info("  - MI_INDEX (大盤統計): 通常提供指定日期的數據。歷史回溯需逐日請求，長度未知。")
        logger.info("  - 個股日成交資訊 (STOCK_DAY_AVG_ALL): 提供指定日期的個股數據。")
        logger.info("  - 個股月成交資訊 (STOCK_MONTH_AVG): 提供指定年月的個股數據。")
        
        # 嘗試獲取一隻股票的近期日成交資訊
        # /openapi/v1/exchangeReport/STOCK_DAY_ALL
        stock_day_url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        # 獲取前一個交易日的數據 (假設)
        # date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        # 由於 TWSE 數據更新時間和假日問題，直接用一個已知的有數據的日期測試更穩定
        test_stock_date = "20231005" 
        stock_params = {"responseExternal": "JSON", "date": test_stock_date}
        logger.info(f"  嘗試獲取 {test_stock_date} 的所有股票日成交資訊 (STOCK_DAY_ALL)...")
        # response_stock_day = requests.get(stock_day_url, params=stock_params, headers=headers, timeout=30)
        # if response_stock_day.status_code == 200:
        #     json_stock_day = response_stock_day.json()
        #     if isinstance(json_stock_day, list) and json_stock_day:
        #         logger.info(f"    成功獲取 {test_stock_date} STOCK_DAY_ALL 數據 {len(json_stock_day)} 筆。"
        #                     f"例如第一筆: Code {json_stock_day[0].get('Code')}, Name {json_stock_day[0].get('Name')}")
        #     else: ...
        logger.info("    為避免請求過大數據，此處不實際獲取 STOCK_DAY_ALL。")
        logger.info("  歷史數據長度：通常需要逐日/逐月請求，API 本身可能不直接提供非常長期的單次查詢。")
        logger.info("  官方 Swagger 頁面是查詢各端點參數和限制的最佳來源。")

    except Exception as e:
        logger.error(f"TWSE OpenAPI 測試失敗: {e}", exc_info=True)
    logger.info("--- TWSE OpenAPI 測試結束 ---")

def test_coingecko_api():
    logger.info("--- 開始測試 CoinGecko API ---")
    try:
        import requests # 前面已導入
        # ... (ping 和 simple_price 測試代碼保持不變) ...
        
        logger.info("CoinGecko API 探索：時間週期與歷史數據長度。")
        # 測試獲取比特幣1天內的每小時歷史數據 (免費版通常支持)
        # /coins/{id}/market_chart?vs_currency=usd&days=1&interval=hourly
        coin_id = "bitcoin"
        vs_currency = "usd"
        days_history = "1" # 獲取1天數據
        # interval: daily, hourly (1-90 days for hourly, >90 days for daily)
        # For minute data, usually need paid plan.
        chart_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        chart_params = {"vs_currency": vs_currency, "days": days_history, "interval": "hourly"}
        
        logger.info(f"  嘗試獲取 {coin_id} 過去 {days_history} 天的每小時圖表數據...")
        response_chart = requests.get(chart_url, params=chart_params, timeout=20)
        response_chart.raise_for_status()
        chart_data = response_chart.json()

        if chart_data and chart_data.get("prices"):
            logger.info(f"    成功獲取 {coin_id} 每小時圖表數據 {len(chart_data['prices'])} 筆。")
            if chart_data['prices']:
                first_p = chart_data['prices'][0]
                last_p = chart_data['prices'][-1]
                logger.info(f"      時間範圍: {datetime.fromtimestamp(first_p[0]/1000)} (價:{first_p[1]}) "
                            f"至 {datetime.fromtimestamp(last_p[0]/1000)} (價:{last_p[1]})")
        else:
            logger.warning(f"    未能獲取 {coin_id} 的每小時圖表數據。")
        
        # 測試日線歷史長度 (例如，過去一年)
        days_history_long = "365"
        chart_params_daily = {"vs_currency": vs_currency, "days": days_history_long} # interval 會自動變為 daily
        logger.info(f"  嘗試獲取 {coin_id} 過去 {days_history_long} 天的日線圖表數據...")
        response_chart_daily = requests.get(chart_url, params=chart_params_daily, timeout=30)
        response_chart_daily.raise_for_status()
        chart_data_daily = response_chart_daily.json()
        if chart_data_daily and chart_data_daily.get("prices"):
             logger.info(f"    成功獲取 {coin_id} 日線圖表數據 {len(chart_data_daily['prices'])} 筆。")
             if chart_data_daily['prices']:
                logger.info(f"      最早日期: {datetime.fromtimestamp(chart_data_daily['prices'][0][0]/1000).strftime('%Y-%m-%d')}")
        else:
            logger.warning(f"    未能獲取 {coin_id} 的一年日線圖表數據。")

        logger.info("CoinGecko API:")
        logger.info("  - 時間週期: /market_chart 端點支持 'daily' 和 'hourly' (小時數據最多回溯90天)。分鐘級數據通常需付費。")
        logger.info("  - 歷史長度: 日線數據可回溯多年 (具體取決於幣種)。免費版對請求頻率有限制。")

    except Exception as e: # 統一捕獲，因為前面已有 HTTPError 的特定處理
        logger.error(f"CoinGecko API 測試失敗: {e}", exc_info=True)
    logger.info("--- CoinGecko API 測試結束 ---")


def test_yfinance_historical_data_and_降級(): # 重命名函式
    logger.info("--- 開始 yfinance 重點測試 (時間週期降級與歷史數據) ---") # 更新日誌訊息
    report_lines = ["\n=== yfinance API 測試報告 (時間週期降級與歷史數據) ==="]

    # 從您提供的列表中提取代號，並分類或選擇代表
    yfinance_symbols_map = {
        "日本3年期國債收益率": "^JP3Y", "日經指數日元主連": "^N225", "費城半導體指數": "^SOX",
        "標普500指數": "^GSPC", "恆生指數": "^HSI", "納斯達克綜合指數": "^IXIC",
        "道瓊斯指數": "^DJI", "富時中國A50指數ETF": "FXI", "上證指數": "000001.SS",
        "國企指數": "^HSCE", "納斯達克100指數期貨": "NQ=F", "標普500指數期貨": "ES=F",
        "道瓊斯指數期貨": "YM=F", "標普500波動率指數": "^VIX", "VIX期貨": "VX=F",
        "日本10年期國債收益率": "^JP10Y", "美國10年期國債收益率": "^TNX", "日本5年期國債收益率": "^JP5Y",
        "台灣加權指數": "^TWII",
        "小麥期貨": "ZW=F", "玉米期貨": "ZC=F", "白銀期貨": "SI=F", "銅期貨": "HG=F",
        "活牛期貨": "LE=F", "日圓兌美元": "JPY=X", "比特幣美元": "BTC-USD",
        "WTI原油期貨": "CL=F", "黃金期貨": "GC=F", "30年美債期貨": "ZB=F",
        "10年美債期貨": "ZN=F", "2年美債期貨": "ZT=F", "黃金ETF": "GLD",
        "Unity Software": "U", "帕沃英格索": "POWI", "聯電": "UMC", "微軟": "MSFT",
        "Meta Platforms": "META", "阿里巴巴": "BABA", "博通": "AVGO", "Arm Holdings": "ARM",
        "特斯拉": "TSLA", "蘋果": "AAPL", "英特爾": "INTC", "谷歌-C": "GOOG",
        "美國超微公司": "AMD", "台積電ADR": "TSM", "阿斯麥": "ASML", "英偉達": "NVDA",
        "瑞銀": "UBS", "中國平安(HK)": "2318.HK", "小米集團-W(HK)": "1810.HK",
        "中芯國際(HK)": "0981.HK", "Adobe": "ADBE", "摩根士丹利": "MS",
        "摩根大通": "JPM", "高盛": "GS",
        "短期期貨恐慌指數ETF": "VIXY", "區域銀行指數ETF": "KRE", "中國海外互聯網ETF": "KWEB",
        "金融行業ETF": "XLF", "美國國債1-3ETF": "SHY", "美國國債3-7ETF": "IEI",
        "20年以上美國國債ETF": "TLT", "美國原油ETF": "USO", "能源指數ETF": "XLE",
        "Breakwave乾散貨航運ETF": "BDRY", "1.5倍做空短期期貨恐慌指數ETF": "UVXY",
        "鈾與核能ETF": "NLR",
        "大阪日經指數期貨": "NKD=F", 
        "三個月SOFR期貨": "SR3=F"
    }
    
    report_lines.append(f"測試時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report_lines.append("yfinance 主要通過 yf.Ticker('SYMBOL') 對象獲取數據。")
    report_lines.append("  - .info: 公司基本資料 (字典)。")
    report_lines.append("  - .history(period, interval, start, end): OHLCV歷史數據 (Pandas DataFrame)。")
    report_lines.append("  - 返回數據格式: Pandas DataFrame (針對 .history()), dict (針對 .info)。\n")

    target_years = [1990, 1980] 
    intervals_to_try = ['1m', '5m', '1h', '1d', '1wk', '1mo']
    
    symbols_for_降級測試 = {"台灣加權指數": "^TWII", "蘋果公司": "AAPL", "WTI原油期貨": "CL=F"}

    import yfinance as yf 

    for name, symbol in symbols_for_降級測試.items():
        report_lines.append(f"\n--- 對 '{symbol}' ({name}) 進行時間週期降級測試 ---")
        successfully_fetched_at_any_interval_for_symbol = False # 更名以區分
        for year_to_check in target_years:
            report_lines.append(f"  嘗試查詢年份: {year_to_check}")
            fetched_for_this_year_and_symbol = False # 更名以區分
            for interval in intervals_to_try:
                if fetched_for_this_year_and_symbol:
                    break

                start_date_param_str = f"{year_to_check}-01-01"
                if interval == '1m':
                    end_date_param_str = (datetime.strptime(start_date_param_str, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
                elif interval in ['2m', '5m', '15m', '30m'] or 'h' in interval : 
                    end_date_param_str = (datetime.strptime(start_date_param_str, "%Y-%m-%d") + timedelta(days=59)).strftime("%Y-%m-%d")
                else: 
                    end_date_param_str = f"{year_to_check}-12-31"
                
                if datetime.strptime(end_date_param_str, "%Y-%m-%d") < datetime.strptime(start_date_param_str, "%Y-%m-%d"):
                     end_date_param_str = start_date_param_str 

                report_lines.append(f"    嘗試 interval: '{interval}', 日期範圍: {start_date_param_str} to {end_date_param_str}")
                log_prefix = f"yfinance_降級測試 ({symbol}, {interval}, {year_to_check}):"
                data = None 
                try:
                    ticker_obj = yf.Ticker(symbol)
                    data = ticker_obj.history(start=start_date_param_str, 
                                              end=end_date_param_str, 
                                              interval=interval,
                                              auto_adjust=False, 
                                              actions=False) 
                    respectful_sleep(0.5, 1.2) 

                    if data is not None and not data.empty:
                        first_date_obj = data.index[0]
                        last_date_obj = data.index[-1]
                        time_format = '%Y-%m-%d %H:%M:%S %Z%z' if interval not in ['1d','5d','1wk','1mo','3mo'] else '%Y-%m-%d'
                        first_date_str = first_date_obj.strftime(time_format) if isinstance(first_date_obj, pd.Timestamp) else str(first_date_obj)
                        last_date_str = last_date_obj.strftime(time_format) if isinstance(last_date_obj, pd.Timestamp) else str(last_date_obj)
                        
                        report_lines.append(f"      成功: 獲取到 {len(data)} 條數據。")
                        report_lines.append(f"        最早: {first_date_str}, 最新: {last_date_str}")
                        logger.info(f"{log_prefix} 成功獲取 {len(data)} 條數據。最早:{first_date_str}, 最新:{last_date_str}")
                        successfully_fetched_at_any_interval_for_symbol = True
                        fetched_for_this_year_and_symbol = True 
                    elif data is not None and data.empty:
                        report_lines.append(f"      失敗: 返回空的 DataFrame。")
                        logger.warning(f"{log_prefix} 返回空的 DataFrame。")
                    else: 
                        report_lines.append(f"      失敗: API 返回 None。")
                        logger.error(f"{log_prefix} API 返回 None。")
                
                except Exception as e:
                    error_msg = str(e).replace('\n', ' ').strip()
                    report_lines.append(f"      錯誤: {type(e).__name__} - {error_msg}")
                    logger.error(f"{log_prefix} 發生錯誤: {type(e).__name__} - {error_msg}", exc_info=False)
                    if "No data found for this date range" in str(e) or \
                       "No price data found" in str(e) or \
                       "history() got an unexpected keyword argument 'actions'" in str(e) or \
                       "single positional argument when using `start` and `end`" in str(e) or \
                       ("period=" in str(e) and "is invalid" in str(e)):
                        logger.info(f"{log_prefix} 錯誤提示無數據或參數問題，將嘗試下一個較粗的時間週期。")
                
                if fetched_for_this_year_and_symbol:
                    report_lines.append(f"      由於在 {interval} 週期已獲取到 {year_to_check} 年數據，停止對此年份更粗週期的嘗試。")
                    break 

            if not fetched_for_this_year_and_symbol:
                 report_lines.append(f"    注意: 未能在任何嘗試的頻率下獲取到 {year_to_check} 年的 '{symbol}' ({name}) 數據。")
        
        if not successfully_fetched_at_any_interval_for_symbol:
             report_lines.append(f"  結論: 未能通過降級策略在任何測試頻率下獲取到 '{symbol}' ({name}) 在目標年份的歷史數據。")

    report_lines.append("\n\n--- 其他 yfinance 代號日線歷史數據長度測試 (period='max') ---")
    limited_general_symbols = dict(list(yfinance_symbols_map.items())[3:10]) 

    for name, symbol in limited_general_symbols.items():
        if symbol in symbols_for_降級測試.values(): 
            continue
        report_lines.append(f"\n  測試代號: {symbol} ({name})")
        log_prefix = f"yfinance_hist_len ({symbol}, daily_max):"
        data = None
        try:
            ticker_obj = yf.Ticker(symbol)
            data = ticker_obj.history(period="max", interval="1d", auto_adjust=False, actions=False)
            respectful_sleep(0.5, 1.0)

            if data is not None and not data.empty:
                first_date_str = data.index[0].strftime('%Y-%m-%d')
                last_date_str = data.index[-1].strftime('%Y-%m-%d')
                report_lines.append(f"    成功獲取 {len(data)} 條日線數據。")
                report_lines.append(f"    歷史範圍: {first_date_str} 至 {last_date_str}")
                logger.info(f"{log_prefix} 成功獲取 {len(data)} 條日線數據。範圍: {first_date_str} 至 {last_date_str}")
            elif data is not None and data.empty:
                report_lines.append(f"    失敗: 返回空的 DataFrame。")
                logger.warning(f"{log_prefix} 返回空的 DataFrame。")
            else:
                report_lines.append(f"    失敗: API 返回 None。")
                logger.error(f"{log_prefix} API 返回 None。")
        except Exception as e:
            error_msg = str(e).replace('\n', ' ').strip()
            report_lines.append(f"    錯誤: {type(e).__name__} - {error_msg}")
            logger.error(f"{log_prefix} 發生錯誤: {type(e).__name__} - {error_msg}", exc_info=False)

    report_lines.append("\n\n--- yfinance API 測試總結 (歷史與降級) ---") # 更新總結標題
    report_lines.append("  - yfinance 無需 API 金鑰，但易受 Yahoo Finance 網站變動和速率限制影響。")
    report_lines.append("  - 小時線 (interval='1h') 數據約有730天歷史限制。")
    report_lines.append("  - 分鐘線 (e.g., '1m', '5m') 數據歷史非常短 (通常 <7-60天)，且對請求的日期範圍有嚴格限制。")
    report_lines.append("  - 對於非常早期的歷史數據 (如1980, 1990)，分鐘線和小時線基本不可用。")
    report_lines.append("  - 降級策略 (從分鐘/小時 -> 日 -> 週 -> 月) 是處理高頻數據缺失的有效方法，但需注意yfinance對歷史短間隔數據的日期範圍限制。")
    report_lines.append("  - 錯誤處理對於 yfinance 至關重要，其錯誤信息多樣，有時不直接拋出異常而是返回空DataFrame。")
    
    logger.info("--- yfinance 重點測試 (歷史與降級) 結束 ---") # 更新日誌訊息
    return report_lines


def test_yfinance_rate_limiting_and_mitigation(num_tickers_to_test=20, iteration_delay_raw=0.1, iteration_delay_stable=0.2):
    """
    專門測試 yfinance 的速率限制行為以及我們緩解機制的有效性。
    """
    report_lines = ["\n\n=== yfinance 速率限制與緩解機制實測 ==="]
    logger.info("--- 開始 yfinance 速率限制與緩解機制實測 ---")
    report_lines.append(f"測試時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    candidate_symbols = [ # 選一些常見的美股代號
        "MSFT", "META", "BABA", "AVGO", "ARM", "TSLA", "AAPL", "INTC", "GOOG", "AMD",
        "TSM", "ASML", "NVDA", "UBS", "ADBE", "MS", "JPM", "GS", "U", "POWI",
        "UMC", "PYPL", "NFLX", "DIS", "V", "MA", "CSCO", "ORCL", "CRM", "QCOM"
    ] # 30個
    if len(candidate_symbols) > num_tickers_to_test:
        test_symbols = random.sample(candidate_symbols, num_tickers_to_test)
    else:
        test_symbols = candidate_symbols
    
    report_lines.append(f"  將使用以下 {len(test_symbols)} 個股票代號進行測試: {', '.join(test_symbols)}")
    report_lines.append(f"  (原始調用循環間延遲: {iteration_delay_raw}秒, 穩定調用循環間延遲: {iteration_delay_stable}秒)\n")


    # 測試 1: 直接、快速、無我們封裝的緩解地調用 yf.Ticker().info
    report_lines.append("  測試 1: 直接、快速調用 yf.Ticker().info (無外部緩解)")
    logger.info(f"  測試 1: 直接調用 yf.Ticker().info 對 {len(test_symbols)} 個代號...")
    success_count_raw = 0
    failure_count_raw = 0
    errors_raw = {}
    start_time_raw = time.time()

    for i, symbol in enumerate(test_symbols):
        log_prefix = f"yfinance_raw_ratelimit ({symbol}, info):"
        try:
            if i > 0: time.sleep(iteration_delay_raw) 
            ticker = yf.Ticker(symbol)
            info = ticker.info 
            if info and info.get('symbol') == symbol and info.get('regularMarketPrice') is not None: # 檢查是否有價格數據
                logger.debug(f"{log_prefix} 成功獲取 {symbol} 的 info。")
                success_count_raw += 1
            else:
                logger.warning(f"{log_prefix} 獲取 {symbol} info 成功但內容不符、為空或無價格數據。")
                failure_count_raw +=1
                errors_raw.setdefault("EmptyOrIncompleteInfo", 0)
                errors_raw["EmptyOrIncompleteInfo"] += 1
        except Exception as e:
            error_type_name = type(e).__name__
            logger.error(f"{log_prefix} 獲取 {symbol} info 時發生錯誤: {error_type_name} - {str(e)[:100]}")
            failure_count_raw += 1
            errors_raw.setdefault(error_type_name, 0)
            errors_raw[error_type_name] += 1
            if "Limit" in str(e) or "Error" in str(e) or "failed to decrypt" in str(e).lower() or "404" in str(e):
                report_lines.append(f"    - 在請求 '{symbol}' (第{i+1}個) 時遇到錯誤: {error_type_name} - {str(e)[:100]}")

        if i % 5 == 0 and i > 0 : respectful_sleep(0.1, 0.2) # 每5個稍微停一下，模擬一點點間隔

    end_time_raw = time.time()
    time_taken_raw = end_time_raw - start_time_raw
    report_lines.append(f"    直接調用結果: {success_count_raw} 成功, {failure_count_raw} 失敗。耗時: {time_taken_raw:.2f} 秒。")
    if errors_raw:
        report_lines.append(f"    原始調用錯誤統計: {errors_raw}")
    logger.info(f"  測試 1 (原始調用) 完成。成功: {success_count_raw}, 失敗: {failure_count_raw}。耗時: {time_taken_raw:.2f} 秒。錯誤: {errors_raw}")
    
    report_lines.append(f"    在兩組測試間等待 10 秒...")
    logger.info(f"  在兩組測試間等待 10 秒...")
    time.sleep(10) 

    # 測試 2: 使用我們 market_data_yfinance.py 中的 fetch_yfinance_data_stable 獲取 .history()
    report_lines.append(f"\n  測試 2: 使用 market_data_yfinance.fetch_yfinance_data_stable 獲取短週期歷史數據")
    logger.info(f"  測試 2: 使用 fetch_yfinance_data_stable 獲取 {len(test_symbols)} 個股票的1個月日線歷史...")
    success_count_stable = 0
    failure_count_stable = 0
    errors_stable = {}
    start_time_stable = time.time()

    try:
        if 'market_data_yfinance' not in globals() and 'mdyf' not in globals():
            import market_data_yfinance as mdyf
        elif 'mdyf' not in globals():
             mdyf = globals()['market_data_yfinance']
    except ImportError:
        logger.error("無法導入 market_data_yfinance 模組！測試無法繼續。")
        report_lines.append("  錯誤: 無法導入 market_data_yfinance 模組！")
        return report_lines


    for i, symbol in enumerate(test_symbols):
        log_prefix = f"yfinance_stable_ratelimit ({symbol}, history):"
        try:
            if i > 0: time.sleep(iteration_delay_stable)
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            data = mdyf.fetch_yfinance_data_stable(symbol, start_date, end_date, interval="1d", attempt_hourly_first=False)
            
            if data is not None and not data.empty:
                logger.debug(f"{log_prefix} 成功獲取 {symbol} 的歷史數據 {len(data)} 條。")
                success_count_stable += 1
            else:
                logger.warning(f"{log_prefix} 未能獲取 {symbol} 的歷史數據 (返回空或None)。")
                failure_count_stable += 1
                errors_stable.setdefault("EmptyOrNoneData", 0)
                errors_stable["EmptyOrNoneData"] += 1
        except Exception as e:
            error_type_name = type(e).__name__
            logger.error(f"{log_prefix} 獲取 {symbol} 歷史數據時發生錯誤: {error_type_name} - {str(e)[:100]}")
            failure_count_stable += 1
            errors_stable.setdefault(error_type_name, 0)
            errors_stable[error_type_name] += 1
            report_lines.append(f"    - 在請求 '{symbol}' (第{i+1}個) 時遇到錯誤: {error_type_name} - {str(e)[:100]}")

    end_time_stable = time.time()
    time_taken_stable = end_time_stable - start_time_stable
    report_lines.append(f"    使用 fetch_yfinance_data_stable 結果: {success_count_stable} 成功, {failure_count_stable} 失敗。耗時: {time_taken_stable:.2f} 秒。")
    if errors_stable:
        report_lines.append(f"    穩定調用錯誤統計: {errors_stable}")
    report_lines.append(f"    (注意: fetch_yfinance_data_stable 包含快取和重試機制。)")
    logger.info(f"  測試 2 (穩定調用) 完成。成功: {success_count_stable}, 失敗: {failure_count_stable}。耗時: {time_taken_stable:.2f} 秒。錯誤: {errors_stable}")

    report_lines.append("\n  yfinance 速率限制與緩解機制測試總結:")
    report_lines.append("  - 直接、快速、無我們封裝的緩解機制的 yfinance 調用 (如 .info) 在連續請求多個代號時，失敗率可能較高，或返回不完整數據，表明易受速率限制影響。")
    report_lines.append("  - 使用帶有快取、延遲、抖動和指數退避重試的封裝函式 (如 fetch_yfinance_data_stable) 能顯著提高批量數據獲取的成功率和系統穩健性。")
    report_lines.append("  - 即便如此，面對 Yahoo Finance 不透明且可能變動的限制策略，仍無法保證100%成功，尤其是在雲端環境或共享IP下，或請求極其頻繁時。")
    report_lines.append("  - 快取是避免重複請求相同數據的最有效方法。對於新數據請求，重試和退避機制是關鍵。")
    logger.info("--- yfinance 速率限制與緩解機制實測結束 ---")
    return report_lines


# --- 主測試執行流程 ---
if __name__ == "__main__":
    logger.info("===== 開始 API 深度測試 =====")
    master_report = [] # 用於收集所有 API 的測試報告行

    # 執行付費 API 的初步測試 (如果金鑰存在)
    logger.info("執行付費 API 測試...")
    fred_report = test_fred_api() 
    if fred_report: master_report.extend(fred_report)
    respectful_sleep()
    alpha_vantage_report = test_alpha_vantage_api()
    if alpha_vantage_report: master_report.extend(alpha_vantage_report)
    respectful_sleep()
    finnhub_report = test_finnhub_api()
    if finnhub_report: master_report.extend(finnhub_report)
    respectful_sleep()
    news_api_report = test_news_api()
    if news_api_report: master_report.extend(news_api_report)
    respectful_sleep()
    fmp_report = test_fmp_api()
    if fmp_report: master_report.extend(fmp_report)
    respectful_sleep()
    polygon_report = test_polygon_api()
    if polygon_report: master_report.extend(polygon_report)
    respectful_sleep()

    # 執行免費/公開 API 的測試
    logger.info("執行免費/公開 API 測試...")
    ecb_report = test_ecb_api()
    if ecb_report: master_report.extend(ecb_report)
    respectful_sleep()
    world_bank_report = test_world_bank_api()
    if world_bank_report: master_report.extend(world_bank_report)
    respectful_sleep()
    oecd_report = test_oecd_api()
    if oecd_report: master_report.extend(oecd_report)
    respectful_sleep()
    fiscal_data_report = test_fiscal_data_api()
    if fiscal_data_report: master_report.extend(fiscal_data_report)
    respectful_sleep()
    bea_report = test_bea_api()
    if bea_report: master_report.extend(bea_report)
    respectful_sleep()
    bls_report = test_bls_api()
    if bls_report: master_report.extend(bls_report)
    respectful_sleep()
    twse_report = test_twse_api()
    if twse_report: master_report.extend(twse_report)
    respectful_sleep()
    coingecko_report = test_coingecko_api()
    if coingecko_report: master_report.extend(coingecko_report)
    respectful_sleep()
    
    # 觀察 yfinance 的行為
    logger.info("執行 yfinance 重點測試 (歷史與降級)...") # 更新日誌
    yfinance_hist_report = test_yfinance_historical_data_and_降級() # 更新函式名稱
    if yfinance_hist_report: master_report.extend(yfinance_hist_report)
    respectful_sleep()

    logger.info("執行 yfinance 速率限制與緩解機制實測...")
    yfinance_rate_limit_report = test_yfinance_rate_limiting_and_mitigation()
    if yfinance_rate_limit_report: master_report.extend(yfinance_rate_limit_report)


    # 在所有測試完成後，將 master_report 寫入檔案
    report_dir = "test_reports"
    os.makedirs(report_dir, exist_ok=True)
    report_filepath = os.path.join(report_dir, f"api_deep_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    try:
        with open(report_filepath, "w", encoding="utf-8") as f:
            for line in master_report:
                f.write(line + "\n")
        logger.info(f"詳細的 API 測試報告已儲存至: {report_filepath}")
    except Exception as write_e:
        logger.error(f"寫入測試報告失敗: {write_e}")

    logger.info("===== API 深度測試執行完畢 =====")
    logger.info("請檢查日誌輸出以了解各 API 的測試情況。")
    logger.info("提醒：本腳本目前僅包含框架，詳細的 API 功能探索和速率限制測試邏輯需在後續步驟中填充。")
    logger.info(f"請確保您的 .env 檔案已正確配置在專案根目錄下，並包含了所需的 API 金鑰。")

# TODO for next steps:
# 1. 逐步在每個 test_xxx_api() 函式中實現連通性測試。
# 2. 接著實現數據列表、時間週期、歷史長度探索。
# 3. 設計並實現速率限制下的緩解機制測試。
# 4. 為其他免費 API (OECD, FiscalData, BEA, BLS, TWSE, CoinGecko) 補充測試函式框架和呼叫。
# 5. 考慮將通用的 API 請求邏輯 (如帶重試、快取的 GET 請求) 提取為輔助函式。
#    (雖然我們已有的 market_data_xxx 模組有類似實現，但這些 API 可能需要不同的客戶端庫或請求方式)
