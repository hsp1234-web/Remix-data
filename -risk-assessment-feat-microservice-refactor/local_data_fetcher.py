# local_data_fetcher.py

import os
import logging
import pandas as pd
from datetime import datetime, timedelta

# --- 配置 ---
OUTPUT_DIR = "local_sample_data"
FRED_API_KEY = os.getenv("FRED_API_KEY") # 請確保在您的本地環境中設定了 FRED_API_KEY

# 指標定義
FRED_INDICATORS = {
    "FRAOIS": "FRAOIS",         # FRED - FRA-OIS 3-Month Spread
    "TEDRATE": "TEDRATE",       # FRED - TED Spread
    "SOFR": "SOFR",             # FRED - Secured Overnight Financing Rate
    "DGS10": "DGS10",           # FRED - 10-Year Treasury Constant Maturity Rate
    "DGS2": "DGS2",             # FRED - 2-Year Treasury Constant Maturity Rate
    "WRESBAL": "WRESBAL",       # FRED - Reserve Balances with Federal Reserve Banks (Weekly)
    "RRPONTSYD": "RRPONTSYD",   # FRED - Overnight Reverse Repurchase Agreements
    "VIXCLS": "VIXCLS"          # FRED - CBOE Volatility Index: VIX Close
}

YFINANCE_TICKERS = {
    "VIX_YF": "^VIX",           # Yahoo Finance - CBOE Volatility Index
    "TLT_YF": "TLT",            # Yahoo Finance - iShares 20+ Year Treasury Bond ETF
    "SPY_YF": "SPY"             # Yahoo Finance - SPDR S&P 500 ETF Trust
}

# 數據獲取時間範圍 (例如，過去3年)
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=3*365)

start_date_str = START_DATE.strftime('%Y-%m-%d')
end_date_str = END_DATE.strftime('%Y-%m-%d')

# --- 日誌設定 ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

def fetch_fred_data():
    logger.info("--- 開始獲取 FRED 數據 ---")
    if not FRED_API_KEY:
        logger.error("錯誤：FRED_API_KEY 環境變數未設定。請在您的本地環境中設定它。")
        logger.error("您可以從 FRED 官網申請免費的 API Key: https://fred.stlouisfed.org/docs/api/api_key.html")
        return

    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
    except ImportError:
        logger.error("錯誤：fredapi 庫未安裝。請運行 'pip install fredapi'")
        return
    except Exception as e:
        logger.error(f"初始化 Fred 客戶端時發生錯誤: {e}")
        return

    for internal_name, series_id in FRED_INDICATORS.items():
        try:
            logger.info(f"正在獲取 FRED 指標: {internal_name} (Series ID: {series_id})...")
            data = fred.get_series(series_id, observation_start=start_date_str, observation_end=end_date_str)
            if data.empty:
                logger.warning(f"指標 {internal_name} ({series_id}) 返回數據為空。")
                continue

            df = data.to_frame(name='value') # 將 Series 轉換為 DataFrame 並命名列為 'value'
            if df.index.tz is not None: # 確保索引是 tz-naive
                df.index = df.index.tz_localize(None)
            df.index.name = 'date' # 確保索引名為 'date'

            filepath = os.path.join(OUTPUT_DIR, f"{internal_name}.csv")
            df.to_csv(filepath)
            logger.info(f"已將 {internal_name} 數據保存到 {filepath} ({len(df)} 行)")
        except Exception as e:
            logger.error(f"獲取或保存 FRED 指標 {internal_name} ({series_id}) 時出錯: {e}")

def fetch_yfinance_data():
    logger.info("\n--- 開始獲取 Yahoo Finance 數據 ---")
    try:
        import yfinance as yf
    except ImportError:
        logger.error("錯誤：yfinance 庫未安裝。請運行 'pip install yfinance'")
        return

    for internal_name, ticker_symbol in YFINANCE_TICKERS.items():
        try:
            logger.info(f"正在獲取 Yahoo Finance Ticker: {internal_name} (Symbol: {ticker_symbol})...")
            # yfinance 的 end date 是不包含的，所以需要加一天
            end_date_yf_str = (END_DATE + timedelta(days=1)).strftime('%Y-%m-%d')

            data_yf = yf.download(ticker_symbol, start=start_date_str, end=end_date_yf_str, progress=False)

            if data_yf.empty:
                logger.warning(f"Ticker {internal_name} ({ticker_symbol}) 返回數據為空。")
                continue

            # 我們主要關心 'Close' 價格作為 'value'
            if 'Close' not in data_yf.columns:
                logger.warning(f"Ticker {internal_name} ({ticker_symbol}) 返回的數據中沒有 'Close' 列。")
                continue

            df = data_yf[['Close']].rename(columns={'Close': 'value'})
            if df.index.tz is not None: # 確保索引是 tz-naive
                df.index = df.index.tz_localize(None)
            df.index.name = 'date'

            filepath = os.path.join(OUTPUT_DIR, f"{internal_name}.csv")
            df.to_csv(filepath)
            logger.info(f"已將 {internal_name} 數據保存到 {filepath} ({len(df)} 行)")
        except Exception as e:
            logger.error(f"獲取或保存 Yahoo Finance Ticker {internal_name} ({ticker_symbol}) 時出錯: {e}")

if __name__ == "__main__":
    logger.info(f"本地數據獲取腳本開始執行。數據將保存到 '{OUTPUT_DIR}' 目錄。")
    logger.info(f"數據時間範圍: {start_date_str} 到 {end_date_str}")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"已創建輸出目錄: {OUTPUT_DIR}")

    fetch_fred_data()
    fetch_yfinance_data()

    logger.info("\n本地數據獲取腳本執行完畢。")
    logger.info(f"請檢查 '{OUTPUT_DIR}' 目錄下的 CSV 文件。")
    logger.info("如果 FRED 數據獲取失敗，請確保您已在本地環境中正確設定了 FRED_API_KEY 環境變數。")
    logger.info("完成後，請將 " + OUTPUT_DIR + " 目錄打包為 sample_data.zip 並上傳到沙盒環境。")
