# fetchers/crypto_fetcher.py
import pandas as pd
import requests
import logging
import time
import random
from typing import Optional, Dict, Any
from datetime import datetime

from ..interfaces.data_fetcher_interface import DataFetcherInterface
# 考慮使用 requests_cache 來快取 API 請求
# import requests_cache

class CryptoFetcher(DataFetcherInterface):
    """
    使用 CoinGecko API 獲取加密貨幣市場數據的穩健實現。
    CoinGecko 的免費 API 有速率限制 (通常是每分鐘 10-50 次)。
    """

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, robustness_config: dict, api_key: Optional[str] = None):
        self.config = robustness_config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = api_key # CoinGecko 的某些端點可能需要 API 金鑰以獲得更高頻率或更多數據

        # 若使用 requests_cache，在此處配置
        # requests_cache.install_cache(
        #     self.config.get('cache_path', 'crypto_api_cache'),
        #     backend='sqlite',
        #     expire_after=self.config.get('cache_expire_after_seconds', 3600) # 例如，快取1小時
        # )
        # self.logger.info(f"Requests-cache installed for CryptoFetcher at {self.config.get('cache_path', 'crypto_api_cache')}")


    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """通用請求處理函數，包含重試邏輯。"""
        current_delay = self.config['delay_min_seconds']
        headers = {}
        if self.api_key:
            # CoinGecko Pro API 使用 x-cg-pro-api-key header
            # params = {**(params or {}), 'x_cg_pro_api_key': self.api_key} # 加到 params 或 headers
            headers['x-cg-pro-api-key'] = self.api_key


        for attempt in range(self.config['retries']):
            try:
                self.logger.debug(f"Requesting CoinGecko: {endpoint}, params: {params}, attempt: {attempt+1}")
                response = requests.get(f"{self.BASE_URL}/{endpoint}", params=params, headers=headers, timeout=10) # 10 秒超時
                response.raise_for_status()  # 如果是 4XX 或 5XX 錯誤，則拋出異常

                # CoinGecko 的速率限制通常在 headers 中返回
                # 'x-ratelimit-limit', 'x-ratelimit-remaining', 'x-ratelimit-reset'
                # 可以考慮記錄或處理這些信息

                return response.json()
            except requests.exceptions.HTTPError as http_err:
                self.logger.error(f"HTTP error for {endpoint} on attempt {attempt + 1}: {http_err} - Response: {response.text[:200] if response else 'No response'}")
                if response and response.status_code == 429: # 速率限制錯誤
                    self.logger.warning("Rate limit hit. Increasing delay significantly.")
                    # 從 header 中讀取建議的等待時間 (如果 CoinGecko 提供)
                    # e.g., retry_after = int(response.headers.get("Retry-After", current_delay * 5))
                    sleep_time = current_delay * 5 # 大幅增加延遲
                elif attempt >= self.config['retries'] - 1:
                    self.logger.critical(f"Failed after {self.config['retries']} retries for {endpoint}.")
                    return None
                else:
                    sleep_time = current_delay + random.uniform(0, current_delay * 0.1)

                self.logger.info(f"Waiting {sleep_time:.2f} seconds before retrying {endpoint}...")
                time.sleep(sleep_time)
                current_delay = min(current_delay * self.config.get('backoff_factor', 2), self.config['delay_max_seconds'])

            except requests.exceptions.RequestException as req_err:
                self.logger.error(f"Request error for {endpoint} on attempt {attempt + 1}: {req_err}")
                if attempt >= self.config['retries'] - 1:
                    return None
                sleep_time = current_delay + random.uniform(0, current_delay * 0.1)
                self.logger.info(f"Waiting {sleep_time:.2f} seconds before retrying {endpoint}...")
                time.sleep(sleep_time)
                current_delay = min(current_delay * self.config.get('backoff_factor', 2), self.config['delay_max_seconds'])
            except Exception as e:
                self.logger.error(f"An unexpected error occurred while fetching {endpoint} on attempt {attempt+1}: {e}", exc_info=True)
                if attempt >= self.config['retries'] - 1:
                    return None
                # 標準重試邏輯
                sleep_time = current_delay + random.uniform(0, current_delay * 0.1)
                self.logger.info(f"Waiting {sleep_time:.2f} seconds before retrying {endpoint}...")
                time.sleep(sleep_time)
                current_delay = min(current_delay * self.config.get('backoff_factor', 2), self.config['delay_max_seconds'])
        return None

    def _get_coin_id(self, symbol: str) -> Optional[str]:
        """
        將常見的交易對符號 (e.g., 'BTC-USD', 'ETH') 轉換為 CoinGecko 的 coin ID。
        CoinGecko 的 symbol 通常是小寫的，如 'bitcoin', 'ethereum'。
        對於 'BTC-USD' 這樣的格式，我們通常只關心 'BTC'。
        """
        # 簡化處理：假設 symbol 是 'bitcoin' 或 'BTC-USD' 中的 'BTC' 部分
        # 一個更穩健的方法是維護一個映射或調用 /coins/list API

        # 嘗試直接使用 symbol (小寫) 作為 id
        # 例如，如果 symbol 是 'BTC-USD'，我們可能需要 'bitcoin'
        # 如果是 'SOL'，我們可能需要 'solana'
        # 這裡需要一個從 symbol 到 coingecko id 的映射機制
        # 為了草圖的簡潔，我們假設 symbol 就是 coingecko id (例如 'bitcoin', 'ethereum')
        # 或者，如果 symbol 是 'BTC-USD'，我們取 'BTC' 並轉小寫

        processed_symbol = symbol.split('-')[0].lower()

        # 這裡可以調用 /coins/list 來查找 symbol 對應的 id，但 /coins/list 很大
        # self.logger.info(f"Attempting to use '{processed_symbol}' as CoinGecko ID for symbol '{symbol}'")
        return processed_symbol


    def fetch(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        獲取指定加密貨幣的歷史 OHLCV 數據。
        CoinGecko API 的 /coins/{id}/market_chart/range 端點返回日線數據。

        Args:
            symbol (str): 加密貨幣的 CoinGecko ID (e.g., 'bitcoin', 'ethereum') 或常見代碼 'BTC', 'ETH'.
            start_date (str): 開始日期 'YYYY-MM-DD'.
            end_date (str): 結束日期 'YYYY-MM-DD'.

        Returns:
            Optional[pd.DataFrame]: 包含 OHLCV 數據的 DataFrame，若獲取失敗則返回 None。
        """
        coin_id = self._get_coin_id(symbol)
        if not coin_id:
            self.logger.error(f"Could not determine CoinGecko ID for symbol {symbol}")
            return None

        try:
            start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
            # CoinGecko 的 to timestamp 是包含的，但為了確保拿到 end_date 當天的數據，通常需要加一天
            # 或者確保 to_timestamp 代表 end_date 的結束時刻
            end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp()) + 86399 # end of day
        except ValueError:
            self.logger.error(f"Invalid date format for {symbol}. Expected YYYY-MM-DD.", exc_info=True)
            return None

        # CoinGecko 的 /market_chart/range 端點 vs_currency 預設為 'usd'
        # 它返回 prices, market_caps, total_volumes
        # [timestamp, value]
        # 注意：CoinGecko 的免費 API 不直接提供日線 OHLC！
        # 它提供的是價格點。若要得到 OHLC，需要：
        # 1. 使用付費 API (如 Pro API 的 /coins/{id}/ohlc)
        # 2. 或者，從日線價格數據中自行聚合 (例如，取每日的開盤價，最高/最低/收盤價可能需要更高頻率數據)
        # 為了符合 DataFetcherInterface 的 OHLCV 結構，這裡我們做一個簡化：
        # 假設 /market_chart/range 返回的價格是每日的收盤價。
        # Open, High, Low 將與 Close 相同，Volume 來自 total_volumes。
        # 這是一個重要的簡化，實際應用中可能需要更精確的數據源。

        self.logger.info(f"Fetching market chart for {coin_id} (symbol: {symbol}) from {start_date} to {end_date}")

        # CoinGecko 的 market_chart/range 最大支持約90天日線數據，如果時間範圍過長，需要分段請求
        # 但這裡的 'precision=daily' 選項 (如果 API 支持) 或者默認行為可能避免此問題
        # 免費 API 似乎不直接支持 'precision' 參數。
        # 我們將 vs_currency 設為 'usd'
        params = {
            'vs_currency': 'usd',
            'from': start_timestamp,
            'to': end_timestamp,
            # 'precision': 'daily' # 檢查 CoinGecko 文檔確認是否支持此參數以及 Pro API 要求
        }

        # 如果使用 Pro API key, 可以加入
        # if self.api_key: params['x_cg_pro_api_key'] = self.api_key

        endpoint = f"coins/{coin_id}/market_chart/range"
        raw_data = self._make_request(endpoint, params)

        if not raw_data or 'prices' not in raw_data:
            self.logger.warning(f"No data or prices found for {coin_id} from CoinGecko for the period.")
            return None

        prices = raw_data.get('prices', [])
        market_caps = raw_data.get('market_caps', []) # 通常不需要，但可以記錄
        total_volumes = raw_data.get('total_volumes', [])

        if not prices:
            self.logger.warning(f"Price data is empty for {coin_id} from CoinGecko.")
            return None

        # 將數據轉換為 DataFrame
        df_prices = pd.DataFrame(prices, columns=['timestamp_ms', 'price'])
        df_volumes = pd.DataFrame(total_volumes, columns=['timestamp_ms', 'volume'])

        # 合併價格和交易量
        df = pd.merge(df_prices, df_volumes, on='timestamp_ms', how='left')

        # 時間戳轉換並設為索引 (CoinGecko 返回毫秒級時間戳)
        df['date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.normalize()

        # 簡化處理：將 price 視為 close，並用它填充 open, high, low
        # 這是一個強假設，因為 CoinGecko 免費 API 的 market_chart 不直接提供日 OHLC
        df['open'] = df['price']
        df['high'] = df['price']
        df['low'] = df['price']
        df['close'] = df['price']
        # adj_close 通常用於股票，加密貨幣較少使用，這裡也用 price 填充
        df['adj_close'] = df['price']

        # 選擇並重排欄位以符合 OHLCV 結構
        # 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'
        df = df[['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]

        # 移除時間戳毫秒列
        # df.drop(columns=['timestamp_ms'], inplace=True)

        # 篩選日期範圍 (因為 CoinGecko 返回的可能是 Unix 時間戳的範圍，轉換後可能略有偏差)
        # 但由於我們是用 start_date 和 end_date 的 timestamp 去請求，理論上不需要再次篩選
        # df = df[(df['date'] >= pd.to_datetime(start_date)) & (df['date'] <= pd.to_datetime(end_date))]

        if df.empty:
            self.logger.warning(f"Data became empty after processing for {coin_id}.")
            return None

        self.logger.info(f"Successfully fetched and processed {len(df)} data points for {coin_id} (symbol: {symbol}).")
        return df

# 示例用法 (用於本地測試，不應包含在最終提交的類中)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config_example = {
        'retries': 3,
        'delay_min_seconds': 1,
        'delay_max_seconds': 5,
        'backoff_factor': 2,
        # 'cache_path': 'coingecko_cache.sqlite', # For requests_cache
        # 'cache_expire_after_seconds': 3600
    }
    fetcher = CryptoFetcher(robustness_config=config_example)

    # 測試比特幣
    btc_data = fetcher.fetch(symbol='bitcoin', start_date='2023-01-01', end_date='2023-01-10')
    if btc_data is not None:
        print("\nBitcoin Data:")
        print(btc_data.head())
        print(f"Shape: {btc_data.shape}")

    # 測試以太坊，使用大寫符號 (假設 _get_coin_id 能處理)
    eth_data = fetcher.fetch(symbol='ETH', start_date='2023-01-01', end_date='2023-01-05')
    if eth_data is not None:
        print("\nEthereum Data (using 'ETH' symbol):")
        print(eth_data.head())

    # 測試一個不存在的幣 (預期返回 None)
    # non_existent_data = fetcher.fetch(symbol='nonexistentcoin123', start_date='2023-01-01', end_date='2023-01-05')
    # if non_existent_data is None:
    #     print("\nCorrectly handled non-existent coin.")

    # 測試日期範圍過長，看是否能處理 (CoinGecko 免費 API 可能有90天限制，但 market_chart/range 或許不同)
    # btc_long_data = fetcher.fetch(symbol='bitcoin', start_date='2022-01-01', end_date='2022-06-01')
    # if btc_long_data is not None:
    #     print("\nBitcoin Long Range Data:")
    #     print(btc_long_data.head())
    #     print(btc_long_data.tail())
    #     print(f"Shape: {btc_long_data.shape}")
    # else:
    #     print("\nFailed to fetch long range data or it was empty.")
