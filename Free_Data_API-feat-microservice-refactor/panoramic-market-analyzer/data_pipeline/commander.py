# data_pipeline/commander.py
import yaml
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Type, Optional, List, Any
import os # 用於路徑處理

# 導入接口和具體實現
from .interfaces.data_fetcher_interface import DataFetcherInterface
from .interfaces.database_interface import DatabaseInterface
from .fetchers.yfinance_fetcher import YFinanceFetcher
from .fetchers.fred_fetcher import FredFetcher
from .fetchers.crypto_fetcher import CryptoFetcher
from .database.duckdb_repository import DuckDBRepository
from .processing.processor import DataProcessor # 導入數據處理器

# 設定 Commander 的日誌記錄器
logger = logging.getLogger(__name__) # 使用 __name__ 會得到 'data_pipeline.commander'

class Commander:
    """
    系統的協調器 (Facade)。
    負責讀取配置、實例化模組、並協調數據獲取、處理與存儲的工作流程。
    """
    def __init__(self, config_path: str,
                 db_path: Optional[str] = None,
                 cache_path: Optional[str] = None,
                 fred_api_key: Optional[str] = None): # 新增 FRED API 金鑰參數
        """
        初始化指揮官。

        Args:
            config_path (str): 主設定檔的路徑。
            db_path (Optional[str]): 數據庫檔案的路徑。若提供，會覆寫設定檔中的路徑。
            cache_path (Optional[str]): API 快取檔案的路徑 (主要給 CryptoFetcher 或其他需要檔案快取的 fetcher)。
                                       若提供，會覆寫設定檔中的路徑。
            fred_api_key (Optional[str]): FRED API 金鑰。若提供，會用於初始化 FredFetcher。
                                          否則 FredFetcher 會嘗試從環境變數讀取。
        """
        self._setup_logging() # 確保日誌已設定
        logger.info(f"Commander initializing with config: {config_path}")

        self.config = self._load_config(config_path)

        # 動態設定/覆寫路徑 (如果提供了參數)
        if db_path:
            self.config['database']['path'] = db_path
            logger.info(f"Database path overridden by argument: {db_path}")
        if cache_path:
            self.config['cache']['path'] = cache_path
            logger.info(f"Cache path overridden by argument: {cache_path}")

        # 確保路徑中的目錄存在
        self._ensure_paths_exist()

        self.fred_api_key = fred_api_key # 保存 FRED API key

        # 數據獲取器工廠
        self.fetcher_factory: Dict[str, Type[DataFetcherInterface]] = {
            'yfinance': YFinanceFetcher,
            'fred': FredFetcher,
            'coingecko': CryptoFetcher, # 與 config.yaml 中 'crypto' source 對應的 fetcher
        }

        # 數據庫工廠
        self.db_factory: Dict[str, Type[DatabaseInterface]] = {
            'duckdb': DuckDBRepository,
        }

        # 實例化數據庫服務和數據處理器
        self.database = self._get_database_service()
        self.processor = self._get_data_processor() # 實例化處理器

        # 在初始化結束時連接數據庫
        try:
            self.database.connect()
        except Exception as e:
            logger.error(f"Commander failed to connect to database during initialization: {e}", exc_info=True)
            # 根據需求，這裡可以決定是否要拋出異常，終止 Commander 實例化
            # raise

        logger.info("Commander initialized successfully.")

    def _setup_logging(self):
        """設定基礎日誌記錄。如果外部已設定，則此處可能不需要太複雜。"""
        # 檢查是否已經有 handlers，避免重複設定
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        # Commander 自己的 logger 也使用這個設定
        global logger # 確保我們修改的是模塊級別的 logger
        logger = logging.getLogger(__name__)
        # 可以根據需要調整 Commander logger 的級別
        # logger.setLevel(logging.DEBUG)


    def _ensure_paths_exist(self):
        """確保設定檔中定義的數據庫和快取路徑的目錄存在。"""
        paths_to_check = []
        if 'database' in self.config and 'path' in self.config['database']:
            paths_to_check.append(self.config['database']['path'])
        if 'cache' in self.config and 'path' in self.config['cache']:
            paths_to_check.append(self.config['cache']['path'])

        for file_path in paths_to_check:
            dir_name = os.path.dirname(file_path)
            if dir_name and not os.path.exists(dir_name): # 確保目錄不是空字串 (例如，相對路徑檔案在當前目錄)
                try:
                    os.makedirs(dir_name, exist_ok=True)
                    logger.info(f"Created directory for path: {dir_name}")
                except Exception as e:
                    logger.error(f"Failed to create directory {dir_name} for path {file_path}: {e}", exc_info=True)


    def _load_config(self, path: str) -> dict:
        logger.info(f"Loading configuration from {path}")
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {path}", exc_info=True)
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration file {path}: {e}", exc_info=True)
            raise


    def _get_fetcher(self, fetcher_type_key: str) -> DataFetcherInterface:
        """
        根據設定檔中 data_sources 下的鍵 (e.g., 'equity', 'macro') 獲取對應的數據獲取器實例。
        """
        fetcher_name = self.config['data_sources'].get(fetcher_type_key)
        if not fetcher_name:
            raise ValueError(f"No fetcher name configured for source key: {fetcher_type_key} in data_sources.")

        fetcher_class = self.fetcher_factory.get(fetcher_name.lower()) # coingecko vs CryptoFetcher
        if not fetcher_class:
            raise ValueError(f"Unsupported fetcher type specified in config: {fetcher_name} (for key {fetcher_type_key})")

        robustness_config = self.config.get('fetcher_robustness', {})

        # 特殊處理需要 API 金鑰的 Fetcher
        if fetcher_class == FredFetcher:
            # 優先使用 Commander 初始化時傳入的 key，其次是設定檔 (如果有的話)，最後讓 FredFetcher 自己處理環境變數
            api_key = self.fred_api_key or self.config.get('api_keys', {}).get('fred')
            logger.info(f"Initializing FredFetcher. API key provided: {'Yes' if api_key else 'No (will try env var)'}")
            return FredFetcher(robustness_config, api_key=api_key)
        elif fetcher_class == CryptoFetcher:
            api_key = self.config.get('api_keys', {}).get('coingecko') # 假設設定檔中有 coingecko API key
            logger.info(f"Initializing CryptoFetcher. API key provided: {'Yes' if api_key else 'No'}")
            # CryptoFetcher 也需要 cache_path 和 expire_after (如果使用 requests_cache)
            # 這裡的 CryptoFetcher 實作中 cache 是註解掉的，但若啟用，則需傳入相關設定
            # crypto_config = {**robustness_config}
            # if 'cache' in self.config:
            #    crypto_config['cache_path'] = os.path.join(os.path.dirname(self.config['cache']['path']), 'crypto_cache.sqlite') # 給 crypto 獨立的 cache
            #    crypto_config['cache_expire_after_seconds'] = self.config['cache'].get('expire_after_days',1) * 86400

            return CryptoFetcher(robustness_config, api_key=api_key)

        return fetcher_class(robustness_config)


    def _get_database_service(self) -> DatabaseInterface:
        db_config = self.config.get('database', {})
        db_type = db_config.get('type')
        db_path = db_config.get('path')

        if not db_type or not db_path:
            logger.error("Database type or path not configured correctly.")
            raise ValueError("Database type or path not configured.")

        db_class = self.db_factory.get(db_type.lower())
        if not db_class:
            logger.error(f"Unsupported database type: {db_type}")
            raise ValueError(f"Unsupported database type: {db_type}")

        logger.info(f"Initializing database service: {db_type} at {db_path}")
        return db_class(db_path=db_path)

    def _get_data_processor(self) -> DataProcessor:
        """實例化數據處理器。未來可以從設定檔讀取處理器特定組態。"""
        processor_config = self.config.get('processor_settings', {}) # 假設設定檔中有處理器設定
        logger.info("Initializing DataProcessor.")
        return DataProcessor(config=processor_config)

    def fetch_single_symbol_data(self, symbol: str, start_date: str, end_date: str,
                                 source_key: str = 'equity', # 'equity', 'macro', 'crypto'
                                 table_name: str = 'ohlcv_daily') -> Optional[pd.DataFrame]:
        """
        獲取單個金融代碼的數據，並可選擇性地存儲到數據庫。
        此方法主要用於演示或單獨獲取。

        Args:
            symbol (str): 金融代碼。
            start_date (str): 開始日期 'YYYY-MM-DD'。
            end_date (str): 結束日期 'YYYY-MM-DD'。
            source_key (str): 數據源的鍵名，對應 config.yaml 中的 data_sources。
            table_name (str): 存儲數據的數據庫表名。

        Returns:
            Optional[pd.DataFrame]: 獲取到的數據，如果獲取或處理失敗則為 None。
        """
        logger.info(f"Fetching data for symbol '{symbol}' from source '{source_key}' for period {start_date} to {end_date}.")
        try:
            fetcher = self._get_fetcher(source_key)
        except ValueError as e:
            logger.error(f"Failed to get fetcher for source key '{source_key}': {e}", exc_info=True)
            return None

        data = fetcher.fetch(symbol, start_date, end_date)

        if data is not None and not data.empty:
            logger.info(f"Successfully fetched {len(data)} rows for {symbol} from {source_key}.")

            # 為數據添加 symbol 列 (如果 fetcher 沒有自動添加)
            if 'symbol' not in data.columns:
                data['symbol'] = symbol # 確保 symbol 列存在以供儲存或處理

            # 根據 source_key 決定如何處理和儲存數據
            if source_key in ['equity', 'crypto']: # 假設這些來源提供 OHLCV 兼容數據
                 # 在儲存前，可以先進行一些基礎處理，例如數據清洗
                data = self.processor.clean_data(data.copy()) # 使用 copy

                # 檢查 'adj_close' 是否存在，yfinance 通常有，crypto 可能沒有標準的
                if 'adj_close' not in data.columns and 'close' in data.columns:
                    logger.warning(f"'adj_close' not found for {symbol}, using 'close' as 'adj_close'.")
                    data['adj_close'] = data['close']

                # 確保所有 OHLCV 欄位都存在，若否，則可能不適合存入 ohlcv_daily
                required_ohlcv_cols = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'symbol']
                if all(col in data.columns for col in required_ohlcv_cols):
                    try:
                        self.database.upsert_ohlcv(data, table_name)
                        logger.info(f"Data for {symbol} from {source_key} saved to table '{table_name}'.")
                    except Exception as e_db:
                        logger.error(f"Failed to save data for {symbol} to table '{table_name}': {e_db}", exc_info=True)
                else:
                    missing_cols = [col for col in required_ohlcv_cols if col not in data.columns]
                    logger.warning(f"Data for {symbol} from {source_key} is missing columns {missing_cols}. Not saving to '{table_name}'. Data head:\n{data.head()}")

            elif source_key == 'macro': # 宏觀數據可能存到不同的表或有不同結構
                # FRED 數據通常是 'date', 'value', 'symbol'
                # 需要決定如何儲存，例如，創建一個 'macro_data_series' 表
                # 或者，如果想統一到 ohlcv_daily，需要轉換結構
                if 'value' in data.columns and 'date' in data.columns and 'symbol' in data.columns:
                    # 簡易轉換：將 'value' 視為 'close'，其他 OHLCV 設為相同值
                    data_ohlcv_compatible = pd.DataFrame({
                        'date': data['date'],
                        'symbol': data['symbol'],
                        'open': data['value'],
                        'high': data['value'],
                        'low': data['value'],
                        'close': data['value'],
                        'adj_close': data['value'], # adj_close
                        'volume': 0 # 宏觀數據通常沒有交易量
                    })
                    data_ohlcv_compatible = self.processor.clean_data(data_ohlcv_compatible.copy())
                    try:
                        self.database.upsert_ohlcv(data_ohlcv_compatible, table_name) # 仍存入 ohlcv_daily
                        logger.info(f"Macro data for {symbol} (transformed) saved to table '{table_name}'.")
                    except Exception as e_db:
                         logger.error(f"Failed to save transformed macro data for {symbol} to table '{table_name}': {e_db}", exc_info=True)
                else:
                    logger.warning(f"Macro data for {symbol} does not have expected 'value', 'date', 'symbol' columns. Not saving. Data head:\n{data.head()}")
            else:
                logger.warning(f"Data source key '{source_key}' not explicitly handled for DB storage. Data for {symbol} fetched but not saved automatically.")
            return data
        else:
            logger.warning(f"No data fetched for {symbol} from {source_key} for the given period.")
            return None


    def run_batch_fetch_and_store(self,
                                  symbols_map: Dict[str, List[str]],
                                  start_date: str,
                                  end_date: str,
                                  table_name: str = 'ohlcv_daily'):
        """
        高階指令：並行獲取一批來自不同源的金融代碼數據並存儲。

        Args:
            symbols_map (Dict[str, List[str]]): 一個字典，鍵是 source_key (如 'equity', 'macro', 'crypto')，
                                               值是要獲取的該源的金融代碼列表。
                                               Example: {'equity': ['AAPL', 'GOOG'], 'macro': ['DGS10']}
            start_date (str): 開始日期 'YYYY-MM-DD'。
            end_date (str): 結束日期 'YYYY-MM-DD'。
            table_name (str): 數據庫表名。
        """
        total_symbols = sum(len(slist) for slist in symbols_map.values())
        logger.info(f"Starting batch fetch for {total_symbols} symbols from {len(symbols_map)} sources...")

        max_workers = self.config.get('concurrency', {}).get('max_workers', 5) # 從設定檔獲取並行數

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # {future: (symbol, source_key)}
            future_to_info: Dict[Any, tuple[str, str]] = {}

            for source_key, symbol_list in symbols_map.items():
                if not symbol_list:
                    logger.info(f"No symbols provided for source key '{source_key}', skipping.")
                    continue
                try:
                    # 每個 source_key 可能對應不同的 fetcher 實例
                    # 但 fetch_single_symbol_data 內部會處理 fetcher 的獲取
                    # 所以這裡我們直接提交任務
                    logger.info(f"Submitting {len(symbol_list)} tasks for source '{source_key}'.")
                    for symbol in symbol_list:
                        future = executor.submit(
                            self.fetch_single_symbol_data,
                            symbol, start_date, end_date, source_key, table_name
                        )
                        future_to_info[future] = (symbol, source_key)
                except ValueError as e:
                    logger.error(f"Skipping source key '{source_key}' due to error: {e}", exc_info=True)

            successful_fetches = 0
            failed_fetches = 0
            for future in as_completed(future_to_info):
                symbol, source_key = future_to_info[future]
                try:
                    data_df = future.result() # fetch_single_symbol_data 返回 DataFrame 或 None
                    if data_df is not None and not data_df.empty:
                        logger.info(f"Successfully processed and stored data for {symbol} from {source_key}.")
                        successful_fetches += 1
                    else:
                        # fetch_single_symbol_data 內部已經記錄了獲取失敗的警告
                        logger.warning(f"Future completed for {symbol} ({source_key}), but no data was returned/stored (check previous logs).")
                        failed_fetches +=1
                except Exception as e_future:
                    logger.error(f"An error occurred in future for {symbol} ({source_key}): {e_future}", exc_info=True)
                    failed_fetches += 1

        logger.info(f"Batch fetch execution completed. Successful: {successful_fetches}, Failed: {failed_fetches}.")


    def get_processed_data(self, symbol: str, table_name: str,
                           start_date: str, end_date: str,
                           indicators: Optional[List[Dict[str, Any]]] = None) -> Optional[pd.DataFrame]:
        """
        從數據庫獲取數據，並可選擇性地應用數據處理和計算技術指標。

        Args:
            symbol (str): 金融代碼。
            table_name (str): 數據庫表名。
            start_date (str): 開始日期 'YYYY-MM-DD'。
            end_date (str): 結束日期 'YYYY-MM-DD'。
            indicators (Optional[List[Dict[str, Any]]]): 一個包含指標計算指令的列表。
                每個字典應包含 'name' (e.g., 'sma', 'ema', 'rsi') 和相應參數 (e.g., 'window').
                Example: [{'name': 'sma', 'window': 20}, {'name': 'rsi', 'window': 14}]

        Returns:
            Optional[pd.DataFrame]: 包含原始數據和計算指標的 DataFrame。
        """
        logger.info(f"Getting processed data for {symbol} from table '{table_name}' for period {start_date} to {end_date}.")

        # 1. 從數據庫獲取原始數據
        raw_data = self.database.get_ohlcv(symbol, table_name, start_date, end_date)
        if raw_data is None or raw_data.empty:
            logger.warning(f"No raw data found for {symbol} in '{table_name}' for the specified period.")
            return None

        logger.info(f"Retrieved {len(raw_data)} rows of raw data for {symbol}.")
        processed_data = raw_data.copy()

        # 2. 數據清洗 (可以作為一個標準步驟)
        processed_data = self.processor.clean_data(processed_data)
        if processed_data.empty:
            logger.warning(f"Data for {symbol} became empty after cleaning.")
            return None

        # 3. 計算技術指標 (如果指定了)
        if indicators:
            logger.info(f"Calculating {len(indicators)} indicators for {symbol}.")
            for indicator_config in indicators:
                name = indicator_config.get('name', '').lower()
                params = {k: v for k, v in indicator_config.items() if k != 'name'}

                if name == 'sma':
                    processed_data = self.processor.calculate_sma(processed_data, **params)
                elif name == 'ema':
                    processed_data = self.processor.calculate_ema(processed_data, **params)
                elif name == 'rsi':
                    processed_data = self.processor.calculate_rsi(processed_data, **params)
                # 可以擴展更多指標
                # elif name == 'macd':
                #     processed_data = self.processor.calculate_macd(processed_data, **params)
                else:
                    logger.warning(f"Unsupported indicator '{name}' specified for {symbol}. Skipping.")
            logger.info(f"Finished calculating indicators for {symbol}.")

        return processed_data


    def close(self):
        """優雅地關閉資源，如數據庫連接。"""
        logger.info("Commander shutting down...")
        if self.database:
            try:
                self.database.disconnect()
            except Exception as e:
                logger.error(f"Error during database disconnection: {e}", exc_info=True)
        logger.info("Commander shutdown complete.")


# 主執行區塊 (用於基本測試 Commander)
if __name__ == '__main__':
    # 創建一個臨時的 config.yaml 用於測試
    test_config_content = """
fetcher_robustness:
  retries: 2
  backoff_factor: 0.5
  delay_min_seconds: 0.1
  delay_max_seconds: 0.5

data_sources:
  equity: yfinance
  macro: fred
  crypto: coingecko

database:
  type: 'duckdb'
  path: './test_commander_market_data.duckdb' # 相對路徑，測試後會刪除

cache: # 用於 CryptoFetcher 等
  path: './test_commander_api_cache.sqlite' # 相對路徑
  expire_after_days: 1

concurrency:
  max_workers: 3

# api_keys: # 可以選擇性地提供 API keys
#   fred: YOUR_FRED_API_KEY_IF_ANY
#   coingecko: YOUR_COINGECKO_API_KEY_IF_ANY (通常免費的不需要)

processor_settings: # 數據處理器相關設定 (目前 processor.py 中未使用)
  some_setting: value
"""
    test_config_path = 'temp_test_config.yaml'
    with open(test_config_path, 'w', encoding='utf-8') as f:
        f.write(test_config_content)

    # 設定日誌
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # 注意：FRED API 金鑰需要透過環境變數 FRED_API_KEY 設定，或者在上面 config 中提供
    # 或者在 Commander 初始化時傳入 fred_api_key 參數
    # fred_key_from_env = os.getenv("FRED_API_KEY")
    # if not fred_key_from_env:
    #     print("WARNING: FRED_API_KEY environment variable not set. Macro data fetching might fail or be limited.")

    commander_instance: Optional[Commander] = None
    try:
        print("Initializing Commander for testing...")
        # 如果你有 FRED API key，可以這樣傳入:
        # commander_instance = Commander(config_path=test_config_path, fred_api_key="YOUR_KEY_HERE")
        commander_instance = Commander(config_path=test_config_path)
        print("Commander initialized.")

        # --- 測試 1: 獲取單個股票數據並存儲 ---
        print("\n--- Test 1: Fetching single equity (AAPL) ---")
        aapl_data = commander_instance.fetch_single_symbol_data(
            symbol='AAPL',
            start_date='2023-10-01',
            end_date='2023-10-05',
            source_key='equity',
            table_name='ohlcv_daily_test1'
        )
        if aapl_data is not None:
            print(f"AAPL data fetched (first 3 rows):\n{aapl_data.head(3)}")

        # --- 測試 2: 獲取單個宏觀數據並存儲 (轉換後) ---
        # print("\n--- Test 2: Fetching single macro data (DGS10) ---")
        # dgs10_data = commander_instance.fetch_single_symbol_data(
        #     symbol='DGS10', # 10-Year Treasury Constant Maturity Rate
        #     start_date='2023-10-01',
        #     end_date='2023-10-05',
        #     source_key='macro',
        #     table_name='ohlcv_daily_test1' # 存到同一個表
        # )
        # if dgs10_data is not None:
        #     print(f"DGS10 data fetched and transformed (first 3 rows):\n{dgs10_data.head(3)}")

        # --- 測試 3: 批次獲取數據 ---
        print("\n--- Test 3: Batch fetching (SPY, BTC-USD, GNPCA) ---")
        # 注意：CryptoFetcher 的 symbol 通常是 coingecko ID，如 'bitcoin'
        # yfinance 的 BTC-USD 也可以
        # GNPCA 是 FRED 的一個序列 (Real Gross National Product)
        symbols_to_fetch = {
            'equity': ['SPY'],
            'crypto': ['bitcoin'], # 使用 coingecko ID
            # 'macro': ['GNPCA']
        }
        commander_instance.run_batch_fetch_and_store(
            symbols_map=symbols_to_fetch,
            start_date='2023-11-01',
            end_date='2023-11-05',
            table_name='ohlcv_daily_batch_test'
        )
        print("Batch fetch completed (check logs for details).")

        # --- 測試 4: 獲取已存儲和處理後的數據 (SPY) ---
        print("\n--- Test 4: Getting processed data for SPY with indicators ---")
        indicators_config = [
            {'name': 'sma', 'window': 5, 'price_col': 'close'}, # 窗口較小以適應少量數據
            {'name': 'ema', 'window': 3, 'price_col': 'close'},
            {'name': 'rsi', 'window': 4, 'price_col': 'close'}
        ]
        spy_processed_data = commander_instance.get_processed_data(
            symbol='SPY',
            table_name='ohlcv_daily_batch_test', # 從批次測試的表中讀取
            start_date='2023-11-01',
            end_date='2023-11-05',
            indicators=indicators_config
        )
        if spy_processed_data is not None:
            print(f"SPY processed data (first 5 rows with indicators):\n{spy_processed_data.head()}")
            print(f"Columns: {spy_processed_data.columns.tolist()}")
        else:
            print("Could not retrieve processed SPY data.")

        # --- 測試 5: 獲取不存在的數據 ---
        print("\n--- Test 5: Getting data for a non-existent symbol ---")
        non_existent_data = commander_instance.get_processed_data(
            symbol='NONEXISTENT',
            table_name='ohlcv_daily_batch_test',
            start_date='2023-01-01',
            end_date='2023-01-05'
        )
        if non_existent_data is None:
            print("Correctly returned None for non-existent symbol.")

    except Exception as e_main_test:
        logger.error(f"Error during Commander testing: {e_main_test}", exc_info=True)
    finally:
        if commander_instance:
            print("\nClosing Commander...")
            commander_instance.close()
            print("Commander closed.")

        # 清理測試檔案
        paths_to_delete = [
            test_config_path,
            commander_instance.config['database']['path'] if commander_instance else './test_commander_market_data.duckdb',
            (commander_instance.config['database']['path'] + ".wal") if commander_instance else './test_commander_market_data.duckdb.wal',
            commander_instance.config['cache']['path'] if commander_instance else './test_commander_api_cache.sqlite'
        ]
        for p_del in paths_to_delete:
            if os.path.exists(p_del):
                try:
                    os.remove(p_del)
                    print(f"Cleaned up test file: {p_del}")
                except Exception as e_clean:
                    print(f"Error cleaning up test file {p_del}: {e_clean}")
        print("Commander test cleanup finished.")
