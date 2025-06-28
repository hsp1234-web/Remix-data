# commander.py

import yaml
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# 動態導入模組
from data_fetchers.yfinance_fetcher import YFinanceFetcher
from data_storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)

class Commander:
    """
    系統的協調器 (Facade)。
    負責讀取配置、實例化模組、並協調數據獲取與存儲的工作流程。
    """
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.fetcher = self._get_fetcher()
        self.repository = self._get_repository()

    def _load_config(self, path: str):
        """加載 YAML 組態檔。"""
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _get_fetcher(self):
        """根據組態檔動態創建數據獲取器實例。"""
        fetcher_type = self.config['fetcher']['type']
        if fetcher_type == 'yfinance':
            return YFinanceFetcher(self.config['fetcher'])
        # 未來可在此處添加其他獲取器，如 FinnhubFetcher
        else:
            raise ValueError(f"不支持的獲取器類型: {fetcher_type}")

    def _get_repository(self):
        """根據組態檔動態創建數據庫倉儲實例。"""
        db_type = self.config['database']['type']
        if db_type == 'duckdb':
            return DuckDBRepository(self.config['database']['path'])
        # 未來可在此處添加其他數據庫，如 TimescaleDBRepository
        else:
            raise ValueError(f"不支持的數據庫類型: {db_type}")

    def fetch_and_store_symbols(self, symbols: list[str], start_date: str, end_date: str):
        """
        使用並行模式獲取並存儲多個股票代號的數據。
        """
        # 根據藍圖：數據下載是 I/O 密集型任務，使用 ThreadPoolExecutor
        max_workers = self.config['concurrency'].get('max_io_workers', 5)

        logger.info(f"啟動 ThreadPoolExecutor (max_workers={max_workers}) 並行獲取 {len(symbols)} 個代號的數據...")

        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for symbol in symbols:
                # 提交任務到執行緒池
                future = executor.submit(self.fetcher.fetch_data, symbol, start_date, end_date)
                futures[future] = symbol

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                data = future.result()
                if data is not None and not data.empty:
                    # 數據獲取成功後，進行存儲
                    logger.info(f"數據獲取成功: {symbol}。準備存儲...")

                    # 準備數據以符合數據庫 schema
                    # yfinance 返回的數據需要添加 symbol 列
                    data['symbol'] = symbol
                    # 選擇需要的列
                    columns_to_save = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock splits']
                    data.rename(columns={'stock splits': 'stock_splits'}, inplace=True) # 修正列名

                    # 確保所有需要的列都存在
                    for col in columns_to_save:
                        if col not in data.columns:
                            data[col] = 0.0 if col not in ['symbol', 'date'] else None

                    data_to_save = data[columns_to_save]

                    self.repository.save_ohlcv(data_to_save, table_name="ohlcv_daily")
                else:
                    logger.warning(f"任務完成，但未返回 {symbol} 的有效數據。")
            except Exception as e:
                logger.error(f"處理 {symbol} 的任務時發生異常: {e}", exc_info=True)

        logger.info("所有數據獲取與存儲任務完成。")

    def close(self):
        """關閉所有資源，如數據庫連接。"""
        self.repository.disconnect()
