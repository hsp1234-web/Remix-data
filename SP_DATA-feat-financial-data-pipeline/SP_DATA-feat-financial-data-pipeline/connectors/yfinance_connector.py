import yfinance as yf
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import logging
import sys
import requests # For session type hint

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__':
        from base import BaseConnector
    else:
        raise

class YFinanceConnector(BaseConnector):
    """使用 yfinance 獲取股價和指數數據。"""

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, session: Optional[requests.Session] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler.")

        super().__init__(config, source_api_name="yfinance")
        self.requests_session = session # Store session if provided


    def fetch_data(self, tickers: List[str], start_date: str, end_date: Optional[str] = None,
                   interval: str = "1d", **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        self.logger.info(f"Fetching yfinance data for tickers: {tickers} from {start_date} to {end_date} with interval {interval}.")

        if not tickers:
            self.logger.warning("No tickers provided to YFinanceConnector fetch_data.")
            return pd.DataFrame(columns=['price_date', 'security_id', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits', 'source_api', 'data_snapshot_timestamp']), "No tickers provided."

        all_ticker_data_list = []
        session_to_use = kwargs.get('session', self.requests_session)

        for ticker_symbol in tickers:
            self.logger.debug(f"Fetching yfinance data for: {ticker_symbol} using session: {'Custom' if session_to_use else 'yfinance Default'}")
            try:
                ticker_obj = yf.Ticker(ticker_symbol, session=session_to_use)

                hist_df = ticker_obj.history(
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    auto_adjust=False,
                    actions=True,
                    progress=False,
                )

                if hist_df.empty:
                    self.logger.warning(f"yfinance returned no data for ticker: {ticker_symbol} (start: {start_date}, end: {end_date}, interval: {interval}).")
                    continue

                hist_df.reset_index(inplace=True)

                date_col_name = None
                if 'Datetime' in hist_df.columns: date_col_name = 'Datetime'
                elif 'Date' in hist_df.columns: date_col_name = 'Date'

                if not date_col_name:
                    self.logger.error(f"Date column ('Date' or 'Datetime') not found in yfinance data for {ticker_symbol}. Columns: {hist_df.columns.to_list()}")
                    continue

                rename_map = {
                    date_col_name: 'price_date', 'Open': 'open_price', 'High': 'high_price',
                    'Low': 'low_price', 'Close': 'close_price', 'Adj Close': 'adj_close_price',
                    'Volume': 'volume', 'Dividends': 'dividends', 'Stock Splits': 'stock_splits'
                }
                current_rename_map = {k: v for k, v in rename_map.items() if k in hist_df.columns}
                df_renamed = hist_df.rename(columns=current_rename_map)

                df_renamed['price_date'] = pd.to_datetime(df_renamed['price_date'])
                if df_renamed['price_date'].dt.tz is not None:
                    df_renamed['price_date'] = df_renamed['price_date'].dt.tz_localize(None)
                df_renamed['price_date'] = df_renamed['price_date'].dt.normalize().dt.date


                df_renamed['security_id'] = ticker_symbol
                df_renamed['source_api'] = self.source_api_name
                df_renamed['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                final_cols = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                              'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                              'source_api', 'data_snapshot_timestamp']

                for fc_col in final_cols:
                    if fc_col not in df_renamed.columns:
                        default_val = 0.0 if fc_col in ['dividends', 'stock_splits'] else pd.NA
                        df_renamed[fc_col] = default_val

                all_ticker_data_list.append(df_renamed[final_cols])
                self.logger.debug(f"Processed yfinance data for {ticker_symbol}, {len(df_renamed)} rows.")

            except Exception as e:
                self.logger.error(f"Error fetching/processing yfinance for {ticker_symbol}: {e}", exc_info=True)

        if not all_ticker_data_list:
            self.logger.warning(f"No data successfully fetched for any yfinance tickers: {tickers}")
            return pd.DataFrame(columns=final_cols), "No data from yfinance for any tickers."

        final_df = pd.concat(all_ticker_data_list, ignore_index=True)

        if final_df.empty:
             self.logger.warning("Final combined yfinance data is empty (all tickers failed or returned no data).")
             return final_df, "Final combined yfinance data is empty."

        for col_to_num in ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'dividends', 'stock_splits']:
            if col_to_num in final_df.columns:
                final_df[col_to_num] = pd.to_numeric(final_df[col_to_num], errors='coerce')
        if 'volume' in final_df.columns:
            final_df['volume'] = pd.to_numeric(final_df['volume'], errors='coerce').astype('Int64')


        self.logger.info(f"Successfully fetched and processed {len(final_df)} total records from yfinance for tickers: {tickers}.")
        return final_df, None

if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger = logging.getLogger("YFinanceConnectorTestRunV3")
    if not test_logger.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger.addHandler(ch)
        test_logger.propagate = False

    sample_config = {}
    connector = YFinanceConnector(config=sample_config, logger_instance=test_logger)

    test_logger.info("\n--- Testing YFinanceConnector for ^MOVE ---")
    move_df_test, move_err_test = connector.fetch_data(tickers=["^MOVE"], start_date="2024-05-01", end_date="2024-05-31")
    if move_err_test: test_logger.error(f"MOVE Test Error: {move_err_test}")
    elif move_df_test is not None:
        test_logger.info(f"MOVE Test OK. Shape: {move_df_test.shape}")
        if not move_df_test.empty: test_logger.info(f"MOVE Head:\n{move_df_test.head().to_string()}")

    test_logger.info("\n--- Testing YFinanceConnector for AAPL, MSFT ---")
    stocks_df_test, stocks_err_test = connector.fetch_data(tickers=["AAPL", "MSFT"], start_date="2024-05-01", end_date="2024-05-10")
    if stocks_err_test: test_logger.error(f"Stocks Test Error: {stocks_err_test}")
    elif stocks_df_test is not None:
        test_logger.info(f"Stocks Test OK. Shape: {stocks_df_test.shape}")
        if not stocks_df_test.empty:
            test_logger.info(f"Stocks Head:\n{stocks_df_test.head().to_string()}")
            unique_tickers_found = stocks_df_test['security_id'].unique()
            if "AAPL" in unique_tickers_found and "MSFT" in unique_tickers_found: test_logger.info("AAPL & MSFT data found.")
            else: test_logger.warning(f"Expected AAPL & MSFT, found: {unique_tickers_found}")

    test_logger.info("\n--- Testing YFinanceConnector for NONEXISTENTTICKERXYZ ---")
    non_df_test, non_err_test = connector.fetch_data(tickers=["NONEXISTENTTICKERXYZ"], start_date="2024-01-01", end_date="2024-01-10")
    if non_err_test and "No data fetched" in non_err_test:
        test_logger.info(f"OK (non-existent ticker): '{non_err_test}', DF empty: {non_df_test.empty if non_df_test is not None else 'N/A'}")
    elif non_df_test is not None and non_df_test.empty:
        test_logger.info(f"OK (non-existent ticker): No error string, but DF empty.")
    else: test_logger.error(f"Fail (non-existent ticker): err='{non_err_test}', df='{non_df_test}'")

    test_logger.info("\n--- Testing YFinanceConnector with empty ticker list ---")
    empty_df_test, empty_err_test = connector.fetch_data(tickers=[], start_date="2024-01-01", end_date="2024-01-10")
    if empty_err_test == "No tickers provided." and (empty_df_test is not None and empty_df_test.empty):
        test_logger.info(f"OK (empty ticker list): '{empty_err_test}', DF empty.")
    else: test_logger.error(f"Fail (empty ticker list): err='{empty_err_test}', df='{empty_df_test}'")

# **對草案的增強和調整摘要（V3 更新）：**
# *   **時區處理：** 在 `fetch_data` 中，將 `price_date` 從 `yfinance` 可能返回的時區感知 (tz-aware) datetime 物件轉換為僅日期 (date) 物件時，明確使用 `.dt.tz_localize(None)` 來移除時區信息，然後再 `.dt.normalize().dt.date`。
# *   **`if __name__ == '__main__':` 測試塊：**
#     *   `test_logger` 的名稱更改為 `YFinanceConnectorTestRunV3` 以與 Connector 版本對應。
#
# 這些調整使 `YFinanceConnector` 在處理日期時區方面更加穩健。其他邏輯與 V2 版本基本一致。
