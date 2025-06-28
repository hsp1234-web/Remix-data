import pytest
import pandas as pd
from datetime import date, datetime, timezone
import os
import shutil
from unittest.mock import patch, MagicMock

from src.connectors.yfinance_connector import YFinanceConnector

# 模組級 logger (測試時可以不用特別設定，pytest 會處理 stdout/stderr)
# import logging
# logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def temp_cache_dir():
    """建立一個臨時快取目錄供測試使用，並在測試結束後清理。"""
    test_dir = os.path.join("temp_test_cache_pytest", "yfinance")
    os.makedirs(test_dir, exist_ok=True)
    yield test_dir # 提供路徑給測試
    shutil.rmtree(os.path.join("temp_test_cache_pytest")) # 清理根目錄

@pytest.fixture
def yf_config(temp_cache_dir):
    """提供一個標準的 YFinanceConnector 設定。"""
    return {
        "requests_per_minute": 120, # 測試時設高一點以避免不必要的等待
        "cache_enabled": True,
        "cache_directory": temp_cache_dir,
        "cache_expire_after_seconds": 60 # 快取快速過期以便測試
    }

@pytest.fixture
def yf_connector(yf_config):
    """實例化一個 YFinanceConnector。"""
    return YFinanceConnector(api_config=yf_config)

def create_mock_history_df(data_dict, index_name='Date'):
    """輔助函數，用於創建 yf.Ticker().history() 返回的模擬 DataFrame。"""
    df = pd.DataFrame(data_dict)
    if index_name in df.columns:
        df[index_name] = pd.to_datetime(df[index_name])
        df = df.set_index(index_name)
    # yfinance.history() 通常返回 DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex) and not df.empty:
         df.index = pd.to_datetime(df.index)
    return df

# --- 測試 get_historical_price ---
def test_get_historical_price_single_ticker_success(yf_connector, yf_config):
    """測試成功獲取單一股票的歷史價格。"""
    ticker_symbol = "AAPL"
    start_date = "2023-01-01"
    end_date = "2023-01-05" # yfinance end is exclusive for daily

    mock_data = {
        'Open': [130.28, 126.89, 126.38], 'High': [130.90, 128.66, 128.11],
        'Low': [124.17, 125.08, 124.38], 'Close': [125.07, 126.36, 125.02],
        'Adj Close': [124.54, 125.83, 124.49], 'Volume': [112117500, 89113600, 85438100],
        'Dividends': [0.0, 0.0, 0.0], 'Stock Splits': [0.0, 0.0, 0.0]
    }
    # yfinance history returns DatetimeIndex
    mock_dates = pd.to_datetime([date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5)])

    mock_df = pd.DataFrame(mock_data, index=mock_dates)
    mock_df.index.name = 'Date'

    with patch('yfinance.Ticker') as mock_ticker:
        mock_ticker.return_value.history.return_value = mock_df

        result_df = yf_connector.get_historical_price([ticker_symbol], start_date, end_date)

    assert not result_df.empty
    assert len(result_df) == 3
    assert result_df['security_id'].iloc[0] == ticker_symbol
    assert 'adj_close_price' in result_df.columns
    assert pd.api.types.is_datetime64_any_dtype(pd.Series(result_df['price_date'])) == False # Should be date objects
    assert isinstance(result_df['price_date'].iloc[0], date)

    # 檢查快取是否被寫入
    if yf_config["cache_enabled"]:
        norm_end_date_for_cache = pd.to_datetime(end_date).strftime('%Y-%m-%d')
        cache_file = yf_connector._get_cache_filepath(ticker_symbol, start_date, norm_end_date_for_cache, "1d")
        assert os.path.exists(cache_file)

def test_get_historical_price_from_cache(yf_connector, yf_config):
    """測試從快取讀取數據。"""
    ticker_symbol = "MSFT"
    start_date = "2023-02-01"
    end_date = "2023-02-03"
    norm_start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    norm_end_date_for_cache = pd.to_datetime(end_date).strftime('%Y-%m-%d')

    # 準備一個假的快取檔案
    cache_file = yf_connector._get_cache_filepath(ticker_symbol, norm_start_date, norm_end_date_for_cache, "1d")

    sample_data = {
        'price_date': [date(2023, 2, 1), date(2023, 2, 2)],
        'security_id': [ticker_symbol, ticker_symbol],
        'open_price': [250.0, 252.0], 'high_price': [255.0, 256.0],
        'low_price': [248.0, 250.0], 'close_price': [253.0, 255.0],
        'adj_close_price': [253.0, 255.0], 'volume': [1000000, 1200000],
        'dividends': [0.0, 0.0], 'stock_splits': [0.0, 0.0],
        'source_api': ['yfinance', 'yfinance'],
        'last_updated_timestamp': [datetime.now(timezone.utc), datetime.now(timezone.utc)]
    }
    cached_df_content = pd.DataFrame(sample_data)
    # 轉換 price_date 為 date object
    cached_df_content['price_date'] = pd.to_datetime(cached_df_content['price_date']).dt.date

    yf_connector._write_to_cache(cached_df_content, cache_file)
    assert os.path.exists(cache_file)

    with patch('yfinance.Ticker') as mock_ticker: # 確保 API 不被調用
        result_df = yf_connector.get_historical_price([ticker_symbol], start_date, end_date)
        mock_ticker.assert_not_called() # 驗證 Ticker() 沒有被調用

    assert not result_df.empty
    assert len(result_df) == 2
    assert result_df['security_id'].iloc[0] == ticker_symbol
    pd.testing.assert_series_equal(pd.Series(result_df['price_date']), pd.Series(cached_df_content['price_date']), check_dtype=False)


def test_get_historical_price_no_data_for_ticker(yf_connector):
    """測試當 yfinance 對某股票返回空 DataFrame 時的情況。"""
    ticker_symbol = "NODATAICKER"
    start_date = "2023-01-01"
    end_date = "2023-01-05"

    with patch('yfinance.Ticker') as mock_ticker:
        mock_ticker.return_value.history.return_value = pd.DataFrame() # API 返回空

        result_df = yf_connector.get_historical_price([ticker_symbol], start_date, end_date)

    assert result_df.empty # Connector 應返回空 DataFrame

def test_get_historical_price_api_error_retry_exceeded(yf_connector):
    """測試 API 錯誤且重試次數用盡的情況。"""
    ticker_symbol = "ERRORAPPL"
    start_date = "2023-01-01"
    end_date = "2023-01-05"

    with patch('yfinance.Ticker') as mock_ticker:
        # 讓 history() 方法總是拋出 HTTPError (或其他 yfinance 可能拋出的錯誤)
        mock_ticker.return_value.history.side_effect = requests.exceptions.HTTPError("Simulated API error")

        result_df = yf_connector.get_historical_price([ticker_symbol], start_date, end_date, max_retries=1, initial_backoff=0.1)

    assert result_df.empty # 最終應返回空 DataFrame
    assert mock_ticker.return_value.history.call_count == 2 # 1 次初始嘗試 + 1 次重試

def test_get_historical_price_empty_ticker_list(yf_connector):
    """測試傳入空股票列表的情況。"""
    result_df = yf_connector.get_historical_price([], "2023-01-01", "2023-01-05")
    assert result_df.empty
    assert list(result_df.columns) == yf_connector._get_standard_columns()


def test_get_historical_price_multiple_tickers_partial_failure(yf_connector):
    """測試多個股票，其中一個成功，一個失敗，一個無數據。"""
    tickers = ["GOODTICK", "FAILTICK", "EMPTYTICK"]
    start_date = "2023-03-01"
    end_date = "2023-03-02"

    # GOODTICK 的模擬數據
    good_data = {'Open': [10.0], 'Close': [10.5], 'Volume': [100]}
    good_dates = pd.to_datetime([date(2023, 3, 1)])
    good_df_api = pd.DataFrame(good_data, index=good_dates); good_df_api.index.name = 'Date'

    # EMPTYTICK 的模擬數據 (API返回空)
    empty_df_api = pd.DataFrame()

    def history_side_effect(*args, **kwargs_yf):
        # 根據 start date (因為 ticker symbol 不直接傳給 history) 或其他方式判斷是哪個 ticker
        # 這裡簡化，假設調用順序與 tickers 列表一致
        # 注意：yf.Ticker(ticker_symbol) 是外部調用，這裡 mock 的是 history 方法
        # 我們需要讓 yf.Ticker() 返回的 mock 物件的 history 方法有不同行為
        # 這裡的 mock_ticker.return_value 指的是同一個 mock_instance
        # 所以我們需要根據 Ticker 實例化時的 ticker_symbol 來改變行為
        # 這比較複雜，簡化測試：讓 Ticker() 每次都返回一個新的 mock instance

        # 此處的 mock_ticker_instance.ticker 可以用來判斷是哪個股票
        # 但 yf.Ticker() 的 mock 需要配置得更細緻
        # 暫時使用 call_count 來模擬不同股票的行為
        call_count = mock_ticker_instance.history.call_count
        if call_count == 1: # GOODTICK
            return good_df_api
        elif call_count == 2: # FAILTICK
            raise requests.exceptions.ConnectionError("Simulated connection error for FAILTICK")
        elif call_count == 3: # EMPTYTICK
            return empty_df_api
        return pd.DataFrame() # Default

    mock_ticker_instance = MagicMock()
    mock_ticker_instance.history.side_effect = history_side_effect

    with patch('yfinance.Ticker', return_value=mock_ticker_instance) as mock_ticker_constructor:
        result_df = yf_connector.get_historical_price(tickers, start_date, end_date, max_retries=0) # 禁用重試以簡化測試

    assert not result_df.empty
    assert len(result_df) == 1 # 只有 GOODTICK 的數據
    assert result_df['security_id'].iloc[0] == "GOODTICK"
    assert mock_ticker_constructor.call_count == len(tickers) # 每個 ticker 都嘗試調用 Ticker()
    assert mock_ticker_instance.history.call_count == len(tickers) # history() 也被調用了三次

def test_cache_expiry(yf_connector, yf_config, temp_cache_dir):
    """測試快取過期邏輯。"""
    ticker_symbol = "EXPIRECACHE"
    start_date = "2023-04-01"
    end_date = "2023-04-02"
    norm_start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    norm_end_date_for_cache = pd.to_datetime(end_date).strftime('%Y-%m-%d')

    cache_file = yf_connector._get_cache_filepath(ticker_symbol, norm_start_date, norm_end_date_for_cache, "1d")

    # 1. 第一次調用，寫入快取
    mock_data_v1 = {'Open': [1.0], 'Close': [1.1], 'Volume': [100]}
    mock_dates_v1 = pd.to_datetime([date(2023, 4, 1)]);
    mock_df_v1 = pd.DataFrame(mock_data_v1, index=mock_dates_v1); mock_df_v1.index.name = 'Date'

    with patch('yfinance.Ticker') as mock_ticker_v1:
        mock_ticker_v1.return_value.history.return_value = mock_df_v1
        yf_connector.get_historical_price([ticker_symbol], start_date, end_date)

    assert os.path.exists(cache_file)
    first_call_mod_time = os.path.getmtime(cache_file)

    # 2. 模擬時間流逝，使快取過期
    # 將 cache_expire_after_seconds 暫時設為極小值
    original_expiry = yf_connector.cache_expire_after_seconds
    yf_connector.cache_expire_after_seconds = 0.1 # 0.1 秒過期
    time.sleep(0.2) # 等待超過過期時間

    # 3. 第二次調用，應重新從 API 獲取 (因為快取已過期)
    mock_data_v2 = {'Open': [2.0], 'Close': [2.1], 'Volume': [200]} # 假設 API 返回新數據
    mock_dates_v2 = pd.to_datetime([date(2023, 4, 1)]);
    mock_df_v2 = pd.DataFrame(mock_data_v2, index=mock_dates_v2); mock_df_v2.index.name = 'Date'

    with patch('yfinance.Ticker') as mock_ticker_v2:
        mock_ticker_v2.return_value.history.return_value = mock_df_v2
        result_df_v2 = yf_connector.get_historical_price([ticker_symbol], start_date, end_date)

    yf_connector.cache_expire_after_seconds = original_expiry # 恢復原始設定

    assert mock_ticker_v2.return_value.history.called # 確認 API 被調用了
    assert not result_df_v2.empty
    assert result_df_v2['open_price'].iloc[0] == 2.0 # 確認是新數據

    # 檢查快取檔案是否被更新
    assert os.path.exists(cache_file)
    second_call_mod_time = os.path.getmtime(cache_file)
    assert second_call_mod_time > first_call_mod_time

def test_yfinance_end_date_behavior_for_daily_interval(yf_connector):
    """測試日線間隔下，end_date 是否被正確處理 (yfinance 的 end 是 exclusive)。"""
    ticker = "SPY"
    user_start_date = "2023-01-03" # 週二
    user_end_date = "2023-01-05"   # 週四 (使用者期望包含這天)

    # Connector 應將 end_date 調整為 "2023-01-06" 傳給 yfinance.history()
    expected_yf_end_date = "2023-01-06"

    # 模擬 yfinance.history 返回的數據 (假設它返回了 3, 4, 5 三天的數據)
    mock_dates = pd.to_datetime([date(2023,1,3), date(2023,1,4), date(2023,1,5)])
    mock_hist_df = pd.DataFrame({'Close': [10,11,12]}, index=mock_dates)
    mock_hist_df.index.name = 'Date'

    with patch('yfinance.Ticker') as mock_yf_ticker:
        mock_yf_ticker.return_value.history.return_value = mock_hist_df

        result_df = yf_connector.get_historical_price(
            tickers=[ticker],
            start_date=user_start_date,
            end_date=user_end_date,
            interval="1d"
        )

        # 驗證傳給 yfinance.history 的 end_date
        called_args, called_kwargs = mock_yf_ticker.return_value.history.call_args
        assert called_kwargs['end'] == expected_yf_end_date

    assert not result_df.empty
    assert len(result_df) == 3
    assert result_df['price_date'].min() == date(2023,1,3)
    assert result_df['price_date'].max() == date(2023,1,5) # 確認使用者期望的結束日期包含在內

def test_yfinance_end_date_behavior_for_intraday_interval(yf_connector):
    """測試非日線間隔下，end_date 是否不被調整。"""
    ticker = "MSFT"
    user_start_date = "2023-01-03"
    user_end_date = "2023-01-03" # 當天
    interval = "1h"

    expected_yf_end_date = "2023-01-03" # 對於非日線，通常 yfinance 的 end 是 inclusive

    mock_dates = pd.to_datetime(["2023-01-03 09:30:00", "2023-01-03 10:30:00"])
    mock_hist_df = pd.DataFrame({'Close': [20,21]}, index=mock_dates)
    mock_hist_df.index.name = 'Datetime'


    with patch('yfinance.Ticker') as mock_yf_ticker:
        mock_yf_ticker.return_value.history.return_value = mock_hist_df

        result_df = yf_connector.get_historical_price(
            tickers=[ticker],
            start_date=user_start_date,
            end_date=user_end_date,
            interval=interval
        )

        called_args, called_kwargs = mock_yf_ticker.return_value.history.call_args
        assert called_kwargs['end'] == expected_yf_end_date # 不應加一天

    assert not result_df.empty
    assert len(result_df) == 2
    assert result_df['price_date'].iloc[0] == date(2023,1,3)
    assert result_df['price_date'].iloc[-1] == date(2023,1,3)
