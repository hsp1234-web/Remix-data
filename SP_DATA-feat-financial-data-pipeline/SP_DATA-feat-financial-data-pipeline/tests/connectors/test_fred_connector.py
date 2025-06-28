import pytest
import pandas as pd
from datetime import date, datetime, timezone
import os
from unittest.mock import patch, MagicMock

from src.connectors.fred_connector import FredConnector
# from fredapi import Fred # No need to import Fred directly for mocking

@pytest.fixture
def fred_api_key_env_var():
    """設定並清理用於測試的 FRED API 金鑰環境變數。"""
    test_key_name = "FRED_API_KEY_PYTEST"
    original_value = os.getenv(test_key_name)
    # 為了測試，我們假設一個有效的假金鑰，除非環境中已設定
    # 在 CI 環境中，這個金鑰可能不存在，FredConnector 應該能處理無金鑰的情況
    # 如果環境中已設定了 FRED_API_KEY，fredapi 會自動使用它，除非我們 mock Fred()
    # 這裡我們不設定實際的金鑰值，讓 FredConnector 嘗試初始化
    # 如果測試需要真實調用 (不建議)，則需確保環境中有金鑰
    # os.environ[test_key_name] = "MOCK_FRED_KEY_FOR_TESTING_IF_NEEDED"
    yield test_key_name # 提供環境變數名給測試使用
    # 清理：恢復原始環境變數值
    if original_value is None:
        del os.environ[test_key_name]
    else:
        os.environ[test_key_name] = original_value


@pytest.fixture
def fred_config(fred_api_key_env_var): # fred_api_key_env_var fixture is not directly used here but ensures setup/teardown
    """提供一個標準的 FredConnector 設定。"""
    return {
        # "api_key": "YOUR_FRED_API_KEY", # 通常會從環境變數讀取，或讓 fredapi 自行處理
        # 如果要在測試中強制使用某個 key，可以在這裡提供，或者 mock os.getenv
        "requests_per_minute": 120 # FRED 官方限制 (有金鑰時)
    }

@pytest.fixture
def fred_connector(fred_config):
    """實例化一個 FredConnector。"""
    # 為了確保測試的隔離性並避免真實 API 調用，我們 mock `fredapi.Fred` class
    with patch('fredapi.Fred') as mock_fred_class:
        # 設定 mock_fred_class() 返回一個 MagicMock 實例 (即 Fred client instance)
        mock_fred_client_instance = MagicMock()
        mock_fred_class.return_value = mock_fred_client_instance

        connector = FredConnector(api_config=fred_config)
        # 將 mock client 附加到 connector 實例上，以便在測試中控制其行為
        connector.mock_fred_client_instance = mock_fred_client_instance
        yield connector


# --- 測試 get_series_data ---

def test_get_series_data_single_id_success(fred_connector):
    """測試成功獲取單一 FRED 序列。"""
    series_id = "GDP"
    start_date = "2022-01-01"
    end_date = "2022-12-31"

    # 模擬 fred_client.get_series() 的返回
    mock_dates = pd.to_datetime([date(2022, 1, 1), date(2022, 4, 1), date(2022, 7, 1), date(2022, 10, 1)])
    mock_values = [24000.0, 24500.0, 25000.0, 25500.0]
    mock_series = pd.Series(mock_values, index=mock_dates, name=series_id)

    fred_connector.mock_fred_client_instance.get_series.return_value = mock_series

    result_df = fred_connector.get_series_data([series_id], start_date, end_date)

    assert not result_df.empty
    assert len(result_df) == 4
    assert result_df['security_id'].iloc[0] == series_id
    assert result_df['metric_name'].iloc[0] == series_id
    assert result_df['metric_value'].iloc[0] == 24000.0
    assert isinstance(result_df['metric_date'].iloc[0], date)
    assert 'last_updated_timestamp' in result_df.columns
    assert 'source_api' in result_df.columns and result_df['source_api'].iloc[0] == "FRED"

    # 驗證 fred_client.get_series 是否被正確調用
    fred_connector.mock_fred_client_instance.get_series.assert_called_once_with(
        series_id=series_id,
        observation_start=start_date,
        observation_end=end_date
    )

def test_get_series_data_multiple_ids(fred_connector):
    """測試獲取多個 FRED 序列。"""
    series_ids = ["GDP", "UNRATE"]
    start_date = "2023-01-01"
    end_date = "2023-03-31"

    # 模擬 GDP 數據
    gdp_dates = pd.to_datetime([date(2023, 1, 1)])
    gdp_values = [26000.0]
    gdp_series = pd.Series(gdp_values, index=gdp_dates, name="GDP")

    # 模擬 UNRATE 數據
    unrate_dates = pd.to_datetime([date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)])
    unrate_values = [3.5, 3.4, 3.5]
    unrate_series = pd.Series(unrate_values, index=unrate_dates, name="UNRATE")

    def get_series_side_effect(series_id, observation_start, observation_end):
        if series_id == "GDP":
            return gdp_series
        elif series_id == "UNRATE":
            return unrate_series
        return pd.Series(dtype='float64') # 對於其他未預期的 series_id 返回空

    fred_connector.mock_fred_client_instance.get_series.side_effect = get_series_side_effect

    result_df = fred_connector.get_series_data(series_ids, start_date, end_date)

    assert not result_df.empty
    assert len(result_df) == 1 + 3 # GDP 1 筆, UNRATE 3 筆
    assert "GDP" in result_df['security_id'].unique()
    assert "UNRATE" in result_df['security_id'].unique()
    assert fred_connector.mock_fred_client_instance.get_series.call_count == 2


def test_get_series_data_no_data_for_id(fred_connector):
    """測試當某個序列 ID 返回無數據時的情況。"""
    series_ids = ["EXISTINGSERIES", "NODATASERIES"]
    # 模擬 EXISTINGSERIES 有數據
    existing_dates = pd.to_datetime([date(2023, 1, 1)])
    existing_values = [100.0]
    existing_series = pd.Series(existing_values, index=existing_dates, name="EXISTINGSERIES")
    # 模擬 NODATASERIES 返回空 Series
    empty_series = pd.Series(dtype='float64', name="NODATASERIES")

    def get_series_side_effect(series_id, **kwargs):
        if series_id == "EXISTINGSERIES":
            return existing_series
        elif series_id == "NODATASERIES":
            return empty_series
        return empty_series

    fred_connector.mock_fred_client_instance.get_series.side_effect = get_series_side_effect
    result_df = fred_connector.get_series_data(series_ids)

    assert not result_df.empty
    assert len(result_df) == 1 # 只有 EXISTINGSERIES 的數據
    assert "EXISTINGSERIES" in result_df['security_id'].values
    assert "NODATASERIES" not in result_df['security_id'].values

def test_get_series_data_api_error_for_one_id(fred_connector):
    """測試當某個序列 ID 獲取時發生 API 錯誤。"""
    series_ids = ["GOODSERIES", "ERRORSERIES"]

    good_series = pd.Series([1.0], index=pd.to_datetime([date(2023,1,1)]), name="GOODSERIES")

    def get_series_side_effect(series_id, **kwargs):
        if series_id == "GOODSERIES":
            return good_series
        elif series_id == "ERRORSERIES":
            raise Exception("Simulated API error for ERRORSERIES")
        return pd.Series(dtype='float64')

    fred_connector.mock_fred_client_instance.get_series.side_effect = get_series_side_effect
    result_df = fred_connector.get_series_data(series_ids)

    assert not result_df.empty
    assert len(result_df) == 1
    assert "GOODSERIES" in result_df['security_id'].values
    assert "ERRORSERIES" not in result_df['security_id'].values # 錯誤的序列不應包含在結果中

def test_get_series_data_empty_ids_list(fred_connector):
    """測試傳入空的 series_ids 列表。"""
    result_df = fred_connector.get_series_data([])
    assert result_df.empty
    assert list(result_df.columns) == fred_connector._get_standard_columns()

def test_get_series_data_fred_client_not_initialized():
    """測試當 Fred client 未成功初始化時的情況。"""
    # 建立一個 FredConnector 實例，但不 mock Fred()，使其初始化失敗 (假設無 API Key)
    with patch.dict(os.environ, {"FRED_API_KEY": ""}): # 確保環境中無key
        with patch('fredapi.Fred', side_effect=Exception("Failed to init Fred")): # 強制初始化失敗
            connector_no_client = FredConnector(api_config={"api_key": None}) # 傳入 None key
            assert connector_no_client.fred_client is None # 確認 client 未初始化
            result_df = connector_no_client.get_series_data(["GDP"])
            assert result_df.empty

def test_fred_connector_no_api_key_in_config(fred_api_key_env_var):
    """測試當 config 中沒有 api_key 時，是否會嘗試從環境變數讀取 (fredapi 的行為)。"""
    # 確保測試環境變數 FRED_API_KEY_PYTEST 未設定，或設定為一個假值
    # Fred() 在 api_key=None 時會嘗試 os.getenv('FRED_API_KEY')
    # 我們這裡的目的是驗證 FredConnector 的初始化邏輯，而非 fredapi 的金鑰查找順序

    # 假設環境變數 FRED_API_KEY 存在一個值
    with patch.dict(os.environ, {"FRED_API_KEY": "env_fred_key_value"}):
        with patch('fredapi.Fred') as mock_fred_class_for_env_test:
            mock_client_instance = MagicMock()
            mock_fred_class_for_env_test.return_value = mock_client_instance

            # config 中不提供 api_key
            config_no_key = {"requests_per_minute": 120}
            connector = FredConnector(api_config=config_no_key)

            # 驗證 Fred() 是否以 None (或環境變數值，取決於 fredapi 內部) 被調用
            # FredConnector 會將 config 中的 api_key (如果不存在則為 None) 傳給 Fred()
            # Fred() 自身會處理 None key 的情況 (嘗試 os.getenv('FRED_API_KEY'))
            mock_fred_class_for_env_test.assert_called_once_with(api_key=None)
            assert connector.fred_client is not None # 應該成功初始化 (因為 mock 了 Fred)

@patch('time.sleep', return_value=None) # Mock time.sleep to speed up tests
def test_rate_limiting_logic(mock_sleep, fred_connector, fred_config):
    """測試速率控制邏輯。"""
    series_ids = ["S1", "S2", "S3"]
    fred_connector._min_request_interval = 0.1 # 設定一個小的間隔 (10ms)

    # 模擬 get_series
    mock_series_data = pd.Series([1], index=pd.to_datetime([date(2023,1,1)]))
    fred_connector.mock_fred_client_instance.get_series.return_value = mock_series_data

    # 第一次調用，不應等待
    fred_connector.get_series_data([series_ids[0]])
    mock_sleep.assert_not_called()

    # 第二次調用，應該會觸發等待 (因為時間差小於 _min_request_interval)
    # 注意：_last_request_time 是在 _wait_for_rate_limit 內部之後，實際API調用之前更新的
    # 這裡的測試稍微簡化，主要看 sleep 是否被調用
    fred_connector.get_series_data([series_ids[1]])
    assert mock_sleep.call_count >= 1 # 至少被調用一次 (取決於實際時間差)

    # 模擬經過足夠時間
    fred_connector._last_request_time = time.time() - (fred_connector._min_request_interval * 2)
    mock_sleep.reset_mock() # 重置 mock_sleep 的調用記錄
    fred_connector.get_series_data([series_ids[2]])
    mock_sleep.assert_not_called() # 這次不應等待


# 注意：關於 API 金鑰的測試：
# - FredConnector 的設計是將 config 中提供的 api_key (如果有的話) 傳給 fredapi.Fred()。
# - 如果 config 中沒有 api_key，則傳遞 None 給 Fred()。
# - fredapi.Fred() 自身會處理 api_key=None 的情況：它會嘗試從環境變數 os.getenv('FRED_API_KEY') 讀取。
# - 如果兩者都沒有，fredapi 會以無金鑰模式運行（功能受限）。
# - 在單元測試中，我們通常 mock `fredapi.Fred` 本身，所以 Connector 是否正確傳遞了 `api_key` 參數給 `Fred()` 是關鍵。
# - Connector 不直接負責從環境變數讀取 FRED_API_KEY；這是 fredapi 的職責。
# - FredConnector 的 `api_config` 中的 `api_key` 欄位是給用戶一個覆蓋環境變數或提供金鑰的選項。
# - `YOUR_FRED_API_KEY` 的佔位符檢查是為了提醒用戶填寫 `config.yaml`。
