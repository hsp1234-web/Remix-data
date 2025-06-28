import pytest
import pandas as pd
import yaml
import os
from unittest.mock import patch, MagicMock, call # Added call for checking call order

# 假設 Connector 檔案都位於 src.connectors 中
from src.data_master import DataMaster
from src.connectors import ( # 導入所有可能用到的 Connector
    YFinanceConnector, FinMindConnector, FredConnector, NYFedConnector,
    PolygonIOConnector, FMPConnector, AlphaVantageConnector, FinnhubConnector, FinLabConnector
)

# --- 測試用的 Fixtures ---
@pytest.fixture
def mock_config_path(tmp_path):
    """創建一個臨時的 YAML 設定檔路徑，並可選擇性地寫入內容。"""
    config_file = tmp_path / "test_config.yaml"
    def _writer(content_dict=None):
        if content_dict:
            with open(config_file, 'w', encoding='utf-8') as f:
                yaml.dump(content_dict, f)
        return str(config_file)
    return _writer

@pytest.fixture
def basic_api_configs():
    """提供一個基礎的多 API 設定字典。"""
    return {
        "apis": {
            "finmind": {"priority": 5, "api_key": "fm_key_test", "requests_per_minute": 100},
            "yfinance": {"priority": 10, "requests_per_minute": 100, "cache_enabled": False},
            "fred": {"priority": 15, "api_key": "fred_key_test", "requests_per_minute": 60},
            # "polygon_io": {"priority": 20, "api_key": "poly_key_test", "requests_per_minute": 5},
        }
    }

# --- Mock Connector 實例和它們的方法 ---
# 我們將 patch Connector 類別本身，使其在 DataMaster 初始化時返回 MagicMock 實例
# 這樣可以避免真實的 Connector 初始化邏輯 (例如 SDK 登入)

@pytest.fixture
def mock_finmind_connector_instance():
    instance = MagicMock(spec=FinMindConnector)
    instance.get_historical_price = MagicMock(return_value=pd.DataFrame()) # 預設返回空
    instance.get_company_profile = MagicMock(return_value=pd.DataFrame())
    instance.get_financial_statement = MagicMock(return_value=pd.DataFrame())
    instance.get_chip_data = MagicMock(return_value=pd.DataFrame())
    # __init__ 應該接受 api_config
    instance.__init__ = MagicMock(return_value=None)
    return instance

@pytest.fixture
def mock_yfinance_connector_instance():
    instance = MagicMock(spec=YFinanceConnector)
    instance.get_historical_price = MagicMock(return_value=pd.DataFrame())
    instance.__init__ = MagicMock(return_value=None)
    return instance

@pytest.fixture
def mock_fred_connector_instance():
    instance = MagicMock(spec=FredConnector)
    instance.get_series_data = MagicMock(return_value=pd.DataFrame())
    instance.__init__ = MagicMock(return_value=None)
    return instance

# --- DataMaster 初始化測試 ---
def test_datamaster_initialization_success(mock_config_path, basic_api_configs,
                                           mock_finmind_connector_instance,
                                           mock_yfinance_connector_instance,
                                           mock_fred_connector_instance):
    """測試 DataMaster 能否成功初始化並載入設定的 Connector。"""
    config_file = mock_config_path(basic_api_configs)

    # Patch Connector 類別，使其在 DataMaster 初始化時返回我們的 mock 實例
    with patch('src.connectors.get_connector_class') as mock_get_class:
        def side_effect_get_class(name):
            if name == 'finmind': return MagicMock(return_value=mock_finmind_connector_instance)
            if name == 'yfinance': return MagicMock(return_value=mock_yfinance_connector_instance)
            if name == 'fred': return MagicMock(return_value=mock_fred_connector_instance)
            raise ValueError(f"Unexpected connector name in test: {name}")
        mock_get_class.side_effect = side_effect_get_class

        dm = DataMaster(config_path=config_file)

    assert "finmind" in dm.connectors
    assert "yfinance" in dm.connectors
    assert "fred" in dm.connectors
    assert dm.connectors["finmind"] == mock_finmind_connector_instance
    assert dm.connectors["yfinance"] == mock_yfinance_connector_instance

    # 驗證 Connector 的 __init__ 是否以正確的 api_config 被調用
    # mock_finmind_connector_instance.__init__.assert_called_once_with(api_config=basic_api_configs["apis"]["finmind"])
    # mock_yfinance_connector_instance.__init__.assert_called_once_with(api_config=basic_api_configs["apis"]["yfinance"])
    # mock_fred_connector_instance.__init__.assert_called_once_with(api_config=basic_api_configs["apis"]["fred"])
    # 由於我們 mock 了 get_connector_class 返回的類別的實例化，所以 __init__ 的 mock 需要在返回的實例上
    # 或者 mock 類別本身，然後檢查類別的 __init__
    # 這裡的 mock_get_class.side_effect 返回的是一個會創建 mock instance 的 Callable (MagicMock)
    # 所以我們需要檢查這個 Callable (MagicMock) 是否被以正確的 config 調用

    # 檢查 ConnectorClass(api_config=api_setting) 的調用
    # 由於 get_connector_class 返回的是一個 MagicMock (它本身是 callable 並返回 mock instance)
    # 我們需要檢查這個 MagicMock (代表類別) 是否被以正確的 config 調用

    # 取得 mock_get_class 返回的 mock class
    mock_finmind_class = mock_get_class('finmind')
    mock_yfinance_class = mock_get_class('yfinance')
    mock_fred_class = mock_get_class('fred')

    mock_finmind_class.assert_called_once_with(api_config=basic_api_configs["apis"]["finmind"])
    mock_yfinance_class.assert_called_once_with(api_config=basic_api_configs["apis"]["yfinance"])
    mock_fred_class.assert_called_once_with(api_config=basic_api_configs["apis"]["fred"])


    assert dm.api_priority_order == ["finmind", "yfinance", "fred"] # 基於 priority 設定

def test_datamaster_initialization_config_not_found(tmp_path):
    """測試設定檔不存在時的情況。"""
    non_existent_config_file = tmp_path / "non_existent.yaml"
    with pytest.raises(FileNotFoundError):
        DataMaster(config_path=str(non_existent_config_file))

def test_datamaster_initialization_bad_config_format(mock_config_path):
    """測試設定檔格式錯誤 (例如不是 YAML 或缺少 'apis')。"""
    bad_config_file_no_apis = mock_config_path({"something_else": "value"})
    with pytest.raises(ValueError, match="格式錯誤或缺少 'apis' 部分"):
        DataMaster(config_path=bad_config_file_no_apis)

    bad_yaml_file = mock_config_path(None) # 建立空檔案
    with open(bad_yaml_file, 'w') as f:
        f.write("this: is: not: valid: yaml:") # 寫入無效 YAML
    with pytest.raises(yaml.YAMLError):
        DataMaster(config_path=bad_yaml_file)


def test_datamaster_initialization_unsupported_connector(mock_config_path, basic_api_configs):
    """測試設定檔中包含不支持的 Connector 名稱。"""
    config_with_bad_api = basic_api_configs.copy()
    config_with_bad_api["apis"]["unknown_api"] = {"priority": 1, "api_key": "bad_key"}
    config_file = mock_config_path(config_with_bad_api)

    # get_connector_class 會對 unknown_api 拋出 ValueError
    # DataMaster 應該能捕獲它並繼續初始化其他 Connector
    with patch('src.connectors.get_connector_class') as mock_get_class_val_err:
        def side_effect_val_err(name):
            if name == 'finmind': return MagicMock(return_value=MagicMock(spec=FinMindConnector))
            if name == 'yfinance': return MagicMock(return_value=MagicMock(spec=YFinanceConnector))
            if name == 'fred': return MagicMock(return_value=MagicMock(spec=FredConnector))
            if name == 'unknown_api': raise ValueError(f"Unsupported connector: {name}")
            return MagicMock() # Default for others if any
        mock_get_class_val_err.side_effect = side_effect_val_err

        dm = DataMaster(config_path=config_file) # 不應拋錯

    assert "unknown_api" not in dm.connectors # 不支持的 Connector 不應被加入
    assert "finmind" in dm.connectors # 其他 Connector 應正常初始化
    assert dm.api_priority_order == ["unknown_api", "finmind", "yfinance", "fred"] # 仍在優先級列表中


# --- 測試 _execute_fetch_operation 和公開的 get_* 方法 ---

@patch('src.connectors.get_connector_class')
def test_get_historical_price_primary_success(mock_get_class, mock_config_path, basic_api_configs,
                                              mock_finmind_connector_instance, mock_yfinance_connector_instance):
    """測試主要 Connector (FinMind) 成功獲取歷史價格。"""
    mock_get_class.side_effect = lambda name: {
        'finmind': MagicMock(return_value=mock_finmind_connector_instance),
        'yfinance': MagicMock(return_value=mock_yfinance_connector_instance),
        'fred': MagicMock(return_value=MagicMock(spec=FredConnector)) # Fred 也 mock 一下
    }.get(name, MagicMock())

    config_file = mock_config_path(basic_api_configs)
    dm = DataMaster(config_path=config_file)

    symbol = "AAPL"; start = "2023-01-01"; end = "2023-01-10"; interval = "1d"
    expected_df = pd.DataFrame({"close": [150.0], "security_id": [symbol]})
    mock_finmind_connector_instance.get_historical_price.return_value = expected_df

    result_df = dm.get_historical_price(symbol, start, end, interval)

    mock_finmind_connector_instance.get_historical_price.assert_called_once_with(symbol, start, end, interval=interval)
    mock_yfinance_connector_instance.get_historical_price.assert_not_called()
    pd.testing.assert_frame_equal(result_df, expected_df)


@patch('src.connectors.get_connector_class')
def test_get_historical_price_fallback_to_secondary(mock_get_class, mock_config_path, basic_api_configs,
                                                     mock_finmind_connector_instance, mock_yfinance_connector_instance):
    """測試主要 Connector 失敗，回退到次要 Connector (YFinance) 並成功。"""
    mock_get_class.side_effect = lambda name: {
        'finmind': MagicMock(return_value=mock_finmind_connector_instance),
        'yfinance': MagicMock(return_value=mock_yfinance_connector_instance),
        'fred': MagicMock(return_value=MagicMock(spec=FredConnector))
    }.get(name, MagicMock())

    config_file = mock_config_path(basic_api_configs)
    dm = DataMaster(config_path=config_file)

    symbol = "MSFT"; start = "2023-02-01"; end = "2023-02-05"
    # 模擬 FinMind 失敗 (返回空 DataFrame)
    mock_finmind_connector_instance.get_historical_price.return_value = pd.DataFrame()
    # 模擬 YFinance 成功
    expected_df_yf = pd.DataFrame({"close": [250.0], "security_id": [symbol]})
    mock_yfinance_connector_instance.get_historical_price.return_value = expected_df_yf

    result_df = dm.get_historical_price(symbol, start, end)

    mock_finmind_connector_instance.get_historical_price.assert_called_once_with(symbol, start, end, interval="1d")
    mock_yfinance_connector_instance.get_historical_price.assert_called_once_with(symbol, start, end, interval="1d")
    pd.testing.assert_frame_equal(result_df, expected_df_yf)

@patch('src.connectors.get_connector_class')
def test_get_historical_price_all_connectors_fail(mock_get_class, mock_config_path, basic_api_configs,
                                                  mock_finmind_connector_instance, mock_yfinance_connector_instance):
    """測試所有 Connector 都獲取歷史價格失敗。"""
    mock_get_class.side_effect = lambda name: {
        'finmind': MagicMock(return_value=mock_finmind_connector_instance),
        'yfinance': MagicMock(return_value=mock_yfinance_connector_instance),
        'fred': MagicMock(return_value=MagicMock(spec=FredConnector))
    }.get(name, MagicMock())

    config_file = mock_config_path(basic_api_configs)
    dm = DataMaster(config_path=config_file)

    # 模擬所有 Connector 都返回空 DataFrame
    mock_finmind_connector_instance.get_historical_price.return_value = pd.DataFrame()
    mock_yfinance_connector_instance.get_historical_price.return_value = pd.DataFrame()
    # FredConnector 沒有 get_historical_price，所以會被跳過

    result_df = dm.get_historical_price("FAILALL", "2023-03-01", "2023-03-02")

    assert result_df.empty # 最終應返回空 DataFrame
    mock_finmind_connector_instance.get_historical_price.assert_called_once()
    mock_yfinance_connector_instance.get_historical_price.assert_called_once()


@patch('src.connectors.get_connector_class')
def test_get_company_profile_specific_connector(mock_get_class, mock_config_path, basic_api_configs,
                                                 mock_finmind_connector_instance):
    """測試 get_company_profile，假設只有 FinMind 有此方法。"""
    # 讓 get_connector_class 對 finmind 返回 mock_finmind_connector_instance
    # 對其他 connector 返回一個沒有 get_company_profile 方法的 mock
    def side_effect_profile(name):
        if name == 'finmind':
            return MagicMock(return_value=mock_finmind_connector_instance)
        else: # yfinance, fred 等
            mock_other = MagicMock()
            del mock_other.get_company_profile # 確保沒有此方法
            return MagicMock(return_value=mock_other)

    mock_get_class.side_effect = side_effect_profile

    config_file = mock_config_path(basic_api_configs)
    dm = DataMaster(config_path=config_file)

    symbol = "2330.TW"
    expected_profile_df = pd.DataFrame({"name": ["TSMC"], "security_id": [symbol]})
    mock_finmind_connector_instance.get_company_profile.return_value = expected_profile_df

    result_df = dm.get_company_profile(symbol)

    mock_finmind_connector_instance.get_company_profile.assert_called_once_with(symbol)
    pd.testing.assert_frame_equal(result_df, expected_profile_df)

@patch('src.connectors.get_connector_class')
def test_get_fred_series_data_uses_fred_connector(mock_get_class, mock_config_path, basic_api_configs,
                                                   mock_fred_connector_instance):
    """測試 get_fred_series_data 是否正確調用 FredConnector。"""
    mock_get_class.side_effect = lambda name: {
        'finmind': MagicMock(return_value=MagicMock(spec=FinMindConnector)), # 其他 connector
        'yfinance': MagicMock(return_value=MagicMock(spec=YFinanceConnector)),
        'fred': MagicMock(return_value=mock_fred_connector_instance) # 關鍵的 Fred mock
    }.get(name, MagicMock())

    config_file = mock_config_path(basic_api_configs)
    dm = DataMaster(config_path=config_file)

    series_ids = ["GDP", "UNRATE"]
    start = "2022-01-01"; end = "2022-12-31"
    expected_fred_df = pd.DataFrame({"metric_value": [123, 3.5], "security_id": series_ids})
    mock_fred_connector_instance.get_series_data.return_value = expected_fred_df

    result_df = dm.get_fred_series_data(series_ids, start, end)

    mock_fred_connector_instance.get_series_data.assert_called_once_with(series_ids, start_date=start, end_date=end)
    pd.testing.assert_frame_equal(result_df, expected_fred_df)

    # 確保其他 Connector 的相關方法未被調用 (如果它們有 get_series_data)
    # (在這個測試中，其他 Connector 的 mock 實例沒有 get_series_data 方法，所以 hasattr 會失敗)


def test_datamaster_handles_connector_init_exception(mock_config_path, basic_api_configs):
    """測試當某個 Connector 初始化失敗時，DataMaster 仍能處理。"""
    config_file = mock_config_path(basic_api_configs)

    with patch('src.connectors.get_connector_class') as mock_get_class_init_fail:
        def side_effect_init_fail(name):
            if name == 'finmind': # 讓 FinMind 初始化成功
                return MagicMock(return_value=MagicMock(spec=FinMindConnector))
            if name == 'yfinance': # 讓 YFinance 初始化失敗
                mock_yf_class = MagicMock()
                mock_yf_class.side_effect = RuntimeError("YFinance init failed in test") # 類實例化時拋錯
                return mock_yf_class
            if name == 'fred':
                return MagicMock(return_value=MagicMock(spec=FredConnector))
            return MagicMock()
        mock_get_class_init_fail.side_effect = side_effect_init_fail

        dm = DataMaster(config_path=config_file) # 初始化不應拋錯

    assert "finmind" in dm.connectors
    assert "yfinance" not in dm.connectors # 初始化失敗的 Connector 不應存在
    assert "fred" in dm.connectors
    # 優先級順序仍然包含 yfinance，但它在 connectors 字典中不存在
    assert dm.api_priority_order == ["finmind", "yfinance", "fred"]

    # 嘗試獲取數據，應跳過 yfinance
    mock_finmind_instance = dm.connectors["finmind"]
    mock_finmind_instance.get_historical_price.return_value = pd.DataFrame({"data": ["finmind_ok"]})

    result = dm.get_historical_price("ANY", "s", "e")
    assert not result.empty
    assert result["data"].iloc[0] == "finmind_ok"
    mock_finmind_instance.get_historical_price.assert_called_once()
    # YFinance 的 get_historical_price 不應被調用，因為實例不存在
    # (如果 YFinance 的 mock 實例存在，則其方法會被調用，但這裡實例化失敗)


# 更多測試可以包括：
# - 測試不同 operation_name 的調用
# - 測試當 Connector 的方法返回非 DataFrame 或 (DataFrame, None) 時的處理
# - 測試 config.yaml 中 priority 缺失的情況
# - 測試空的 'apis' 設定
# - 測試 _execute_fetch_operation 中對 primary_args 和 keyword_args 的處理
# - 測試 DataMaster 是否正確地將 api_config 傳遞給每個 Connector 的建構函數
#   (已在 test_datamaster_initialization_success 中部分驗證)
