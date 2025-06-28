import pytest
import pandas as pd
from datetime import date, datetime, timezone
import os
from unittest.mock import patch, MagicMock
import requests # For simulating requests.exceptions

from src.connectors.polygon_io_connector import PolygonIOConnector

@pytest.fixture
def polygon_config():
    """提供一個標準的 PolygonIOConnector 設定。"""
    # 在測試中，我們通常會 mock API 調用，所以 API key 可以是假的
    # 但為了 Connector 初始化不報錯，還是需要提供一個
    return {
        "api_key": "TEST_POLYGON_API_KEY",
        "requests_per_minute": 60, # 測試時可以設高一點，因為我們會 mock time.sleep
        "base_url": "https://api.polygon.io" # 可以被測試覆蓋
    }

@pytest.fixture
def polygon_connector(polygon_config):
    """實例化一個 PolygonIOConnector。"""
    # 我們將 mock time.sleep 和 requests.Session.get
    with patch('time.sleep', return_value=None): # 避免測試等待
        connector = PolygonIOConnector(api_config=polygon_config)
        # 將 mock session 附加到 connector 實例上，以便在測試中控制其行為
        connector.session = MagicMock(spec=requests.Session)
        connector.session.headers = {} # 模擬真實 session 的 headers
        yield connector

# --- 輔助函數 ---
def mock_polygon_response(status_code: int, json_data: Optional[dict] = None, text_data: Optional[str] = None):
    """創建一個模擬的 requests.Response 物件。"""
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    if json_data is not None:
        mock_resp.json.return_value = json_data
    # 如果 json_data 為 None 但 status_code 不是 204，json() 可能會拋錯
    elif status_code != 204 :
        mock_resp.json.side_effect = requests.exceptions.JSONDecodeError("No JSON", "doc", 0)

    mock_resp.text = text_data if text_data is not None else (str(json_data) if json_data else "")

    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_resp)
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp

# --- 測試 _make_request ---
def test_make_request_success(polygon_connector, polygon_config):
    """測試 _make_request 成功的情況。"""
    endpoint = "/v2/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-01-01"
    mock_json = {"status": "OK", "results": [{"v": 100}]}
    polygon_connector.session.get.return_value = mock_polygon_response(200, json_data=mock_json)

    data, error = polygon_connector._make_request(endpoint)

    assert error is None
    assert data == mock_json
    polygon_connector.session.get.assert_called_once()
    call_args = polygon_connector.session.get.call_args
    assert call_args[0][0].startswith(polygon_config["base_url"])
    assert call_args[1]['params']['apiKey'] == polygon_config["api_key"]


def test_make_request_api_error_in_json(polygon_connector):
    """測試 API 在 JSON 回應中返回錯誤狀態。"""
    endpoint = "/v1/meta/symbols/AAPL/company"
    mock_json_error = {"status": "ERROR", "error": "Invalid API key."}
    polygon_connector.session.get.return_value = mock_polygon_response(200, json_data=mock_json_error) # HTTP 200 但內容是錯誤

    data, error = polygon_connector._make_request(endpoint)

    assert data is None
    assert error == "Invalid API key."

def test_make_request_http_403_error(polygon_connector):
    """測試 HTTP 403 (Forbidden) 錯誤。"""
    endpoint = "/forbidden/endpoint"
    polygon_connector.session.get.return_value = mock_polygon_response(403, text_data="Forbidden access")

    data, error = polygon_connector._make_request(endpoint)

    assert data is None
    assert "403" in error
    assert "Forbidden access" in error


@patch('time.sleep', return_value=None) # 確保 time.sleep 被 mock
def test_rate_limiting_is_called(mock_sleep_time, polygon_config):
    """測試速率控制是否按預期調用 time.sleep。"""
    # 設定一個較小的 requests_per_minute 以更容易觸發等待
    config_low_rpm = polygon_config.copy()
    config_low_rpm["requests_per_minute"] = 2 # 每分鐘2次，即每30秒一次
    connector_low_rpm = PolygonIOConnector(api_config=config_low_rpm)
    connector_low_rpm.session = MagicMock(spec=requests.Session) # Mock session

    # 第一次調用
    connector_low_rpm.session.get.return_value = mock_polygon_response(200, json_data={"status":"OK"})
    connector_low_rpm._make_request("/test1")
    mock_sleep_time.assert_not_called() # 第一次不應等待

    # 第二次調用 (幾乎立即)，應該觸發等待
    connector_low_rpm.session.get.return_value = mock_polygon_response(200, json_data={"status":"OK"})
    connector_low_rpm._make_request("/test2")
    mock_sleep_time.assert_called_once() # 第二次應該等待
    # 可以進一步驗證 sleep 的時間是否接近預期 (約 30 秒)
    # args, _ = mock_sleep_time.call_args
    # assert args[0] == pytest.approx(30, abs=0.1) # pytest.approx 用於浮點數比較

# --- 測試 get_historical_price ---
def test_get_historical_price_success(polygon_connector):
    symbol = "MSFT"
    start_date_str = "2023-01-03"
    end_date_str = "2023-01-04"

    mock_api_results = [
        {"t": 1672704000000, "o": 240.0, "h": 242.0, "l": 239.0, "c": 241.0, "v": 100000}, # 2023-01-03
        {"t": 1672790400000, "o": 241.5, "h": 243.5, "l": 240.5, "c": 242.5, "v": 120000}  # 2023-01-04
    ]
    mock_json = {"status": "OK", "results": mock_api_results, "resultsCount": 2, "ticker": symbol}
    polygon_connector.session.get.return_value = mock_polygon_response(200, json_data=mock_json)

    df = polygon_connector.get_historical_price(symbol, start_date_str, end_date_str)

    assert not df.empty
    assert len(df) == 2
    assert df['security_id'].iloc[0] == symbol
    assert df['open_price'].iloc[0] == 240.0
    assert df['price_date'].iloc[0] == date(2023, 1, 3)
    assert df['price_date'].iloc[1] == date(2023, 1, 4)
    assert 'last_updated_timestamp' in df.columns
    assert df['source_api'].iloc[0] == 'polygon.io'

def test_get_historical_price_no_results(polygon_connector):
    """測試 API 返回成功但無數據的情況。"""
    symbol = "EMPTYTIC"
    mock_json = {"status": "OK", "results": [], "resultsCount": 0, "ticker": symbol}
    polygon_connector.session.get.return_value = mock_polygon_response(200, json_data=mock_json)

    df = polygon_connector.get_historical_price(symbol, "2023-01-01", "2023-01-02")
    assert df.empty # 應返回空 DataFrame

def test_get_historical_price_api_call_fails(polygon_connector):
    """測試 API 調用失敗 (例如 HTTP 錯誤)。"""
    polygon_connector.session.get.return_value = mock_polygon_response(500, text_data="Server Error")
    df = polygon_connector.get_historical_price("FAIL", "2023-01-01", "2023-01-02")
    assert df.empty

# --- 測試 get_company_profile ---
def test_get_company_profile_success(polygon_connector):
    symbol = "GOOG"
    mock_profile_data = {
        "ticker": "GOOG", "name": "Alphabet Inc.", "description": "A tech company.",
        "sic_description": "Internet Services", "locale": "us", "primary_exchange": "XNAS",
        "market_cap": 1500000000000, "total_employees": 150000, "list_date": "2004-08-19",
        "homepage_url": "https://abc.xyz", "branding": {"logo_url": "https://example.com/logo.png"}
    }
    mock_json = {"status": "OK", "results": mock_profile_data, "request_id": "test_req_id"}
    polygon_connector.session.get.return_value = mock_polygon_response(200, json_data=mock_json)

    df = polygon_connector.get_company_profile(symbol)

    assert not df.empty
    assert len(df) == 1
    assert df['security_id'].iloc[0] == symbol
    assert df['name'].iloc[0] == "Alphabet Inc."
    assert df['industry'].iloc[0] == "Internet Services" # sic_description -> industry
    assert df['country'].iloc[0] == "US"
    assert df['logo_url'].iloc[0] == "https://example.com/logo.png"
    assert 'market_cap' in df.columns
    assert pd.api.types.is_numeric_dtype(df['market_cap'])


def test_get_company_profile_results_key_missing(polygon_connector):
    """測試 API 回應中缺少 'results' 鍵。"""
    mock_json_no_results = {"status": "OK", "message": "Some info but no results key"}
    polygon_connector.session.get.return_value = mock_polygon_response(200, json_data=mock_json_no_results)
    df = polygon_connector.get_company_profile("XYZ")
    assert df.empty

def test_get_company_profile_api_call_fails(polygon_connector):
    polygon_connector.session.get.return_value = mock_polygon_response(401, json_data={"error": "Unauthorized"})
    df = polygon_connector.get_company_profile("AUTHFAIL")
    assert df.empty

# 可以添加更多針對特定錯誤情況或邊界條件的測試。
# 例如：
# - 測試 API 金鑰未設定時 PolygonIOConnector 初始化是否拋出 ValueError。
# - 測試 _make_request 對於 JSON 解析失敗的處理。
# - 測試不同欄位在 API 回應中缺失時，DataFrame 的填充情況。
# - 測試時區處理 (如果 Polygon API 返回帶時區的時間戳)。
# - 測試請求參數的正確傳遞。

def test_polygon_connector_init_no_api_key():
    """測試初始化時未提供 API 金鑰。"""
    with pytest.raises(ValueError, match="Polygon.io API 金鑰未設定。"):
        PolygonIOConnector(api_config={}) # 空設定或缺少 api_key

    with pytest.raises(ValueError, match="Polygon.io API 金鑰未設定。"):
        PolygonIOConnector(api_config={"api_key": None, "requests_per_minute": 5})

    with pytest.raises(ValueError, match="Polygon.io API 金鑰未設定。"):
        PolygonIOConnector(api_config={"api_key": "", "requests_per_minute": 5})

def test_make_request_non_json_response(polygon_connector):
    """測試 API 返回非 JSON 格式的回應。"""
    endpoint = "/some/endpoint"
    polygon_connector.session.get.return_value = mock_polygon_response(200, text_data="This is not JSON")
    # mock_polygon_response 內部已設定 json.side_effect = JSONDecodeError

    data, error = polygon_connector._make_request(endpoint)

    assert data is None
    assert "無法解析 JSON 回應" in error
    assert "200" in error # 應包含狀態碼

def test_make_request_204_no_content(polygon_connector):
    """測試 API 返回 204 No Content。"""
    endpoint = "/empty/endpoint"
    polygon_connector.session.get.return_value = mock_polygon_response(204) # No json_data or text_data

    data, error = polygon_connector._make_request(endpoint)

    assert error is None
    assert data == {} # 應返回空字典代表成功但無數據
