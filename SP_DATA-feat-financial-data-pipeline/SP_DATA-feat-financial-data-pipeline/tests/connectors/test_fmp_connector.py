import pytest
import pandas as pd
from datetime import date, datetime, timezone
import os
from unittest.mock import patch, MagicMock
import requests # For simulating requests.exceptions

from src.connectors.fmp_connector import FMPConnector

@pytest.fixture
def fmp_config():
    """提供一個標準的 FMPConnector 設定。"""
    return {
        "api_key": "TEST_FMP_API_KEY",
        "requests_per_minute": 200,
        "base_url_v3": "https://financialmodelingprep.com/api/v3",
        "base_url_v4": "https://financialmodelingprep.com/api/v4"
    }

@pytest.fixture
def fmp_connector(fmp_config):
    """實例化一個 FMPConnector。"""
    with patch('time.sleep', return_value=None): # Mock time.sleep
        connector = FMPConnector(api_config=fmp_config)
        connector.session = MagicMock(spec=requests.Session) # Mock session
        yield connector

# --- 輔助函數 ---
def mock_fmp_response(status_code: int, json_data: Optional[Any] = None, text_data: Optional[str] = None):
    """創建一個模擬的 requests.Response 物件 for FMP。"""
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    if json_data is not None:
        mock_resp.json.return_value = json_data
    elif status_code != 204:
        mock_resp.json.side_effect = requests.exceptions.JSONDecodeError("No JSON", "doc", 0)

    mock_resp.text = text_data if text_data is not None else (str(json_data) if json_data else "")

    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_resp)
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp

# --- 測試 _make_request ---
def test_fmp_make_request_success_list_response(fmp_connector, fmp_config):
    """測試 _make_request 成功，API 返回列表。"""
    endpoint = "/profile/AAPL"
    mock_json_list = [{"symbol": "AAPL", "companyName": "Apple Inc."}]
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_json_list)

    data, error = fmp_connector._make_request(endpoint, api_version="v3")

    assert error is None
    assert data == mock_json_list
    fmp_connector.session.get.assert_called_once()
    call_args = fmp_connector.session.get.call_args
    assert call_args[0][0].startswith(fmp_config["base_url_v3"])
    assert call_args[1]['params']['apikey'] == fmp_config["api_key"]

def test_fmp_make_request_success_dict_response(fmp_connector):
    """測試 _make_request 成功，API 返回字典 (例如歷史價格)。"""
    endpoint = "/historical-price-full/MSFT"
    mock_json_dict = {"symbol": "MSFT", "historical": [{"date": "2023-01-01", "close": 240.0}]}
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_json_dict)

    data, error = fmp_connector._make_request(endpoint, params={"from": "2023-01-01"})

    assert error is None
    assert data == mock_json_dict

def test_fmp_make_request_api_error_in_dict_json(fmp_connector):
    """測試 API 在字典 JSON 回應中返回 "Error Message"。"""
    endpoint = "/some/endpoint"
    mock_json_error = {"Error Message": "Invalid request for this endpoint."}
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_json_error)

    data, error = fmp_connector._make_request(endpoint)

    assert data is None
    assert error == "Invalid request for this endpoint."

def test_fmp_make_request_api_error_in_list_json(fmp_connector):
    """測試 API 在列表 JSON 回應的第一個元素中返回 "Error Message"。"""
    endpoint = "/another/endpoint"
    mock_json_error_list = [{"Error Message": "Ticker not found."}]
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_json_error_list)

    data, error = fmp_connector._make_request(endpoint)

    assert data is None
    assert error == "Ticker not found."


def test_fmp_make_request_http_401_unauthorized(fmp_connector):
    """測試 HTTP 401 (Unauthorized) 錯誤。"""
    endpoint = "/premium/data"
    fmp_connector.session.get.return_value = mock_fmp_response(401, text_data="Invalid API key")

    data, error = fmp_connector._make_request(endpoint)

    assert data is None
    assert error == "FMP API 金鑰無效或未授權。"


# --- 測試 get_historical_price ---
def test_get_historical_price_success(fmp_connector):
    symbol = "AAPL"
    mock_api_data = {
        "symbol": "AAPL",
        "historical": [
            {"date": "2023-01-03", "open": 130.0, "close": 125.0, "volume": 1000},
            {"date": "2023-01-04", "open": 125.5, "close": 126.0, "volume": 1200}
        ]
    }
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_api_data)
    df = fmp_connector.get_historical_price(symbol, from_date="2023-01-03", to_date="2023-01-04")

    assert not df.empty
    assert len(df) == 2
    assert df['security_id'].iloc[0] == symbol
    assert df['open_price'].iloc[0] == 130.0
    assert df['price_date'].iloc[0] == date(2023, 1, 3)
    assert 'adj_close_price' in df.columns # FMP historical-price-full 包含 adjClose

def test_get_historical_price_no_historical_data(fmp_connector):
    symbol = "NODATA"
    mock_api_data = {"symbol": "NODATA", "historical": []} # 空的 historical 列表
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_api_data)
    df = fmp_connector.get_historical_price(symbol)
    assert df.empty

def test_get_historical_price_api_error(fmp_connector):
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data={"Error Message": "Limit reached"})
    df = fmp_connector.get_historical_price("ERROR")
    assert df.empty

# --- 測試 get_company_profile ---
def test_get_company_profile_success(fmp_connector):
    symbol = "MSFT"
    mock_profile = [{
        "symbol": "MSFT", "companyName": "Microsoft Corp.", "sector": "Technology",
        "industry": "Software", "website": "http://www.microsoft.com", "mktCap": 2000000000000,
        "image": "msft.png"
    }]
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_profile)
    df = fmp_connector.get_company_profile(symbol)

    assert not df.empty
    assert len(df) == 1
    assert df['security_id'].iloc[0] == symbol
    assert df['name'].iloc[0] == "Microsoft Corp."
    assert df['market_cap'].iloc[0] == 2000000000000.0
    assert df['image_url'].iloc[0] == "msft.png"

def test_get_company_profile_empty_list_response(fmp_connector):
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=[]) # API 返回空列表
    df = fmp_connector.get_company_profile("EMPTY")
    assert df.empty

# --- 測試 get_income_statement ---
def test_get_income_statement_annual_success(fmp_connector):
    symbol = "GOOG"
    mock_statements = [
        {"date": "2022-12-31", "symbol": "GOOG", "reportedCurrency": "USD", "revenue": 282836000000, "netIncome": 59972000000, "period": "FY"},
        {"date": "2021-12-31", "symbol": "GOOG", "reportedCurrency": "USD", "revenue": 257637000000, "netIncome": 76033000000, "period": "FY"}
    ]
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=mock_statements)
    df = fmp_connector.get_income_statement(symbol, period="annual", limit=2)

    assert not df.empty
    # 每個財報會產生多行 (每個指標一行)
    num_metrics_per_statement = len(mock_statements[0]) - len(['date', 'symbol', 'reportedCurrency', 'cik', 'fillingDate', 'acceptedDate', 'calendarYear', 'period', 'link', 'finalLink'])
    assert len(df) == 2 * num_metrics_per_statement

    revenue_rows = df[df['metric_name'] == 'revenue']
    assert len(revenue_rows) == 2
    assert revenue_rows['metric_value'].iloc[0] == 282836000000
    assert revenue_rows['report_date'].iloc[0] == date(2022, 12, 31)
    assert revenue_rows['period_type'].iloc[0] == 'annual'
    assert revenue_rows['fmp_period_reported'].iloc[0] == 'FY'

def test_get_income_statement_no_data(fmp_connector):
    fmp_connector.session.get.return_value = mock_fmp_response(200, json_data=[]) # API 返回空列表
    df = fmp_connector.get_income_statement("NODATAINC", period="quarter")
    assert df.empty

# 其他測試
def test_fmp_connector_init_no_api_key():
    with pytest.raises(ValueError, match="FMP API 金鑰未設定。"):
        FMPConnector(api_config={})

    with pytest.raises(ValueError, match="FMP API 金鑰未設定。"):
        FMPConnector(api_config={"api_key": None})

    with pytest.raises(ValueError, match="FMP API 金鑰未設定。"):
        FMPConnector(api_config={"api_key": ""})

@patch('time.sleep', return_value=None)
def test_fmp_rate_limiting_called(mock_tsleep, fmp_config):
    config_low_rpm_fmp = fmp_config.copy()
    config_low_rpm_fmp["requests_per_minute"] = 1 # 每分鐘1次，即每60秒一次
    connector_fmp_low_rpm = FMPConnector(api_config=config_low_rpm_fmp)
    connector_fmp_low_rpm.session = MagicMock(spec=requests.Session)

    connector_fmp_low_rpm.session.get.return_value = mock_fmp_response(200, json_data=[{"symbol":"T1"}])
    connector_fmp_low_rpm._make_request("/test1")
    mock_tsleep.assert_not_called()

    connector_fmp_low_rpm.session.get.return_value = mock_fmp_response(200, json_data=[{"symbol":"T2"}])
    connector_fmp_low_rpm._make_request("/test2")
    mock_tsleep.assert_called_once()
    # args_sleep, _ = mock_tsleep.call_args
    # assert args_sleep[0] == pytest.approx(60, abs=0.1)
