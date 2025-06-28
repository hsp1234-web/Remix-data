import pytest
import pandas as pd
from datetime import date, datetime, timezone
import os
from unittest.mock import patch, MagicMock
import requests # For simulating requests.exceptions

from src.connectors.alpha_vantage_connector import AlphaVantageConnector

@pytest.fixture
def av_config():
    """提供一個標準的 AlphaVantageConnector 設定。"""
    return {
        "api_key": "TEST_AV_API_KEY",
        "requests_per_minute": 5, # 免費方案限制
        "base_url": "https://www.alphavantage.co/query"
    }

@pytest.fixture
def av_connector(av_config):
    """實例化一個 AlphaVantageConnector。"""
    with patch('time.sleep', return_value=None): # Mock time.sleep
        connector = AlphaVantageConnector(api_config=av_config)
        connector.session = MagicMock(spec=requests.Session) # Mock session
        yield connector

# --- 輔助函數 ---
def mock_av_response(status_code: int, json_data: Optional[dict] = None, text_data: Optional[str] = None):
    """創建一個模擬的 requests.Response 物件 for Alpha Vantage。"""
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
def test_av_make_request_success(av_connector, av_config):
    """測試 _make_request 成功。"""
    params = {"function": "OVERVIEW", "symbol": "IBM"}
    mock_json_data = {"Symbol": "IBM", "Name": "International Business Machines"}
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_json_data)

    data, error = av_connector._make_request(params)

    assert error is None
    assert data == mock_json_data
    av_connector.session.get.assert_called_once()
    call_args = av_connector.session.get.call_args
    assert call_args[0][0] == av_config["base_url"]
    expected_req_params = params.copy()
    expected_req_params['apikey'] = av_config["api_key"]
    assert call_args[1]['params'] == expected_req_params

def test_av_make_request_error_message_in_json(av_connector):
    """測試 API 在 JSON 中返回 "Error Message"。"""
    params = {"function": "TIME_SERIES_DAILY", "symbol": "BAD"}
    mock_json_error = {"Error Message": "Invalid API call."}
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_json_error)

    data, error = av_connector._make_request(params)
    assert data is None
    assert error == "Invalid API call."

def test_av_make_request_information_in_json(av_connector):
    """測試 API 在 JSON 中返回 "Information" (通常表示速率限制)。"""
    params = {"function": "GLOBAL_QUOTE", "symbol": "MSFT"}
    mock_json_info = {"Information": "Rate limit exceeded."}
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_json_info)

    data, error = av_connector._make_request(params)
    assert data is None
    assert error == "Rate limit exceeded."


def test_av_make_request_http_500_error(av_connector):
    params = {"function": "ANY_FUNC", "symbol": "ANY"}
    av_connector.session.get.return_value = mock_av_response(500, text_data="Server Error")
    data, error = av_connector._make_request(params)
    assert data is None
    assert "500" in error
    assert "Server Error" in error

# --- 測試 get_historical_price_daily_adjusted ---
def test_get_historical_price_success(av_connector):
    symbol = "IBM"
    mock_api_data = {
        "Meta Data": {"2. Symbol": "IBM"},
        "Time Series (Daily)": {
            "2023-01-04": {"1. open": "140.0", "4. close": "142.0", "6. volume": "1000"},
            "2023-01-03": {"1. open": "139.0", "4. close": "140.5", "6. volume": "1200"}
        }
    }
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_api_data)
    df = av_connector.get_historical_price_daily_adjusted(symbol)

    assert not df.empty
    assert len(df) == 2
    assert df['security_id'].iloc[0] == symbol.upper()
    # 排序後，第一筆應是 2023-01-03
    assert df['price_date'].iloc[0] == date(2023, 1, 3)
    assert df['close_price'].iloc[0] == 140.5
    assert 'adj_close_price' in df.columns # 即使 API 沒返回，也應填充

def test_get_historical_price_no_time_series_data(av_connector):
    symbol = "NODATA"
    mock_api_data = {"Meta Data": {"2. Symbol": "NODATA"}} # 沒有 "Time Series (Daily)"
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_api_data)
    df = av_connector.get_historical_price_daily_adjusted(symbol)
    assert df.empty

# --- 測試 get_company_overview ---
def test_get_company_overview_success(av_connector):
    symbol = "MSFT"
    mock_overview_data = {
        "Symbol": "MSFT", "Name": "Microsoft Corporation", "Sector": "Technology",
        "MarketCapitalization": "2000000000000", "PERatio": "30.5"
    }
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_overview_data)
    df = av_connector.get_company_overview(symbol)

    assert not df.empty
    assert len(df) == 1
    assert df['security_id'].iloc[0] == symbol.upper()
    assert df['name'].iloc[0] == "Microsoft Corporation"
    assert df['market_cap'].iloc[0] == 2000000000000.0
    assert df['pe_ratio'].iloc[0] == 30.5

def test_get_company_overview_api_error(av_connector):
    av_connector.session.get.return_value = mock_av_response(200, json_data={"Error Message": "No data."})
    df = av_connector.get_company_overview("ERROR")
    assert df.empty

# --- 測試 get_income_statement_annual ---
def test_get_income_statement_annual_success(av_connector):
    symbol = "AAPL"
    mock_income_data = {
        "symbol": "AAPL",
        "annualReports": [
            {"fiscalDateEnding": "2022-09-30", "reportedCurrency": "USD", "totalRevenue": "394328000000", "netIncome": "99803000000"},
            {"fiscalDateEnding": "2021-09-30", "reportedCurrency": "USD", "totalRevenue": "365817000000", "netIncome": "94680000000"}
        ]
    }
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_income_data)
    df = av_connector.get_income_statement_annual(symbol)

    assert not df.empty
    num_metrics_per_report = len(mock_income_data["annualReports"][0]) - 2 # - fiscalDateEnding, reportedCurrency
    assert len(df) == 2 * num_metrics_per_report

    revenue_rows = df[df['metric_name'] == 'totalRevenue']
    assert len(revenue_rows) == 2
    # 排序後，2021 在前
    assert revenue_rows['metric_value'].iloc[0] == 365817000000.0
    assert revenue_rows['report_date'].iloc[0] == date(2021, 9, 30)
    assert revenue_rows['period_type'].iloc[0] == 'annual'


def test_get_income_statement_no_annual_reports(av_connector):
    symbol = "NOFIN"
    mock_income_data = {"symbol": "NOFIN", "annualReports": []} # 空列表
    av_connector.session.get.return_value = mock_av_response(200, json_data=mock_income_data)
    df = av_connector.get_income_statement_annual(symbol)
    assert df.empty

# 其他
def test_av_connector_init_no_api_key():
    with pytest.raises(ValueError, match="Alpha Vantage API 金鑰未設定。"):
        AlphaVantageConnector(api_config={})

@patch('time.sleep', return_value=None)
def test_av_rate_limiting_called(mock_av_sleep, av_config):
    config_low_rpm_av = av_config.copy()
    config_low_rpm_av["requests_per_minute"] = 1 # 1 RPM = 60s interval
    connector_av_low_rpm = AlphaVantageConnector(api_config=config_low_rpm_av)
    connector_av_low_rpm.session = MagicMock(spec=requests.Session)

    # 第一次
    connector_av_low_rpm.session.get.return_value = mock_av_response(200, json_data={"Symbol":"S1"})
    connector_av_low_rpm._make_request({"function":"OVERVIEW", "symbol":"S1"})
    mock_av_sleep.assert_not_called()

    # 第二次
    connector_av_low_rpm.session.get.return_value = mock_av_response(200, json_data={"Symbol":"S2"})
    connector_av_low_rpm._make_request({"function":"OVERVIEW", "symbol":"S2"})
    mock_av_sleep.assert_called_once()
    # args_s, _ = mock_av_sleep.call_args
    # assert args_s[0] == pytest.approx(60, abs=0.1)
