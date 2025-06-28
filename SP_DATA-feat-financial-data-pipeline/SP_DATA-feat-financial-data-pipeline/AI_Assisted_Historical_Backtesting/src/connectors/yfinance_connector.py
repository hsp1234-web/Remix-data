import urllib.request
import urllib.parse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger
from AI_Assisted_Historical_Backtesting.src.utils.error_handler import retry_with_exponential_backoff

logger = get_logger(__name__)

# Yahoo Finance v7/v8 API (非官方) 的基礎 URL
# 注意：這些是非官方端點，可能會更改或失效。
YF_BASE_URL_V7 = "https://query1.finance.yahoo.com/v7/finance/download"
YF_BASE_URL_V8 = "https://query1.finance.yahoo.com/v8/finance/chart" # v8 用於獲取更細粒度的數據

# 預設的快取目錄
DEFAULT_CACHE_DIR = "yf_cache"
# 預設快取過期時間（秒），例如 1 小時
DEFAULT_CACHE_EXPIRY_SECONDS = 3600

# 模擬瀏覽器的 User-Agent，有些 API 可能需要
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

class YFinanceConnector:
    """
    用於從 Yahoo Finance (非官方 API) 獲取股票、期貨等市場數據的連接器。
    實現了本地文件快取和指數退避重試機制。
    遵循零依賴原則 (除了 Python 標準庫)。
    """

    def __init__(self, cache_dir_base=None, cache_expiry_seconds=DEFAULT_CACHE_EXPIRY_SECONDS, user_agent=DEFAULT_USER_AGENT):
        """
        初始化 YFinanceConnector。

        Args:
            cache_dir_base (str, optional): 快取目錄的基礎路徑。
                                           如果為 None，則會在項目根目錄的 'data/' 下創建 'yf_cache'。
                                           實際路徑會是 `data_path_from_env_or_default/cache_dir_name`。
            cache_expiry_seconds (int, optional): 快取文件的過期時間（秒）。預設 3600 秒 (1 小時)。
            user_agent (str, optional): 請求時使用的 User-Agent。
        """
        if cache_dir_base:
            self.cache_dir = cache_dir_base
        else:
            # 嘗試從環境變量獲取數據目錄，否則使用項目相對路徑
            # 這部分可以做得更通用，例如從一個全局配置模塊讀取
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # AI_Assisted_Historical_Backtesting
            data_dir = os.path.join(project_root, "data")
            self.cache_dir = os.path.join(data_dir, DEFAULT_CACHE_DIR)

        if not os.path.exists(self.cache_dir):
            try:
                os.makedirs(self.cache_dir)
                logger.info(f"創建快取目錄: {self.cache_dir}")
            except OSError as e:
                logger.error(f"創建快取目錄 {self.cache_dir} 失敗: {e}", exc_info=True)
                # 如果無法創建快取目錄，快取功能將受影響，但不應阻止初始化
                self.cache_dir = None # 禁用快取

        self.cache_expiry_seconds = cache_expiry_seconds
        self.user_agent = user_agent
        logger.info(f"YFinanceConnector 初始化成功。快取目錄: {self.cache_dir if self.cache_dir else '已禁用'}, 快取過期時間: {self.cache_expiry_seconds} 秒。")

    def _get_cache_filepath(self, ticker, params_str):
        """生成快取文件的完整路徑。"""
        if not self.cache_dir:
            return None
        # 使用 ticker 和參數字符串的哈希值（或簡化版）作為文件名，避免特殊字符
        # 為了零依賴，不使用 hashlib，用簡單的替換
        filename_safe_ticker = "".join(c if c.isalnum() else "_" for c in ticker)
        filename_safe_params = "".join(c if c.isalnum() else "_" for c in params_str if c not in "=&?")[:50] # 取前50個字符
        cache_filename = f"{filename_safe_ticker}_{filename_safe_params}.json" # 假設都是 JSON 數據
        return os.path.join(self.cache_dir, cache_filename)

    def _read_from_cache(self, filepath):
        """從快取文件讀取數據並檢查是否過期。"""
        if not filepath or not os.path.exists(filepath):
            return None

        try:
            file_mod_time = os.path.getmtime(filepath)
            if (time.time() - file_mod_time) > self.cache_expiry_seconds:
                logger.info(f"快取文件 {filepath} 已過期。")
                os.remove(filepath) # 刪除過期快取
                return None

            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.info(f"從快取文件 {filepath} 成功讀取數據。")
                # 假設快取的是 JSON 字符串，直接返回字符串，讓調用者解析
                return content
        except Exception as e:
            logger.warning(f"讀取快取文件 {filepath} 失敗: {e}", exc_info=True)
            return None

    def _write_to_cache(self, filepath, content_str):
        """將數據寫入快取文件。"""
        if not filepath or not isinstance(content_str, str):
            return
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content_str)
            logger.info(f"數據已成功寫入快取文件: {filepath}")
        except Exception as e:
            logger.warning(f"寫入快取文件 {filepath} 失敗: {e}", exc_info=True)

    def _make_request_headers(self):
        """創建請求頭。"""
        return {
            "User-Agent": self.user_agent
            # Yahoo Finance 有時可能需要 'Cookie' 和 'Crumb' 來訪問某些數據，
            # 但這會使零依賴請求變得非常複雜 (需要先獲取 cookie/crumb)。
            # download 端點 (v7) 通常不需要。
        }

    @retry_with_exponential_backoff(max_retries=4, initial_delay=3, backoff_factor=2.5,
                                    allowed_exceptions=(urllib.error.URLError, TimeoutError, ConnectionResetError, json.JSONDecodeError))
    def _fetch_data_from_yahoo(self, url, params=None):
        """
        內部輔助函數，用於從 Yahoo Finance API 獲取數據。
        應用重試邏輯。

        Args:
            url (str): 請求的完整 URL。
            params (dict, optional): URL 查詢參數 (v8 chart API 可能需要)。

        Returns:
            str: API 返回的響應體文本。

        Raises:
            urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError
        """
        query_string = urllib.parse.urlencode(params) if params else ""
        full_url = f"{url}?{query_string}" if query_string else url

        # 移除 URL 中的敏感信息 (如果有的話) 以供日誌記錄
        log_url = full_url
        # (如果 params 中包含 api_key 等，可以在這裡處理)

        logger.info(f"向 Yahoo Finance 發起請求: {log_url}")
        headers = self._make_request_headers()
        request = urllib.request.Request(full_url, headers=headers)

        try:
            with urllib.request.urlopen(request, timeout=25) as response: # 增加超時
                status_code = response.getcode()
                response_body_bytes = response.read()
                # Yahoo Finance 的 CSV 下載通常是 utf-8，chart API 是 JSON (utf-8)
                response_body_str = response_body_bytes.decode('utf-8', 'ignore')

                logger.debug(f"Yahoo Finance API 響應狀態碼: {status_code} for URL: {log_url}")
                if status_code != 200:
                    logger.error(f"Yahoo Finance API 請求失敗，狀態碼: {status_code}, URL: {log_url}, 響應 (前500字符): {response_body_str[:500]}")
                    # 嘗試解析可能的錯誤信息 (Yahoo 通常直接返回錯誤文本或 HTML)
                    # 如果是 JSON API (如 v8 chart)，錯誤可能是 JSON 格式
                    try:
                        error_data = json.loads(response_body_str)
                        error_message = error_data.get("chart", {}).get("error", {}).get("description", "未知 API 錯誤")
                        logger.error(f"Yahoo Finance API 錯誤詳情 (JSON): {error_message}")
                    except json.JSONDecodeError:
                        # 如果不是 JSON，就記錄原始響應的一部分
                        logger.error(f"Yahoo Finance API 錯誤響應不是有效的 JSON。原始響應片段: {response_body_str[:200]}")
                    raise urllib.error.HTTPError(full_url, status_code, f"Yahoo API Error: {response_body_str[:200]}", response.headers, response.fp) # type: ignore

                # 即使是 200，內容也可能表示錯誤（例如，v8 chart API 的 JSON 內部錯誤結構）
                if "finance.yahoo.com" in response_body_str and ("DOCTYPE html" in response_body_str.upper() or "lookup" in response_body_str.lower()):
                     logger.warning(f"Yahoo Finance API 響應看起來像 HTML 頁面或查找頁面，可能表示 ticker '{params.get('symbol', 'N/A') if params else 'N/A'}' 無效或 API 端點問題. URL: {log_url}")
                     # 這種情況下，我們可能不希望重試，或者將其視為一種特定錯誤
                     # 為了簡化，這裡我們讓它繼續，如果後續解析失敗（例如期待CSV但得到HTML），則會觸發錯誤

                return response_body_str

        except urllib.error.HTTPError as e:
            logger.error(f"Yahoo Finance API HTTP 錯誤: {e.code} {e.reason}. URL: {log_url}")
            try:
                error_content = e.read().decode('utf-8', 'ignore')
                logger.error(f"Yahoo Finance API 錯誤響應體: {error_content[:500]}")
            except Exception as read_err:
                logger.error(f"讀取 HTTPError 響應體失敗: {read_err}")
            raise
        except urllib.error.URLError as e: # 包括無法解析主機名、無法連接等
            logger.error(f"Yahoo Finance API URL 錯誤 (網路問題): {e.reason}. URL: {log_url}")
            raise
        except TimeoutError:
            logger.error(f"Yahoo Finance API 請求超時. URL: {log_url}")
            raise
        except json.JSONDecodeError as e: # 如果期望 JSON 但解析失敗
            logger.error(f"無法解析來自 Yahoo Finance API 的 JSON 響應: {e}. URL: {log_url}", exc_info=True)
            raise # 讓 retry 機制捕獲
        except Exception as e:
            logger.error(f"獲取 Yahoo Finance 數據時發生未知錯誤: {e}. URL: {log_url}", exc_info=True)
            raise


    def get_historical_data_csv(self, ticker, start_date_str, end_date_str, interval="1d"):
        """
        獲取指定股票代碼的歷史數據 (OHLCV)，以 CSV 格式字符串返回。
        使用 Yahoo Finance v7 download 端點。

        Args:
            ticker (str): 股票代碼 (例如 "AAPL", "MSFT", "^GSPC" (S&P 500), "EURUSD=X" (外匯))。
            start_date_str (str): 開始日期，格式 "YYYY-MM-DD"。
            end_date_str (str): 結束日期，格式 "YYYY-MM-DD"。
                                Yahoo API 需要的是 Unix 時間戳。
            interval (str, optional): 數據間隔。可以是 "1d" (每日), "1wk" (每週), "1mo" (每月)。
                                     預設 "1d"。

        Returns:
            str or None: CSV 格式的歷史數據字符串，如果成功。
                         列頭通常是: Date,Open,High,Low,Close,Adj Close,Volume
                         如果發生錯誤或無數據，則返回 None。
        """
        try:
            # 將 YYYY-MM-DD 轉換為 Unix 時間戳 (UTC)
            # Yahoo Finance API 期望的是 UTC 午夜的時間戳
            period1 = int(datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            # 結束日期通常需要加一天才能包含當天數據，或者取結束日期的午夜
            # 為了包含 end_date_str 當天的數據，時間戳應為 end_date_str 的 23:59:59 或 end_date_str + 1 天的 00:00:00
            period2 = int((datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            logger.error(f"日期格式無效: start='{start_date_str}', end='{end_date_str}'. 應為 YYYY-MM-DD。")
            return None

        params = {
            "period1": str(period1),
            "period2": str(period2),
            "interval": interval,
            "events": "history", # 也可以是 "div" (股息), "split" (拆股)
            "includeAdjustedClose": "true"
        }

        # 構建快取文件名相關的參數字符串
        params_str_for_cache = f"v7_csv_{start_date_str}_{end_date_str}_{interval}"
        cache_filepath = self._get_cache_filepath(ticker, params_str_for_cache)

        # 嘗試從快取讀取
        if self.cache_dir and cache_filepath:
            cached_data_str = self._read_from_cache(cache_filepath)
            if cached_data_str:
                return cached_data_str

        # 如果快取未命中或過期，則從 API 獲取
        request_url = f"{YF_BASE_URL_V7}/{urllib.parse.quote(ticker)}" # ticker 需要 URL 編碼

        try:
            csv_data_str = self._fetch_data_from_yahoo(request_url, params)

            if csv_data_str:
                # 驗證 CSV 數據是否合理
                # 1. 是否為 HTML 錯誤頁面 (簡單檢查)
                if csv_data_str.strip().lower().startswith("<!doctype html") or \
                   csv_data_str.strip().lower().startswith("<html"):
                    logger.error(f"Ticker '{ticker}' 的請求返回了 HTML 頁面，可能表示錯誤或 ticker 無效。URL params: {params}")
                    return None

                # 2. 檢查是否包含預期的 CSV 列頭
                # 移除可能的 UTF-8 BOM (如果存在)
                lines = csv_data_str.splitlines()
                if not lines:
                    logger.warning(f"Ticker '{ticker}' 的 CSV 數據為空。URL params: {params}")
                    return None

                header_line = lines[0].lstrip('\ufeff') # 移除 BOM
                expected_headers = ["Date", "Open", "High", "Low", "Close"] # 至少應包含這些

                # 有些地區的 Yahoo Finance 可能返回本地化列頭，或順序不同
                # 這裡做一個基本檢查
                header_parts = [h.strip('"') for h in header_line.split(',')]

                if not all(expected_header in header_parts for expected_header in expected_headers):
                    logger.warning(f"從 Yahoo Finance 收到的 CSV 數據格式不符合預期 for ticker {ticker}. "
                                   f"列頭: '{header_line}'. 響應 (前200字符): {csv_data_str[:200]}. URL params: {params}")
                    # 檢查是否是 Yahoo 的 "Could not get data" 類型的 JSON 錯誤信息 (儘管我們期望CSV)
                    # {"chart":{"result":null,"error":{"code":"Not Found","description":"No data found, symbol may be delisted"}}}
                    try:
                        json_error_check = json.loads(csv_data_str)
                        if json_error_check.get("chart", {}).get("error"):
                            logger.error(f"Ticker '{ticker}' 返回了 JSON 錯誤信息而非 CSV: {json_error_check['chart']['error']}")
                            return None
                    except json.JSONDecodeError:
                        # 不是 JSON 錯誤，那就是未知的非 CSV 內容
                        pass # 繼續執行下面的快取和返回，但已有警告

                    # 如果格式嚴重不符，可以選擇返回 None
                    # return None

                if self.cache_dir and cache_filepath:
                    self._write_to_cache(cache_filepath, csv_data_str)
                return csv_data_str
            else:
                # _fetch_data_from_yahoo 內部應該已經記錄了錯誤
                logger.warning(f"未能從 Yahoo Finance 獲取 ticker '{ticker}' 的 CSV 數據。")
                return None
        except Exception as e:
            # logger.error(f"獲取 ticker '{ticker}' 的 CSV 數據時發生頂層錯誤: {e}", exc_info=True)
            # _fetch_data_from_yahoo 內部應該已經記錄並可能重試，這裡捕獲最終的錯誤
            # 如果 retry_with_exponential_backoff 耗盡重試後仍失敗，會重新拋出異常
            return None


    def get_chart_data_json(self, ticker, interval="1d", range_str=None, start_timestamp=None, end_timestamp=None):
        """
        獲取指定股票代碼的圖表數據 (OHLCV, adjclose 等)，以解析後的 JSON (dict) 返回。
        使用 Yahoo Finance v8 chart 端點。此端點返回 JSON 格式。

        Args:
            ticker (str): 股票代碼。
            interval (str, optional): 數據間隔。例如 "1m", "2m", "5m", "15m", "30m", "60m", "90m",
                                     "1h", "1d", "5d", "1wk", "1mo", "3mo"。預設 "1d"。
            range_str (str, optional): 數據範圍。例如 "1d", "5d", "1mo", "3mo", "6mo", "1y",
                                      "2y", "5y", "10y", "ytd", "max"。
                                      如果提供了 range_str，則忽略 start/end_timestamp。
            start_timestamp (int, optional): 開始時間的 Unix 時間戳 (UTC)。
            end_timestamp (int, optional): 結束時間的 Unix 時間戳 (UTC)。

        Returns:
            dict or None: 解析後的 JSON 數據 (通常包含 'chart': {'result': [{}], 'error': ...})。
                          如果成功，result[0] 包含 'meta', 'timestamp', 'indicators'。
                          'indicators': {'quote': [{'open': [], 'high': [], ...}], 'adjclose': [{'adjclose': []}]}
                          如果發生錯誤或無數據，則返回 None。
        """
        params = {
            "symbol": ticker,
            "interval": interval,
            "indicators": "quote,adjclose", # 獲取 OHLCV 和調整後收盤價
            # "events": "div,split,capitalGains", # 可以包含事件
            "includePrePost": "false", # 是否包含盤前盤後數據 (對於日內數據有用)
        }
        if range_str:
            params["range"] = range_str
            params_str_for_cache = f"v8_json_{interval}_{range_str}"
        elif start_timestamp and end_timestamp:
            params["period1"] = str(start_timestamp)
            params["period2"] = str(end_timestamp)
            # 將時間戳轉換為日期字符串以用於快取文件名，避免文件名過長
            start_dt_str = datetime.fromtimestamp(start_timestamp, tz=timezone.utc).strftime("%Y%m%d")
            end_dt_str = datetime.fromtimestamp(end_timestamp, tz=timezone.utc).strftime("%Y%m%d")
            params_str_for_cache = f"v8_json_{interval}_{start_dt_str}_{end_dt_str}"
        else:
            logger.error("必須提供 range_str 或 (start_timestamp 和 end_timestamp) 中的一組參數。")
            return None

        cache_filepath = self._get_cache_filepath(ticker, params_str_for_cache)

        if self.cache_dir and cache_filepath:
            cached_data_str = self._read_from_cache(cache_filepath)
            if cached_data_str:
                try:
                    return json.loads(cached_data_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"快取的 JSON 數據無效 {cache_filepath}: {e}. 將重新獲取。")

        request_url = f"{YF_BASE_URL_V8}/{urllib.parse.quote(ticker)}"

        try:
            json_data_str = self._fetch_data_from_yahoo(request_url, params)
            if json_data_str:
                try:
                    data = json.loads(json_data_str)
                    # 檢查 API 返回的 JSON 中是否有錯誤指示
                    if data.get("chart", {}).get("error") is not None:
                        error_info = data["chart"]["error"]
                        logger.error(f"Yahoo Finance v8 API 返回錯誤 for ticker {ticker}: "
                                     f"Code: {error_info.get('code')}, Description: {error_info.get('description')}")
                        return None # API 內部錯誤，不應快取

                    # 確保結果存在且不為空
                    if not data.get("chart", {}).get("result"):
                        logger.warning(f"Yahoo Finance v8 API 返回結果為空 for ticker {ticker}. "
                                       f"Params: {params}")
                        return None # 可能表示 ticker 無效或該時段無數據

                    if self.cache_dir and cache_filepath:
                        self._write_to_cache(cache_filepath, json_data_str)
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"解析 Yahoo Finance v8 API 的 JSON 響應失敗 for ticker {ticker}: {e}. "
                                 f"響應 (前500字符): {json_data_str[:500]}")
                    # 這種情況下，不應快取損壞的 JSON
                    return None
            else:
                logger.warning(f"未能從 Yahoo Finance 獲取 ticker '{ticker}' 的 v8 chart JSON 數據。")
                return None
        except Exception as e:
            # logger.error(f"獲取 ticker '{ticker}' 的 v8 chart JSON 數據時發生頂層錯誤: {e}", exc_info=True)
            return None

if __name__ == "__main__":
    import logging
    import sys # 需要 sys 來修改 path
    # --- 為了直接運行此文件進行測試 ---
    current_dir_yf = os.path.dirname(os.path.abspath(__file__)) # .../connectors
    project_src_dir_yf = os.path.abspath(os.path.join(current_dir_yf, '..')) # .../src
    project_root_yf = os.path.abspath(os.path.join(project_src_dir_yf, '..')) # AI_Assisted_Historical_Backtesting

    # 將項目根目錄添加到 sys.path，以便 from AI_Assisted_Historical_Backtesting.src... 能夠工作
    if project_root_yf not in sys.path:
        sys.path.insert(0, project_root_yf)
        # print(f"Temporarily added to sys.path for __main__: {project_root_yf}")

    from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME
    setup_logger(logger_name=PROJECT_LOGGER_NAME, level=logging.DEBUG)

    logger.info("--- YFinanceConnector (__main__) 測試開始 ---")

    # 創建一個臨時的快取目錄用於測試
    test_cache_base = os.path.join(project_root_yf, "data", "test_yf_cache_main")
    if not os.path.exists(test_cache_base):
        os.makedirs(test_cache_base)

    connector = YFinanceConnector(cache_dir_base=test_cache_base, cache_expiry_seconds=60) # 60秒過期用於測試

    # 測試1: 獲取 AAPL 的 CSV 歷史數據
    logger.info("\n測試1: 獲取 AAPL 的 CSV 歷史數據 (2023-01-01 到 2023-01-10)")
    aapl_csv = connector.get_historical_data_csv("AAPL", "2023-01-01", "2023-01-10")
    if aapl_csv:
        logger.info(f"成功獲取 AAPL CSV 數據。行數: {len(aapl_csv.splitlines())}")
        logger.info(f"數據 (前3行):\n{os.linesep.join(aapl_csv.splitlines()[:3])}")

        # 再次獲取，應從快取讀取
        logger.info("再次獲取 AAPL CSV (應從快取)")
        aapl_csv_cached = connector.get_historical_data_csv("AAPL", "2023-01-01", "2023-01-10")
        if aapl_csv_cached == aapl_csv:
            logger.info("從快取獲取的數據與首次獲取一致。")
        else:
            logger.error("快取測試失敗，數據不一致或未從快取獲取。")
    else:
        logger.error("獲取 AAPL CSV 數據失敗。")

    # 測試2: 獲取 ^GSPC (S&P 500) 的 JSON 圖表數據 (最近1個月)
    logger.info("\n測試2: 獲取 ^GSPC 的 JSON 圖表數據 (最近1個月, 間隔1d)")
    sp500_json = connector.get_chart_data_json("^GSPC", interval="1d", range_str="1mo")
    if sp500_json and sp500_json.get("chart", {}).get("result"):
        result = sp500_json["chart"]["result"][0]
        logger.info(f"成功獲取 ^GSPC JSON 數據。Meta: {result.get('meta', {}).get('symbol')}, "
                    f"時間戳數量: {len(result.get('timestamp', []))}")
        if result.get('timestamp'):
             logger.info(f"第一個時間戳: {datetime.fromtimestamp(result['timestamp'][0], tz=timezone.utc)}")
    else:
        logger.error("獲取 ^GSPC JSON 數據失敗或返回空。")

    # 測試3: 無效的 ticker
    logger.info("\n測試3: 獲取無效 ticker 'INVALIDTICKERXYZ123' 的 CSV 數據")
    invalid_csv = connector.get_historical_data_csv("INVALIDTICKERXYZ123", "2023-01-01", "2023-01-05")
    if invalid_csv is None:
        logger.info("成功處理無效 ticker (CSV)，返回 None。")
    else:
        logger.error(f"處理無效 ticker (CSV) 失敗，返回了數據: {invalid_csv[:100]}")

    logger.info("\n測試4: 獲取無效 ticker 'INVALIDTICKERXYZ123' 的 JSON 數據")
    invalid_json = connector.get_chart_data_json("INVALIDTICKERXYZ123", range_str="5d")
    if invalid_json is None or invalid_json.get("chart", {}).get("error") is not None:
        logger.info(f"成功處理無效 ticker (JSON)。API 錯誤: {invalid_json.get('chart', {}).get('error') if invalid_json else 'None returned'}")
    else:
        logger.error(f"處理無效 ticker (JSON) 失敗，返回了數據: {str(invalid_json)[:200]}")

    # 測試5: 快取過期
    logger.info("\n測試5: 快取過期 (等待超過60秒)")
    # 假設 AAPL CSV 仍在快取中，我們修改過期時間為0，然後再次獲取
    connector.cache_expiry_seconds = 0
    logger.info("再次獲取 AAPL CSV (快取應已過期)")
    aapl_csv_expired = connector.get_historical_data_csv("AAPL", "2023-01-01", "2023-01-10")
    if aapl_csv_expired and aapl_csv_expired == aapl_csv: # 內容應該還是一樣的
        logger.info("快取過期後重新獲取數據成功。")
        # 這裡無法簡單驗證它是否真的重新下載了，除非 mock network call
        # 但至少能驗證過期邏輯不會導致錯誤
    else:
        logger.error("快取過期測試失敗。")

    # 清理測試快取目錄
    try:
        import shutil
        if os.path.exists(test_cache_base):
            shutil.rmtree(test_cache_base)
            logger.info(f"已清理測試快取目錄: {test_cache_base}")
    except Exception as e:
        logger.warning(f"清理測試快取目錄 {test_cache_base} 失敗: {e}")

    logger.info("--- YFinanceConnector (__main__) 測試結束 ---")
