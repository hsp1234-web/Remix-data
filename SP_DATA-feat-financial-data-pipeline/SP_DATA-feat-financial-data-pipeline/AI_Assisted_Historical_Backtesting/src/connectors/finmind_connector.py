import urllib.request
import urllib.parse
import json
import os
from datetime import datetime
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger
from AI_Assisted_Historical_Backtesting.src.utils.error_handler import retry_with_exponential_backoff

logger = get_logger(__name__)

# FinMind API 基礎 URL (假設，需要根據實際 API 文檔確認)
# 通常 FinMind 的 Python SDK 會封裝這些細節。
# 如果我們直接調用 API，需要知道這些端點。
# 示例: https://api.finmindtrade.com/api/v4/data
FINMIND_API_BASE_URL = os.getenv("FINMIND_API_BASE_URL", "https://api.finmindtrade.com/api/v4/data")

# 預設的 FinMind API Token 環境變量名稱
DEFAULT_FINMIND_TOKEN_ENV_VAR = "FINMIND_API_TOKEN"

# 模擬瀏覽器的 User-Agent
DEFAULT_USER_AGENT_FINMIND = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


class FinMindConnectorError(Exception):
    """FinMindConnector特定的異常基類。"""
    pass

class FinMindAPIError(FinMindConnectorError):
    """當 FinMind API 返回錯誤時拋出。"""
    def __init__(self, message, status_code=None, response_data=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data

class FinMindConnector:
    """
    用於從 FinMind API 獲取台灣市場數據（如財報、籌碼）的連接器。
    目標是遵循零依賴原則，直接與 API 交互。
    """
    def __init__(self, api_token=None, api_token_env_var=DEFAULT_FINMIND_TOKEN_ENV_VAR, user_agent=DEFAULT_USER_AGENT_FINMIND):
        """
        初始化 FinMindConnector。

        Args:
            api_token (str, optional): FinMind API Token。如果未提供，將嘗試從環境變量讀取。
            api_token_env_var (str, optional): 包含 FinMind API Token 的環境變量名稱。
                                             預設為 "FINMIND_API_TOKEN"。
            user_agent (str, optional): 請求時使用的 User-Agent。

        Raises:
            ValueError: 如果 API Token 既未直接提供，也未在環境變量中找到 (如果 API 需要 Token)。
                        目前假設公開數據可能不需要 Token，但敏感或高頻數據通常需要。
        """
        if api_token:
            self.api_token = api_token
        else:
            self.api_token = os.getenv(api_token_env_var)

        # FinMind 的某些公開數據可能不需要 token，但大部分進階數據會需要。
        # 這裡暫時不強制要求 token，但在需要 token 的方法中會檢查。
        if not self.api_token:
            logger.warning(f"FinMind API Token 未在環境變量 '{api_token_env_var}' 中找到或直接提供。"
                           "某些 API 端點可能無法訪問。")

        self.user_agent = user_agent
        logger.info(f"FinMindConnector 初始化。API Token {'已提供' if self.api_token else '未提供'}。")

    def _make_request_headers(self):
        """創建請求頭。"""
        headers = {
            "User-Agent": self.user_agent
            # "Authorization": f"Bearer {self.api_token}" # 如果 API 使用 Bearer Token
        }
        # FinMind API 的 token 通常是作為 URL 參數 `token` 傳遞，而不是在 header 中。
        return headers

    @retry_with_exponential_backoff(max_retries=3, initial_delay=2, backoff_factor=2,
                                    allowed_exceptions=(urllib.error.URLError, TimeoutError, ConnectionResetError, FinMindAPIError))
    def _fetch_finmind_data(self, dataset: str, data_id: str, start_date: str, end_date: str = None, params: dict = None):
        """
        內部輔助函數，用於從 FinMind API 獲取數據。

        Args:
            dataset (str): FinMind API 的資料集名稱 (例如 "TaiwanStockPrice", "FinancialStatements").
            data_id (str): 股票代號或特定數據 ID。
            start_date (str): 開始日期 "YYYY-MM-DD"。
            end_date (str, optional): 結束日期 "YYYY-MM-DD"。如果 None，通常 API 會返回到最新。
            params (dict, optional): 額外的 API 參數。

        Returns:
            dict: API 返回的解析後的 JSON 數據。

        Raises:
            FinMindAPIError: 如果 API 請求失敗或返回錯誤信息。
            urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError
        """
        if not self.api_token:
            # 對於某些需要 token 的數據，這裡應該報錯或返回
            # 但由於不確定哪些 dataset 需要，暫時先允許請求，讓 API 返回錯誤
            logger.warning(f"正在嘗試請求 FinMind 數據集 '{dataset}' 但未提供 API token。")


        query_params = {
            "dataset": dataset,
            "data_id": data_id,
            "start_date": start_date,
        }
        if end_date:
            query_params["end_date"] = end_date
        if self.api_token: # 僅在 token 存在時添加到參數中
            query_params["token"] = self.api_token

        if params:
            query_params.update(params)

        query_string = urllib.parse.urlencode(query_params)
        request_url = f"{FINMIND_API_BASE_URL}?{query_string}"

        log_url_no_token = request_url.replace(f"&token={self.api_token}", "&token=REDACTED") if self.api_token else request_url
        logger.info(f"向 FinMind API 發起請求: {log_url_no_token}")

        headers = self._make_request_headers()
        request = urllib.request.Request(request_url, headers=headers)

        try:
            with urllib.request.urlopen(request, timeout=30) as response: # 增加超時
                status_code = response.getcode()
                response_body_bytes = response.read()
                response_body_str = response_body_bytes.decode('utf-8', 'ignore')

                logger.debug(f"FinMind API 響應狀態碼: {status_code} for URL: {log_url_no_token}")

                try:
                    data = json.loads(response_body_str)
                except json.JSONDecodeError as e:
                    logger.error(f"無法解析來自 FinMind API 的 JSON 響應: {e}. URL: {log_url_no_token}. "
                                 f"響應體 (前500字符): {response_body_str[:500]}")
                    raise FinMindAPIError(f"JSON 解析錯誤: {e}", response_data=response_body_str) from e

                # FinMind API 通常在 JSON 響應中包含 "msg" 和 "status"
                # 或者直接在 data 字段中返回數據列表
                # {"msg": "success", "status": 200, "data": [...]}
                # {"msg": "Forbidden, token is wrong or miss", "status": 403}
                # {"message":"token error","status_code":401} (另一種可能的錯誤格式)

                api_status = data.get("status") # FinMind v4
                api_status_code = data.get("status_code") # FinMind v3?
                api_msg = data.get("msg", data.get("message", ""))

                if api_status == 200 or (status_code == 200 and "data" in data and not api_status and not api_status_code): # 成功
                    logger.info(f"成功從 FinMind API 獲取數據集 '{dataset}' for '{data_id}'.")
                    return data # 返回整個 JSON 響應，調用者可以取 data['data']

                # 處理 API 返回的業務邏輯錯誤
                # 使用 status_code (HTTP狀態碼) 和 api_status/api_status_code (業務狀態碼) 共同判斷
                effective_api_status = api_status if api_status is not None else api_status_code

                if effective_api_status is not None and effective_api_status != 200:
                    logger.error(f"FinMind API 業務錯誤: Status={effective_api_status}, Msg='{api_msg}'. URL: {log_url_no_token}")
                    raise FinMindAPIError(f"API Error: {api_msg}", status_code=effective_api_status, response_data=data)
                elif status_code != 200: # HTTP 層面錯誤，但業務層面未明確指示
                     logger.error(f"FinMind API HTTP 請求失敗，狀態碼: {status_code}, Msg='{api_msg}'. 響應: {response_body_str[:500]}")
                     raise FinMindAPIError(f"HTTP Error {status_code}: {api_msg}", status_code=status_code, response_data=data)

                # 如果 HTTP 200 但沒有 data 字段，且沒有明確的業務錯誤狀態，也視為問題
                if "data" not in data:
                    logger.warning(f"FinMind API 響應成功 (HTTP 200) 但缺少 'data' 字段 for {dataset} / {data_id}. Msg='{api_msg}'. Resp: {str(data)[:200]}")
                    # 根據 API 設計，這也可能是一種錯誤或空結果
                    # 暫時也拋出異常，讓 retry 或調用者決定
                    raise FinMindAPIError(f"響應成功但缺少 'data' 字段. Msg='{api_msg}'", status_code=status_code, response_data=data)

                return data # 理論上應該在前面返回了

        except urllib.error.HTTPError as e: # 通常是 4xx, 5xx 錯誤，urlopen 會直接拋出
            error_body = ""
            # HTTPError 對象的 read() 方法只能調用一次。
            # 我們先嘗試讀取，如果成功，再嘗試解析。
            error_body = ""
            processed_api_error = False
            try:
                error_body_bytes = e.read()
                error_body = error_body_bytes.decode('utf-8', 'ignore')
                logger.debug(f"HTTPError body read successfully for {e.code}. Length: {len(error_body)}")

                try:
                    error_json = json.loads(error_body)
                    logger.debug(f"HTTPError body parsed as JSON: {error_json}")
                    msg = error_json.get("msg", error_json.get("message"))
                    if not msg:
                        msg = str(e.reason) if e.reason else "Unknown API Error from JSON"
                    status = error_json.get("status", error_json.get("status_code", e.code))

                    logger.error(f"FinMind API HTTP 錯誤 (已解析JSON): Code={e.code}, ParsedStatus={status}, Msg='{msg}'. URL: {log_url_no_token}.")
                    # 直接從這裡拋出解析後的 FinMindAPIError
                    new_exception = FinMindAPIError(f"HTTP Error from API: {msg}", status_code=status, response_data=error_json)
                    processed_api_error = True
                    raise new_exception from e # 拋出由 JSON 解析出的錯誤

                except json.JSONDecodeError:
                    logger.error(f"FinMind API HTTP 錯誤 (非JSON響應): Code={e.code}, Reason='{e.reason}'. Body: {error_body[:200]}. URL: {log_url_no_token}.")
                    new_exception = FinMindAPIError(f"HTTP Error {e.code}: {e.reason}. Non-JSON response.", status_code=e.code, response_data=error_body)
                    processed_api_error = True
                    raise new_exception from e # 拋出非 JSON 響應的錯誤

            except Exception as inner_err:
                # 這個 inner_err 捕獲的是 e.read(), decode(), 或者上面 raise 之前的其他錯誤
                # 如果 processed_api_error 為 True，說明上面的 raise 已經發生，不應該再到這裡，除非 raise 本身有問題
                if not processed_api_error:
                    logger.error(f"處理 HTTPError (Code={e.code}, Reason='{e.reason}') 的響應體時發生內部錯誤: {inner_err}. URL: {log_url_no_token}", exc_info=True)
                    raise FinMindAPIError(f"HTTP Error {e.code}: {e.reason}. Failed to process error body.", status_code=e.code, response_data=error_body if error_body else str(e)) from e
                else:
                    # 如果 processed_api_error 為 True，意味著上面的 raise FinMindAPIError 已經被這個 except Exception 捕獲了
                    # 這種情況下，我們應該重新拋出 inner_err (它就是我們剛 raise 的 FinMindAPIError)
                    raise inner_err

        except urllib.error.URLError as e:
            logger.error(f"FinMind API URL 錯誤 (網路問題): {e.reason}. URL: {log_url_no_token}")
            raise # 由 retry 機制處理
        except TimeoutError:
            logger.error(f"FinMind API 請求超時. URL: {log_url_no_token}")
            raise # 由 retry 機制處理
        # FinMindAPIError 應該由 retry 機制處理，或者如果它是從內部邏輯拋出的 (如JSON解析失敗後包裝的)
        # 這裡的 except FinMindAPIError 是為了捕獲那些不由 retry 直接處理的、但在 try 塊中新拋出的 FinMindAPIError
        except FinMindAPIError:
            raise # 如果已經是 FinMindAPIError，直接重新拋出，讓 retry 或上層處理
        except Exception as e: # 捕獲其他Python級別的意外錯誤
            logger.error(f"獲取 FinMind 數據時發生未知底層錯誤: {e}. URL: {log_url_no_token}", exc_info=True)
            # 將非預期的 Python 級別錯誤包裝成 FinMindAPIError
            raise FinMindAPIError(f"未知底層錯誤: {e}", response_data=str(e)) from e


    def get_financial_statements(self, stock_id: str, start_date: str, end_date: str = None):
        """
        獲取指定股票的綜合損益表、資產負債表等財報數據。
        FinMind API dataset: "FinancialStatements" (基於推測，需確認)
        或者可能是 "TaiwanStockFinancialStatements"

        Args:
            stock_id (str): 股票代號 (例如 "2330")。
            start_date (str): 開始日期 "YYYY-MM-DD"。財報通常按季度或年度，API 可能會調整日期。
            end_date (str, optional): 結束日期 "YYYY-MM-DD"。

        Returns:
            list or None: 包含財報數據的字典列表 (通常是 data['data'])，或在錯誤時返回 None。
                          每條數據的具體字段取決於 FinMind API 的定義。
        """
        # 實際的 dataset 名稱和參數需要查閱 FinMind API 文檔
        # 假設 dataset 名稱為 "FinancialStatements"
        # 假設 API 會自動處理季度/年度，我們傳遞日期範圍
        # 假設財報數據需要 token (通常是這樣)
        if not self.api_token:
            logger.error("獲取財報數據需要 FinMind API Token，但未提供。")
            # raise ValueError("API Token is required for financial statements.")
            # 或者返回 None，讓調用者處理
            return None

        logger.info(f"請求財報數據 for stock_id={stock_id}, start={start_date}, end={end_date}")
        try:
            # FinMind 的 `FinancialStatements` dataset 可能需要 type 參數, e.g., 'BalanceSheet', 'IncomeStatement'
            # 如果 API 設計是返回所有類型的報表，則不需要 type。
            # 假設我們獲取的是一個通用的財報集合，或者 API 默認返回綜合損益表。
            # 這裡的 'dataset' 和 'params' 需要根據 FinMind 的實際 API 調整。
            # 例如，如果一個端點獲取所有報表：
            response_data = self._fetch_finmind_data(
                dataset="FinancialStatements", # 或者 "TaiwanStockFinancialStatements"
                data_id=stock_id,
                start_date=start_date,
                end_date=end_date
                # params={"type": "IncomeStatement"} # 如果需要指定報表類型
            )
            # 確保 response_data 是字典類型再用 .get()
            return response_data.get("data") if isinstance(response_data, dict) else None
        except FinMindAPIError as e: # 捕獲 _fetch_finmind_data 可能最終拋出的 API 錯誤
            logger.error(f"獲取股票 {stock_id} 的財報數據失敗 (API Error): {e}")
            return None
        except Exception as e: # 捕獲其他如網絡錯誤 (如果 retry 後仍失敗) 或意外Python錯誤
            logger.error(f"獲取股票 {stock_id} 的財報數據時發生意外的頂層錯誤: {e}", exc_info=True)
            return None


    def get_chip_data(self, stock_id: str, start_date: str, end_date: str = None, chip_type: str = "InstitutionalInvestorsBuySell"):
        """
        獲取指定股票的籌碼數據，例如法人買賣超、融資融券等。
        FinMind API dataset 示例: "InstitutionalInvestorsBuySell", "MarginPurchaseShortSale" (需確認)

        Args:
            stock_id (str): 股票代號。
            start_date (str): 開始日期 "YYYY-MM-DD"。
            end_date (str, optional): 結束日期 "YYYY-MM-DD"。
            chip_type (str, optional): 籌碼數據的類型/資料集名稱。
                                       例如 "InstitutionalInvestorsBuySell" (三大法人買賣超),
                                       "MarginPurchaseShortSale" (融資融券餘額),
                                       "Shareholding" (股權分散表) 等。
                                       默認為 "InstitutionalInvestorsBuySell"。

        Returns:
            list or None: 包含籌碼數據的字典列表 (data['data'])，或在錯誤時返回 None。
        """
        if not self.api_token: # 籌碼數據通常也需要 token
            logger.error(f"獲取籌碼數據 ({chip_type}) 需要 FinMind API Token，但未提供。")
            return None

        logger.info(f"請求籌碼數據 ({chip_type}) for stock_id={stock_id}, start={start_date}, end={end_date}")
        try:
            response_data = self._fetch_finmind_data(
                dataset=chip_type, # 使用傳入的 chip_type 作為 dataset
                data_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )
            return response_data.get("data") if isinstance(response_data, dict) else None
        except FinMindAPIError as e:
            logger.error(f"獲取股票 {stock_id} 的籌碼數據 ({chip_type}) 失敗 (API Error): {e}")
            return None
        except Exception as e:
            logger.error(f"獲取股票 {stock_id} 的籌碼數據 ({chip_type}) 時發生意外的頂層錯誤: {e}", exc_info=True)
            return None

    # 可以根據 FinMind API 文檔添加更多方法，例如：
    # get_stock_prices, get_index_prices, get_exchange_rates, etc.

if __name__ == "__main__":
    import logging
    import sys
    from datetime import datetime, timedelta # 確保 timedelta 被導入

    # --- 為了直接運行此文件進行測試 ---
    current_dir_fm = os.path.dirname(os.path.abspath(__file__)) # .../connectors
    project_src_dir_fm = os.path.abspath(os.path.join(current_dir_fm, '..')) # .../src
    project_root_fm = os.path.abspath(os.path.join(project_src_dir_fm, '..')) # AI_Assisted_Historical_Backtesting

    # 將項目根目錄添加到 sys.path，以便 from AI_Assisted_Historical_Backtesting.src... 能夠工作
    if project_root_fm not in sys.path:
        sys.path.insert(0, project_root_fm)
        # print(f"Temporarily added to sys.path for FinMind __main__: {project_root_fm}")

    from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME
    setup_logger(logger_name=PROJECT_LOGGER_NAME, level=logging.DEBUG)

    logger.info("--- FinMindConnector (__main__) 測試開始 ---")

    # 測試需要有效的 FinMind API Token，從環境變量 FINMIND_API_TOKEN_TEST 中讀取
    # 請確保在測試前設置此環境變量
    # 例如: export FINMIND_API_TOKEN_TEST="your_actual_finmind_api_token"
    api_token_for_test = os.getenv("FINMIND_API_TOKEN_TEST")

    if not api_token_for_test:
        logger.warning("環境變量 'FINMIND_API_TOKEN_TEST' 未設置。將跳過 FinMindConnector 的 __main__ API 調用測試。")
        logger.warning("請設置有效的 FinMind API Token 以進行測試: export FINMIND_API_TOKEN_TEST=\"your_token\"")
        # 即使沒有 token，也測試一下初始化
        try:
            connector_no_token = FinMindConnector()
            logger.info("FinMindConnector (無 token) 初始化成功。")
        except Exception as e:
            logger.error(f"FinMindConnector (無 token) 初始化失敗: {e}")
    else:
        try:
            connector = FinMindConnector(api_token=api_token_for_test)
            today_str = datetime.now().strftime("%Y-%m-%d")
            start_date_str = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d") # 大約三個月前
            start_date_financials = (datetime.now() - timedelta(days=2*365)).strftime("%Y-%m-%d") # 大約兩年前

            # 測試1：獲取三大法人買賣超 (InstitutionalInvestorsBuySell)
            logger.info(f"\n測試1: 獲取 2330 (台積電) 的三大法人買賣超數據 ({start_date_str} 到 {today_str})")
            chip_data_institutional = connector.get_chip_data(
                stock_id="2330",
                start_date=start_date_str,
                end_date=today_str,
                chip_type="InstitutionalInvestorsBuySell" # 這是 FinMind Python SDK 中的一個 table name
            )
            if chip_data_institutional:
                logger.info(f"成功獲取 2330 法人買賣超數據。條數: {len(chip_data_institutional)}")
                if chip_data_institutional:
                    logger.info(f"  部分數據示例 (最多3條): {chip_data_institutional[:3]}")
            else:
                logger.warning("獲取 2330 法人買賣超數據失敗或返回空。可能是 API 端點/參數不對或 Token 問題。")

            # 測試2：獲取融資融券餘額 (MarginPurchaseShortSale)
            logger.info(f"\n測試2: 獲取 2330 (台積電) 的融資融券餘額數據 ({start_date_str} 到 {today_str})")
            chip_data_margin = connector.get_chip_data(
                stock_id="2330",
                start_date=start_date_str,
                end_date=today_str,
                chip_type="MarginPurchaseShortSale" # 這是 FinMind Python SDK 中的一個 table name
            )
            if chip_data_margin:
                logger.info(f"成功獲取 2330 融資融券數據。條數: {len(chip_data_margin)}")
                if chip_data_margin:
                    logger.info(f"  部分數據示例 (最多3條): {chip_data_margin[:3]}")
            else:
                logger.warning("獲取 2330 融資融券數據失敗或返回空。")

            # 測試3：獲取財報數據 (FinancialStatements)
            logger.info(f"\n測試3: 獲取 2330 (台積電) 的財報數據 ({start_date_financials} 到 {today_str})")
            # 注意：FinancialStatements 可能是一個很大的包，或者需要指定報表類型
            # FinMind SDK 中 FinancialStatements 似乎是獲取綜合損益表、資產負債表等
            financial_data = connector.get_financial_statements(
                stock_id="2330",
                start_date=start_date_financials, # 財報通常不需要太近的日期
                end_date=today_str
            )
            if financial_data:
                logger.info(f"成功獲取 2330 財報數據。條數: {len(financial_data)}")
                if financial_data:
                    # 財報數據通常較複雜，只打印部分信息
                    logger.info(f"  一條財報數據示例 (部分字段): {{'date': {financial_data[0].get('date')}, 'type': {financial_data[0].get('type')}, 'value_count': len(financial_data[0].get('value', []))}}")
            else:
                logger.warning("獲取 2330 財報數據失敗或返回空。")

            # 測試4：無效的 stock_id 或 dataset
            logger.info("\n測試4: 嘗試獲取無效 stock_id '999999' 的 'InstitutionalInvestorsBuySell'")
            invalid_chip_data = connector.get_chip_data(stock_id="999999", start_date="2023-01-01", chip_type="InstitutionalInvestorsBuySell")
            if invalid_chip_data is None or not invalid_chip_data: # API 可能返回空列表 []
                logger.info("處理無效 stock_id 的籌碼數據請求成功 (返回 None 或空列表)。")
            else:
                logger.warning(f"處理無效 stock_id 的籌碼數據請求似乎返回了數據: {invalid_chip_data[:1]}")


        except ValueError as ve: # 例如 API Token 未找到的初始化錯誤
            logger.error(f"FinMindConnector 初始化或配置錯誤: {ve}")
        except Exception as e:
            logger.error(f"FinMindConnector __main__ 測試期間發生意外錯誤: {e}", exc_info=True)

    logger.info("--- FinMindConnector (__main__) 測試結束 ---")
