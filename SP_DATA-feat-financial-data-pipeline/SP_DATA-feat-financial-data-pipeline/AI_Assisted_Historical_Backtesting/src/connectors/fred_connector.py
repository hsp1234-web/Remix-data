import urllib.request
import json
import os
from datetime import datetime
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger
from AI_Assisted_Historical_Backtesting.src.utils.error_handler import retry_with_exponential_backoff

logger = get_logger(__name__)

# 預設的 FRED API 金鑰環境變量名稱
DEFAULT_FRED_API_KEY_ENV_VAR = "FRED_API_KEY"
# FRED API 端點
FRED_API_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"

class FredConnector:
    """
    用於從 FRED (Federal Reserve Economic Data) API 獲取經濟數據的連接器。
    遵循零依賴原則 (除了 Python 標準庫)。
    """
    def __init__(self, api_key=None, api_key_env_var=DEFAULT_FRED_API_KEY_ENV_VAR):
        """
        初始化 FredConnector。

        Args:
            api_key (str, optional): FRED API 金鑰。如果未提供，將嘗試從環境變量讀取。
            api_key_env_var (str, optional): 包含 FRED API 金鑰的環境變量名稱。
                                            預設為 "FRED_API_KEY"。

        Raises:
            ValueError: 如果 API 金鑰既未直接提供，也未在環境變量中找到。
        """
        if api_key:
            self.api_key = api_key
        else:
            self.api_key = os.getenv(api_key_env_var)

        if not self.api_key:
            error_msg = (f"FRED API 金鑰未找到。請直接提供 api_key 參數，"
                         f"或設置環境變量 '{api_key_env_var}'。")
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info("FredConnector 初始化成功。")

    @retry_with_exponential_backoff(max_retries=3, initial_delay=2, backoff_factor=2,
                                    allowed_exceptions=(urllib.error.URLError, TimeoutError, ConnectionResetError))
    def get_series_observations(
        self,
        series_id,
        start_date_str=None,
        end_date_str=None,
        file_type="json",
        limit=100000, # FRED API 的預設限制，也是最大限制
        sort_order="asc", # asc (升序), desc (降序)
        observation_start_str=None, # YYYY-MM-DD
        observation_end_str=None # YYYY-MM-DD
    ):
        """
        獲取指定經濟序列的觀測數據。

        Args:
            series_id (str): FRED 序列 ID (例如 "CPIAUCSL", "GDP", "UNRATE")。
            start_date_str (str, optional): 請求數據的開始日期，格式 "YYYY-MM-DD"。
                                            注意：FRED API 使用 observation_start 和 observation_end。
                                            此參數會被映射到 observation_start。
            end_date_str (str, optional): 請求數據的結束日期，格式 "YYYY-MM-DD"。
                                          此參數會被映射到 observation_end。
            file_type (str, optional): 返回的數據格式，預設為 "json"。可以是 "xml", "txt", "xls" 等。
                                       此連接器主要設計用於處理 "json"。
            limit (int, optional): 返回的最大觀測點數量。預設 100000 (FRED API 最大值)。
            sort_order (str, optional): 觀測數據的排序方式 ("asc" 或 "desc")。預設 "asc"。
            observation_start_str (str, optional): API 參數 observation_start。如果提供，將覆蓋 start_date_str。
            observation_end_str (str, optional): API 參數 observation_end。如果提供，將覆蓋 end_date_str。

        Returns:
            dict or None: 包含觀測數據的字典 (如果 file_type="json")，或者在發生錯誤時返回 None。
                          數據字典結構示例:
                          {
                              "realtime_start": "2024-06-26",
                              "realtime_end": "2024-06-26",
                              "observation_start": "1776-07-04",
                              "observation_end": "9999-12-31",
                              "units": "lin",
                              "output_type": 1,
                              "file_type": "json",
                              "order_by": "observation_date",
                              "sort_order": "asc",
                              "count": 1,
                              "offset": 0,
                              "limit": 100000,
                              "observations": [
                                  {"realtime_start": "2024-06-26", "realtime_end": "2024-06-26",
                                   "date": "2023-01-01", "value": "123.456"},
                                  ...
                              ]
                          }

        Raises:
            urllib.error.HTTPError: 如果 API 返回 HTTP 錯誤狀態碼 (例如 400, 401, 500)。
            urllib.error.URLError: 如果發生網路相關錯誤 (例如無法連接到服務器)。
            json.JSONDecodeError: 如果 API 返回的不是有效的 JSON (當 file_type="json")。
            TimeoutError: 如果請求超時。
        """
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": file_type,
            "limit": str(limit),
            "sort_order": sort_order,
        }

        # 處理日期參數
        # API 使用 observation_start 和 observation_end
        obs_start = observation_start_str if observation_start_str else start_date_str
        obs_end = observation_end_str if observation_end_str else end_date_str

        if obs_start:
            try:
                datetime.strptime(obs_start, "%Y-%m-%d")
                params["observation_start"] = obs_start
            except ValueError:
                logger.warning(f"提供的開始日期 '{obs_start}' 格式無效，應為 YYYY-MM-DD。將忽略此參數。")

        if obs_end:
            try:
                datetime.strptime(obs_end, "%Y-%m-%d")
                params["observation_end"] = obs_end
            except ValueError:
                logger.warning(f"提供的結束日期 '{obs_end}' 格式無效，應為 YYYY-MM-DD。將忽略此參數。")

        query_string = urllib.parse.urlencode(params)
        request_url = f"{FRED_API_ENDPOINT}?{query_string}"

        logger.info(f"向 FRED API 發起請求: {series_id}, 開始: {obs_start}, 結束: {obs_end}")
        logger.debug(f"完整請求 URL (API 金鑰已隱藏): {FRED_API_ENDPOINT}?{urllib.parse.urlencode({k:v for k,v in params.items() if k != 'api_key'})}&api_key=REDACTED")

        try:
            with urllib.request.urlopen(request_url, timeout=20) as response: # 增加超時時間
                status_code = response.getcode()
                response_body = response.read()

                logger.debug(f"FRED API 響應狀態碼: {status_code}")
                if status_code != 200:
                    # HTTPError 通常在 urlopen 內部就會拋出，但這裡也做一層檢查
                    logger.error(f"FRED API 請求失敗，狀態碼: {status_code}, 響應: {response_body[:500].decode('utf-8', 'ignore')}")
                    # 根據 FRED API 文檔，錯誤信息通常在響應體中
                    # 嘗試解析錯誤信息
                    try:
                        error_data = json.loads(response_body)
                        error_message = error_data.get("error_message", "未知API錯誤")
                        logger.error(f"FRED API 錯誤詳情: {error_message}")
                        # 重新拋出一個更具體的異常，或者讓 retry 機制處理
                        # 為了讓 retry 機制能正確捕獲 HTTPError，我們需要確保它被拋出
                        # 如果 urlopen 沒有拋出，我們可以自己構造一個
                        raise urllib.error.HTTPError(request_url, status_code, error_message, response.headers, response.fp) # type: ignore
                    except json.JSONDecodeError:
                        logger.error("FRED API 錯誤響應不是有效的 JSON。")
                        raise urllib.error.HTTPError(request_url, status_code, "非JSON錯誤響應", response.headers, response.fp) # type: ignore

                if file_type == "json":
                    try:
                        data = json.loads(response_body)
                        logger.info(f"成功從 FRED API 獲取並解析了序列 '{series_id}' 的數據。共 {data.get('count')} 條觀測。")
                        return data
                    except json.JSONDecodeError as e:
                        logger.error(f"無法解析來自 FRED API 的 JSON 響應: {e}. 響應體 (前500字符): {response_body[:500].decode('utf-8', 'ignore')}")
                        raise # 重新拋出異常，讓 retry 機制或調用者處理
                else:
                    # 對於非 JSON 格式，直接返回原始響應體 (bytes)
                    # 調用者需要自行處理
                    logger.info(f"成功從 FRED API 獲取了序列 '{series_id}' 的 {file_type} 格式數據。")
                    return response_body

        except urllib.error.HTTPError as e:
            # HTTPError 通常包含 code 和 reason
            logger.error(f"FRED API HTTP 錯誤: {e.code} {e.reason}. URL: {request_url[:request_url.find('api_key=')]}api_key=REDACTED")
            # 嘗試讀取錯誤響應體
            try:
                error_content = e.read().decode('utf-8', 'ignore')
                logger.error(f"FRED API 錯誤響應體: {error_content[:500]}")
                # 嘗試解析 FRED API 的標準錯誤格式
                try:
                    error_json = json.loads(error_content)
                    if "error_message" in error_json:
                         logger.error(f"FRED API 錯誤信息: {error_json['error_message']}")
                except json.JSONDecodeError:
                    pass # 如果錯誤響應不是 JSON，則忽略
            except Exception as read_err:
                logger.error(f"讀取 HTTPError 響應體失敗: {read_err}")
            raise # 重新拋出，以便 retry 機制可以捕獲
        except urllib.error.URLError as e:
            logger.error(f"FRED API URL 錯誤 (網路問題): {e.reason}. URL: {request_url[:request_url.find('api_key=')]}api_key=REDACTED")
            raise
        except TimeoutError:
            logger.error(f"FRED API 請求超時. URL: {request_url[:request_url.find('api_key=')]}api_key=REDACTED")
            raise
        except Exception as e:
            logger.error(f"獲取 FRED 數據時發生未知錯誤: {e}", exc_info=True)
            raise
        return None # 理論上不應到達此處，除非所有 retry 都失敗且未拋出異常

if __name__ == "__main__":
    # --- 為了直接運行此文件進行測試 ---
    import sys
    # 假設 logger.py 和 error_handler.py 在上一級目錄的 utils 中
    # 這段代碼是為了讓 `from AI_Assisted_Historical_Backtesting.src.utils...` 能工作
    # 如果直接執行此文件，需要確保項目根目錄在 PYTHONPATH 中
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_src_dir = os.path.abspath(os.path.join(current_dir, '..')) # 指向 src
    # project_root_parent = os.path.abspath(os.path.join(project_src_dir, '..', '..')) # 指向 AI_Backtester 的父目錄

    # 將 src 目錄加入 sys.path，這樣 from utils.logger 就能工作
    if project_src_dir not in sys.path:
        sys.path.insert(0, project_src_dir)

    from utils.logger import setup_logger, PROJECT_LOGGER_NAME

    # 設置日誌
    setup_logger(logger_name=PROJECT_LOGGER_NAME, level=logging.DEBUG)
    logger.info("--- FredConnector (__main__) 測試開始 ---")

    # 測試需要有效的 FRED API 金鑰，從環境變量 FRED_API_KEY_TEST 中讀取
    # 請確保在測試前設置此環境變量
    # 例如: export FRED_API_KEY_TEST="your_actual_fred_api_key"
    api_key_for_test = os.getenv("FRED_API_KEY_TEST")

    if not api_key_for_test:
        logger.warning("環境變量 'FRED_API_KEY_TEST' 未設置。將跳過 FredConnector 的 __main__ 測試。")
        logger.warning("請設置有效的 FRED API 金鑰以進行測試: export FRED_API_KEY_TEST=\"your_key\"")
    else:
        try:
            connector = FredConnector(api_key=api_key_for_test)

            # 測試1：獲取 CPIAUCSL (Consumer Price Index for All Urban Consumers: All Items in U.S. City Average)
            logger.info("\n測試1: 獲取 CPIAUCSL 數據 (最近5條)")
            cpi_data = connector.get_series_observations(
                series_id="CPIAUCSL",
                limit=5,
                sort_order="desc", # 獲取最新的5條
                observation_start_str="2020-01-01" # 確保數據不會太多
            )
            if cpi_data and "observations" in cpi_data:
                logger.info(f"成功獲取 CPIAUCSL 數據。觀測點數量: {len(cpi_data['observations'])}")
                for obs in cpi_data["observations"]:
                    logger.info(f"  日期: {obs['date']}, 值: {obs['value']}")
            else:
                logger.error("獲取 CPIAUCSL 數據失敗或返回空。")

            # 測試2：獲取 UNRATE (Unemployment Rate) 在特定時間範圍的數據
            logger.info("\n測試2: 獲取 UNRATE 數據 (2022-01-01 到 2022-12-31)")
            unrate_data = connector.get_series_observations(
                series_id="UNRATE",
                start_date_str="2022-01-01",
                end_date_str="2022-12-31"
            )
            if unrate_data and "observations" in unrate_data:
                logger.info(f"成功獲取 UNRATE 數據。觀測點數量: {len(unrate_data['observations'])}")
                if unrate_data["observations"]:
                    logger.info(f"  第一個觀測點: {unrate_data['observations'][0]}")
                    logger.info(f"  最後一個觀測點: {unrate_data['observations'][-1]}")
            else:
                logger.error("獲取 UNRATE 數據失敗或返回空。")

            # 測試3：無效的 series_id
            logger.info("\n測試3: 嘗試獲取無效的 series_id 'INVALIDSERIESID'")
            try:
                invalid_data = connector.get_series_observations(series_id="INVALIDSERIESID")
                if invalid_data: # 即使API返回錯誤，如果解析為json也可能不是None
                    logger.info(f"無效 series_id 請求返回: {invalid_data.get('error_message', '無錯誤信息但非預期結果')}")
            except urllib.error.HTTPError as e:
                logger.info(f"成功捕獲到無效 series_id 的 HTTPError: {e.code} - {e.reason}")
                try:
                    error_body = e.read().decode()
                    error_json = json.loads(error_body)
                    logger.info(f"API 錯誤信息: {error_json.get('error_message')}")
                except:
                    logger.warning("無法解析錯誤響應體為JSON。")
            except Exception as e:
                logger.error(f"測試無效 series_id 時發生非預期錯誤: {e}")

            # 測試4：日期格式錯誤 (由 get_series_observations 內部處理並警告)
            logger.info("\n測試4: 使用無效日期格式")
            connector.get_series_observations(series_id="GDP", start_date_str="2023/01/01")

            # 測試5: 請求 XML 格式 (注意: __main__ 測試主要關注 JSON)
            logger.info("\n測試5: 請求 GDP 數據為 XML 格式 (只檢查是否返回 bytes)")
            gdp_xml = connector.get_series_observations(series_id="GDP", file_type="xml", limit=1)
            if isinstance(gdp_xml, bytes):
                logger.info(f"成功獲取 GDP 的 XML 數據 (長度: {len(gdp_xml)} bytes)。內容 (前100字符): {gdp_xml[:100].decode('utf-8', 'ignore')}")
            else:
                logger.error("請求 XML 格式數據失敗。")

        except ValueError as ve:
            logger.error(f"FredConnector 初始化失敗: {ve}")
        except Exception as e:
            logger.error(f"FredConnector __main__ 測試期間發生意外錯誤: {e}", exc_info=True)

    logger.info("--- FredConnector (__main__) 測試結束 ---")
