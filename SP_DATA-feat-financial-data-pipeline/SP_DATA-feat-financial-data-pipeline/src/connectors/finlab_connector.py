# import finlab
# import pandas as pd
# import time
# import logging
# from datetime import datetime, date # date is not used, but datetime is
# from typing import Dict, Any, Optional, List, Tuple

# # 預設日誌記錄器
# logger = logging.getLogger(__name__)

# class FinLabConnector:
#     """
#     用於從 FinLab API (透過其 SDK) 獲取台灣市場金融數據的連接器。
#     注意：此 Connector 的核心功能 (數據獲取) 依賴於 FinLab SDK 的 dataset 名稱是否正確。
#     在 dataset 名稱確認前，核心數據獲取方法可能無法正常工作或僅為框架。
#     """

#     def __init__(self, api_config: Dict[str, Any]):
#         """
#         初始化 FinLabConnector。

#         Args:
#             api_config (Dict[str, Any]): 包含此 API 設定的字典，
#                                          應包含 'api_key' (FinLab Token) 和 'requests_per_minute'。
#                                          例如:
#                                          {
#                                              "api_key": "YOUR_FINLAB_TOKEN",
#                                              "requests_per_minute": 100, # 假設值
#                                              # "finlab_datasets": { ... } // 可選，用於映射標準名稱到 FinLab dataset 名稱
#                                          }
#         """
#         self.api_token = api_config.get("api_key")
#         if not self.api_token:
#             logger.error("FinLab API Token (api_key) 未在設定中提供。")
#             raise ValueError("FinLab API Token 未設定。")

#         self.requests_per_minute = api_config.get("requests_per_minute", 100)
#         self._last_request_time = 0
#         self._min_request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0

#         # FinLab SDK 的 data client (finlab.data)
#         # SDK 通常在內部處理登入和請求
#         try:
#             finlab.login(self.api_token) # 使用 token 登入
#             self.data_client = finlab.data
#             logger.info(f"FinLabConnector 初始化並使用 Token 登入成功。RPM: {self.requests_per_minute}")
#         except Exception as e:
#             logger.error(f"FinLab SDK 初始化或登入失敗：{e}", exc_info=True)
#             # 如果登入失敗，後續操作會出錯，但 SDK 可能不會在 login() 時立即拋錯
#             # 而是等到實際調用 data_client 時。這裡先記錄。
#             self.data_client = None # 標記 client 不可用
#             # raise RuntimeError(f"FinLab SDK 初始化或登入失敗: {e}") # 或者直接拋出錯誤終止

#         # 用於映射標準數據類型到 FinLab dataset 名稱 (如果需要)
#         # self.dataset_map = api_config.get("finlab_datasets", {})


#     def _wait_for_rate_limit(self):
#         """
#         等待直到可以安全地發出下一個 API 請求。
#         注意：FinLab SDK 本身可能沒有精細的速率控制，這裡的控制是基於 Connector 層面。
#         """
#         if self._min_request_interval == 0 or not self.data_client: # 如果 client 未初始化則不等待
#             return
#         now = time.time()
#         elapsed_time = now - self._last_request_time
#         wait_time = self._min_request_interval - elapsed_time
#         if wait_time > 0:
#             logger.debug(f"FinLab 速率控制：等待 {wait_time:.2f} 秒。")
#             time.sleep(wait_time)
#         self._last_request_time = time.time()

#     def _fetch_finlab_data(self, dataset_name: str, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
#         """
#         通用的 FinLab 數據獲取方法。

#         Args:
#             dataset_name (str): FinLab SDK 中的 dataset 名稱 (例如 "price:收盤價", "monthly_revenue:當月營收")。
#             **kwargs: 傳遞給 FinLab SDK get() 方法的其他參數 (例如 table, start_date, end_date, stock_id)。

#         Returns:
#             Tuple[Optional[pd.DataFrame], Optional[str]]:
#                 成功時返回 (獲取的 DataFrame, None)，
#                 失敗時返回 (None, 錯誤訊息字串)。
#         """
#         if not self.data_client:
#             err_msg = "FinLab data client 未初始化 (可能登入失敗)。"
#             logger.error(err_msg)
#             return None, err_msg

#         self._wait_for_rate_limit()
#         logger.debug(f"FinLab API 請求：Dataset='{dataset_name}', Params='{kwargs}'")

#         try:
#             # FinLab SDK 的 get 方法通常是 finlab.data.get(dataset_name, table=..., etc.)
#             # 或者直接 finlab.data.talib(...) 等
#             # 這裡假設我們使用 get 方法，並傳入 table (如果有的話)
#             # FinLab 的 dataset name 通常格式是 "category:item"
#             # 例如 "price:收盤價", "financial_statement:ROE"
#             # SDK 的 get 方法可能直接接受這個字串

#             # 檢查 dataset_name 是否包含 ':', 如果是，則 table 可能是 item 部分
#             table_name = kwargs.pop('table', None) # 允許外部傳入 table
#             if not table_name and ':' in dataset_name:
#                 parts = dataset_name.split(':', 1)
#                 # dataset_name_for_sdk = parts[0] # 例如 'price'
#                 # table_name = parts[1] # 例如 '收盤價'
#                 # 然而，FinLab SDK 的 get() 通常直接接受 "price:收盤價"
#                 # 所以這裡不需要拆分，除非我們要用更底層的調用
#                 pass


#             # FinLab SDK 的 `finlab.data.get(dataset_name)` 返回 DataFrame
#             # 並且其內部會處理 API 調用
#             # 我們需要確認 `get` 是否接受 `start_date`, `end_date`, `stock_id` 等參數
#             # 根據 FinLab 文件，`get` 似乎不直接接受這些過濾參數，
#             # 而是返回整個 dataset，然後由用戶自行過濾。
#             # 這意味著速率控制和數據量可能需要謹慎處理。
#             # 如果是這樣，kwargs 裡面的 start_date, end_date, stock_id 需要在獲取後處理。

#             # 假設 finlab.data.get(dataset_name) 返回的是一個包含所有股票和日期的 DataFrame
#             # 並且我們需要自己過濾。這對於大型 dataset 可能效率不高。
#             # 另一種可能是某些 dataset (如 price) 可以接受 stock_id 等參數。
#             # 需要查閱最新的 FinLab SDK 文檔來確定 `get` 的確切行為。

#             # 模擬 SDK 調用，並假設它返回 DataFrame
#             # 實際調用: df = self.data_client.get(dataset_name)
#             # 由於我們不知道確切的 SDK 行為，這裡先留空並返回錯誤
#             # ---- START OF PLACEHOLDER ----
#             logger.warning(f"FinLabConnector._fetch_finlab_data: 實際的 FinLab SDK 調用邏輯待實現。Dataset: {dataset_name}")
#             # 假設 API 檢測報告中的 'finlab_api_test.py' 包含正確的調用方式，
#             # 我們需要參考該腳本。
#             # 如果 FinLab SDK 的 get 方法是 `finlab.data.get('price:收盤價')` 這種形式，
#             # 並且它返回一個包含所有股票的 DataFrame，那麼過濾必須在之後進行。
#             # 如果是 `finlab.data.get_stock_data(stock_id, start_date, end_date, '收盤價')` 這種形式，則更直接。

#             # 根據 "外部金融 API 檢測詳細報告" 的 FinLab 部分：
#             # 成功: price:收盤價 (日線價格), monthly_revenue:當月營收, company_main_business (公司主要業務)。
#             # 失敗 (API 錯誤：Dataset not exists)： financial_statement:ROE, chip:外資買賣超, benchmark:發行量加權股價指數。
#             # 這表明 dataset_name 可能是 "price:收盤價" 這樣的形式。
#             # 我們需要知道如何基於 stock_id, start_date, end_date 來過濾。

#             # 暫時返回錯誤，直到 SDK 調用方式明確
#             return None, f"FinLab SDK 調用方式待確認 for dataset '{dataset_name}'"
#             # ---- END OF PLACEHOLDER ----

#             # 以下是假設 SDK 調用成功並返回 df 後的處理
#             # if df is None: # SDK 可能在內部錯誤時返回 None
#             #     logger.warning(f"FinLab SDK for dataset '{dataset_name}' 返回 None。")
#             #     return pd.DataFrame(), f"SDK for {dataset_name} returned None" # 返回空 DF 和錯誤
#             # if df.empty:
#             #     logger.info(f"FinLab SDK for dataset '{dataset_name}' 返回空 DataFrame。")
#             #     return df, None # 返回空 DF，無錯誤

#             # logger.info(f"FinLab SDK 成功獲取 dataset '{dataset_name}', {len(df)} 筆記錄。")
#             # return df, None

#         except AttributeError as e: # 例如 self.data_client 為 None
#             logger.error(f"FinLab SDK 屬性錯誤 (可能 client 未初始化): {e}", exc_info=True)
#             return None, f"FinLab SDK 屬性錯誤: {e}"
#         except Exception as e: # 捕獲 FinLab SDK 可能拋出的其他例外
#             logger.error(f"FinLab SDK 獲取 dataset '{dataset_name}' 時發生錯誤：{e}", exc_info=True)
#             return None, f"FinLab SDK 錯誤 for {dataset_name}: {e}"


#     def get_stock_price(self, stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
#         """
#         獲取指定股票的歷史日線價格數據。
#         FinLab dataset name: "price:收盤價", "price:開盤價", etc.

#         Args:
#             stock_id (str): 股票代碼 (例如 "2330")。
#             start_date (str): 開始日期 (YYYY-MM-DD)。
#             end_date (str): 結束日期 (YYYY-MM-DD)。

#         Returns:
#             pd.DataFrame: 包含 OHLCV 數據的 DataFrame，若失敗則為空 DataFrame。
#                           欄位：'price_date', 'security_id', 'open_price', 'high_price',
#                                 'low_price', 'close_price', 'volume',
#                                 'source_api', 'last_updated_timestamp'
#         """
#         logger.info(f"FinLab: 請求股票 {stock_id} 從 {start_date} 到 {end_date} 的價格數據。")

#         # FinLab 的價格數據通常是寬表，以股票代碼為欄位名，日期為索引
#         # 我們需要獲取 開盤價, 最高價, 最低價, 收盤價, 成交量
#         price_datasets = {
#             'open_price': 'price:開盤價',
#             'high_price': 'price:最高價',
#             'low_price': 'price:最低價',
#             'close_price': 'price:收盤價',
#             'volume': 'price:成交股數' # 或 'price:成交金額' / 1000
#         }

#         all_series = {}
#         any_fetch_failed = False

#         for key, dataset_name in price_datasets.items():
#             # 假設 _fetch_finlab_data 返回的是一個 DataFrame，日期為索引，股票代碼為欄位
#             # 並且我們需要從中選取特定股票和日期範圍的數據
#             raw_df_full, error = self._fetch_finlab_data(dataset_name) # Kwargs for filtering not used here

#             if error or raw_df_full is None: # 如果 fetch 本身失敗
#                 logger.warning(f"FinLab: 無法獲取 {dataset_name} for {stock_id}。錯誤: {error}")
#                 any_fetch_failed = True
#                 all_series[key] = pd.Series(dtype='float64', name=key) # 空 Series
#                 continue

#             if raw_df_full.empty or stock_id not in raw_df_full.columns:
#                 logger.info(f"FinLab: Dataset '{dataset_name}' 中無股票 {stock_id} 的數據。")
#                 all_series[key] = pd.Series(dtype='float64', name=key)
#                 continue

#             # 選取特定股票的 Series
#             stock_series = raw_df_full[stock_id]

#             # 過濾日期 (假設索引是 DatetimeIndex)
#             try:
#                 stock_series.index = pd.to_datetime(stock_series.index)
#                 filtered_series = stock_series.loc[start_date:end_date]
#                 all_series[key] = filtered_series.rename(key)
#             except Exception as e:
#                 logger.error(f"FinLab: 過濾 {dataset_name} for {stock_id} ({start_date}-{end_date}) 時出錯: {e}", exc_info=True)
#                 all_series[key] = pd.Series(dtype='float64', name=key)
#                 any_fetch_failed = True

#         if any_fetch_failed and not any(not s.empty for s in all_series.values()):
#             logger.warning(f"FinLab: 未能成功獲取 {stock_id} 的任何價格組件。")
#             return pd.DataFrame()

#         # 合併所有 Series 成一個 DataFrame
#         # 使用 outer join 以保留所有日期，然後處理 NaN
#         final_df = pd.concat(all_series.values(), axis=1, join='outer')

#         if final_df.empty:
#             logger.info(f"FinLab: {stock_id} 在 {start_date} 到 {end_date} 期間無價格數據。")
#             return pd.DataFrame()

#         final_df.index.name = 'price_date'
#         final_df.reset_index(inplace=True)

#         final_df['security_id'] = stock_id
#         final_df['source_api'] = 'finlab'
#         final_df['last_updated_timestamp'] = datetime.now()

#         # 確保所有標準欄位存在
#         standard_columns = [
#             'price_date', 'security_id', 'open_price', 'high_price', 'low_price',
#             'close_price', 'volume', 'source_api', 'last_updated_timestamp'
#         ]
#         for col in standard_columns:
#             if col not in final_df.columns:
#                 final_df[col] = pd.NA # 或 0 for volume?

#         # 轉換 volume 單位 (如果 FinLab 成交股數是以 "張" 為單位，則需 *1000)
#         # 假設 'price:成交股數' 是 "股"
#         if 'volume' in final_df.columns:
#             final_df['volume'] = pd.to_numeric(final_df['volume'], errors='coerce')

#         return final_df[standard_columns]


#     # --- 以下為財報、籌碼等數據獲取方法的框架 ---
#     # --- 這些方法的實現高度依賴於 FinLab dataset name 的確認 ---

#     def get_financial_statement_metric(self, stock_id: str, metric_dataset_name: str, start_date: str, end_date: str) -> pd.DataFrame:
#         """
#         獲取單一財務指標的歷史數據。
#         例如 metric_dataset_name = "financial_statement:ROE"

#         返回長格式 DataFrame: 'report_date', 'security_id', 'metric_name', 'metric_value', ...
#         """
#         logger.info(f"FinLab: 請求股票 {stock_id} 的財務指標 {metric_dataset_name} ({start_date}-{end_date})。")
#         # 假設 _fetch_finlab_data 返回 DataFrame: index=date, columns=stock_ids
#         raw_df_full, error = self._fetch_finlab_data(metric_dataset_name)

#         if error or raw_df_full is None or raw_df_full.empty or stock_id not in raw_df_full.columns:
#             logger.warning(f"FinLab: 無法獲取或無數據 for {metric_dataset_name}, stock {stock_id}。錯誤: {error}")
#             return pd.DataFrame()

#         stock_series = raw_df_full[stock_id]
#         try:
#             stock_series.index = pd.to_datetime(stock_series.index)
#             filtered_series = stock_series.loc[start_date:end_date].dropna()
#         except Exception as e:
#             logger.error(f"FinLab: 過濾財務指標 {metric_dataset_name} for {stock_id} 時出錯: {e}", exc_info=True)
#             return pd.DataFrame()

#         if filtered_series.empty:
#             return pd.DataFrame()

#         df_long = filtered_series.reset_index()
#         df_long.columns = ['report_date', 'metric_value']
#         df_long['security_id'] = stock_id
#         df_long['metric_name'] = metric_dataset_name # 或從 dataset_name 中提取更純淨的指標名
#         df_long['period_type'] = 'quarterly' # 假設 FinLab 財報是季度，需確認
#         df_long['currency'] = 'TWD' # 假設台股
#         df_long['source_api'] = 'finlab'
#         df_long['last_updated_timestamp'] = datetime.now()

#         # 這裡的欄位應對應到您的 `fact_financial_statement` schema
#         return df_long


#     # def get_chip_data_metric(self, stock_id: str, chip_dataset_name: str, start_date: str, end_date: str) -> pd.DataFrame:
#     #     """
#     #     獲取單一籌碼指標的歷史數據。
#     #     例如 chip_dataset_name = "chip:外資買賣超張數"
#     #     返回長格式 DataFrame: 'transaction_date', 'security_id', 'metric_name', 'metric_value', ...
#     #     """
#     #     # 實現邏輯類似 get_financial_statement_metric
#     #     pass


#     # --- Connector 的主要職責是獲取原始數據並做最基本的轉換 ---
#     # --- 更複雜的 schema 對應和數據清洗可能在 DataMaster 或後續流程中 ---

# if __name__ == '__main__':
#     # 簡易測試 (需要您在環境中設定 FINLAB_API_TOKEN)
#     # 並且需要確認 FinLab SDK 的行為和 dataset 名稱
#     import os
#     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#     # 由於 FinLabConnector 的實現高度依賴 SDK 細節和 dataset 名稱，
#     # 這裡的測試代碼主要是個框架，可能無法直接運行成功，除非 SDK 行為符合假設。

#     api_key_from_env = os.getenv("FINLAB_API_TOKEN")
#     if not api_key_from_env:
#         logger.error("測試 FinLabConnector 需要設定環境變數 FINLAB_API_TOKEN。")
#     else:
#         logger.warning("FinLabConnector 的測試目前是概念性的，因其實現依賴待確認的 SDK 細節。")
#         # test_config = {
#         #     "api_key": api_key_from_env,
#         #     "requests_per_minute": 60
#         # }
#         # connector = FinLabConnector(api_config=test_config)

#         # if connector.data_client: # 檢查 client 是否成功初始化
#         #     logger.info("--- 測試 FinLab get_stock_price (概念性) ---")
#         #     # 實際的 stock_id, start_date, end_date 應替換
#         #     price_df = connector.get_stock_price("2330", "2023-01-01", "2023-01-10")
#         #     if not price_df.empty:
#         #         logger.info(f"2330 FinLab 歷史價格 (概念性測試，前5筆):\n{price_df.head().to_string()}")
#         #     else:
#         #         logger.warning("未能獲取 2330 FinLab 歷史價格 (概念性測試)。")

#         #     # ... 更多測試，例如財務指標 (如果 dataset name 已知)
#         # else:
#         #     logger.error("FinLabConnector data client 未初始化，跳過測試。")

#         logger.info("FinLabConnector 測試完成 (概念性)。")

# --- 佔位符 ---
# FinLabConnector 的完整實現高度依賴於其 SDK 的具體行為以及 dataset 名稱的準確性。
# 在這些資訊明確之前，提供一個完整的、可工作的 Connector 風險較高。
# 因此，此檔案暫時作為一個結構框架和註釋討論。
# 一旦 FinLab SDK 的使用方式和 dataset 名稱得到確認 (例如，透過研究其文檔或成功的測試腳本)，
# 上述的註釋掉的代碼可以作為實現的起點。

# 關鍵問題：
# 1. FinLab SDK `login()` 的確切行為和錯誤處理。
# 2. `finlab.data.get(dataset_name)` 是否是獲取所有類型數據的主要方法？
# 3. `get()` 方法是否接受 `stock_id`, `start_date`, `end_date` 等過濾參數？
#    或者它總是返回整個 dataset，需要客戶端過濾？
# 4. "price:收盤價", "financial_statement:ROE" 這樣格式的 dataset name 是否可以直接用於 `get()`？
# 5. API 檢測報告中提到的 "Dataset not exists" 錯誤，是指這些 dataset name 完全錯誤，
#    還是免費版 Token 無權限訪問？

# 在上述問題釐清前，FinLabConnector 的功能將受限。
# 我會先建立此檔案的框架，並在其他 Connector 完成後，如果時間允許且有更明確的 FinLab SDK 使用資訊，再回來完善它。
pass # 讓檔案在語法上有效
