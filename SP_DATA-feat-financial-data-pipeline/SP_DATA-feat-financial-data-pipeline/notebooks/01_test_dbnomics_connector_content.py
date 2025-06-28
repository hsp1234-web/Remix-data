# %% [markdown]
# # 集成測試：DBnomicsConnector
#
# 本 Notebook 用於實際測試 `DBnomicsConnector` 能否從 DBnomics API 獲取數據，
# 並將其轉換為我們定義的標準化格式。

# %%
# 基本導入
import pandas as pd
import sys
import os
import logging

# %% [markdown]
# ## 環境設置與導入 Connector
#
# 為了能夠導入我們自定義的 Connector，我們可能需要將 `src` 目錄添加到 Python 路徑中。
# (請根據您在 Colab 中組織專案的實際路徑進行調整)

# %%
# 假設此 notebook 在 'notebooks/' 目錄下，而 'src/' 在其父目錄的同級
# project_root = os.path.abspath(os.path.join(os.getcwd(), '..')) # 如果從 notebooks/ 目錄運行
# print(f"推測的專案根目錄: {project_root}")
# if project_root not in sys.path:
#    sys.path.insert(0, project_root)
# print(f"目前的 sys.path: {sys.path}")

# 在 Colab 中，如果您的專案根目錄是 /content/drive/MyDrive/FinancialData/
# 並且您直接在該目錄下運行 Colab (或已將其設為工作目錄)
# 或者，如果 FinancialData 是 sys.path 的一部分
# 您可能可以直接導入
# sys.path.append('/content/drive/MyDrive/FinancialData') # 確保專案根目錄在 path 中

try:
    from src.connectors.dbnomics_connector import DBnomicsConnector
    from src.connectors.base_connector import BaseConnector # 檢查 BaseConnector 是否也能導入
    # 如果需要加載配置:
    # import yaml
    # def load_config(config_path='config/config.yaml'):
    #     try:
    #         with open(config_path, 'r') as f:
    #             return yaml.safe_load(f)
    #     except FileNotFoundError:
    #         print(f"配置文件 {config_path} 未找到。請確保路徑正確。")
    #         return None
    # config = load_config('../config/config.yaml') # 如果從 notebooks/ 目錄運行
    # 如果直接在專案根目錄運行 Colab:
    # config = load_config('config/config.yaml')
except ImportError as e:
    print(f"導入錯誤: {e}")
    print("請確保:")
    print("1. 您已在專案的根目錄下 (例如 /content/drive/MyDrive/FinancialData/)")
    print("2. 或者，已將專案的根目錄添加到 sys.path (例如 sys.path.append('/content/drive/MyDrive/FinancialData'))")
    print("3. `src/connectors/__init__.py` 和 `src/__init__.py` (如果需要) 文件存在，使它們成為包。")
    raise

# %% [markdown]
# ## 配置日誌記錄
#
# 為了能看到 Connector 中的日誌輸出。

# %%
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)]) # 輸出到 notebook

# %% [markdown]
# ## 實例化 Connector 並定義目標系列

# %%
# 假設我們有一個簡化的 config 或直接傳入 None (如果 Connector 設計允許)
# 如果您的 BaseConnector 或 DBnomicsConnector 需要 config，請確保正確加載和傳遞
# connector_config = config if 'config' in locals() and config else {}
# db_connector = DBnomicsConnector(config=connector_config)

# 為了簡化，如果 DBnomicsConnector 的 config 是可選的，可以直接這樣：
try:
    db_connector = DBnomicsConnector(config=None) # 或者傳入您加載的 config
    print("DBnomicsConnector 實例化成功。")
except Exception as e:
    print(f"DBnomicsConnector 實例化失敗: {e}")
    raise

# %%
# 我們之前討論的 FRED 指標系列
series_to_fetch = [
    "FRED/FEDFUNDS",       # 聯邦基金利率
    "FRED/DGS10",          # 美國十年期公債殖利率
    "FRED/VIXCLS",         # VIX 波動率指數
    "FRED/DCOILWTICO"      # WTI 原油價格
]

# 可以添加一個無效的 ID 來測試錯誤處理
# series_to_fetch.append("FRED/NONEXISTENTSERIES")
# series_to_fetch.append("INVALID_FORMAT_ID")

# %% [markdown]
# ## 執行數據獲取與轉換

# %%
if 'db_connector' in locals():
    print(f"開始從 DBnomics 獲取以下系列數據: {series_to_fetch}")
    macro_data_df, error = db_connector.get_multiple_series(series_ids=series_to_fetch)

    if error:
        print(f"\n獲取數據過程中發生錯誤或部分失敗:")
        print(error)

    if macro_data_df is not None:
        print("\n成功獲取並轉換的數據 (部分預覽):")
    else:
        print("\n未能獲取任何數據。")

# %% [markdown]
# ## 驗證輸出的 DataFrame

# %%
if 'macro_data_df' in locals() and macro_data_df is not None and not macro_data_df.empty:
    print("\n--- DataFrame Head (前5行) ---")
    print(macro_data_df.head())

    print("\n--- DataFrame Info (數據類型和非空值) ---")
    macro_data_df.info()

    print("\n--- DataFrame Describe (數值型數據統計摘要) ---")
    # 為了更好地顯示 describe()，我們可以只選擇數值列
    numeric_cols = macro_data_df.select_dtypes(include=['number']).columns
    if not numeric_cols.empty:
        print(macro_data_df[numeric_cols].describe())
    else:
        print("DataFrame 中沒有數值型列可供描述。")

    print("\n--- 檢查 metric_name 的唯一值 ---")
    if 'metric_name' in macro_data_df.columns:
        print(macro_data_df['metric_name'].value_counts())
    else:
        print("'metric_name' 欄位不存在。")

    print("\n--- 檢查 source_api 的唯一值 ---")
    if 'source_api' in macro_data_df.columns:
        print(macro_data_df['source_api'].value_counts())
    else:
        print("'source_api' 欄位不存在。")

    print("\n--- 檢查日期範圍 ---")
    if 'metric_date' in macro_data_df.columns:
        try:
            # 確保 metric_date 是日期時間類型以便進行 min/max 操作
            # 如果在 transform_to_canonical 中已轉為 date 對象，這裡可以直接用
            # 如果是字符串，需要先轉換
            # temp_date_series = pd.to_datetime(macro_data_df['metric_date'], errors='coerce')
            # print(f"最早日期: {temp_date_series.min()}")
            # print(f"最晚日期: {temp_date_series.max()}")
            # 由於 transform_to_canonical 將其轉為 date object，可以直接使用
            print(f"最早日期: {macro_data_df['metric_date'].min()}")
            print(f"最晚日期: {macro_data_df['metric_date'].max()}")
        except Exception as e:
            print(f"計算日期範圍時出錯: {e}")
    else:
        print("'metric_date' 欄位不存在。")

else:
    if 'macro_data_df' in locals() and macro_data_df is not None and macro_data_df.empty :
        print("\n獲取的 DataFrame 為空，但無錯誤。可能API返回了空數據集或所有數據點均無效。")
    elif 'macro_data_df' not in locals() or macro_data_df is None:
        print("\n由於獲取數據失敗，無法進行 DataFrame 驗證。")


# %% [markdown]
# ## (可選) 數據可視化
#
# 如果數據成功獲取，可以取消以下註釋並執行繪圖。
# 需要安裝 `matplotlib` (`!pip install matplotlib`)

# %%
# import matplotlib.pyplot as plt
# import seaborn as sns

# if 'macro_data_df' in locals() and macro_data_df is not None and not macro_data_df.empty:
#     plt.figure(figsize=(12, 8))
#     sns.set_theme(style="whitegrid")

#     # 繪製每個指標的時間序列圖
#     for metric in macro_data_df['metric_name'].unique():
#         metric_subset = macro_data_df[macro_data_df['metric_name'] == metric]
#         # 確保 metric_date 是 datetime-like 以便繪圖
#         # 如果 metric_date 已經是 date object，可以直接用於 x 軸
#         plt.plot(pd.to_datetime(metric_subset['metric_date']), metric_subset['metric_value'], label=metric)

#     plt.title('DBnomics Macro Economic Data')
#     plt.xlabel('Date')
#     plt.ylabel('Value')
#     plt.legend(loc='best')
#     plt.xticks(rotation=45)
#     plt.tight_layout()
#     plt.show()
# else:
#     print("\n沒有數據可供繪製。")
