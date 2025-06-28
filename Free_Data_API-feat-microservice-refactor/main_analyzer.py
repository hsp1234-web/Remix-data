import os
import re
import logging
from datetime import datetime

# 從同一專案的其他模組導入
import file_utils
import market_data_yfinance
import market_data_fred
import pandas as pd

# 日誌設定 (與其他模組一致)
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# --- 配置區域 ---
INPUT_DIR = "IN_Source_Reports"
# OUTPUT_DIR 和 MANIFEST_FILEPATH 已在 file_utils 中定義
PROMPT_OUTPUT_DIR = file_utils.OUTPUT_DIR # 沿用 file_utils 中的定義

# 檔名解析的正則表達式
# YYYY年第WW週 (YYYY是四位數字，WW是一或兩位數字)
# 例如: "市場評論_2023年第05週_內部版.txt" 或 "周報2024年第1週重點.txt"
FILENAME_PATTERN = re.compile(r".*(\d{4})年第(\d{1,2})週.*\.txt$", re.IGNORECASE)

# --- 核心功能 ---

def parse_filename(filename: str) -> tuple[int | None, int | None]:
    """
    從檔名解析年份和週數。
    檔名應符合 "...YYYY年第WW週...txt" 格式。

    Args:
        filename (str): 要解析的檔名。

    Returns:
        tuple[int | None, int | None]: (年份, 週數)。如果解析失敗則回傳 (None, None)。
    """
    match = FILENAME_PATTERN.match(filename)
    if match:
        year_str, week_str = match.groups()
        try:
            year = int(year_str)
            week = int(week_str)
            if 1 <= week <= 53:  # 週數基本驗證
                # 驗證年份範圍，例如，不太可能有1900年之前的報告
                if 1970 <= year <= datetime.now().year + 5:
                    return year, week
                else:
                    logger.warning(f"檔名 '{filename}' 中的年份 '{year_str}' 超出合理範圍。")
                    return None, None
            else:
                logger.warning(f"檔名 '{filename}' 中的週數 '{week_str}' 無效。")
                return None, None
        except ValueError:
            logger.error(f"無法從檔名 '{filename}' 中轉換年份或週數為整數。")
            return None, None
    else:
        logger.warning(f"檔名 '{filename}' 不符合預期的格式 '...YYYY年第WW週...txt'。")
        return None, None

def get_week_date_range(year: int, week_number: int) -> tuple[str, str] | None:
    """
    根據年份和 ISO 週數，計算該週的開始日期 (星期一) 和結束日期 (星期日)。
    """
    try:
        # ISO 週的第一天是星期一。ISO 週的定義：包含該年第一個星期四的那一週是第一週。
        # datetime.strptime 會使用該年的1月1日作為計算基點。
        # %G - ISO 8601 year
        # %V - ISO 8601 week number (01-53)
        # %u - ISO 8601 weekday (1-7, Monday is 1)
        start_of_week_dt = datetime.strptime(f'{year}-{week_number}-1', "%G-%V-%u")
        end_of_week_dt = start_of_week_dt + pd.Timedelta(days=6)
        return start_of_week_dt.strftime("%Y-%m-%d"), end_of_week_dt.strftime("%Y-%m-%d")
    except ValueError as e:
        logger.error(f"無法計算年份 {year} 第 {week_number} 週的日期範圍: {e}。"
                       f"這可能發生在該年份沒有那麼多ISO週的情況 (例如，某年只有52週，但輸入了53)。")
        # 檢查該年實際有多少ISO週
        try:
            last_day_of_year = datetime(year, 12, 31)
            max_iso_week = last_day_of_year.isocalendar()[1]
            if week_number > max_iso_week:
                 logger.error(f"{year}年實際上只有 {max_iso_week} 個ISO週。")
        except:
            pass # 忽略此處的額外檢查錯誤
        return None


def format_dataframe_for_prompt(df: pd.DataFrame | None, title: str) -> str:
    """
    將 DataFrame 格式化為字串以包含在 Prompt 中。
    """
    if df is None or df.empty:
        return f"{title}:\n    數據無法獲取或為空。\n\n"
    
    # 將索引轉換為 YYYY-MM-DD 格式的字串 (如果索引是 DatetimeIndex)
    if isinstance(df.index, pd.DatetimeIndex):
        formatted_index = df.index.strftime('%Y-%m-%d')
    else:
        formatted_index = df.index.astype(str) # 如果不是 datetime，直接轉字串

    # 創建一個新的 DataFrame 用於格式化，以避免 SettingWithCopyWarning
    # 並且只處理常見的 OHLCV 和單值數據
    display_df = pd.DataFrame(index=formatted_index)

    output_str = f"{title}:\n"
    for col in df.columns:
        # 檢查是否為數值類型，進行格式化
        if pd.api.types.is_numeric_dtype(df[col]):
            # 對於價格相關的欄位，保留2-4位小數；成交量保留整數
            if col.lower() in ['open', 'high', 'low', 'close', 'adj close', 'price']:
                 display_df[col] = df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "N/A")
            elif col.lower() in ['volume']:
                 display_df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "N/A")
            else: # 其他數值 (如 FRED 指標)
                 display_df[col] = df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "N/A")
        else:
            display_df[col] = df[col].astype(str) # 非數值直接轉字串

    # 逐行格式化
    for idx, row in display_df.iterrows():
        row_str = f"    {idx}: "
        row_items = []
        for col_name, value in row.items():
            row_items.append(f"{col_name}: {value}")
        row_str += ", ".join(row_items)
        output_str += row_str + "\n"
        
    output_str += "\n"
    return output_str


def process_source_reports():
    """
    掃描輸入資料夾，根據增量更新機制處理源報告檔案。
    """
    logger.info(f"開始掃描輸入資料夾: {INPUT_DIR}")
    
    if not os.path.isdir(INPUT_DIR):
        logger.error(f"輸入資料夾 {INPUT_DIR} 不存在。請先建立並放入報告檔案。")
        return

    manifest_data = file_utils.load_manifest()
    logger.info(f"已載入處理清單，包含 {len(manifest_data)} 個已處理檔案的記錄。")
    
    processed_count = 0
    skipped_count = 0
    failed_parse_count = 0

    for filename in os.listdir(INPUT_DIR):
        filepath = os.path.join(INPUT_DIR, filename)
        
        if not os.path.isfile(filepath) or not filename.lower().endswith(".txt"):
            logger.debug(f"跳過非 txt 檔案或子資料夾: {filename}")
            continue

        logger.info(f"檢查檔案: {filepath}")
        year, week = parse_filename(filename)

        if year is None or week is None:
            logger.warning(f"無法從檔名 '{filename}' 解析年份和週數，跳過此檔案。")
            failed_parse_count += 1
            continue
        
        logger.info(f"成功解析檔名 '{filename}': 年份={year}, 週數={week}")

        try:
            with open(filepath, 'rb') as f: # 以二進位模式讀取以計算雜湊值
                file_content_bytes = f.read()
            
            current_file_hash = file_utils.calculate_sha256_hash(file_content_bytes)

            if file_utils.file_should_be_processed(filepath, current_file_hash, manifest_data):
                logger.info(f"檔案 '{filename}' 為新的或已修改，需要處理。雜湊值: {current_file_hash}")
                processing_successful = False # 預設為失敗
                try:
                    qualitative_report_content = file_content_bytes.decode('utf-8')
                    logger.info(f"成功讀取檔案 '{filename}' 內容進行處理。")

                    # 1. 獲取該週的日期範圍
                    date_range = get_week_date_range(year, week)
                    if not date_range:
                        logger.error(f"無法獲取 {year}年第{week}週 的日期範圍，跳過檔案 '{filename}' 的數據獲取。")
                        failed_parse_count +=1 # 也算一種處理失敗
                        # 即使日期範圍失敗，我們是否仍要更新 manifest？取決於策略。
                        # 這裡選擇不更新，下次會再嘗試。或者標記為永久失敗。
                        # 為了簡單起見，目前不更新 manifest，讓它下次再試。
                        continue # 跳到下一個檔案

                    start_date_str, end_date_str = date_range
                    logger.info(f"報告 '{filename}' ({year}年第{week}週) 的日期範圍: {start_date_str} 至 {end_date_str}")

                    # 2. 準備 Prompt 內容列表
                    prompt_sections = []
                    prompt_sections.append(f"===== 分析任務文本 =====\n")
                    prompt_sections.append(f"報告來源檔案: {filename}\n")
                    prompt_sections.append(f"報告時間: {year}年 第{week:02d}週\n")
                    prompt_sections.append(f"報告涵蓋日期範圍 (ISO 週): {start_date_str} 至 {end_date_str}\n\n")
                    
                    prompt_sections.append(f"--- 質化市場評論 ---\n")
                    prompt_sections.append(qualitative_report_content + "\n\n")
                    
                    prompt_sections.append(f"--- 量化市場數據 ---\n\n")

                    # 3. 獲取 yfinance 數據
                    yfinance_tickers = {
                        "S&P 500 (^GSPC)": "^GSPC",
                        "台灣加權指數 (^TWII)": "^TWII",
                        "美國20年期以上公債ETF (TLT)": "TLT",
                        "美國高收益公司債ETF (HYG)": "HYG",
                        "美國投資級公司債ETF (LQD)": "LQD",
                        "CBOE 波動率指數 (^VIX)": "^VIX",
                        "新台幣兌美元 (TWD=X)": "TWD=X",
                        "S&P 500 ETF (SPY)": "SPY"
                    }
                    prompt_sections.append(f"**宏觀市場與 ETF (yfinance 日線 OHLCV):**\n\n")
                    for name, ticker_symbol in yfinance_tickers.items():
                        # 小時線數據通常用於更短分析週期，週報可能以日線為主
                        # 如果需要小時線，可調整 attempt_hourly_first=True 和 interval
                        # 為了週報，將 end_date 稍微延長幾天，確保獲取到週五的數據（如果週日是 end_date_str）
                        yf_end_date = (datetime.strptime(end_date_str, "%Y-%m-%d") + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
                        
                        df_yf = market_data_yfinance.fetch_yfinance_data_stable(
                            ticker_symbol, start_date_str, yf_end_date, interval="1d", attempt_hourly_first=False
                        )
                        # 過濾掉不在精確日期範圍內的數據 (yfinance 可能會多給幾天)
                        if df_yf is not None:
                            df_yf = df_yf[(df_yf.index >= start_date_str) & (df_yf.index <= end_date_str)]

                        prompt_sections.append(format_dataframe_for_prompt(df_yf, name))
                        time.sleep(0.5) # 輕微延遲

                    # 4. 獲取 FRED 數據
                    # FRED 數據的頻率可能不同 (日/週/月)，獲取的時間窗口可能需要比一週稍長以確保取到數據點
                    # 例如，對於月度數據，如果該週沒有發布點，則會為空。
                    # 我們可以獲取包含該週的一個月範圍，然後在格式化時顯示。
                    # 或者，簡單地使用週的開始和結束日期。
                    fred_series_ids = {
                        "VIX 收盤價 (VIXCLS - FRED)": "VIXCLS", # ^VIX 的備援或補充
                        "10年期美國公債殖利率 (DGS10 - FRED)": "DGS10",
                        "聯邦基金有效利率 (FEDFUNDS - FRED)": "FEDFUNDS" # 月數據
                    }
                    prompt_sections.append(f"**重要經濟指標 (FRED):**\n\n")
                    # 對於 FRED，我們用週的起始和結束，如果數據頻率低，format_dataframe 會處理空情況
                    for name, series_id in fred_series_ids.items():
                        # FEDFUNDS 是月數據，用週的區間可能取不到，需要調整獲取邏輯或接受可能為空
                        # 這裡先用週的區間嘗試
                        df_fred = market_data_fred.fetch_fred_series_no_key(
                            series_id, start_date_str, end_date_str
                        )
                        prompt_sections.append(format_dataframe_for_prompt(df_fred, name))
                        time.sleep(0.5)

                    # 5. 獲取台指期貨連續合約數據
                    # 需要從 year, week 轉換為 start_year, start_month, end_year, end_month
                    # 週報通常關注當週情況，所以 start/end month 就是該週所在的月份
                    # 注意：fetch_continuous_taifex_futures 需要月份，不是週
                    report_month = datetime.strptime(start_date_str, "%Y-%m-%d").month
                    report_year = datetime.strptime(start_date_str, "%Y-%m-%d").year
                    
                    # 如果週橫跨兩個月，以週開始的月份為準，或結束的月份？
                    # 這裡以週開始的月份為準，獲取該月份的連續期貨數據，然後再篩選出本週的。
                    # 或者，我們可以直接獲取 start_year, start_month 到 end_year, end_month 的數據
                    # 這裡的 end_month 應該是 end_date_str 的月份
                    end_report_month = datetime.strptime(end_date_str, "%Y-%m-%d").month
                    end_report_year = datetime.strptime(end_date_str, "%Y-%m-%d").year

                    logger.info(f"準備獲取 {report_year}-{report_month} 至 {end_report_year}-{end_report_month} 的台指期貨數據。")
                    df_taifex = market_data_yfinance.fetch_continuous_taifex_futures(
                        report_year, report_month, end_report_year, end_report_month, interval="1d"
                    )
                    if df_taifex is not None:
                         # 過濾出本週的數據
                        df_taifex_week = df_taifex[(df_taifex.index >= start_date_str) & (df_taifex.index <= end_date_str)]
                        prompt_sections.append(format_dataframe_for_prompt(df_taifex_week, "台指期貨連續合約 (向後調整 - 本週數據)"))
                    else:
                        prompt_sections.append(format_dataframe_for_prompt(None, "台指期貨連續合約 (向後調整 - 本週數據)"))

                    # 6. 組成最終 Prompt 並儲存
                    prompt_sections.append(f"--- 任務指示 ---\n")
                    prompt_sections.append(f"請基於以上質化評論和量化數據，進行市場策略分析、機會發掘與風險評估。\n\n")
                    
                    prompt_sections.append(f"--- AI 研究與數據考量 (初步建議) ---\n")
                    prompt_sections.append(f"1. 數據儲存: 原始獲取的量化數據已透過客戶端快取儲存於本地 .sqlite 檔案中。若需長期儲存或供其他工具使用，可考慮將各數據源的 DataFrame 轉換為 Parquet 或 CSV 格式。\n")
                    prompt_sections.append(f"2. 缺失值處理: 根據數據特性選擇前向填充 (ffill)、插值等方法。\n")
                    prompt_sections.append(f"3. 異常值處理: 可使用 Z-score 或 IQR 等方法識別和處理。\n")
                    prompt_sections.append(f"4. 數據標準化/歸一化: 對於神經網路等模型，通常需要將數值特徵縮放到相似範圍 (如 Min-Max Scaling 或 Z-score Standardization)。\n")
                    prompt_sections.append(f"5. 特徵工程: 可考慮計算技術指標 (均線、RSI等)、滯後特徵、時間特徵等以增強模型輸入。\n")
                    prompt_sections.append(f"6. 台指選擇權歷史數據: yfinance 無法提供詳細的台指選擇權歷史 OCHLV 或希臘字母，請從 TAIFEX 官網或專業數據商獲取。\n")
                    # (可以加入更具體的指示)

                    final_prompt_content = "".join(prompt_sections)
                    
                    # 清理原始檔名，移除副檔名，並替換不適合路徑的字元
                    base_report_filename = os.path.splitext(filename)[0]
                    safe_report_filename = re.sub(r'[\\/*?:"<>|]', "_", base_report_filename) # 替換非法字元
                    
                    output_prompt_filename = f"Prompt_{year}年第{week:02d}週_{safe_report_filename}.txt"
                    output_prompt_filepath = os.path.join(PROMPT_OUTPUT_DIR, output_prompt_filename)
                    
                    with open(output_prompt_filepath, 'w', encoding='utf-8') as pf:
                        pf.write(final_prompt_content)
                    logger.info(f"已生成分析任務文本: {output_prompt_filepath}")
                    
                    processing_successful = True # 標記處理成功

                except UnicodeDecodeError:
                    logger.error(f"無法使用 UTF-8 解碼檔案 '{filename}'。請確保檔案為 UTF-8 編碼。")
                    failed_parse_count +=1 
                except Exception as e:
                    logger.error(f"處理檔案 '{filename}' (數據獲取或Prompt生成階段) 時發生未預期錯誤: {e}", exc_info=True)
                
                # 根據 processing_successful 的結果更新 manifest
                if processing_successful:
                    file_utils.update_manifest(filepath, current_file_hash, manifest_data)
                    file_utils.save_manifest(manifest_data)
                    logger.info(f"已更新 '{filename}' 的處理狀態到清單中。")
                    processed_count += 1
                else:
                    logger.error(f"檔案 '{filename}' 處理失敗，未更新其在清單中的狀態。")
                    # 這裡可以考慮是否要記錄"嘗試失敗"的狀態，避免無限重試某些確實有問題的檔案
            else:
                logger.info(f"檔案 '{filename}' 內容未變更，跳過處理。")
                skipped_count += 1
        
        except FileNotFoundError:
            logger.error(f"檔案 '{filepath}' 在處理過程中突然找不到，可能已被刪除或移動。")
        except Exception as e:
            logger.error(f"讀取或計算檔案 '{filepath}' 雜湊值時發生錯誤: {e}")
            failed_parse_count +=1

    logger.info("-" * 50)
    logger.info("所有輸入檔案檢查完畢。")
    logger.info(f"總共處理的檔案數: {processed_count}")
    logger.info(f"因未變更而跳過的檔案數: {skipped_count}")
    logger.info(f"因檔名解析失敗或讀取錯誤而跳過的檔案數: {failed_parse_count}")
    logger.info(f"最新的處理清單已儲存於: {file_utils.MANIFEST_FILEPATH}")
    logger.info("-" * 50)

# --- 主執行流程 ---
if __name__ == "__main__":
    logger.info("===== 全景市場分析儀開始執行 =====")
    
    # 建立一些模擬的輸入檔案來測試
    os.makedirs(INPUT_DIR, exist_ok=True)
    
    test_files_content = {
        "市場評論_2023年第01週_版本A.txt": "這是2023年第1週的報告內容。",
        "内部參考_2023年第02週分析.txt": "這是2023年第2週的報告。",
        "周報2023年第02週更新版.txt": "這是2023年第2週的更新版報告內容。", # 內容不同，檔名相似
        "無效檔名格式_2023_W03.txt": "這個檔名格式不對。",
        "市場分析_2024年第53週_特殊.txt": "2024年可能有53週。",
        "市場分析_2024年第5週.txt": "2024年第五週。" # 檔名中沒有 "第" 和 "週" 之間的數字
    }

    # 第一次運行前，可以清空 manifest 和輸出資料夾 (僅為測試方便)
    # if os.path.exists(file_utils.MANIFEST_FILEPATH):
    #     os.remove(file_utils.MANIFEST_FILEPATH)
    # if os.path.exists(file_utils.OUTPUT_DIR):
    #     import shutil
    #     # shutil.rmtree(file_utils.OUTPUT_DIR) # 小心使用，會刪除整個資料夾
    #     # 只刪除裡面的檔案
    #     for item in os.listdir(file_utils.OUTPUT_DIR):
    #         item_path = os.path.join(file_utils.OUTPUT_DIR, item)
    #         if os.path.isfile(item_path) and item != file_utils.MANIFEST_FILENAME :
    #             os.remove(item_path)


    logger.info("--- 測試回合 1：建立初始檔案 ---")
    for filename, content in test_files_content.items():
        with open(os.path.join(INPUT_DIR, filename), 'w', encoding='utf-8') as f:
            f.write(content)
            
    process_source_reports()

    logger.info("\n--- 測試回合 2：部分檔案內容變更，新增檔案 ---")
    # 修改一個檔案
    with open(os.path.join(INPUT_DIR, "市場評論_2023年第01週_版本A.txt"), 'w', encoding='utf-8') as f:
        f.write("這是2023年第1週的報告內容 (已更新)。")
    # 新增一個檔案
    with open(os.path.join(INPUT_DIR, "新報告_2024年第10週探索.txt"), 'w', encoding='utf-8') as f:
        f.write("這是2024年第10週的新增報告。")

    process_source_reports()
    
    logger.info("\n--- 測試回合 3：無變更 ---")
    process_source_reports()

    # 清理測試檔案 (可選)
    # logger.info("正在清理測試輸入檔案...")
    # for fname in test_files_content.keys():
    #     fpath = os.path.join(INPUT_DIR, fname)
    #     if os.path.exists(fpath):
    #         os.remove(fpath)
    # new_report_path = os.path.join(INPUT_DIR, "新報告_2024年第10週探索.txt")
    # if os.path.exists(new_report_path):
    #     os.remove(new_report_path)
    # logger.info("測試輸入檔案已清理。")
    
    logger.info("===== 全景市場分析儀執行完畢 =====")
    logger.info(f"請檢查 '{PROMPT_OUTPUT_DIR}' 中的輸出檔案以及 '{file_utils.MANIFEST_FILEPATH}'。")
    logger.info(f"快取數據位於 '{market_data_yfinance.CACHE_DIR}' 和 '{market_data_fred.FRED_CACHE_DIR}'。")
    logger.info("若要進行更全面的測試，請確保 IN_Source_Reports 資料夾中有不同日期和內容的報告檔案。")
    logger.info("注意：首次執行或處理新報告時，由於需要從網路獲取數據，可能耗時較長。後續執行因有快取會顯著加快。")
