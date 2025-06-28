# -*- coding: utf-8 -*-
"""
數據解析器模組 (Parsers)

提供通用的檔案內容解析功能，主要基於 Pandas。
"""
import pandas as pd
import io
from typing import Dict, Any, Optional, Iterator

from taifex_pipeline.core.logger_setup import get_logger

logger = get_logger(__name__)

def parse_file_stream_to_dataframe(
    file_stream: io.BytesIO,
    parser_config: Dict[str, Any],
    file_name_for_log: str = "UnknownFile"
) -> Optional[pd.DataFrame]: # 或者 Union[pd.DataFrame, Iterator[pd.DataFrame]] 如果支持 chunksize
    """
    根據提供的 parser_config，將檔案串流解析為 pandas DataFrame。

    目前主要支持 CSV 格式的解析 (pd.read_csv)。
    parser_config 字典中的鍵應對應 pd.read_csv 的參數。

    Args:
        file_stream (io.BytesIO): 檔案內容的位元組串流。應支持 seek(0)。
        parser_config (Dict[str, Any]): 解析參數字典，例如：
            {
                "sep": ",",
                "skiprows": 0,
                "encoding": "utf-8",
                "header": "infer",
                "names": None, # or list of names if header is None
                "dtype": {"column_name": str},
                "chunksize": None # or integer for iterator
                # ... 其他 pd.read_csv 參數
            }
        file_name_for_log (str): 檔名，用於日誌輸出。

    Returns:
        Optional[pd.DataFrame]: 解析後的 DataFrame。如果解析失敗，則返回 None。
                                 如果啟用了 chunksize，理論上應返回 Iterator[pd.DataFrame]，
                                 但目前簡化為：如果 chunksize 被設置，則只處理第一個 chunk 或報錯提示。
                                 實際的流式處理應在調用此函式的轉換管線中完成。
                                 **更新：** 為了與計畫描述（清洗函式處理分塊）一致，
                                 如果提供了 chunksize，此函數將返回一個 DataFrame 迭代器。
    """
    file_stream.seek(0) # 確保從頭讀取

    # 從 parser_config 中提取 encoding，如果沒有則使用預設值或讓 pandas 推斷 (但最好指定)
    encoding = parser_config.get('encoding', 'utf-8') # 預設為 utf-8
    chunksize = parser_config.get('chunksize')

    # 複製一份 parser_config 以免修改原始字典，並移除我們已單獨處理的 'encoding' (雖然 pandas 也接受)
    # pandas 的 read_csv 可以直接處理 BytesIO，它會根據 encoding 參數解碼
    read_csv_kwargs = parser_config.copy()

    # `encoding` 參數是 pd.read_csv 本身就有的，所以不需要從 kwargs 中移除
    # `chunksize` 也是

    logger.info(f"開始解析檔案 '{file_name_for_log}'。使用 parser_config: {read_csv_kwargs}")

    try:
        # 使用 pd.read_csv 進行解析
        # BytesIO 本身是二進位的，pandas 會根據 encoding 參數處理
        df_or_iterator = pd.read_csv(file_stream, **read_csv_kwargs)

        if chunksize is not None:
            logger.info(f"檔案 '{file_name_for_log}' 以 chunksize={chunksize} 進行分塊讀取，返回迭代器。")
            # 此時 df_or_iterator 是一個 TextFileReader (Iterator[DataFrame])
            return df_or_iterator # type: ignore
        else:
            # df_or_iterator 是一個 DataFrame
            if not isinstance(df_or_iterator, pd.DataFrame):
                # 理論上，如果 chunksize is None, read_csv 應該返回 DataFrame
                # 這是一個防禦性檢查
                logger.error(f"檔案 '{file_name_for_log}': pd.read_csv 未按預期返回 DataFrame (chunksize=None)。"
                               f"實際返回類型: {type(df_or_iterator)}")
                return None

            logger.info(f"檔案 '{file_name_for_log}' 解析成功，共 {len(df_or_iterator)} 行。"
                        f"欄位: {df_or_iterator.columns.tolist()}")
            return df_or_iterator

    except pd.errors.EmptyDataError:
        logger.warning(f"檔案 '{file_name_for_log}' 為空或不包含數據，返回空的 DataFrame。")
        # 根據需求，也可以返回 None 或拋出異常
        return pd.DataFrame() # 返回一個空的 DataFrame
    except UnicodeDecodeError as ude:
        logger.error(f"檔案 '{file_name_for_log}' 使用編碼 '{encoding}' 解碼失敗: {ude}。 "
                       f"請檢查 parser_config 中的 'encoding' 設定是否正確。", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"檔案 '{file_name_for_log}' 解析過程中發生未預期錯誤: {e}", exc_info=True)
        return None

# --- 範例使用 ---
if __name__ == "__main__":
    # from taifex_pipeline.core.logger_setup import setup_global_logger
    # setup_global_logger(log_level_console=logging.DEBUG)
    logger.info("開始執行 parsers.py 範例...")

    # 範例1: 基本 CSV
    csv_content1 = "colA,colB,colC\n1,apple,true\n2,banana,false\n3,cherry,true"
    stream1 = io.BytesIO(csv_content1.encode('utf-8'))
    config1 = {"sep": ",", "encoding": "utf-8", "header": 0}
    df1 = parse_file_stream_to_dataframe(stream1, config1, "sample1.csv")
    if df1 is not None:
        logger.info(f"範例1 DataFrame:\n{df1}\nTypes:\n{df1.dtypes}")
        assert len(df1) == 3
        assert df1['colB'].tolist() == ['apple', 'banana', 'cherry']

    # 範例2: MS950 編碼，有 skip rows 和指定 dtype
    # 注意：直接在 UTF-8 環境的 Python 腳本中創建 MS950 字節串比較麻煩
    # 這裡用 UTF-8 模擬，並假設 parser_config 指定了正確的 MS950
    csv_content2_ms950_sim = "# 這是一行註解\n# 這是第二行註解\n代號,名稱,價格\nTXF,台指期,15000.50\nMXF,小台指,15000.25"
    stream2 = io.BytesIO(csv_content2_ms950_sim.encode('utf-8')) # 假設這是 MS950 內容
    config2 = {
        "sep": ",",
        "encoding": "ms950", # 假設實際檔案是 MS950
        "skiprows": 2,
        "header": 0, # skiprows 後的第一行是標頭
        "dtype": {"代號": str, "名稱": str, "價格": float}
    }
    # 由於我們用 UTF-8 字串模擬，測試時 encoding 也用 utf-8
    config2_test = config2.copy()
    config2_test["encoding"] = "utf-8"
    df2 = parse_file_stream_to_dataframe(stream2, config2_test, "sample2_ms950_sim.csv")
    if df2 is not None:
        logger.info(f"範例2 DataFrame:\n{df2}\nTypes:\n{df2.dtypes}")
        assert len(df2) == 2
        assert df2['價格'].dtype == float
        assert df2['價格'].iloc[0] == 15000.50

    # 範例3: 無標頭，需提供 names
    csv_content3 = "val1,100,x\nval2,200,y"
    stream3 = io.BytesIO(csv_content3.encode('utf-8'))
    config3 = {"sep": ",", "encoding": "utf-8", "header": None, "names": ["ID", "Value", "Category"]}
    df3 = parse_file_stream_to_dataframe(stream3, config3, "sample3_noheader.csv")
    if df3 is not None:
        logger.info(f"範例3 DataFrame:\n{df3}")
        assert df3.columns.tolist() == ["ID", "Value", "Category"]
        assert df3['Value'].iloc[0] == 100

    # 範例4: 使用 chunksize
    csv_content4 = "A,B\n1,2\n3,4\n5,6\n7,8\n9,10"
    stream4 = io.BytesIO(csv_content4.encode('utf-8'))
    config4 = {"sep": ",", "encoding": "utf-8", "header": 0, "chunksize": 2}
    iterator_df4 = parse_file_stream_to_dataframe(stream4, config4, "sample4_chunked.csv")

    if iterator_df4 is not None:
        logger.info("範例4: 迭代處理分塊數據...")
        all_chunks_df_list = []
        if isinstance(iterator_df4, pd.DataFrame): # 防禦性：如果未返回迭代器
             logger.warning("parse_file_stream_to_dataframe 未按預期返回迭代器 (chunksize已設定)。")
             all_chunks_df_list.append(iterator_df4)
        else: # 是迭代器
            for i, chunk_df in enumerate(iterator_df4):
                logger.info(f"  Chunk {i+1}:\n{chunk_df}")
                assert isinstance(chunk_df, pd.DataFrame)
                all_chunks_df_list.append(chunk_df)

        if all_chunks_df_list:
            combined_df4 = pd.concat(all_chunks_df_list)
            logger.info(f"範例4 合併後的 DataFrame (共 {len(combined_df4)} 行):\n{combined_df4}")
            assert len(combined_df4) == 5 # 2 + 2 + 1
        else:
            logger.warning("範例4 未能成功處理任何數據塊。")


    # 範例5: 空檔案
    stream5 = io.BytesIO(b"")
    config5 = {"sep": ",", "encoding": "utf-8", "header": 0}
    df5 = parse_file_stream_to_dataframe(stream5, config5, "sample5_empty.csv")
    if df5 is not None:
        logger.info(f"範例5 (空檔案) DataFrame (應為空):\n{df5}")
        assert df5.empty

    # 範例6: 錯誤的編碼
    csv_content6_big5 = "欄位一,欄位二\n測試,資料".encode('big5', errors='ignore') # 模擬一個非UTF-8的內容
    stream6 = io.BytesIO(csv_content6_big5)
    config6 = {"sep": ",", "encoding": "utf-8", "header": 0} # 故意用錯誤的UTF-8去解
    df6 = parse_file_stream_to_dataframe(stream6, config6, "sample6_wrong_encoding.csv")
    assert df6 is None, "錯誤編碼應導致解析失敗並返回 None"
    logger.info(f"範例6 (錯誤編碼) DataFrame (預期 None): {df6}")

    logger.info("parsers.py 範例執行完畢。")
