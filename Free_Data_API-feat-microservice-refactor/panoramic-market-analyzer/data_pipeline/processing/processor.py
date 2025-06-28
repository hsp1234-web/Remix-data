# data_pipeline/processing/processor.py
import pandas as pd
import logging
from typing import Optional

class DataProcessor:
    """
    負責數據處理、轉換和基礎分析任務的類。
    例如：計算技術指標、數據清洗、特徵工程等。
    """

    def __init__(self, config: Optional[dict] = None):
        """
        初始化數據處理器。

        Args:
            config (Optional[dict]): 處理相關的組態設定 (如果有的話)。
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = config if config is not None else {}
        self.logger.info("DataProcessor initialized.")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        執行基礎的數據清洗操作。
        例如：處理缺失值、移除異常值等。

        Args:
            df (pd.DataFrame): 待清洗的數據。

        Returns:
            pd.DataFrame: 清洗後的數據。
        """
        self.logger.info(f"Starting data cleaning for DataFrame with shape {df.shape}.")

        # 示例：向前填充常見 OHLCV 數據中的缺失值
        # 假設 'date' 和 'symbol' 是存在的
        cols_to_ffill = ['open', 'high', 'low', 'close', 'adj_close']
        for col in cols_to_ffill:
            if col in df.columns:
                df[col] = df.groupby('symbol')[col].ffill()

        # 交易量缺失值通常用 0 填充
        if 'volume' in df.columns:
            df['volume'] = df.groupby('symbol')['volume'].fillna(0)

        # 移除完全是 NaN 的行 (除了 symbol 和 date)
        # df.dropna(subset=cols_to_ffill, how='all', inplace=True)

        self.logger.info(f"Data cleaning completed. DataFrame shape: {df.shape}.")
        return df

    def calculate_sma(self, df: pd.DataFrame, window: int, price_col: str = 'close', output_col_name: Optional[str] = None) -> pd.DataFrame:
        """
        計算簡單移動平均線 (SMA)。

        Args:
            df (pd.DataFrame): 包含價格數據的 DataFrame。必須有 'symbol' 和 price_col。
            window (int): 移動平均的窗口大小。
            price_col (str): 用於計算 SMA 的價格欄位名稱 (預設 'close')。
            output_col_name (Optional[str]): SMA 結果的欄位名稱。若為 None，則自動生成 (e.g., 'sma_20')。


        Returns:
            pd.DataFrame: 增加了 SMA 欄位的 DataFrame。
        """
        if price_col not in df.columns:
            self.logger.error(f"Price column '{price_col}' not found in DataFrame. Cannot calculate SMA.")
            return df

        if output_col_name is None:
            output_col_name = f"sma_{window}"

        self.logger.info(f"Calculating SMA with window {window} on column '{price_col}', output to '{output_col_name}'.")

        # 分組計算每個 symbol 的 SMA
        df[output_col_name] = df.groupby('symbol')[price_col].transform(
            lambda x: x.rolling(window=window, min_periods=max(1, window // 2)).mean() # 允許初期數據較少時也計算
        )
        return df

    def calculate_ema(self, df: pd.DataFrame, window: int, price_col: str = 'close', output_col_name: Optional[str] = None) -> pd.DataFrame:
        """
        計算指數移動平均線 (EMA)。

        Args:
            df (pd.DataFrame): 包含價格數據的 DataFrame。必須有 'symbol' 和 price_col。
            window (int): 移動平均的窗口大小 (span)。
            price_col (str): 用於計算 EMA 的價格欄位名稱 (預設 'close')。
            output_col_name (Optional[str]): EMA 結果的欄位名稱。若為 None，則自動生成 (e.g., 'ema_20')。

        Returns:
            pd.DataFrame: 增加了 EMA 欄位的 DataFrame。
        """
        if price_col not in df.columns:
            self.logger.error(f"Price column '{price_col}' not found in DataFrame. Cannot calculate EMA.")
            return df

        if output_col_name is None:
            output_col_name = f"ema_{window}"

        self.logger.info(f"Calculating EMA with window {window} on column '{price_col}', output to '{output_col_name}'.")

        df[output_col_name] = df.groupby('symbol')[price_col].transform(
            lambda x: x.ewm(span=window, adjust=False, min_periods=max(1, window // 2)).mean()
        )
        return df

    def calculate_rsi(self, df: pd.DataFrame, window: int = 14, price_col: str = 'close', output_col_name: Optional[str] = None) -> pd.DataFrame:
        """
        計算相對強弱指數 (RSI)。

        Args:
            df (pd.DataFrame): 包含價格數據的 DataFrame。
            window (int): RSI 的窗口期，預設為 14。
            price_col (str): 用於計算的價格欄位，預設為 'close'。
            output_col_name (Optional[str]): RSI 結果的欄位名稱。若為 None，則自動生成 (e.g., 'rsi_14')。

        Returns:
            pd.DataFrame: 增加了 RSI 欄位的 DataFrame。
        """
        if price_col not in df.columns:
            self.logger.error(f"Price column '{price_col}' not found. Cannot calculate RSI.")
            return df

        if output_col_name is None:
            output_col_name = f"rsi_{window}"

        self.logger.info(f"Calculating RSI with window {window} on column '{price_col}', output to '{output_col_name}'.")

        def rsi_calculation(series: pd.Series) -> pd.Series:
            delta = series.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=window, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()

            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi

        df[output_col_name] = df.groupby('symbol')[price_col].transform(rsi_calculation)
        return df

    # 可以在此處添加更多數據處理方法，例如：
    # - calculate_macd
    # - calculate_bollinger_bands
    # - normalize_features
    # - feature_engineering (e.g., lag features, volatility)

# 示例用法 (用於本地測試)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # 創建一些模擬數據
    data = {
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'] * 2),
        'symbol': ['AAPL'] * 5 + ['MSFT'] * 5,
        'open': [150, 151, 150, 152, 153, 250, 251, 250, 252, 253],
        'high': [152, 153, 151, 154, 155, 252, 253, 251, 254, 255],
        'low': [149, 150, 149, 151, 152, 249, 250, 249, 251, 252],
        'close': [151, 150, 151, 153, 154, 251, 250, 251, 253, 254],
        'adj_close': [151, 150, 151, 153, 154, 251, 250, 251, 253, 254],
        'volume': [1e6, 1.2e6, 1.1e6, 1.3e6, 1.4e6, 0.8e6, 0.9e6, 0.85e6, 0.95e6, 1e6]
    }
    sample_df = pd.DataFrame(data)

    # 引入一些缺失值用於測試清洗
    sample_df.loc[2, 'close'] = None # AAPL 2023-01-03 close
    sample_df.loc[7, 'volume'] = None # MSFT 2023-01-03 volume


    processor = DataProcessor()

    print("Original DataFrame:")
    print(sample_df)

    # 測試數據清洗
    cleaned_df = processor.clean_data(sample_df.copy()) # 使用 copy 以免修改原始 df
    print("\nCleaned DataFrame:")
    print(cleaned_df)

    # 測試 SMA 計算
    df_with_sma = processor.calculate_sma(cleaned_df.copy(), window=3)
    print("\nDataFrame with SMA (window=3):")
    print(df_with_sma[['symbol', 'date', 'close', 'sma_3']])

    # 測試 EMA 計算
    df_with_ema = processor.calculate_ema(cleaned_df.copy(), window=3)
    print("\nDataFrame with EMA (window=3):")
    print(df_with_ema[['symbol', 'date', 'close', 'ema_3']])

    # 測試 RSI 計算
    # 需要更多數據才能讓 RSI 有意義，這裡只是功能性測試
    data_rsi = {
        'date': pd.to_datetime([f'2023-01-{i:02d}' for i in range(1, 21)]),
        'symbol': ['TEST'] * 20,
        'close': [
            50, 52, 55, 53, 56, 58, 60, 59, 57, 55,
            53, 50, 48, 50, 52, 54, 53, 51, 50, 49
        ]
    }
    df_for_rsi = pd.DataFrame(data_rsi)
    df_with_rsi = processor.calculate_rsi(df_for_rsi, window=5) # 使用較小窗口測試
    print("\nDataFrame with RSI (window=5):")
    print(df_with_rsi)
