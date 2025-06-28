import pandas as pd
from typing import Dict, Any, List, Optional, Tuple # Added Optional, Tuple
import logging # Added logging

from .base_connector import BaseConnector

# Instantiate logger at module level as per BaseConnector's practice
logger = logging.getLogger(__name__)

class DBnomicsConnector(BaseConnector):
    """
    用於從 DBnomics API 獲取宏觀經濟數據的具體連接器。
    """

    BASE_URL = "https://api.db.nomics.world/v22/series"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化 DBnomicsConnector。
        DBnomics 通常不需要 API Key。

        Args:
            config (Optional[Dict[str, Any]]): 全局配置字典。
        """
        super().__init__(source_name="dbnomics", config=config) # API Key is None by default

    def fetch_data(self, series_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        從 DBnomics API 獲取指定系列 ID 的原始數據。

        Args:
            series_id: 要獲取的系列 ID，格式為 'PROVIDER_CODE/SERIES_CODE'
                       (e.g., 'FRED/FEDFUNDS').

        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[str]]:
                成功時返回 (API 響應字典, None)，失敗時返回 (None, 錯誤訊息)。
        """
        if '/' not in series_id:
            error_msg = f"Invalid series_id format: '{series_id}'. Expected 'PROVIDER_CODE/SERIES_CODE'."
            logger.error(f"[{self.source_name}] {error_msg}")
            return None, error_msg

        # provider_code, series_code = series_id.split('/') # This was in the draft, but split is unsafe if series_id could have more slashes
        # A safer way if series_code itself might contain '/' (though unlikely for FRED)
        parts = series_id.split('/', 1)
        if len(parts) != 2:
            error_msg = f"Invalid series_id format after split: '{series_id}'. Could not determine provider and series code."
            logger.error(f"[{self.source_name}] {error_msg}")
            return None, error_msg
        provider_code, series_code = parts

        url = f"{self.BASE_URL}/{provider_code}/{series_code}"

        logger.info(f"[{self.source_name}] Fetching data for series_id: {series_id} from URL: {url}")
        # 調用基類的 _make_request 方法來處理請求和重試
        raw_data, error = self._make_request(url) # _make_request returns a tuple

        if error:
            logger.error(f"[{self.source_name}] Error fetching data for {series_id}: {error}")
            return None, error
        if not raw_data:
            logger.warning(f"[{self.source_name}] No data returned for {series_id} from {url}, but no explicit error from _make_request.")
            return None, f"No data returned for {series_id}"

        return raw_data, None

    def transform_to_canonical(self, raw_data: Dict[str, Any], series_id: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        將從 DBnomics API 獲取的原始數據轉換為標準化的 DataFrame。

        Args:
            raw_data: fetch_data 方法返回的原始 JSON 數據 (字典格式)。
            series_id: 正在處理的系列 ID，用於填充 metric_name。

        Returns:
            Tuple[Optional[pd.DataFrame], Optional[str]]:
                成功時返回 (符合 fact_macro_economic_data schema 的 Pandas DataFrame, None)，
                數據無效或為空時返回 (None, 錯誤訊息)。
        """
        logger.debug(f"[{self.source_name}] transform_to_canonical called for {series_id} with raw_data keys: {raw_data.keys() if isinstance(raw_data, dict) else 'Not a dict'}")

        try:
            # 1. 安全地提取 'period' 和 'value' 列表
            series_docs = raw_data.get("series", {}).get("docs", [])
            if not series_docs:
                err_msg = f"No 'series.docs' found in raw_data for {series_id}."
                logger.warning(f"[{self.source_name}] {err_msg} Raw data: {str(raw_data)[:500]}")
                return None, err_msg

            doc = series_docs[0]
            periods = doc.get("period")
            values = doc.get("value")

            if periods is None or values is None:
                err_msg = f"'period' or 'value' array not found in series_docs[0] for {series_id}."
                logger.warning(f"[{self.source_name}] {err_msg} Doc keys: {doc.keys()}")
                return None, err_msg

            if not isinstance(periods, list) or not isinstance(values, list):
                err_msg = f"'period' or 'value' is not a list for {series_id}."
                logger.warning(f"[{self.source_name}] {err_msg} Period type: {type(periods)}, Value type: {type(values)}")
                return None, err_msg

            if len(periods) != len(values):
                err_msg = f"Length of 'period' ({len(periods)}) and 'value' ({len(values)}) arrays do not match for {series_id}."
                logger.warning(f"[{self.source_name}] {err_msg}")
                return None, err_msg

            if not periods:
                logger.info(f"[{self.source_name}] 'period' and 'value' arrays are empty for {series_id}. Returning empty DataFrame.")
                return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp']), None

            df = pd.DataFrame({'metric_date': periods, 'metric_value': values})

            df['metric_date'] = pd.to_datetime(df['metric_date'], errors='coerce').dt.date
            df['metric_name'] = series_id
            df['metric_value'] = pd.to_numeric(df['metric_value'], errors='coerce')
            df['source_api'] = self.source_name
            df['last_updated_timestamp'] = datetime.now(timezone.utc)

            original_rows = len(df)
            df.dropna(subset=['metric_date', 'metric_value'], inplace=True)
            cleaned_rows = len(df)
            if original_rows > cleaned_rows:
                logger.info(f"[{self.source_name}] Dropped {original_rows - cleaned_rows} rows with NaT dates or NaN values for {series_id}.")

            if df.empty:
                logger.info(f"[{self.source_name}] DataFrame became empty after cleaning for {series_id}. Original data might have been entirely invalid.")
                return df, None

            expected_columns = ["metric_date", "metric_name", "metric_value", "source_api", "last_updated_timestamp"]
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_columns]

            logger.info(f"[{self.source_name}] Successfully transformed data for {series_id}. Shape: {df.shape}")
            return df, None

        except Exception as e:
            logger.exception(f"[{self.source_name}] Unexpected error during transformation for {series_id}: {e}")
            return None, f"Unexpected error during transformation for {series_id}: {str(e)}"

    def get_multiple_series(self, series_ids: List[str]) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        一個端到端的便利方法，用於獲取並轉換一個或多個系列 ID 的數據。

        Args:
            series_ids: 一個包含多個系列 ID 的列表。

        Returns:
            Tuple[Optional[pd.DataFrame], Optional[str]]:
                成功時返回 (包含所有請求數據的、合併後的標準化 DataFrame, None)，
                若所有系列均獲取或轉換失敗，則返回 (None, 錯誤列表/摘要)。
        """
        all_data_frames: List[pd.DataFrame] = []
        errors: List[str] = []

        for series_id in series_ids:
            logger.info(f"[{self.source_name}] Processing series: {series_id}")
            try:
                raw_data, fetch_error = self.fetch_data(series_id=series_id)

                if fetch_error:
                    logger.error(f"[{self.source_name}] Failed to fetch data for {series_id}: {fetch_error}")
                    errors.append(f"Fetch error for {series_id}: {fetch_error}")
                    continue

                if not raw_data:
                    logger.warning(f"[{self.source_name}] No raw data returned for {series_id}, skipping transformation.")
                    errors.append(f"No raw data for {series_id}")
                    continue

                transformed_df, transform_error = self.transform_to_canonical(raw_data=raw_data, series_id=series_id)

                if transform_error:
                    logger.error(f"[{self.source_name}] Failed to transform data for {series_id}: {transform_error}")
                    errors.append(f"Transform error for {series_id}: {transform_error}")
                    continue

                if transformed_df is not None and not transformed_df.empty:
                    all_data_frames.append(transformed_df)
                elif transformed_df is None:
                     logger.warning(f"[{self.source_name}] Transformation for {series_id} returned None explicitly (should be covered by transform_error).")
                else: # Empty DataFrame but no error from transform_to_canonical
                    logger.info(f"[{self.source_name}] Transformation for {series_id} resulted in an empty DataFrame (e.g. all data invalid or no data points). Not appending.")

            except Exception as e:
                logger.exception(f"[{self.source_name}] Unexpected error processing series {series_id}: {e}")
                errors.append(f"Unexpected error for {series_id}: {str(e)}")

        if not all_data_frames:
            # 返回一個空的、但欄位正確的 DataFrame，並且沒有錯誤訊息
            # (除非在 errors 列表中已經有其他 series 的 fetch/transform 錯誤)
            final_error_message = None
            if errors: # 如果之前處理其他series時已經有錯誤
                final_error_message = f"No dataframes were successfully processed OR some series had errors. Errors: {'; '.join(errors)}"
                logger.warning(f"[{self.source_name}] {final_error_message}")
            else: # 完全沒有任何數據幀，也沒有任何錯誤
                logger.warning(
                    f"[{self.source_name}] 未能成功處理任何系列數據或所有系列數據均為空，將返回一個空的 DataFrame。"
                )

            # 我們需要一個方法或屬性來獲取標準欄位
            # 這裡我們先假設有一個 self.get_canonical_columns() 的輔助方法
            # 或者直接從 schemas.json (理想情況) 或硬編碼
            canonical_columns = [
                'metric_date', 'metric_name', 'metric_value',
                'source_api', 'last_updated_timestamp'
            ]
            # 如果有 errors，那麼 error message 應該包含它們，即使我們返回空DF
            return pd.DataFrame(columns=canonical_columns), final_error_message # Return error message if any errors occurred for other series

        try:
            concatenated_df = pd.concat(all_data_frames, ignore_index=True)
            logger.info(f"[{self.source_name}] Successfully concatenated {len(all_data_frames)} dataframes. Final shape: {concatenated_df.shape}")
            if errors:
                logger.warning(f"[{self.source_name}] Some series failed during processing: {'; '.join(errors)}")
                return concatenated_df, f"Partial success. Errors: {'; '.join(errors)}"
            return concatenated_df, None
        except Exception as e:
            logger.exception(f"[{self.source_name}] Failed to concatenate dataframes: {e}")
            # 如果 concat 本身失敗，之前的 errors 也應該被包含
            error_summary = f"Failed to concatenate dataframes: {str(e)}. Previous errors: {'; '.join(errors) if errors else 'None'}"
            return None, error_summary
