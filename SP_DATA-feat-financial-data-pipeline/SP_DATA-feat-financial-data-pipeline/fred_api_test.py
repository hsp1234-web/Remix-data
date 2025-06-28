import requests
import pandas as pd
from datetime import datetime, timezone
import logging
import json # Added for more detailed error logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FRED_API_KEY = "USER_PROVIDED_FRED_KEY_REDACTED" # Key was provided by user
FRED_API_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

def fetch_fred_series_data(api_key: str, series_id: str, start_date: str, end_date: str, file_type: str = "json") -> tuple[pd.DataFrame | None, str | None]:
    """
    Fetches data for a given series ID from the FRED API and transforms it into a pandas DataFrame.

    Args:
        api_key (str): Your FRED API key.
        series_id (str): The series ID to fetch (e.g., "GDP").
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.
        file_type (str): The type of file to request (json, xml, etc.). Defaults to "json".

    Returns:
        tuple[pd.DataFrame | None, str | None]: A tuple containing the DataFrame with
                                                 standardized columns if successful,
                                                 None otherwise, and an error message
                                                 string if an error occurred, None otherwise.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": file_type,
        "observation_start": start_date,
        "observation_end": end_date,
        # "sort_order": "asc", # Default is ascending
        # "units": "lin" # Default is levels (lin)
    }

    logger.info(f"Fetching FRED data for series_id: {series_id}, from {start_date} to {end_date}")

    try:
        response = requests.get(FRED_API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)

        raw_data = response.json()
        logger.debug(f"Raw response for {series_id}: {json.dumps(raw_data, indent=2)}")

        observations = raw_data.get("observations")
        if not observations:
            logger.warning(f"No 'observations' data found for series_id: {series_id}. API Response: {raw_data}")
            return None, f"No 'observations' data found for series_id: {series_id}."

        if not isinstance(observations, list) or not observations: # Check if it's a list and not empty
            logger.warning(f"'observations' is not a list or is empty for series_id: {series_id}. Type: {type(observations)}")
            return None, f"'observations' is not a list or is empty for series_id: {series_id}."


        # Filter out placeholder values like '.'
        valid_observations = [obs for obs in observations if obs.get('value') != '.']
        if not valid_observations:
            logger.warning(f"No valid data points (all were '.') for series_id: {series_id} in the given range.")
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp']), None


        df = pd.DataFrame(valid_observations)

        # Standardize column names
        df.rename(columns={"date": "metric_date", "value": "metric_value"}, inplace=True)

        # Convert types
        df['metric_date'] = pd.to_datetime(df['metric_date'], errors='coerce').dt.date
        df['metric_value'] = pd.to_numeric(df['metric_value'], errors='coerce')

        # Add additional standardized columns
        df['metric_name'] = series_id
        df['source_api'] = "fred"
        df['last_updated_timestamp'] = datetime.now(timezone.utc)

        # Select and order columns according to the canonical model
        # Ensure all expected columns are present, even if some observations were filtered out
        canonical_columns = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp']

        # Handle cases where df might be empty after filtering but before selecting columns
        if df.empty:
             logger.info(f"DataFrame is empty after processing for {series_id} (all values might have been invalid or filtered).")
             # Return an empty DataFrame with the correct columns
             return pd.DataFrame(columns=canonical_columns), None

        final_df = df[canonical_columns]

        # Drop rows where essential data (metric_date or metric_value) became NaT/NaN after conversion
        final_df.dropna(subset=['metric_date', 'metric_value'], inplace=True)

        if final_df.empty and not df.empty : # If it became empty after dropna but wasn't before
            logger.warning(f"DataFrame for {series_id} became empty after dropping rows with invalid dates or values.")

        logger.info(f"Successfully fetched and transformed data for {series_id}. Shape: {final_df.shape}")
        return final_df, None

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error for {series_id}: {e}. Response text: {e.response.text if e.response else 'No response text'}")
        return None, f"HTTP error for {series_id}: {e}. Response: {e.response.text if e.response else 'N/A'}"
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error for {series_id}: {e}")
        return None, f"Connection error for {series_id}: {e}"
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error for {series_id}: {e}")
        return None, f"Timeout error for {series_id}: {e}"
    except requests.exceptions.RequestException as e:
        logger.error(f"General request error for {series_id}: {e}")
        return None, f"General request error for {series_id}: {e}"
    except Exception as e: # Catch any other unexpected errors during processing
        logger.error(f"An unexpected error occurred while processing {series_id}: {e}", exc_info=True)
        return None, f"An unexpected error occurred: {str(e)}"

if __name__ == "__main__":
    logger.info("--- Starting FRED API Test Script ---")

    series_to_test = {
        "GDP": "US Gross Domestic Product",
        "FEDFUNDS": "Effective Federal Funds Rate",
        "CPIAUCSL": "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
        "UNRATE": "Civilian Unemployment Rate"
    }
    start_date_test = "2020-01-01"
    end_date_test = "2023-12-31"

    all_series_data = {}

    for series_id, description in series_to_test.items():
        logger.info(f"\nTesting Series: {series_id} ({description})")
        df, error_message = fetch_fred_series_data(FRED_API_KEY, series_id, start_date_test, end_date_test)

        if error_message:
            logger.error(f"Failed to fetch data for {series_id}: {error_message}")
            all_series_data[series_id] = {"status": "error", "message": error_message, "data": None}
        elif df is not None:
            if df.empty:
                logger.warning(f"Received empty DataFrame for {series_id} (possibly no data in range or all data invalid).")
                all_series_data[series_id] = {"status": "success_empty", "message": "No data points returned or all data invalid/filtered.", "data": df}
            else:
                logger.info(f"Successfully fetched data for {series_id}. First 5 rows:")
                logger.info("\n" + df.head().to_string())
                all_series_data[series_id] = {"status": "success", "message": None, "data": df}
        else: # Should be caught by error_message but as a fallback
            logger.error(f"Failed to fetch data for {series_id} for an unknown reason (df is None but no error_message).")
            all_series_data[series_id] = {"status": "error", "message": "Unknown error, DataFrame is None.", "data": None}


    logger.info("\n\n--- FRED API Test Script Finished ---")
    logger.info("Summary of test results:")
    for series_id, result in all_series_data.items():
        data_shape = result['data'].shape if result['data'] is not None else "N/A"
        logger.info(f"  Series ID: {series_id}, Status: {result['status']}, Shape: {data_shape}, Error: {result.get('message', 'None')}")

    # Example of how to combine into one DataFrame if needed for a report
    # combined_df = pd.concat([res['data'] for res in all_series_data.values() if res['data'] is not None and not res['data'].empty])
    # if not combined_df.empty:
    #     logger.info("\n--- Combined DataFrame (first 10 rows) ---")
    #     logger.info("\n" + combined_df.head(10).to_string())
    # else:
    #     logger.info("\nNo data to combine into a single DataFrame.")
