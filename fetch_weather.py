# fetch_weather_aggregator.py
import requests
import csv
from datetime import datetime, date, timezone, timedelta # Import necessary datetime components
import os
import sys
import logging
from typing import Dict, Optional, List, Any # For type hinting

# Attempt to import pandas. A check at the script's entry point will enforce this.
try:
    import pandas as pd
except ImportError:
    # This allows the script to be parsed by linters even if pandas isn't immediately available
    # during static analysis in some environments. The runtime check is critical.
    pd = None # Assign None to satisfy linters that pd might be undefined.


# --- Configuration ---
DEFAULT_LATITUDE: float = 43.65  # Example: Toronto, Canada
DEFAULT_LONGITUDE: float = -79.38 # Example: Toronto, Canada
CSV_FILENAME: str = "weather_log.csv"
DATE_FORMAT_STR: str = "%Y-%m-%d"

# --- Logging Setup ---
LOG_FORMAT: str = '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)s] - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    stream=sys.stdout
)
logger = logging.getLogger(__name__) # Use a specific logger instance for this module

# --- File Path Configuration ---
try:
    SCRIPT_DIR: str = os.path.dirname(os.path.abspath(__file__))
except NameError: 
    SCRIPT_DIR: str = os.getcwd()
    logger.warning(f"__file__ is not defined. Using current working directory: {SCRIPT_DIR}")
CSV_FILEPATH: str = os.path.join(SCRIPT_DIR, CSV_FILENAME)

# --- API Configuration ---
API_BASE_URL: str = "https://api.open-meteo.com/v1/forecast"
API_REQUEST_TIMEOUT_SECONDS: int = 30

# --- CSV Structure Definition ---
CSV_FIELDNAMES: List[str] = [
    "entry_date_utc",
    "record_type",
    "latitude",
    "longitude",
    "api_reported_time_utc",
    "temperature_celsius",
    "windspeed_kmh",
    "weathercode",
    "temp_high_celsius",
    "temp_low_celsius",
    "temp_avg_celsius",
    "windspeed_max_kmh",
]


def get_api_url(latitude: float, longitude: float) -> str:
    """
    Constructs the Open-Meteo API URL for fetching current weather data.
    """
    # Corrected: Uses '&' instead of '¤' for query parameters.
    return f"{API_BASE_URL}?latitude={latitude}&longitude={longitude}&&t_weather=true"


def fetch_weather_data(latitude: float = DEFAULT_LATITUDE, longitude: float = DEFAULT_LONGITUDE) -> Optional[Dict[str, Any]]:
    """
    Fetches and prepares a "raw" weather data record from the Open-Meteo API.
    """
    api_url = get_api_url(latitude, longitude)
    logger.info(f"Fetching current weather data from API: {api_url}")

    try:
        response = requests.get(api_url, timeout=API_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status() 
        
        api_response_data = response.json()
        logger.debug(f"API Response (first 1000 chars): {str(api_response_data)[:1000]}")

        current_weather = api_response_data.get("current_weather")
        if not isinstance(current_weather, dict):
            logger.error(f"'current_weather' key missing or not a dictionary in API response. Full response: {api_response_data}")
            return None

        required_api_keys = ["temperature", "windspeed", "weathercode", "time"]
        for key in required_api_keys:
            if key not in current_weather:
                logger.error(f"Essential key '{key}' missing from 'current_weather' data. Data: {current_weather}")
                return None
        
        api_time_str = current_weather.get("time")
        if not isinstance(api_time_str, str) or not api_time_str:
            logger.error(f"API 'time' value is invalid or missing. Received: '{api_time_str}'")
            return None
        
        try:
            # Handle ISO8601 strings, including those ending with 'Z' for UTC.
            if api_time_str.endswith('Z'):
                api_time_dt = datetime.fromisoformat(api_time_str[:-1] + '+00:00')
            else:
                # datetime.fromisoformat handles various ISO8601 formats, including those with timezone offsets.
                api_time_dt = datetime.fromisoformat(api_time_str)
            
            # Ensure the datetime object is timezone-aware and converted to UTC for consistent date extraction.
            # Open-Meteo API 'time' field is specified as UTC.
            if api_time_dt.tzinfo is None:
                logger.warning(f"API time string '{api_time_str}' was naive. Assuming UTC as per Open-Meteo standard for 'time'.")
                api_time_dt = api_time_dt.replace(tzinfo=timezone.utc)
            else:
                api_time_dt = api_time_dt.astimezone(timezone.utc) # Convert to UTC if it had a different offset
            
            entry_date_str = api_time_dt.strftime(DATE_FORMAT_STR) # Format as "YYYY-MM-DD"
        except ValueError as e:
            logger.error(f"Failed to parse API time string '{api_time_str}'. Error: {e}")
            return None

        raw_record = {
            "entry_date_utc": entry_date_str,
            "record_type": "raw",
            "latitude": api_response_data.get("latitude"), # API provides this at top level
            "longitude": api_response_data.get("longitude"), # API provides this at top level
            "api_reported_time_utc": api_time_str,
            "temperature_celsius": current_weather.get("temperature"),
            "windspeed_kmh": current_weather.get("windspeed"),
            "weathercode": current_weather.get("weathercode"),
            # Initialize summary fields to None. They will be written as empty strings by DictWriter's restval
            # or by specific handling if None is passed for a key.
            "temp_high_celsius": None, "temp_low_celsius": None,
            "temp_avg_celsius": None, "windspeed_max_kmh": None,
        }
        logger.info(f"Successfully fetched and prepared raw weather data for {entry_date_str}.")
        return raw_record

    except requests.exceptions.Timeout:
        logger.error(f"API request timed out ({API_REQUEST_TIMEOUT_SECONDS}s) for URL: {api_url}.")
    except requests.exceptions.HTTPError as http_err:
        status_code = "N/A"
        response_text_snippet = "N/A"
        if hasattr(http_err, 'response') and http_err.response is not None:
            status_code = http_err.response.status_code
            response_text_snippet = http_err.response.text[:500]
        logger.error(f"HTTP error {status_code} for {api_url}. Response snippet: {response_text_snippet}. Error: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"API request failed for {api_url}. Error: {req_err}")
    except ValueError as json_err: # Catches JSONDecodeError if response is not valid JSON.
        response_text_snippet = "N/A"
        if 'response' in locals() and response is not None: # Ensure response object exists
             response_text_snippet = response.text[:500]
        logger.error(f"Failed to decode JSON from API response. URL: {api_url}. Error: {json_err}. Response snippet: {response_text_snippet}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in fetch_weather_data: {e}", exc_info=True)
    return None


def append_record_to_csv(record: Dict[str, Any], filepath: str = CSV_FILEPATH, fieldnames: List[str] = CSV_FIELDNAMES) -> bool:
    """
    Appends a data record to the CSV file, creating it with headers if it doesn't exist.
    """
    record_type_info = record.get('record_type', 'unknown type')
    logger.info(f"Attempting to append record (type: {record_type_info}) to CSV: {filepath}")
    try:
        # Ensure parent directory for the CSV file exists.
        parent_dir = os.path.dirname(filepath)
        if parent_dir: # Check if parent_dir is not an empty string (e.g. if filepath is just "file.csv")
            os.makedirs(parent_dir, exist_ok=True)
        
        file_exists = os.path.isfile(filepath)
        # Check if file has content (size > 0) to determine if header is needed.
        # os.path.getsize can raise FileNotFoundError if file_exists was True due to a race condition (unlikely here but good to be aware)
        # However, we are opening in "a" mode, so file will be created if not existing.
        # The key is whether to write the header.
        needs_header = not (file_exists and os.path.getsize(filepath) > 0)
        
        with open(filepath, "a", newline="", encoding="utf-8") as csvfile:
            # `restval=''` ensures that if a field from `fieldnames` is missing in `record`,
            # an empty string is written for that cell.
            # `extrasaction='raise'` will cause an error if `record` has keys not in `fieldnames`.
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='raise', restval='')
            
            if needs_header:
                writer.writeheader()
                logger.info(f"Written CSV header to new or empty file: {filepath}")
            
            # Prepare a row dictionary ensuring all keys from fieldnames are present, defaulting to restval (empty string).
            row_to_write = {fn: record.get(fn, writer.restval) for fn in fieldnames}
            writer.writerow(row_to_write)

        logger.info(f"Record (type: {record_type_info}) successfully appended to {filepath}")
        return True
    except IOError as e:
        logger.error(f"IOError during CSV append operation for {filepath}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during CSV append operation for {filepath}: {e}", exc_info=True)
    return False


def aggregate_daily_data(filepath: str = CSV_FILEPATH, fieldnames: List[str] = CSV_FIELDNAMES) -> bool:
    """
    Performs daily aggregation on 'raw' weather data stored in the CSV file.
    Raw records for previous days are aggregated into 'daily' summary records,
    and the original raw records for those processed days are removed.
    The CSV file is then atomically rewritten with the updated data.
    """
    if pd is None:
        logger.critical("Pandas library is not available. Aggregation cannot proceed.")
        return False # Should have been caught by main guard, but defensive check.
        
    logger.info(f"Initiating daily data aggregation for CSV file: {filepath}")
    
    if not os.path.isfile(filepath) or os.path.getsize(filepath) == 0:
        logger.info("CSV file is non-existent or empty. No data available for aggregation.")
        return True # No action needed, so consider this a success for this specific task.

    try:
        # Read lat/lon and weathercode as object to robustly handle non-numeric/mixed values, then convert/use as needed.
        df = pd.read_csv(
            filepath, 
            dtype={'latitude': object, 'longitude': object, 'weathercode': object},
            keep_default_na=True, 
            na_values=['', 'None', 'NaN', 'null', '#N/A'] # Define strings to interpret as NA
        )

        df['entry_date_utc'] = df['entry_date_utc'].astype(str) # Ensure it's string type before parsing
        df['parsed_entry_date'] = pd.to_datetime(df['entry_date_utc'], format=DATE_FORMAT_STR, errors='coerce').dt.date
        
        logger.info(f"Loaded {len(df)} records from CSV for aggregation analysis.")
        if df.empty:
            logger.info("DataFrame is empty after loading CSV. No aggregation possible.")
            return True # Successfully handled an empty (but valid) file.

        raw_df = df[df['record_type'] == 'raw'].copy()
        existing_summaries_df = df[df['record_type'] != 'raw'].copy()
        
        # Clean temporary parsing columns from existing_summaries_df.
        if 'parsed_entry_date' in existing_summaries_df.columns:
            existing_summaries_df.drop(columns=['parsed_entry_date'], inplace=True)

        if raw_df.empty:
            logger.info("No 'raw' records found. No new daily aggregation to perform.")
            # Rewrite existing summaries to ensure consistent formatting if needed.
            temp_filepath_rewrite = filepath + ".tmp_rewrite_summaries"
            
            # Ensure existing_summaries_df conforms to schema before writing
            df_to_write = existing_summaries_df.copy() # Work on a copy
            for col_name in fieldnames: # Ensure all fieldnames exist
                if col_name not in df_to_write.columns:
                    df_to_write[col_name] = pd.NA # Use pandas NA type
            df_to_write = df_to_write.reindex(columns=fieldnames) # Order columns

            df_to_write.to_csv(temp_filepath_rewrite, index=False, header=True, date_format=DATE_FORMAT_STR, na_rep='')
            os.replace(temp_filepath_rewrite, filepath)
            logger.info(f"CSV rewritten with {len(df_to_write)} existing summary records (no raw data processed).")
            return True

        # Prepare raw data for aggregation: Convert relevant columns to numeric, coercing errors.
        # Grouping keys 'latitude', 'longitude' and aggregation value columns.
        cols_to_make_numeric = ['latitude', 'longitude', 'temperature_celsius', 'windspeed_kmh']
        for col in cols_to_make_numeric:
            if col not in raw_df.columns:
                raw_df[col] = pd.NA 
                logger.warning(f"Column '{col}' not found in raw_df; created with NAs.")
            raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')

        today_utc = datetime.now(timezone.utc).date()
        
        # Filter raw_df for records with valid, past dates.
        raw_for_aggregation_df = raw_df[
            (raw_df['parsed_entry_date'].notna()) & 
            (raw_df['parsed_entry_date'] < today_utc)
        ].copy()
        
        # Identify raw records to keep (current day, future days, or records with unparseable dates).
        raw_to_keep_df = raw_df[
            (raw_df['parsed_entry_date'].isna()) | 
            (raw_df['parsed_entry_date'] >= today_utc)
        ].copy()
        if 'parsed_entry_date' in raw_to_keep_df.columns: # Clean temporary parsing column
            raw_to_keep_df.drop(columns=['parsed_entry_date'], inplace=True)

        newly_created_daily_summaries = []
        if not raw_for_aggregation_df.empty:
            num_unique_groupings = raw_for_aggregation_df.groupby(['parsed_entry_date', 'latitude', 'longitude']).ngroups
            logger.info(f"Aggregating 'raw' data for {num_unique_groupings} day(s)/location(s).")

            # Group by parsed date, latitude, and longitude. Keep groups with NA keys as per description.
            aggregated_results_df = raw_for_aggregation_df.groupby(
                ['parsed_entry_date', 'latitude', 'longitude'], as_index=False, dropna=False
            ).agg(
                temp_high_celsius=('temperature_celsius', 'max'),
                temp_low_celsius=('temperature_celsius', 'min'),
                temp_avg_celsius=('temperature_celsius', 'mean'),
                windspeed_max_kmh=('windspeed_kmh', 'max')
            )
            
            for _, agg_row in aggregated_results_df.iterrows():
                # Skip if grouping keys are NA, as per description for dropna=False in groupby
                if pd.isna(agg_row['parsed_entry_date']) or \
                   pd.isna(agg_row['latitude']) or \
                   pd.isna(agg_row['longitude']):
                    logger.warning(
                        f"Skipping summary generation for group with NA keys: "
                        f"Date='{agg_row['parsed_entry_date']}', "
                        f"Lat='{agg_row['latitude']}', Lon='{agg_row['longitude']}'"
                    )
                    continue

                summary_rec = {
                    "entry_date_utc": agg_row['parsed_entry_date'].strftime(DATE_FORMAT_STR),
                    "record_type": "daily", 
                    "latitude": agg_row['latitude'], 
                    "longitude": agg_row['longitude'],
                    # Raw data specific fields are set to None for summary records
                    "api_reported_time_utc": None, "temperature_celsius": None, 
                    "windspeed_kmh": None, "weathercode": None,
                    # Round aggregated numeric values and handle potential NaNs from aggregation
                    "temp_high_celsius": round(agg_row['temp_high_celsius'], 2) if pd.notnull(agg_row['temp_high_celsius']) else None,
                    "temp_low_celsius": round(agg_row['temp_low_celsius'], 2) if pd.notnull(agg_row['temp_low_celsius']) else None,
                    "temp_avg_celsius": round(agg_row['temp_avg_celsius'], 2) if pd.notnull(agg_row['temp_avg_celsius']) else None,
                    "windspeed_max_kmh": round(agg_row['windspeed_max_kmh'], 2) if pd.notnull(agg_row['windspeed_max_kmh']) else None,
                }
                newly_created_daily_summaries.append(summary_rec)
            
            if newly_created_daily_summaries:
                 logger.info(f"Generated {len(newly_created_daily_summaries)} new 'daily' summary records.")
        else:
            logger.info("No 'raw' data from previous days was eligible for aggregation.")

        # Combine DataFrames for Final CSV Output
        final_dataframes_list = []
        if not existing_summaries_df.empty:
            final_dataframes_list.append(existing_summaries_df)
        if newly_created_daily_summaries:
            final_dataframes_list.append(pd.DataFrame(newly_created_daily_summaries))
        if not raw_to_keep_df.empty:
            final_dataframes_list.append(raw_to_keep_df)

        if not final_dataframes_list:
            logger.info("No data remaining after aggregation. Creating an empty CSV with headers.")
            final_combined_df = pd.DataFrame(columns=fieldnames)
        else:
            final_combined_df = pd.concat(final_dataframes_list, ignore_index=True, sort=False)
        
        # Ensure all defined CSV_FIELDNAMES are present and in order.
        for col_name in fieldnames:
            if col_name not in final_combined_df.columns:
                final_combined_df[col_name] = pd.NA # Use pandas NA type
        final_combined_df = final_combined_df.reindex(columns=fieldnames)

        # Atomic Write to CSV
        temp_csv_filepath = filepath + ".tmp_rewrite_aggregated"
        final_combined_df.to_csv(temp_csv_filepath, index=False, header=True, columns=fieldnames, date_format=DATE_FORMAT_STR, na_rep='')
        os.replace(temp_csv_filepath, filepath)

        logger.info(f"Daily aggregation complete. CSV '{filepath}' rewritten with {len(final_combined_df)} records.")
        return True

    except pd.errors.EmptyDataError: # Raised by pd.read_csv if file is empty AND no columns/data can be parsed.
        logger.warning(f"Pandas EmptyDataError: CSV file '{filepath}' is empty or unreadable by Pandas. No aggregation performed.")
        # If the file is truly empty (0 bytes), the initial os.path.getsize() check handles it.
        # This might occur if file has only headers but pandas can't parse them, or other malformations.
        return True # Not a failure of aggregation itself if the source is problematic but readable as empty.
    except Exception as e:
        logger.error(f"Critical error during daily data aggregation for '{filepath}': {e}", exc_info=True)
        return False


def main():
    """
    Main script execution flow:
    1. Fetches new "raw" weather data.
    2. Appends this raw data to the persistent CSV log.
    3. Triggers daily aggregation process for past data.
    Determines overall script success based on these steps for CI/CD exit codes.
    """
    logger.info("--- Weather Data Fetcher & Aggregator Script Started ---")
    
    new_raw_data = fetch_weather_data()
    
    # Track success of the initial data fetch and append operations.
    # This variable helps decide if aggregation should run.
    can_proceed_to_aggregation = False

    if new_raw_data:
        if append_record_to_csv(new_raw_data):
            logger.info("Successfully appended newly fetched raw data to the log.")
            can_proceed_to_aggregation = True
        else:
            # This is critical; if we can't save new data, the script's primary purpose might be failing.
            logger.critical("CRITICAL FAILURE: Failed to append new raw data to CSV. Aborting further operations.")
            sys.exit(1) # Exit with error code.
    else:
        logger.warning("Failed to fetch new raw weather data during this run. Aggregation will still be attempted on existing data.")
        # No new data to append, so conceptually append step didn't fail in a way that blocks aggregation.
        can_proceed_to_aggregation = True

    # Perform daily aggregation on the contents of the CSV file.
    aggregation_process_success = False
    if can_proceed_to_aggregation:
        aggregation_process_success = aggregate_daily_data()
        if not aggregation_process_success:
            logger.critical("CRITICAL FAILURE: The daily aggregation process encountered an error.")
            sys.exit(1) # Exit with error code.
    # If append_record_to_csv failed, we would have already exited.

    # Determine final script outcome.
    if new_raw_data and can_proceed_to_aggregation and aggregation_process_success: # Implies append was successful
        logger.info("--- Script Finished Successfully (New data fetched & aggregated) ---")
        sys.exit(0)
    elif not new_raw_data and can_proceed_to_aggregation and aggregation_process_success:
        # Fetch failed (or no new data), but append was not applicable or successful, and aggregation was ok.
        logger.warning("--- Script Finished (New data FETCH FAILED or NO new data, aggregation ran successfully) ---")
        # Decide if this scenario is an overall failure. For now, treat as operational success if aggregation ok.
        sys.exit(0) # Or 1, if fetching new data is absolutely mandatory for overall script success.
    else:
        # This covers cases where can_proceed_to_aggregation was false (which would have exited earlier due to append failure)
        # or other unexpected state. This is a generic failure catch-all.
        logger.error("--- Script Finished With Unexpected Errors or Incomplete Operations ---")
        sys.exit(1)


if __name__ == "__main__":
    # Critical dependency check for Pandas at script entry.
    if pd is None: # Check if pandas failed to import at the top.
        # Ensure basic logging is configured if pandas is missing, as logger might not be fully set up.
        logging.basicConfig(level=logging.CRITICAL, format=LOG_FORMAT, stream=sys.stderr)
        logging.critical("FATAL ERROR: The 'pandas' library is not installed or failed to import, but it is essential for this script.")
        logging.critical("Please install it using your Python package manager (e.g., 'pip install pandas').")
        sys.exit(1) # Exit if pandas is not available.
    
    logger.info(f"Pandas library successfully imported. Version: {pd.__version__}")
    main()