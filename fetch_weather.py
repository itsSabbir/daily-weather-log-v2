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
    pass


# --- Configuration ---
# Global script configurations. Best practice often involves environment variables
# or dedicated config files for production, especially for sensitive items like API keys.
DEFAULT_LATITUDE: float = 43.65  # Example: Toronto, Canada
DEFAULT_LONGITUDE: float = -79.38 # Example: Toronto, Canada
# Unified CSV file for storing raw data points and their aggregated summaries.
CSV_FILENAME: str = "weather_log.csv"
# Consistent date format string for 'entry_date_utc' in CSVs and for parsing.
DATE_FORMAT_STR: str = "%Y-%m-%d"

# --- Logging Setup ---
# Configures detailed logging, outputting to stdout for visibility in automated environments.
LOG_FORMAT: str = '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
logging.basicConfig(
    level=logging.INFO,    # Default logging level (INFO, DEBUG, ERROR, etc.).
    format=LOG_FORMAT,     # Apply the defined log format.
    stream=sys.stdout      # Direct logs to standard output.
)

# --- File Path Configuration ---
# Determines the absolute path to the CSV file, ensuring script portability.
try:
    SCRIPT_DIR: str = os.path.dirname(os.path.abspath(__file__))
except NameError: # Fallback for environments where __file__ is not defined (e.g., interactive REPL).
    SCRIPT_DIR: str = os.getcwd()
    logging.warning(f"Warning: __file__ is not defined. Using current working directory: {SCRIPT_DIR}")
CSV_FILEPATH: str = os.path.join(SCRIPT_DIR, CSV_FILENAME) # Full path to the CSV data log.

# --- API Configuration ---
API_BASE_URL: str = "https://api.open-meteo.com/v1/forecast" # Base URL for the Open-Meteo API.
API_REQUEST_TIMEOUT_SECONDS: int = 30 # Increased timeout for API requests for resilience.

# --- CSV Structure Definition ---
# Defines the complete set of columns for the weather_log.csv file.
# This structure accommodates 'raw' data, 'daily' summaries, and future 'weekly' summaries.
CSV_FIELDNAMES: List[str] = [
    "entry_date_utc",         # Date string (YYYY-MM-DD) for the record. For summaries, it's the date being summarized.
    "record_type",            # Type: "raw", "daily", "weekly".
    "latitude",               # Latitude of the observation/summary.
    "longitude",              # Longitude of the observation/summary.
    # --- Fields primarily for "raw" records ---
    "api_reported_time_utc",  # Full ISO8601 timestamp from the API for raw data.
    "temperature_celsius",    # Instantaneous temperature (for "raw" records).
    "windspeed_kmh",          # Instantaneous wind speed (for "raw" records).
    "weathercode",            # Instantaneous WMO weather code (for "raw" records).
    # --- Fields primarily for "daily" or "weekly" summary records ---
    "temp_high_celsius",      # Highest temperature recorded for the period.
    "temp_low_celsius",       # Lowest temperature recorded for the period.
    "temp_avg_celsius",       # Average temperature for the period.
    "windspeed_max_kmh",      # Maximum wind speed recorded for the period.
]


def get_api_url(latitude: float, longitude: float) -> str:
    """
    Constructs the Open-Meteo API URL for fetching current weather data.
    """
    # Includes latitude, longitude, and 'current_weather=true' parameter.
    # Note: Double ampersand '&&' in original code, should be single '&'. Corrected.
    return f"{API_BASE_URL}?latitude={latitude}&longitude={longitude}¤t_weather=true"


def fetch_weather_data(latitude: float = DEFAULT_LATITUDE, longitude: float = DEFAULT_LONGITUDE) -> Optional[Dict[str, Any]]:
    """
    Fetches and prepares a "raw" weather data record from the Open-Meteo API.

    Handles API request, response parsing, validation, and formats the data
    according to the defined CSV structure for raw records.

    Returns:
        A dictionary for a "raw" record if successful, None otherwise.
    """
    api_url = get_api_url(latitude, longitude)
    logging.info(f"Fetching current weather data from API: {api_url}")

    try:
        response = requests.get(api_url, timeout=API_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx).
        
        api_response_data = response.json()
        logging.debug(f"API Response (first 1000 chars): {str(api_response_data)[:1000]}")

        current_weather = api_response_data.get("current_weather")
        if not isinstance(current_weather, dict):
            logging.error(f"'current_weather' key missing or not a dictionary in API response. Full response: {api_response_data}")
            return None

        # Validate presence of essential keys in the 'current_weather' object.
        required_api_keys = ["temperature", "windspeed", "weathercode", "time"]
        for key in required_api_keys:
            if key not in current_weather:
                logging.error(f"Essential key '{key}' missing from 'current_weather' data. Data: {current_weather}")
                return None
        
        # Safely parse the API's reported time.
        api_time_str = current_weather.get("time")
        if not isinstance(api_time_str, str) or not api_time_str: # Check if it's a non-empty string
            logging.error(f"API 'time' value is invalid or missing. Received: '{api_time_str}'")
            return None
        
        try:
            # Handle ISO8601 strings, including those ending with 'Z' for UTC.
            if api_time_str.endswith('Z'):
                api_time_dt = datetime.fromisoformat(api_time_str[:-1] + '+00:00')
            else:
                api_time_dt = datetime.fromisoformat(api_time_str)
            entry_date_str = api_time_dt.strftime(DATE_FORMAT_STR) # Format as "YYYY-MM-DD"
        except ValueError as e: # Catch parsing errors from datetime.fromisoformat
            logging.error(f"Failed to parse API time string '{api_time_str}'. Error: {e}")
            return None

        # Construct the "raw" record dictionary.
        raw_record = {
            "entry_date_utc": entry_date_str,
            "record_type": "raw",
            "latitude": api_response_data.get("latitude"),
            "longitude": api_response_data.get("longitude"),
            "api_reported_time_utc": api_time_str, # Store the original API timestamp string.
            "temperature_celsius": current_weather.get("temperature"),
            "windspeed_kmh": current_weather.get("windspeed"),
            "weathercode": current_weather.get("weathercode"),
            # Initialize summary fields to None (or an appropriate NA marker for your CSV writer).
            "temp_high_celsius": None, "temp_low_celsius": None,
            "temp_avg_celsius": None, "windspeed_max_kmh": None,
        }
        logging.info(f"Successfully fetched and prepared raw weather data for {entry_date_str}.")
        return raw_record

    except requests.exceptions.Timeout:
        logging.error(f"API request timed out ({API_REQUEST_TIMEOUT_SECONDS}s) for URL: {api_url}.")
    except requests.exceptions.HTTPError as http_err:
        status = http_err.response.status_code if hasattr(http_err, 'response') else "N/A"
        text = http_err.response.text[:500] if hasattr(http_err, 'response') and http_err.response is not None else "N/A"
        logging.error(f"HTTP error {status} for {api_url}. Response snippet: {text}. Error: {http_err}")
    except requests.exceptions.RequestException as req_err: # Catches other network-related errors.
        logging.error(f"API request failed for {api_url}. Error: {req_err}")
    except ValueError as json_err: # Catches JSONDecodeError if response is not valid JSON.
        text = response.text[:500] if 'response' in locals() and response is not None else "N/A"
        logging.error(f"Failed to decode JSON from API response. URL: {api_url}. Error: {json_err}. Response snippet: {text}")
    except Exception as e: # Catch-all for any other unexpected errors.
        logging.error(f"An unexpected error occurred in fetch_weather_data: {e}", exc_info=True)
    return None


def append_record_to_csv(record: Dict[str, Any], filepath: str = CSV_FILEPATH, fieldnames: List[str] = CSV_FIELDNAMES) -> bool:
    """
    Appends a data record to the CSV file, creating it with headers if it doesn't exist.
    Uses csv.DictWriter for robust writing of dictionary-based records.
    """
    record_type_info = record.get('record_type', 'unknown type')
    logging.info(f"Attempting to append record (type: {record_type_info}) to CSV: {filepath}")
    try:
        file_exists_and_has_content = os.path.isfile(filepath) and os.path.getsize(filepath) > 0
        os.makedirs(os.path.dirname(filepath), exist_ok=True) # Ensure directory exists.
        
        with open(filepath, "a", newline="", encoding="utf-8") as csvfile:
            # `restval=''` ensures that if a field from `fieldnames` is missing in `record`,
            # an empty string is written for that cell, maintaining CSV structure.
            # `extrasaction='raise'` will cause an error if `record` has keys not in `fieldnames`.
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='raise', restval='')
            
            if not file_exists_and_has_content:
                writer.writeheader()
                logging.info(f"Written CSV header to new or empty file: {filepath}")
            
            # Ensure the record only contains keys defined in fieldnames or prepare it.
            # This prepares a row dictionary ensuring all keys from fieldnames are present.
            row_to_write = {fn: record.get(fn, '') for fn in fieldnames} # Default to empty string for missing
            writer.writerow(row_to_write)

        logging.info(f"Record (type: {record_type_info}) successfully appended to {filepath}")
        return True
    except IOError as e:
        logging.error(f"IOError during CSV append operation for {filepath}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Unexpected error during CSV append operation for {filepath}: {e}", exc_info=True)
    return False


def aggregate_daily_data(filepath: str = CSV_FILEPATH, fieldnames: List[str] = CSV_FIELDNAMES) -> bool:
    """
    Performs daily aggregation on 'raw' weather data stored in the CSV file.
    Raw records for previous days are aggregated into 'daily' summary records,
    and the original raw records for those processed days are removed.
    The CSV file is then atomically rewritten with the updated data.
    """
    logging.info(f"Initiating daily data aggregation for CSV file: {filepath}")
    
    if not os.path.isfile(filepath) or os.path.getsize(filepath) == 0:
        logging.info("CSV file is non-existent or empty. No data available for aggregation.")
        return True # No action needed, so consider this a success for this specific task.

    try:
        # Load data into Pandas DataFrame for efficient processing.
        # Specify `dtype` for key columns to ensure correct interpretation or allow flexibility.
        # `na_values` helps correctly interpret various forms of missing data.
        df = pd.read_csv(filepath, dtype={'latitude': float, 'longitude': float}, keep_default_na=True, 
                         na_values=['', 'None', 'NaN', 'null', '#N/A'])

        # Robustly parse date columns. If parsing fails, values become NaT (Not a Time).
        # 'entry_date_utc' should be YYYY-MM-DD strings. Convert to Pandas datetime then extract .dt.date.
        df['entry_date_utc'] = df['entry_date_utc'].astype(str)
        df['parsed_entry_date'] = pd.to_datetime(df['entry_date_utc'], format=DATE_FORMAT_STR, errors='coerce').dt.date
        # 'api_reported_time_utc' can be full timestamps; parse and keep as datetime for potential use.
        # df['parsed_api_time'] = pd.to_datetime(df['api_reported_time_utc'], errors='coerce', utc=True)
        # Note: 'api_reported_time_utc' is primarily for raw records and not directly used in daily aggregation logic here.

        logging.info(f"Loaded {len(df)} records from CSV for aggregation analysis.")
        if df.empty:
            logging.info("DataFrame is empty after loading CSV. No aggregation possible.")
            return True

        # --- Data Segregation ---
        # Separate 'raw' records from existing 'daily' or other 'summary' records.
        # Use .copy() to avoid Pandas SettingWithCopyWarning when modifying these slices.
        raw_df = df[df['record_type'] == 'raw'].copy()
        existing_summaries_df = df[df['record_type'] != 'raw'].copy()
        
        # Clean temporary parsing columns from existing_summaries_df as they are not needed further for this part.
        existing_summaries_df.drop(columns=['parsed_entry_date'], errors='ignore', inplace=True)


        if raw_df.empty:
            logging.info("No 'raw' records found. No new daily aggregation to perform.")
            # If the file only contained summaries, just ensure it's written back cleanly (e.g. consistent NA, column order).
            # This handles the case where the script runs but there's no new raw data.
            # Writing back ensures consistent format if 'df.to_csv' parameters below are different from how file was made.
            temp_filepath_no_raw = filepath + ".tmp_no_raw"
            existing_summaries_df.reindex(columns=fieldnames).to_csv(temp_filepath_no_raw, index=False, header=True, date_format=DATE_FORMAT_STR, na_rep='')
            os.replace(temp_filepath_no_raw, filepath)
            logging.info(f"CSV rewritten with {len(existing_summaries_df)} existing summary records (no raw data processed).")
            return True

        # --- Prepare Raw Data for Aggregation ---
        # Ensure columns used in numeric aggregations are of a numeric type.
        # If a column like 'temperature_celsius' doesn't exist, it's added with NAs to prevent GroupBy errors.
        numeric_agg_cols = ['temperature_celsius', 'windspeed_kmh']
        for col in numeric_agg_cols:
            if col not in raw_df.columns:
                raw_df[col] = pd.NA # Use pandas NA for missing numeric data
                logging.warning(f"Column '{col}' not found in raw_df; created with NAs for aggregation.")
            raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce') # Convert, forcing errors to NaT/NaN.

        # --- Identify Records for Aggregation ---
        # Aggregate raw records for all days strictly *before* the current UTC day.
        today_utc = datetime.now(timezone.utc).date()
        
        # Filter raw_df for records with valid, past dates.
        raw_for_aggregation_df = raw_df[
            (raw_df['parsed_entry_date'].notna()) & (raw_df['parsed_entry_date'] < today_utc)
        ].copy()
        
        # Identify raw records to keep (current day, future days, or records with unparseable dates).
        raw_to_keep_df = raw_df[
            (raw_df['parsed_entry_date'].isna()) | (raw_df['parsed_entry_date'] >= today_utc)
        ].copy()
        # Clean temporary parsing columns from raw_to_keep_df.
        raw_to_keep_df.drop(columns=['parsed_entry_date'], errors='ignore', inplace=True)


        # --- Perform Aggregation ---
        newly_created_daily_summaries = []
        if not raw_for_aggregation_df.empty:
            num_unique_days_to_agg = len(raw_for_aggregation_df['parsed_entry_date'].unique())
            logging.info(f"Aggregating 'raw' data for {num_unique_days_to_agg} previous day(s).")

            # Group by the parsed date, latitude, and longitude to create daily summaries per location.
            # `as_index=False` converts grouped keys back into regular columns.
            aggregated_results_df = raw_for_aggregation_df.groupby(
                ['parsed_entry_date', 'latitude', 'longitude'], as_index=False
            ).agg(
                # Define aggregation functions for each summary field.
                temp_high_celsius=('temperature_celsius', 'max'),
                temp_low_celsius=('temperature_celsius', 'min'),
                temp_avg_celsius=('temperature_celsius', 'mean'),
                windspeed_max_kmh=('windspeed_kmh', 'max')
                # Add more complex aggregations if needed, e.g., for 'weathercode':
                # weathercode_dominant=('weathercode', lambda x: x.mode()[0] if not x.mode().empty else pd.NA)
            )
            
            # Convert aggregated results into the dictionary format for CSV_FIELDNAMES.
            for _, agg_row in aggregated_results_df.iterrows():
                summary_rec = {
                    "entry_date_utc": agg_row['parsed_entry_date'].strftime(DATE_FORMAT_STR), # Format date back to string.
                    "record_type": "daily", "latitude": agg_row['latitude'], "longitude": agg_row['longitude'],
                    "api_reported_time_utc": None, "temperature_celsius": None, "windspeed_kmh": None, "weathercode": None,
                    # Round aggregated numeric values and handle potential NaNs.
                    "temp_high_celsius": round(agg_row['temp_high_celsius'], 2) if pd.notnull(agg_row['temp_high_celsius']) else None,
                    "temp_low_celsius": round(agg_row['temp_low_celsius'], 2) if pd.notnull(agg_row['temp_low_celsius']) else None,
                    "temp_avg_celsius": round(agg_row['temp_avg_celsius'], 2) if pd.notnull(agg_row['temp_avg_celsius']) else None,
                    "windspeed_max_kmh": round(agg_row['windspeed_max_kmh'], 2) if pd.notnull(agg_row['windspeed_max_kmh']) else None,
                }
                newly_created_daily_summaries.append(summary_rec)
            
            if newly_created_daily_summaries:
                 logging.info(f"Successfully generated {len(newly_created_daily_summaries)} new 'daily' summary records.")
        else:
            logging.info("No 'raw' data from previous days was eligible for aggregation in this run.")

        # --- Combine DataFrames for Final CSV Output ---
        # List to hold DataFrames that will form the new CSV content.
        final_dataframes_list = []
        if not existing_summaries_df.empty:
            final_dataframes_list.append(existing_summaries_df)
        if newly_created_daily_summaries:
            final_dataframes_list.append(pd.DataFrame(newly_created_daily_summaries))
        if not raw_to_keep_df.empty:
            final_dataframes_list.append(raw_to_keep_df)

        # Reconstruct the main DataFrame.
        if not final_dataframes_list:
            logging.info("No data remaining after aggregation (all raw data was old and aggregated, no new raw data or existing summaries).")
            # Create an empty DataFrame with headers to ensure the file exists and is correctly formatted.
            final_combined_df = pd.DataFrame(columns=fieldnames)
        else:
            final_combined_df = pd.concat(final_dataframes_list, ignore_index=True, sort=False)
        
        # Ensure all defined CSV_FIELDNAMES are present, adding them with NA if missing.
        # Then, reorder columns to match CSV_FIELDNAMES for consistency.
        for col_name in fieldnames:
            if col_name not in final_combined_df.columns:
                final_combined_df[col_name] = pd.NA # Or use '' based on desired NA representation
        final_combined_df = final_combined_df.reindex(columns=fieldnames)


        # --- Atomic Write to CSV ---
        # Write to a temporary file first, then atomically replace the original file.
        # This prevents data corruption if the script crashes during the write operation.
        temp_csv_filepath = filepath + ".tmp_rewrite"
        # `na_rep=''` ensures NaN/NA values are written as empty strings in the CSV.
        final_combined_df.to_csv(temp_csv_filepath, index=False, header=True, columns=fieldnames, date_format=DATE_FORMAT_STR, na_rep='')
        os.replace(temp_csv_filepath, filepath) # Atomic operation (on most OSes).

        logging.info(f"Daily aggregation complete. CSV '{filepath}' rewritten with {len(final_combined_df)} records.")
        return True

    except pd.errors.EmptyDataError:
        logging.warning(f"Pandas EmptyDataError: CSV file '{filepath}' might be empty or unreadable by Pandas.")
        return True # Not a failure of aggregation itself if the source is problematic but readable as empty.
    except Exception as e:
        logging.error(f"Critical error during daily data aggregation for '{filepath}': {e}", exc_info=True)
        return False


def main():
    """
    Main script execution flow:
    1. Fetches new "raw" weather data.
    2. Appends this raw data to the persistent CSV log.
    3. Triggers daily aggregation process for past data.
    Determines overall script success based on these steps for CI/CD exit codes.
    """
    logging.info("--- Weather Data Fetcher & Aggregator Script Started ---")
    
    # Attempt to fetch new raw data.
    new_raw_data = fetch_weather_data()
    
    # Track success of the initial data fetch and append operations.
    data_fetch_append_success = False
    if new_raw_data:
        if append_record_to_csv(new_raw_data):
            logging.info("Successfully appended newly fetched raw data to the log.")
            data_fetch_append_success = True
        else:
            # This is critical; if we can't save new data, the script's primary purpose might be failing.
            logging.critical("CRITICAL FAILURE: Failed to append new raw data to CSV. Aborting further operations.")
            sys.exit(1) # Exit with error code.
    else:
        logging.warning("Failed to fetch new raw weather data during this run. Aggregation will still be attempted on existing data.")
        # We can still proceed to aggregate data even if the current fetch fails.
        # Consider data_fetch_append_success as True in terms of not blocking aggregation.
        # However, the final exit code might reflect the fetch failure.
        data_fetch_append_success = True # Allows aggregation to proceed.

    # Perform daily aggregation on the contents of the CSV file.
    aggregation_process_success = aggregate_daily_data()

    if not aggregation_process_success:
        logging.critical("CRITICAL FAILURE: The daily aggregation process encountered an error. CSV might be in an inconsistent state.")
        sys.exit(1) # Exit with error code.
    
    # Determine final script outcome.
    if new_raw_data and data_fetch_append_success and aggregation_process_success:
        logging.info("--- Weather Data Fetcher & Aggregator Script Finished Successfully (New data fetched & aggregated) ---")
        sys.exit(0)
    elif not new_raw_data and data_fetch_append_success and aggregation_process_success:
        # Fetch failed, but script didn't error out during append (because there was nothing to append), and aggregation was ok.
        logging.warning("--- Weather Data Fetcher & Aggregator Script Finished (New data FETCH FAILED, aggregation ran) ---")
        # Decide if this scenario is an overall failure. For now, treat as operational success if aggregation ok.
        sys.exit(0) # Or 1, if fetching new data is absolutely mandatory for overall script success.
    else:
        # This covers cases where data_fetch_append_success became false (which would have exited earlier)
        # or other unexpected state. This is a generic failure catch-all.
        logging.error("--- Weather Data Fetcher & Aggregator Script Finished With Unexpected Errors ---")
        sys.exit(1)


if __name__ == "__main__":
    # Critical dependency check for Pandas at script entry.
    try:
        import pandas as pd
        logging.info(f"Pandas library successfully imported. Version: {pd.__version__}")
    except ImportError:
        logging.critical("FATAL ERROR: The 'pandas' library is not installed, but it is essential for this script.")
        logging.critical("Please install it using your Python package manager (e.g., 'pip install pandas').")
        sys.exit(1) # Exit if pandas is not available.
    
    main()