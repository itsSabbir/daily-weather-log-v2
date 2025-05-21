# fetch_weather.py
import requests
import csv
from datetime import datetime, timezone # Explicitly import timezone for UTC objects
import os
import sys
import logging
from typing import Dict, Optional, List # For improved code readability and static analysis

# --- Configuration ---
# It's generally a best practice to use environment variables or a configuration file
# for settings that might change between environments (dev, prod) or contain sensitive info.
# For this script, latitude/longitude are not sensitive, but API keys for other services would be.
DEFAULT_LATITUDE: float = 43.65  # Example: Toronto, Canada
DEFAULT_LONGITUDE: float = -79.38 # Example: Toronto, Canada
CSV_FILENAME: str = "weather_data.csv" # Name of the CSV file to store data

# --- Logging Setup ---
# Configure logging to provide detailed output, which is crucial for debugging,
# especially when running in automated environments like GitHub Actions.
LOG_FORMAT: str = '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
logging.basicConfig(
    level=logging.INFO,    # Set the default logging level (e.g., INFO, DEBUG, ERROR)
    format=LOG_FORMAT,     # Use the defined log format
    stream=sys.stdout      # Output logs to standard output (useful for GitHub Actions)
)
# Example for adding a file handler for local debugging:
# try:
#     log_file_path = os.path.join(os.path.dirname(__file__), "fetch_weather_local.log")
#     file_handler = logging.FileHandler(log_file_path)
#     file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
#     logging.getLogger().addHandler(file_handler)
#     logging.info(f"Local logging also configured to: {log_file_path}")
# except NameError:
#     pass # __file__ not defined, likely interactive mode

# --- File Path Configuration ---
# Determine the absolute path for the CSV file relative to the script's location.
# This ensures the script correctly locates the CSV file whether run locally or in a CI/CD pipeline.
try:
    # __file__ is the path to the current script. os.path.abspath ensures it's an absolute path.
    # os.path.dirname gets the directory containing the script.
    SCRIPT_DIR: str = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # __file__ is not defined if the script is run in an interactive interpreter (e.g., Python REPL).
    # In such cases, use the current working directory as a fallback.
    SCRIPT_DIR: str = os.getcwd()
    logging.warning(f"__file__ not defined, using current working directory: {SCRIPT_DIR}")

# Construct the full path to the CSV file.
CSV_FILEPATH: str = os.path.join(SCRIPT_DIR, CSV_FILENAME)

# --- API Configuration ---
API_BASE_URL: str = "https://api.open-meteo.com/v1/forecast"
API_REQUEST_TIMEOUT_SECONDS: int = 20 # Increased timeout for better resilience against slow network

# Define the expected structure of the data rows to be written to the CSV.
# This also dictates the header row of the CSV file.
CSV_FIELDNAMES: List[str] = [
    "fetch_timestamp_utc",    # Timestamp (UTC) when our script fetched the data
    "api_reported_time_utc",  # Timestamp (UTC) of the weather data as reported by the API
    "temperature_celsius",    # Current temperature in Celsius
    "windspeed_kmh",          # Current wind speed in km/h
    "weathercode",            # WMO Weather interpretation code
    "latitude",               # Latitude for which data was fetched
    "longitude"               # Longitude for which data was fetched
]


def get_api_url(latitude: float, longitude: float) -> str:
    """
    Constructs the full API URL for Open-Meteo with specified latitude and longitude
    to fetch current weather data.
    """
    # Parameters:
    #   latitude, longitude: Geographical coordinates
    #   current_weather=true: Specific Open-Meteo parameter to request current conditions
    return f"{API_BASE_URL}?latitude={latitude}&longitude={longitude}¤t_weather=true"


def fetch_weather_data(latitude: float = DEFAULT_LATITUDE, longitude: float = DEFAULT_LONGITUDE) -> Optional[Dict[str, any]]:
    """
    Fetches current weather data from the Open-Meteo API for the given coordinates.

    Args:
        latitude (float): Latitude for the weather data. Defaults to DEFAULT_LATITUDE.
        longitude (float): Longitude for the weather data. Defaults to DEFAULT_LONGITUDE.

    Returns:
        Optional[Dict[str, any]]: A dictionary containing parsed and structured weather data
                                  if the fetch and parse are successful. Returns None on any error.
    """
    api_url = get_api_url(latitude, longitude)
    logging.info(f"Attempting to fetch weather data from: {api_url}")

    try:
        # Make the GET request to the API.
        # `timeout` prevents the script from hanging indefinitely.
        response = requests.get(api_url, timeout=API_REQUEST_TIMEOUT_SECONDS)

        # `raise_for_status()` will raise an `requests.exceptions.HTTPError`
        # if the HTTP request returned an unsuccessful status code (4xx or 5xx).
        response.raise_for_status()

        # Parse the JSON response from the API.
        api_response_data = response.json()
        logging.debug(f"Raw API response: {api_response_data}") # Log raw response for debugging if needed

        # Extract the 'current_weather' block, which contains the data we need.
        current_weather_data = api_response_data.get("current_weather")

        # Validate that 'current_weather' data is present and is a dictionary.
        if not isinstance(current_weather_data, dict):
            logging.error(
                f"'current_weather' data is missing or not a dictionary in API response. "
                f"Response: {str(api_response_data)[:500]}" # Log a snippet of the response
            )
            return None

        # Validate that all essential keys are present in the 'current_weather' data.
        # This ensures data integrity before attempting to use potentially missing keys.
        required_api_keys = ["temperature", "windspeed", "weathercode", "time"]
        for key in required_api_keys:
            if key not in current_weather_data:
                logging.error(
                    f"Essential key '{key}' not found in 'current_weather' data block. "
                    f"Data: {current_weather_data}"
                )
                return None

        # Prepare our structured data dictionary based on CSV_FIELDNAMES.
        # It's good practice to explicitly map API fields to our desired structure.
        parsed_data = {
            "fetch_timestamp_utc": datetime.now(timezone.utc).isoformat(timespec='seconds'),
            "api_reported_time_utc": current_weather_data.get("time"), # API provides this in ISO8601 format, assume UTC
            "temperature_celsius": current_weather_data.get("temperature"),
            "windspeed_kmh": current_weather_data.get("windspeed"),
            "weathercode": current_weather_data.get("weathercode"),
            # Include latitude and longitude from the API response itself for verification,
            # as the API might return data for the closest grid point.
            "latitude": api_response_data.get("latitude"),
            "longitude": api_response_data.get("longitude")
        }
        logging.info(f"Successfully fetched and parsed weather data: {parsed_data}")
        return parsed_data

    # Specific error handling for common `requests` library exceptions.
    except requests.exceptions.Timeout:
        logging.error(f"Request to API timed out after {API_REQUEST_TIMEOUT_SECONDS} seconds for URL: {api_url}.")
    except requests.exceptions.HTTPError as http_err:
        # Log the HTTP error and a snippet of the response body if available.
        response_text_snippet = http_err.response.text[:500] if http_err.response else "N/A"
        logging.error(
            f"HTTP error occurred: {http_err} - Status: {http_err.response.status_code} "
            f"Response: {response_text_snippet}"
        )
    except requests.exceptions.RequestException as req_err:
        # Handles other network-related errors (e.g., DNS failure, connection refused).
        logging.error(f"An error occurred during the API request: {req_err}")
    except ValueError:  # Catches JSONDecodeError if response is not valid JSON
        response_text_snippet = response.text[:500] if response else "N/A"
        logging.error(
            f"Could not decode JSON response from API. URL: {api_url}. "
            f"Response text: {response_text_snippet}"
        )
    except Exception as e:
        # A catch-all for any other unexpected errors during the process.
        # `exc_info=True` includes traceback information in the log.
        logging.error(f"An unexpected error occurred in fetch_weather_data: {e}", exc_info=True)

    return None # Return None if any error occurred


def write_data_to_csv(data: Dict[str, any], filepath: str = CSV_FILEPATH, fieldnames: List[str] = CSV_FIELDNAMES) -> bool:
    """
    Appends a dictionary of data as a new row to a CSV file.
    If the CSV file does not exist, it creates the file and writes the header row.

    Args:
        data (Dict[str, any]): The dictionary of data to write (keys should match fieldnames).
        filepath (str): The full path to the CSV file.
        fieldnames (List[str]): A list of strings representing the CSV column headers.

    Returns:
        bool: True if the data was written successfully, False otherwise.
    """
    logging.info(f"Attempting to write data to CSV: {filepath}")
    try:
        # Check if the CSV file already exists to determine if headers need to be written.
        file_exists = os.path.isfile(filepath)

        # Ensure the directory for the CSV file exists.
        # `exist_ok=True` prevents an error if the directory already exists.
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Open the file in append mode ('a'). `newline=''` prevents extra blank rows on Windows.
        # `encoding='utf-8'` is a good default for text files.
        with open(filepath, "a", newline="", encoding="utf-8") as csvfile:
            # `csv.DictWriter` allows writing dictionaries directly to CSV rows.
            # `extrasaction='ignore'` ensures that if `data` has keys not in `fieldnames`,
            # they are ignored rather than raising an error.
            # `extrasaction='raise'` (default) would raise ValueError if data had extra keys.
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')

            if not file_exists:
                # If the file is new, write the header row.
                writer.writeheader()
                logging.info(f"CSV header written to new file: {filepath}")

            # Write the actual data row.
            writer.writerow(data)

        logging.info(f"Data successfully written to {filepath}")
        return True

    except IOError as e:
        # Handles file I/O related errors (e.g., permission denied, disk full).
        logging.error(f"IOError writing to CSV file {filepath}: {e}", exc_info=True)
    except Exception as e:
        # Catch-all for other unexpected errors during CSV writing.
        logging.error(f"An unexpected error occurred during CSV writing to {filepath}: {e}", exc_info=True)

    return False


def main():
    """
    Main execution function for the script.
    It orchestrates fetching weather data and writing it to a CSV file.
    Exits with status code 0 on success, 1 on failure.
    """
    logging.info("--- Weather Data Fetcher Script Started ---")

    # Step 1: Fetch weather data
    weather_data = fetch_weather_data() # Uses default lat/lon

    if weather_data:
        # Step 2: If data fetching was successful, write it to CSV
        if write_data_to_csv(weather_data):
            logging.info("--- Weather Data Fetcher Script Finished Successfully ---")
            sys.exit(0) # Exit with success code (0) for GitHub Actions
        else:
            logging.error("Failed to write data to CSV. Check logs for details.")
            # Fall through to exit with failure
    else:
        logging.error("Failed to fetch weather data. Check logs for details.")
        # Fall through to exit with failure

    logging.info("--- Weather Data Fetcher Script Finished With Errors ---")
    sys.exit(1) # Exit with failure code (1) for GitHub Actions


if __name__ == "__main__":
    # This ensures main() is called only when the script is executed directly,
    # not when imported as a module.
    main()