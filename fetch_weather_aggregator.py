# fetch_weather.py
import requests
import csv
from datetime import datetime, timezone # Use timezone-aware UTC objects
import os
import sys
import logging
from typing import Dict, Optional, List # For type hinting

# --- Configuration ---
# Best practice: Use environment variables or a config file for sensitive/configurable items.
# For simplicity here, keeping them as constants but acknowledging this.
# For Open-Meteo, lat/lon are not sensitive, but API keys for other services would be.
DEFAULT_LATITUDE: float = 43.65  # Example: Toronto
DEFAULT_LONGITUDE: float = -79.38 # Example: Toronto
CSV_FILENAME: str = "weather_data.csv"

# Setup logging
LOG_FORMAT: str = '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)
# You could also add a FileHandler here if desired, e.g., for local testing
# file_handler = logging.FileHandler("fetch_weather_local.log")
# file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
# logging.getLogger().addHandler(file_handler)

# Determine the absolute path for the CSV file relative to the script's location
# This ensures it works correctly both locally and in GitHub Actions
try:
    SCRIPT_DIR: str = os.path.dirname(os.path.abspath(__file__))
except NameError: # Handle case where __file__ might not be defined (e.g. interactive interpreter)
    SCRIPT_DIR: str = os.getcwd()
CSV_FILEPATH: str = os.path.join(SCRIPT_DIR, CSV_FILENAME)

API_BASE_URL: str = "https://api.open-meteo.com/v1/forecast"
API_REQUEST_TIMEOUT_SECONDS: int = 20 # Increased timeout for resilience

# Define the expected structure of the data we want to write
CSV_FIELDNAMES: List[str] = [
    "fetch_timestamp_utc",    # When our script fetched it
    "api_reported_time_utc",  # Timestamp of the weather data from the API
    "temperature_celsius",
    "windspeed_kmh",
    "weathercode",
    "latitude",
    "longitude"
]


def get_api_url(latitude: float, longitude: float) -> str:
    """Constructs the API URL with specified latitude and longitude."""
    return f"{API_BASE_URL}?latitude={latitude}&longitude={longitude}&&current_weather=true"


def fetch_weather_data(latitude: float = DEFAULT_LATITUDE, longitude: float = DEFAULT_LONGITUDE) -> Optional[Dict[str, any]]:
    """
    Fetches current weather data from the Open-Meteo API for the given coordinates.

    Returns:
        A dictionary containing parsed weather data if successful, None otherwise.
    """
    api_url = get_api_url(latitude, longitude)
    logging.info(f"Attempting to fetch weather data from: {api_url}")

    try:
        response = requests.get(api_url, timeout=API_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
        
        api_response_data = response.json()
        logging.debug(f"Raw API response: {api_response_data}")

        current_weather_data = api_response_data.get("current_weather")
        if not isinstance(current_weather_data, dict):
            logging.error(f"'current_weather' data is missing or not a dictionary in API response. Response: {api_response_data}")
            return None

        # Validate required keys exist in the current_weather object
        required_api_keys = ["temperature", "windspeed", "weathercode", "time"]
        for key in required_api_keys:
            if key not in current_weather_data:
                logging.error(f"Essential key '{key}' not found in 'current_weather' data block. Data: {current_weather_data}")
                return None

        # Prepare our structured data dictionary
        parsed_data = {
            "fetch_timestamp_utc": datetime.now(timezone.utc).isoformat(timespec='seconds'),
            "api_reported_time_utc": current_weather_data.get("time"), # Assuming API time is UTC
            "temperature_celsius": current_weather_data.get("temperature"),
            "windspeed_kmh": current_weather_data.get("windspeed"),
            "weathercode": current_weather_data.get("weathercode"),
            "latitude": api_response_data.get("latitude"), # Include lat/lon for context in CSV
            "longitude": api_response_data.get("longitude")
        }
        logging.info(f"Successfully fetched and parsed weather data: {parsed_data}")
        return parsed_data

    except requests.exceptions.Timeout:
        logging.error(f"Request to API timed out after {API_REQUEST_TIMEOUT_SECONDS} seconds.")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - Response: {response.text[:500] if response else 'N/A'}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred during the API request: {req_err}")
    except ValueError:  # Catches JSONDecodeError
        logging.error(f"Could not decode JSON response from API. Response text: {response.text[:500] if response else 'N/A'}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in fetch_weather_data: {e}", exc_info=True)
    
    return None


def write_data_to_csv(data: Dict[str, any], filepath: str = CSV_FILEPATH, fieldnames: List[str] = CSV_FIELDNAMES) -> bool:
    """
    Appends data to a CSV file. Creates the file with headers if it doesn't exist.

    Returns:
        True if write was successful, False otherwise.
    """
    logging.info(f"Attempting to write data to CSV: {filepath}")
    try:
        file_exists = os.path.isfile(filepath)
        
        # Ensure the directory for the CSV exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore') # Ignore extra keys in data
            if not file_exists:
                writer.writeheader()
                logging.info(f"CSV header written to new file: {filepath}")
            writer.writerow(data)
        logging.info(f"Data successfully written to {filepath}")
        return True
    except IOError as e:
        logging.error(f"IOError writing to CSV file {filepath}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV writing to {filepath}: {e}", exc_info=True)
    return False


def main():
    """Main execution function."""
    logging.info("--- Weather Data Fetcher Script Started ---")
    
    weather_data = fetch_weather_data()
    
    if weather_data:
        if write_data_to_csv(weather_data):
            logging.info("--- Weather Data Fetcher Script Finished Successfully ---")
            sys.exit(0) # Explicitly exit with success
        else:
            logging.error("Failed to write data to CSV.")
    else:
        logging.error("Failed to fetch weather data.")
        
    logging.info("--- Weather Data Fetcher Script Finished With Errors ---")
    sys.exit(1) # Exit with failure


if __name__ == "__main__":
    main()