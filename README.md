# Daily Weather Log

![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub Actions CI Status](https://github.com/itsSabbir/daily-weather-log-v2/actions/workflows/main.yml/badge.svg)](https://github.com/itsSabbir/daily-weather-log-v2/actions/workflows/main.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

Automated daily collection and aggregation of weather data, primarily configured for Toronto, Canada, using the public Open-Meteo API and scheduled execution via GitHub Actions.

## Project Goal

The primary goal of this project is to create a robust and automated system that:

1.  **Fetches Current Weather Data:** Regularly retrieves current weather conditions (temperature, wind speed, weather code) for a predefined location (default: Toronto, Canada) from the Open-Meteo API.
2.  **Stores Raw Data:** Logs each fetched data point as a "raw" entry into a persistent CSV file (`weather_log.csv`).
3.  **Performs Daily Aggregation:** At each run, processes collected "raw" data for all *past* days. For each past day, it calculates summary statistics:
    *   High Temperature (Celsius)
    *   Low Temperature (Celsius)
    *   Average Temperature (Celsius)
    *   Maximum Wind Speed (km/h)
4.  **Data Rollup & Cleanup:** Replaces the multiple "raw" entries for each processed past day with a single "daily" summary record in the CSV file. This keeps the CSV file size manageable over time while preserving valuable daily insights.
5.  **Robust Operation:** Ensures the script is resilient against common issues like API failures, network timeouts, and data inconsistencies, with clear logging for monitoring.
6.  **Data Integrity:** Utilizes atomic file write operations during the aggregation process to prevent data loss in case of script crashes.

## How It Works

The core of the project is the `fetch_weather_aggregator.py` Python script, orchestrated by a GitHub Actions workflow (typically located in `.github/workflows/`).

### `fetch_weather_aggregator.py` Script Functionality:

*   **Setup and Configuration:** Defines constants for location (e.g., Toronto: Latitude 43.65, Longitude -79.38), API details, CSV filename (`weather_log.csv`), and logging.
*   **Fetch Weather Data:** Constructs the API URL, makes an HTTP GET request, parses the JSON response, validates data, and formats it into a "raw" record.
*   **Store Raw Data:** Appends the "raw" record to `weather_log.csv`, creating the file with headers if it doesn't exist.
*   **Daily Aggregation (using Pandas):** Reads `weather_log.csv`, identifies "raw" records for past UTC days, calculates daily summary statistics, and creates new "daily" summary records.
*   **Data Rollup & Atomic Write:** Combines existing summaries, new daily summaries, and non-aggregated raw data. Writes this to a temporary file, then atomically replaces the original `weather_log.csv`.
*   **Error Handling & Logging:** Comprehensive error handling and detailed logging to `stdout`.

### GitHub Actions Workflow:

*   **Scheduled Trigger:** Runs automatically at regular intervals (e.g., hourly or daily).
*   **Environment Setup:** Checks out code, sets up Python, installs dependencies from `requirements.txt`.
*   **Execute Script:** Runs `fetch_weather_aggregator.py`.
*   **Commit & Push Data:** If `weather_log.csv` is updated, the workflow commits and pushes the changes back to the repository.

## Data Structure (`weather_log.csv`)

The CSV file `weather_log.csv` contains the following columns:

| Column Name           | Description                                                                     |
| :-------------------- | :------------------------------------------------------------------------------ |
| `entry_date_utc`      | Date (YYYY-MM-DD, UTC) the record pertains to.                                  |
| `record_type`         | "raw" (individual fetch) or "daily" (aggregated summary).                       |
| `latitude`            | Latitude of the observation/summary.                                            |
| `longitude`           | Longitude of the observation/summary.                                           |
| `api_reported_time_utc`| Full ISO8601 timestamp from API (for "raw" records).                            |
| `temperature_celsius` | Instantaneous temperature (for "raw" records).                                  |
| `windspeed_kmh`       | Instantaneous wind speed (for "raw" records).                                   |
| `weathercode`         | Instantaneous WMO weather code (for "raw" records).                             |
| `temp_high_celsius`   | Highest temperature for the period (for "daily" records).                       |
| `temp_low_celsius`    | Lowest temperature for the period (for "daily" records).                        |
| `temp_avg_celsius`    | Average temperature for the period (for "daily" records).                       |
| `windspeed_max_kmh`   | Maximum wind speed for the period (for "daily" records).                        |

*Fields specific to one record type will be empty/null for other record types.*

## Setup and Usage

### Prerequisites

*   Python 3.9+ (or as specified by the project)
*   Git

### Local Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/itsSabbir/daily-weather-log-v2.git
    cd daily-weather-log-v2
    ```
    *(Replace `itsSabbir` and `daily-weather-log-v2`)*

2.  **Create and activate a virtual environment:**
    ```bash
    # For Linux/macOS
    python3 -m venv venv
    source venv/bin/activate

    # For Windows (PowerShell)
    python -m venv venv
    .\venv\Scripts\Activate.ps1
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the script manually (for testing):**
    ```bash
    python fetch_weather_aggregator.py
    ```

### Automated Execution (GitHub Actions)

The project is designed for automated execution via GitHub Actions.
1.  Ensure a workflow file (e.g., `.github/workflows/main.yml` or `weather_fetcher.yml`) is present and correctly configured.
2.  Enable GitHub Actions on your repository.
3.  The workflow will run on its schedule, execute the script, and commit `weather_log.csv` changes.

## `requirements.txt`

This file lists the Python packages required by the project. See the section below on how to create/update it. Key dependencies typically include:

*   `pandas`
*   `requests`

## Contributing

Contributions are welcome! Please fork the repository, create a feature branch, make your changes, and then open a Pull Request. Ensure your code adheres to existing styles and that any relevant documentation is updated.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for full details.
---