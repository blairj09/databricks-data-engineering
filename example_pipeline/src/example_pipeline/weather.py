"""
Weather data fetcher using the Open-Meteo Historical Weather API.

This module provides functions to fetch historical weather data for NYC
and convert it to formats suitable for use with Spark/Databricks.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


# NYC coordinates
NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060

# Open-Meteo Historical Weather API endpoint
ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Weather variables to fetch (hourly)
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "weather_code",
]


def fetch_weather(
    start_date: str,
    end_date: str,
    latitude: float = NYC_LATITUDE,
    longitude: float = NYC_LONGITUDE,
) -> pd.DataFrame:
    """
    Fetch historical weather data from Open-Meteo API.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        latitude: Latitude of location (default: NYC)
        longitude: Longitude of location (default: NYC)

    Returns:
        DataFrame with hourly weather data including:
        - datetime: Timestamp of the observation
        - temperature_2m: Temperature in Celsius
        - relative_humidity_2m: Relative humidity percentage
        - precipitation: Total precipitation in mm
        - rain: Rain in mm
        - snowfall: Snowfall in cm
        - wind_speed_10m: Wind speed in km/h
        - weather_code: WMO weather interpretation code

    Raises:
        requests.RequestException: If API request fails
        ValueError: If response is invalid
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "America/New_York",
    }

    response = requests.get(ARCHIVE_API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if "hourly" not in data:
        raise ValueError("Invalid response: missing 'hourly' data")

    hourly = data["hourly"]

    # Build DataFrame from response
    df = pd.DataFrame({
        "datetime": pd.to_datetime(hourly["time"]),
        "temperature_2m": hourly.get("temperature_2m"),
        "relative_humidity_2m": hourly.get("relative_humidity_2m"),
        "precipitation": hourly.get("precipitation"),
        "rain": hourly.get("rain"),
        "snowfall": hourly.get("snowfall"),
        "wind_speed_10m": hourly.get("wind_speed_10m"),
        "weather_code": hourly.get("weather_code"),
    })

    # Add derived columns useful for analysis
    df["date"] = df["datetime"].dt.date
    df["hour"] = df["datetime"].dt.hour
    df["is_raining"] = df["rain"] > 0
    df["is_snowing"] = df["snowfall"] > 0

    return df


def fetch_weather_for_date_range(
    start_date: str,
    end_date: str,
    chunk_days: int = 30,
) -> pd.DataFrame:
    """
    Fetch weather data for a large date range, chunking requests to avoid timeouts.

    The Open-Meteo API can handle large ranges, but chunking provides better
    reliability and progress visibility.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        chunk_days: Number of days per request (default: 30)

    Returns:
        DataFrame with all weather data for the date range
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    chunks = []
    current_start = start

    while current_start < end:
        current_end = min(current_start + timedelta(days=chunk_days - 1), end)

        chunk_df = fetch_weather(
            start_date=current_start.strftime("%Y-%m-%d"),
            end_date=current_end.strftime("%Y-%m-%d"),
        )
        chunks.append(chunk_df)

        current_start = current_end + timedelta(days=1)

    return pd.concat(chunks, ignore_index=True)


def categorize_weather(weather_code: int) -> str:
    """
    Convert WMO weather code to human-readable category.

    See: https://open-meteo.com/en/docs#weathervariables

    Args:
        weather_code: WMO weather interpretation code

    Returns:
        Human-readable weather category
    """
    if weather_code is None:
        return "Unknown"

    code = int(weather_code)

    if code == 0:
        return "Clear"
    elif code in (1, 2, 3):
        return "Partly Cloudy"
    elif code in (45, 48):
        return "Foggy"
    elif code in (51, 53, 55, 56, 57):
        return "Drizzle"
    elif code in (61, 63, 65, 66, 67):
        return "Rain"
    elif code in (71, 73, 75, 77):
        return "Snow"
    elif code in (80, 81, 82):
        return "Rain Showers"
    elif code in (85, 86):
        return "Snow Showers"
    elif code in (95, 96, 99):
        return "Thunderstorm"
    else:
        return "Other"


def add_weather_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add weather category column to DataFrame.

    Args:
        df: DataFrame with weather_code column

    Returns:
        DataFrame with additional weather_category column
    """
    df = df.copy()
    df["weather_category"] = df["weather_code"].apply(categorize_weather)
    return df
