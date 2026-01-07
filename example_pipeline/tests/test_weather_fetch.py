"""
Tests for the weather data fetcher module.

These tests verify the Open-Meteo API client functionality,
including data fetching, parsing, and categorization.
"""

import pytest
from unittest.mock import patch, Mock
import pandas as pd

from example_pipeline.weather import (
    fetch_weather,
    categorize_weather,
    add_weather_categories,
    NYC_LATITUDE,
    NYC_LONGITUDE,
)


# Sample API response for mocking
SAMPLE_API_RESPONSE = {
    "latitude": NYC_LATITUDE,
    "longitude": NYC_LONGITUDE,
    "hourly": {
        "time": [
            "2023-01-01T00:00",
            "2023-01-01T01:00",
            "2023-01-01T02:00",
        ],
        "temperature_2m": [2.5, 2.1, 1.8],
        "relative_humidity_2m": [85.0, 87.0, 89.0],
        "precipitation": [0.0, 0.1, 0.0],
        "rain": [0.0, 0.1, 0.0],
        "snowfall": [0.0, 0.0, 0.0],
        "wind_speed_10m": [12.5, 11.2, 10.8],
        "weather_code": [3, 61, 0],
    },
}


class TestCategorizeWeather:
    """Tests for weather code categorization."""

    def test_clear_sky(self):
        assert categorize_weather(0) == "Clear"

    def test_partly_cloudy(self):
        assert categorize_weather(1) == "Partly Cloudy"
        assert categorize_weather(2) == "Partly Cloudy"
        assert categorize_weather(3) == "Partly Cloudy"

    def test_foggy(self):
        assert categorize_weather(45) == "Foggy"
        assert categorize_weather(48) == "Foggy"

    def test_rain(self):
        assert categorize_weather(61) == "Rain"
        assert categorize_weather(63) == "Rain"
        assert categorize_weather(65) == "Rain"

    def test_snow(self):
        assert categorize_weather(71) == "Snow"
        assert categorize_weather(73) == "Snow"

    def test_thunderstorm(self):
        assert categorize_weather(95) == "Thunderstorm"

    def test_unknown(self):
        assert categorize_weather(None) == "Unknown"

    def test_other(self):
        assert categorize_weather(999) == "Other"


class TestFetchWeather:
    """Tests for the weather API fetch function."""

    @patch("example_pipeline.weather.requests.get")
    def test_fetch_weather_success(self, mock_get):
        """Test successful weather data fetch."""
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_API_RESPONSE
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        df = fetch_weather("2023-01-01", "2023-01-01")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "datetime" in df.columns
        assert "temperature_2m" in df.columns
        assert "is_raining" in df.columns
        assert "is_snowing" in df.columns

    @patch("example_pipeline.weather.requests.get")
    def test_fetch_weather_derived_columns(self, mock_get):
        """Test that derived columns are calculated correctly."""
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_API_RESPONSE
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        df = fetch_weather("2023-01-01", "2023-01-01")

        # Check derived columns
        assert "date" in df.columns
        assert "hour" in df.columns

        # Check is_raining calculation (second row has rain > 0)
        assert df.iloc[0]["is_raining"] == False
        assert df.iloc[1]["is_raining"] == True

    @patch("example_pipeline.weather.requests.get")
    def test_fetch_weather_api_error(self, mock_get):
        """Test handling of API errors."""
        mock_get.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            fetch_weather("2023-01-01", "2023-01-01")

    @patch("example_pipeline.weather.requests.get")
    def test_fetch_weather_invalid_response(self, mock_get):
        """Test handling of invalid API response."""
        mock_response = Mock()
        mock_response.json.return_value = {"invalid": "response"}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="missing 'hourly' data"):
            fetch_weather("2023-01-01", "2023-01-01")


class TestAddWeatherCategories:
    """Tests for adding weather categories to DataFrame."""

    def test_add_categories(self):
        """Test adding weather category column."""
        df = pd.DataFrame({
            "weather_code": [0, 61, 71, 95],
            "temperature_2m": [20.0, 15.0, -2.0, 25.0],
        })

        result = add_weather_categories(df)

        assert "weather_category" in result.columns
        assert result.iloc[0]["weather_category"] == "Clear"
        assert result.iloc[1]["weather_category"] == "Rain"
        assert result.iloc[2]["weather_category"] == "Snow"
        assert result.iloc[3]["weather_category"] == "Thunderstorm"

    def test_add_categories_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            "weather_code": [0, 61],
            "temperature_2m": [20.0, 15.0],
        })

        result = add_weather_categories(df)

        assert "weather_category" not in df.columns
        assert "weather_category" in result.columns
