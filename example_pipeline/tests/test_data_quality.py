"""
Tests for data quality expectations and validation.

These tests verify that our data quality checks work correctly
and that the transformations handle edge cases appropriately.

Note: These tests load fixture JSON directly (not via Spark) since
they're validating the fixture data itself, not Spark transformations.
"""

import json
import pathlib

import pytest


def load_json_fixture(filename: str) -> list[dict]:
    """Load a JSON fixture file directly as Python dicts."""
    path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
    return json.loads(path.read_text())


class TestWeatherDataQuality:
    """Tests for weather data quality checks."""

    def test_temperature_reasonable_range(self):
        """Test that temperatures are within reasonable range."""
        weather_data = load_json_fixture("sample_weather.json")

        for record in weather_data:
            temp = record.get("temperature_2m")
            if temp is not None:
                # NYC temperatures should be between -50 and 50 Celsius
                assert -50 <= temp <= 50, f"Temperature {temp} out of range"

    def test_humidity_valid_range(self):
        """Test that humidity values are valid percentages."""
        weather_data = load_json_fixture("sample_weather.json")

        for record in weather_data:
            humidity = record.get("relative_humidity_2m")
            if humidity is not None:
                assert 0 <= humidity <= 100, f"Humidity {humidity} out of range"

    def test_precipitation_non_negative(self):
        """Test that precipitation values are non-negative."""
        weather_data = load_json_fixture("sample_weather.json")

        for record in weather_data:
            precip = record.get("precipitation")
            if precip is not None:
                assert precip >= 0, f"Precipitation {precip} is negative"

    def test_weather_category_present(self):
        """Test that weather category is present for all records."""
        weather_data = load_json_fixture("sample_weather.json")

        for record in weather_data:
            assert "weather_category" in record
            assert record["weather_category"] is not None
            assert len(record["weather_category"]) > 0

    def test_is_raining_consistent_with_rain(self):
        """Test that is_raining flag is consistent with rain amount."""
        weather_data = load_json_fixture("sample_weather.json")

        for record in weather_data:
            rain = record.get("rain", 0)
            is_raining = record.get("is_raining", False)

            if rain > 0:
                assert is_raining is True, "is_raining should be True when rain > 0"
            else:
                assert is_raining is False, "is_raining should be False when rain = 0"


class TestTripDataQuality:
    """Tests for taxi trip data quality checks."""

    def test_fare_non_negative(self):
        """Test that fares are non-negative."""
        trips_data = load_json_fixture("sample_trips.json")

        for trip in trips_data:
            fare = trip.get("fare_amount")
            assert fare >= 0, f"Fare {fare} is negative"

    def test_trip_distance_non_negative(self):
        """Test that trip distances are non-negative."""
        trips_data = load_json_fixture("sample_trips.json")

        for trip in trips_data:
            distance = trip.get("trip_distance")
            assert distance >= 0, f"Distance {distance} is negative"

    def test_pickup_before_dropoff(self):
        """Test that pickup time is before dropoff time."""
        trips_data = load_json_fixture("sample_trips.json")

        for trip in trips_data:
            pickup = trip.get("pickup_datetime")
            dropoff = trip.get("dropoff_datetime")
            assert pickup < dropoff, f"Pickup {pickup} is after dropoff {dropoff}"

    def test_required_fields_present(self):
        """Test that all required fields are present."""
        trips_data = load_json_fixture("sample_trips.json")
        required_fields = [
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_zip",
            "dropoff_zip",
            "trip_distance",
            "fare_amount",
        ]

        for trip in trips_data:
            for field in required_fields:
                assert field in trip, f"Missing required field: {field}"
                assert trip[field] is not None, f"Field {field} is null"


class TestJoinDataQuality:
    """Tests for weather-taxi join data quality."""

    def test_join_coverage(self):
        """Test that trips can be matched to weather data."""
        trips_data = load_json_fixture("sample_trips.json")
        weather_data = load_json_fixture("sample_weather.json")

        # Extract unique date-hours from weather data
        weather_hours = set()
        for w in weather_data:
            weather_hours.add((w["date"], w["hour"]))

        # Check that trips have matching weather
        matched_count = 0
        for trip in trips_data:
            # Extract date and hour from pickup_datetime
            dt = trip["pickup_datetime"]
            date = dt.split("T")[0]
            hour = int(dt.split("T")[1].split(":")[0])

            if (date, hour) in weather_hours:
                matched_count += 1

        # At least some trips should match
        match_rate = matched_count / len(trips_data)
        assert match_rate > 0, "No trips matched to weather data"
        print(f"Join match rate: {match_rate:.1%}")
