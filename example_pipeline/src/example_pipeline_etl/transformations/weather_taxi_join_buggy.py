"""
DEMO FILE: Buggy version of the weather-taxi join transformation.

This file intentionally contains a bug for demonstration purposes.
The bug: Timezone mismatch between weather data (America/New_York)
and taxi data (UTC) causes the join to produce mostly NULL weather values.

DO NOT USE IN PRODUCTION - this is for the debugging demo only.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# @dp.table(
#     comment="[BUGGY] Taxi trips with weather - DO NOT USE",
#     table_properties={"quality": "silver"},
# )
# @dp.expect_or_fail("valid_pickup_datetime", "pickup_datetime IS NOT NULL")
def taxi_weather_joined_buggy():
    """
    BUGGY VERSION - Joins taxi trips with weather data.

    BUG: The taxi data timestamps are in UTC, but the weather data
    timestamps are in America/New_York timezone. This causes a
    ~5 hour offset, resulting in most joins returning NULL weather.

    The fix: Convert taxi pickup_datetime to America/New_York timezone
    before extracting date and hour for the join.
    """
    # Read taxi trips
    trips = spark.table("sample_trips_example_pipeline")

    # Read weather data
    weather = spark.table("weather_hourly")

    # BUG: We're using UTC timestamps from taxi data directly
    # but weather data is in America/New_York timezone!
    # This causes a timezone mismatch in the join.
    trips_with_key = trips.withColumn(
        "pickup_date", F.to_date("pickup_datetime")  # Using UTC date
    ).withColumn(
        "pickup_hour", F.hour("pickup_datetime")  # Using UTC hour
    )

    weather_with_key = weather.select(
        F.col("date").alias("weather_date"),  # This is in America/New_York
        F.col("hour").alias("weather_hour"),  # This is in America/New_York
        "temperature_2m",
        "weather_category",
    )

    # The join will fail to match correctly due to timezone offset
    # A trip at 10:00 UTC is actually 5:00 or 6:00 in NYC (depending on DST)
    # So we'll try to match with 10:00 NYC weather, which is wrong
    joined = trips_with_key.join(
        weather_with_key,
        (trips_with_key.pickup_date == weather_with_key.weather_date)
        & (trips_with_key.pickup_hour == weather_with_key.weather_hour),
        "left",
    )

    return joined.select(
        "pickup_datetime",
        "fare_amount",
        "temperature_2m",  # Will be NULL for many rows due to timezone bug
        "weather_category",  # Will be NULL for many rows
    )


# THE FIX: Use this corrected version
def taxi_weather_joined_fixed():
    """
    FIXED VERSION - Properly handles timezone conversion.

    The fix: Convert taxi timestamps to America/New_York timezone
    before extracting date/hour for the join.
    """
    trips = spark.table("sample_trips_example_pipeline")
    weather = spark.table("weather_hourly")

    # FIX: Convert UTC to America/New_York before extracting date/hour
    trips_with_key = trips.withColumn(
        "pickup_datetime_nyc",
        F.from_utc_timestamp("pickup_datetime", "America/New_York")
    ).withColumn(
        "pickup_date", F.to_date("pickup_datetime_nyc")
    ).withColumn(
        "pickup_hour", F.hour("pickup_datetime_nyc")
    )

    weather_with_key = weather.select(
        F.col("date").alias("weather_date"),
        F.col("hour").alias("weather_hour"),
        "temperature_2m",
        "weather_category",
    )

    joined = trips_with_key.join(
        weather_with_key,
        (trips_with_key.pickup_date == weather_with_key.weather_date)
        & (trips_with_key.pickup_hour == weather_with_key.weather_hour),
        "left",
    )

    return joined.select(
        "pickup_datetime",
        "fare_amount",
        "temperature_2m",
        "weather_category",
    )


# Debugging helper function - use this to diagnose the issue
def diagnose_timezone_issue():
    """
    Helper function to diagnose the timezone mismatch.

    Run this interactively to see the problem:
    - Shows sample timestamps from both sources
    - Reveals the offset between UTC and NYC time
    """
    trips = spark.table("sample_trips_example_pipeline")
    weather = spark.table("weather_hourly")

    print("=== Sample Taxi Timestamps (UTC) ===")
    trips.select(
        "pickup_datetime",
        F.to_date("pickup_datetime").alias("date_utc"),
        F.hour("pickup_datetime").alias("hour_utc"),
    ).show(5)

    print("=== Sample Weather Timestamps (NYC) ===")
    weather.select("datetime", "date", "hour").show(5)

    print("=== The Problem ===")
    print("Taxi timestamps are in UTC, weather timestamps are in NYC timezone.")
    print("A 10:00 UTC trip should match with 5:00 or 6:00 NYC weather,")
    print("but we're incorrectly matching it with 10:00 NYC weather.")
