"""
Declarative pipeline transformation for joining weather data with taxi trips.

This table creates the silver layer by combining taxi trip data
with weather conditions at the time of pickup.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    comment="Taxi trips enriched with weather conditions at pickup time",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "tpep_pickup_datetime",
    },
)
@dp.expect_or_fail("valid_pickup_datetime", "tpep_pickup_datetime IS NOT NULL")
@dp.expect_or_drop("valid_fare", "fare_amount >= 0")
@dp.expect("has_weather_data", "temperature_2m IS NOT NULL")
@dp.expect("reasonable_temp", "temperature_2m BETWEEN -50 AND 50")
def taxi_weather_joined():
    """
    Joins taxi trips with weather data based on pickup datetime.

    The join matches trips to the weather observation from the same hour,
    enabling analysis of how weather conditions affect taxi operations.

    Includes data quality expectations:
    - pickup_datetime must not be null (fail if violated)
    - fare_amount must be non-negative (fail if violated)
    - temperature should be present (warning if missing)
    - temperature should be reasonable (warning if out of range)
    """
    # Read taxi trips
    trips = spark.table("sample_trips_example_pipeline")

    # Read weather data
    weather = spark.table("weather_hourly")

    # Create join key: date + hour
    trips_with_key = trips.withColumn(
        "pickup_date", F.to_date("tpep_pickup_datetime")
    ).withColumn(
        "pickup_hour", F.hour("tpep_pickup_datetime")
    )

    weather_with_key = weather.select(
        F.col("date").alias("weather_date"),
        F.col("hour").alias("weather_hour"),
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "rain",
        "snowfall",
        "wind_speed_10m",
        "weather_code",
        "weather_category",
        "is_raining",
        "is_snowing",
    )

    # Join on date and hour
    joined = trips_with_key.join(
        weather_with_key,
        (trips_with_key.pickup_date == weather_with_key.weather_date)
        & (trips_with_key.pickup_hour == weather_with_key.weather_hour),
        "left",
    )

    # Select final columns
    return joined.select(
        # Trip columns
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_zip",
        "dropoff_zip",
        "trip_distance",
        "fare_amount",
        # Weather columns
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "rain",
        "snowfall",
        "wind_speed_10m",
        "weather_code",
        "weather_category",
        "is_raining",
        "is_snowing",
        # Derived columns
        "pickup_date",
        "pickup_hour",
    )
