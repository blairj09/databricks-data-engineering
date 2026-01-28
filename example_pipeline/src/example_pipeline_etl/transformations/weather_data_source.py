"""
Declarative pipeline transformation for ingesting weather data.

This table serves as the bronze/raw layer for weather data,
fetching historical weather from the Open-Meteo API.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    FloatType,
    IntegerType,
    DateType,
    BooleanType,
    StringType,
)


# Schema for weather data
WEATHER_SCHEMA = StructType([
    StructField("datetime", TimestampType(), False),
    StructField("temperature_2m", FloatType(), True),
    StructField("relative_humidity_2m", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("rain", FloatType(), True),
    StructField("snowfall", FloatType(), True),
    StructField("wind_speed_10m", FloatType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("hour", IntegerType(), True),
    StructField("is_raining", BooleanType(), True),
    StructField("is_snowing", BooleanType(), True),
])


@dp.table(
    comment="Hourly weather data for NYC from Open-Meteo Historical API",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "datetime",
    },
)
@dp.expect("valid_datetime", "datetime IS NOT NULL")
@dp.expect("reasonable_temperature", "temperature_2m BETWEEN -50 AND 50")
@dp.expect("valid_humidity", "relative_humidity_2m BETWEEN 0 AND 100")
def weather_hourly():
    """
    Ingests hourly weather data for NYC.

    Data is fetched from Open-Meteo and includes:
    - Temperature, humidity, precipitation
    - Rain and snowfall amounts
    - Wind speed and weather codes

    For the demo, this reads from a pre-loaded table.
    In production, this would use a streaming source or scheduled refresh.
    """
    # For the demo, we read from a pre-populated table
    # In production, you might use:
    # - A streaming source (Auto Loader from cloud storage)
    # - A scheduled Python script that calls the API
    # - An external ingestion tool

    # Read from staged weather data (pre-loaded for demo reliability)
    return (
        spark.table("weather_raw")
        .select(
            F.col("datetime").cast(TimestampType()),
            F.col("temperature_2m").cast(FloatType()),
            F.col("relative_humidity_2m").cast(FloatType()),
            F.col("precipitation").cast(FloatType()),
            F.col("rain").cast(FloatType()),
            F.col("snowfall").cast(FloatType()),
            F.col("wind_speed_10m").cast(FloatType()),
            F.col("weather_code").cast(IntegerType()),
            F.to_date("datetime").alias("date"),
            F.hour("datetime").alias("hour"),
            (F.col("rain") > 0).alias("is_raining"),
            (F.col("snowfall") > 0).alias("is_snowing"),
        )
        .withColumn(
            "weather_category",
            F.when(F.col("weather_code") == 0, "Clear")
            .when(F.col("weather_code").isin(1, 2, 3), "Partly Cloudy")
            .when(F.col("weather_code").isin(45, 48), "Foggy")
            .when(F.col("weather_code").isin(51, 53, 55, 56, 57), "Drizzle")
            .when(F.col("weather_code").isin(61, 63, 65, 66, 67), "Rain")
            .when(F.col("weather_code").isin(71, 73, 75, 77), "Snow")
            .when(F.col("weather_code").isin(80, 81, 82), "Rain Showers")
            .when(F.col("weather_code").isin(85, 86), "Snow Showers")
            .when(F.col("weather_code").isin(95, 96, 99), "Thunderstorm")
            .otherwise("Other"),
        )
    )
