"""
DLT transformation for weather impact analysis metrics.

This table creates the gold layer with aggregated metrics
showing how weather conditions affect taxi operations.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    comment="Aggregated metrics showing weather impact on taxi operations",
    table_properties={
        "quality": "gold",
    },
)
def weather_impact_summary():
    """
    Aggregates taxi trip data by weather category to analyze impact.

    Produces metrics including:
    - Average fare by weather condition
    - Trip count by weather condition
    - Average trip distance by weather condition

    This enables questions like:
    - Do rainy days have higher fares?
    - Does snow reduce trip volume?
    - How does temperature affect ridership?
    """
    trips = spark.table("taxi_weather_joined")

    return trips.groupBy("weather_category").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("temperature_2m"), 1).alias("avg_temperature"),
        F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
    ).orderBy(F.desc("trip_count"))


@dp.table(
    comment="Hourly taxi metrics with weather conditions",
    table_properties={
        "quality": "gold",
    },
)
def hourly_weather_metrics():
    """
    Aggregates taxi trips by hour and weather condition.

    Useful for time-series analysis of weather impact,
    identifying patterns like:
    - Rush hour performance in rain vs clear weather
    - Overnight demand during snow events
    """
    trips = spark.table("taxi_weather_joined")

    return trips.groupBy(
        "pickup_date",
        "pickup_hour",
        "weather_category",
        "temperature_2m",
        "is_raining",
        "is_snowing",
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    ).orderBy("pickup_date", "pickup_hour")


@dp.table(
    comment="Temperature band analysis for taxi operations",
    table_properties={
        "quality": "gold",
    },
)
def temperature_band_analysis():
    """
    Analyzes taxi operations by temperature bands.

    Groups temperatures into meaningful bands:
    - Freezing (< 0°C)
    - Cold (0-10°C)
    - Cool (10-18°C)
    - Comfortable (18-25°C)
    - Warm (25-30°C)
    - Hot (> 30°C)
    """
    trips = spark.table("taxi_weather_joined")

    return trips.withColumn(
        "temperature_band",
        F.when(F.col("temperature_2m") < 0, "Freezing (<0°C)")
        .when(F.col("temperature_2m") < 10, "Cold (0-10°C)")
        .when(F.col("temperature_2m") < 18, "Cool (10-18°C)")
        .when(F.col("temperature_2m") < 25, "Comfortable (18-25°C)")
        .when(F.col("temperature_2m") < 30, "Warm (25-30°C)")
        .otherwise("Hot (>30°C)"),
    ).groupBy("temperature_band").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.min("temperature_2m"), 1).alias("min_temp"),
        F.round(F.max("temperature_2m"), 1).alias("max_temp"),
    ).orderBy("min_temp")
