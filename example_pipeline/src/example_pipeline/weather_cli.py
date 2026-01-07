"""
CLI entry point for fetching weather data and loading to Databricks.

This script is used by the weather_integration job to:
1. Fetch historical weather data from Open-Meteo API
2. Write it to a Delta table in the specified catalog/schema
"""

import argparse
from databricks.connect import DatabricksSession

from example_pipeline.weather import fetch_weather_for_date_range, add_weather_categories


def main():
    """Main entry point for weather data fetch job."""
    parser = argparse.ArgumentParser(description="Fetch weather data for NYC")
    parser.add_argument("--catalog", required=True, help="Target catalog")
    parser.add_argument("--schema", required=True, help="Target schema")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--table-name", default="weather_raw", help="Target table name")

    args = parser.parse_args()

    # Initialize Spark session
    spark = DatabricksSession.builder.getOrCreate()

    # Set catalog and schema
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    print(f"Fetching weather data from {args.start_date} to {args.end_date}")

    # Fetch weather data
    weather_df = fetch_weather_for_date_range(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    # Add weather categories
    weather_df = add_weather_categories(weather_df)

    print(f"Fetched {len(weather_df)} weather records")

    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(weather_df)

    # Write to Delta table
    full_table_name = f"{args.catalog}.{args.schema}.{args.table_name}"
    print(f"Writing to {full_table_name}")

    spark_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    print(f"Successfully wrote weather data to {full_table_name}")

    # Show sample
    spark.table(full_table_name).show(5)


if __name__ == "__main__":
    main()
