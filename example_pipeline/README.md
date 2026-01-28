# example_pipeline

A demonstration project showing Data Engineering best practices with Positron and Databricks. This project integrates NYC taxi trip data with historical weather data to analyze how weather conditions impact taxi operations.

## Use Case: Weather Impact on NYC Taxi Operations

**Business Question**: How does weather affect taxi ridership and fares in NYC?

This pipeline:
1. Fetches historical weather data from the [Open-Meteo API](https://open-meteo.com/)
2. Joins weather data with NYC taxi trips by pickup datetime
3. Produces aggregated metrics showing weather impact on taxi operations

## Project Structure

* `src/example_pipeline/`: Shared Python code
  * `weather.py` - Open-Meteo API client for fetching weather data
  * `weather_cli.py` - CLI entry point for the weather fetch job
  * `taxis.py` - Taxi data utilities
* `src/example_pipeline_etl/transformations/`: Lakeflow Declarative Pipelines transformations
  * `weather_data_source.py` - Bronze layer: raw weather data
  * `weather_taxi_join.py` - Silver layer: trips enriched with weather
  * `weather_impact_metrics.py` - Gold layer: aggregated analytics
* `resources/`: Databricks Asset Bundle configurations
  * `example_pipeline_etl.pipeline.yml` - Declarative pipeline definition
  * `weather_integration.job.yml` - Job to orchestrate weather fetch + pipeline
* `tests/`: Unit tests with Databricks Connect
* `fixtures/`: Sample data for testing


## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

*  Make sure you have the UV package manager installed.
   It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
*  Run `uv sync --dev` to install the project's dependencies.


# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a pipeline called
    `[dev yourname] example_pipeline_etl` to your workspace.
    You can find that resource by opening your workspace and clicking on **Jobs & Pipelines**.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```
   Note this includes a job that runs the weather integration pipeline daily
   (defined in resources/weather_integration.job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

5. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```
