# Data Engineering with Positron + Databricks

A webinar demonstration showing how to use Positron as an IDE for building production Data Engineering pipelines with Databricks.

## Overview

This repository accompanies a webinar that highlights:

1. **Positron** as a professional IDE for data work (vs notebook-centric development)
2. **Databricks Asset Bundles (DABs)** for infrastructure-as-code deployments
3. **Lakeflow Declarative Pipelines** for declarative pipeline development
4. **AI-assisted development** with Positron Assistant

## The Use Case

**Business Question**: How does weather impact NYC taxi operations?

We build a data pipeline that:
- Fetches historical weather data from the [Open-Meteo API](https://open-meteo.com/)
- Joins weather conditions with NYC taxi trip data
- Produces analytics showing weather impact on ridership and fares

## Repository Structure

```
├── slides/                    # Quarto presentation
│   └── webinar.qmd           # reveal.js slide deck
├── example_pipeline/          # Databricks Asset Bundle project
│   ├── databricks.yml        # DAB configuration
│   ├── resources/            # Job and pipeline definitions
│   ├── src/
│   │   ├── example_pipeline/         # Python modules
│   │   │   ├── weather.py           # Open-Meteo API client
│   │   │   └── weather_cli.py       # CLI for weather fetch job
│   │   └── example_pipeline_etl/    # Declarative pipeline transformations
│   │       └── transformations/
│   │           ├── weather_data_source.py    # Bronze: raw weather
│   │           ├── weather_taxi_join.py      # Silver: enriched trips
│   │           └── weather_impact_metrics.py # Gold: aggregations
│   ├── tests/                # pytest test suite
│   └── fixtures/             # Sample data for testing
└── .beads/                   # Issue tracking (bd)
```

## Getting Started

### Prerequisites

- [Positron](https://positron.posit.co/) or VS Code with Databricks extension
- [uv](https://docs.astral.sh/uv/) package manager
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
- Access to a Databricks workspace

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/blairj09/databricks-data-engineering.git
   cd databricks-data-engineering
   ```

2. Install dependencies:
   ```bash
   cd example_pipeline
   uv sync --dev
   ```

3. Configure Databricks authentication:
   ```bash
   databricks configure
   ```

4. Deploy to your workspace:
   ```bash
   databricks bundle deploy --target dev
   ```

### Running Tests

```bash
cd example_pipeline
uv run pytest
```

### Viewing the Slides

```bash
cd slides
quarto preview webinar.qmd
```

## Key Concepts Demonstrated

### Why IDE > Notebooks for Data Engineering

| Challenge | IDE Solution |
|-----------|--------------|
| Meaningless git diffs | Standard Python files with clean diffs |
| No code review possible | PR-based workflows with readable changes |
| Can't unit test | pytest with Databricks Connect |
| Copy-paste code | Proper imports and shared modules |
| Production debugging | Breakpoints and stack traces |

### Lakeflow Declarative Pipelines

Declarative pipeline definitions with built-in:
- Dependency management
- Data quality expectations
- Incremental processing
- Schema evolution

### Databricks Asset Bundles (DABs)

Infrastructure-as-code for Databricks:
- Version-controlled job/pipeline definitions
- Environment-specific deployments (dev/prod)
- Repeatable, automated deployments

## Resources

- [Positron](https://positron.posit.co/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
- [Open-Meteo API](https://open-meteo.com/)

## License

MIT
