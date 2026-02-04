# EIA Energy Data Pipeline

## Overview
This project ingests public electricity generation data from the U.S. Energy Information Administration (EIA) API, stores raw and cleaned data in local SQLite databases, and provides tools for aggregation, analysis, and visualization. The pipeline is designed for incremental updates, reproducibility, and consistent data handling.

## Motivation
The EIA datasets are updated frequently. This pipeline ensures that new data can be ingested without duplicating existing records and produces a clean, aggregated dataset suitable for analysis. Separating raw and transformed data helps maintain data integrity and enables consistent historical analyses.

## Pipeline Features
- **Incremental Data Ingestion**: Fetches data from the EIA API and resumes from the last saved offset.  
- **Raw Data Storage**: Stores all API responses in the `raw_generation` table with unique constraints to prevent duplication.  
- **Data Transformation**:  
  - Mapping tables for `states`, `units`, and `fuels`.  
  - Aggregates electricity generation into the `clean_generation` table keyed by `(year, state_code, fuel_code)`.  
  - Enforces unit consistency, foreign keys, and indexes for performance.  
- **Duplicate Handling**: Detects repeated rows during ingestion and stops if duplicates exceed a threshold.  
- **Error Handling**: Safely handles API errors and keyboard interrupts without corrupting the database.

### Scripts

- **main.py** -- Command-line interface to run the pipeline. Supports flags:  
  - `--ingest` -- Run only the data ingestion step.  
  - `--transform` -- Run only the transformation step.  
  - `--visualize` -- Run only the visualization step.
  - `--all` -- Run both ingestion and transformation steps.  
- **crawler.py** -- Handles fetching raw data from the EIA API, pagination, and duplicate detection.  
- **transform.py** -- Builds mapping tables (`states`, `units`, `fuels`) and aggregates raw data into `clean_generation`.  
- **visualize.py** -- Queries the clean database and generates visualizations of electricity generation trends.

### Database Schema

**raw_generation**
- Columns: `period`, `plantCode`, `plantName`, `fuel2002`, `fuelTypeDescription`, `state`, `stateDescription`, `primeMover`, `generation`, `units`, `ingestionTimestamp`  
- Unique constraint: `(period, plantCode, fuel2002)`

**crawl_metadata**
- Columns: `pipeline`, `lastOffset`, `lastTimestamp`  
- Primary key: `pipeline`

**clean_generation**
- Columns: `year`, `state_code`, `fuel_code`, `generation`, `units`, `updated_at`  
- Unique constraint: `(year, state_code, fuel_code)`  
- Foreign keys:  
  - `state_code` → `states(state_code)`  
  - `fuel_code` → `fuels(fuel_code)`  
  - `units` → `units(units_raw)`  
- Indexes: `year`, `(fuel_code, year)`, `state_code`

**Mapping Tables**
- `states`: Maps state codes to state descriptions.  
- `units`: Maps raw unit text to normalized units (e.g., `"megawatthours"` → `"MWh"`).  
- `fuels`: Maps fuel codes to human-readable fuel descriptions.

## Next Steps
- Expand visualization scripts with additional plots and analyses.  
- Add automated checks for new API data.  
- Implement unit tests for ingestion, transformation, and database functions.  
- Optionally integrate with interactive dashboards.

## Technologies
- Python 3  
- SQLite  
- EIA Open Data API  
- matplotlib, numpy, pyyaml, python-dotenv

## Usage

1. Create a `.env` file with your EIA API key:

  ```text
  EIA-API-KEY=<your_api_key>
  ```

2. Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```

3. Run the full pipeline:
  ```bash
  python3 -m src.main --all
  ```