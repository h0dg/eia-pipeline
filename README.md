# EIA Energy Data Pipeline

## Overview
This project ingests public electricity generation data from the U.S. Energy Information Administration (EIA) API, stores raw and cleaned data in a local SQLite database, and prepares it for analysis and visualization. The goal is to provide a reliable, incremental pipeline for exploring electricity generation trends by fuel type and state over time.

## Motivation
The EIA dataset is updated frequently. This project is designed to incrementally ingest new data, avoid duplication, and produce a clean, aggregated dataset suitable for analysis. By separating raw and transformed data, the pipeline ensures consistency and reproducibility.

## Current Progress
- **Data Crawling**: Successfully implemented a crawler to fetch data from the EIA API, handle pagination, and resume from the last saved offset.  
- **Raw Storage**: All API responses are stored in a `raw_generation` table in SQLite with unique constraints to prevent duplicates.  
- **Data Transformation**:  
  - Mapping tables created for `States`, `Units`, and `Fuels`.  
  - Aggregated electricity generation stored in a `clean_generation` table, keyed by `(year, state_code, fuel_code)`.  
  - Foreign keys and indexes added to improve query performance.  
- **Duplicate Handling**: The crawler detects duplicate rows and stops crawling once a threshold of repeated entries is reached.  
- **Error Handling**: API errors and failed requests are caught, allowing the crawler to safely exit without corrupting the database.

## Pipeline Structure

### Scripts
- **crawler.py** – Fetches raw data from the EIA API and inserts it into `raw_generation`. Supports resuming via offset stored in `CrawlMetadata`.  
- **transform.py** – Builds mapping tables (`States`, `Units`, `Fuels`) and aggregates raw data into `clean_generation`. Handles unit consistency and duplicate detection.  
- **visualize.py** (planned) – Will query `clean_generation` to perform analysis and generate visualizations.

### Database Schema

**raw_generation**
- Columns: `period`, `plantCode`, `plantName`, `fuel2002`, `fuelTypeDescription`, `state`, `stateDescription`, `primeMover`, `generation`, `units`, `ingestionTimestamp`  
- Unique constraint: `(period, plantCode, fuel2002)`  

**CrawlMetadata**
- Columns: `pipeline`, `lastOffset`, `lastTimestamp`  
- Primary key: `pipeline`  

**clean_generation**
- Columns: `year`, `state_code`, `fuel_code`, `generation`, `units`, `updated_at`  
- Unique constraint: `(year, state_code, fuel_code)`  
- Foreign keys: `state_code` → `States(state_code)`, `fuel_code` → `Fuels(fuel_code)`, `units` → `Units(units_raw)`  
- Indexes: on `year`, `(fuel_code, year)`, and `state_code`  

**Mapping Tables**
- `States`: maps state codes to state descriptions  
- `Units`: maps raw unit text to normalized units (e.g., "megawatthours" → "MWh")  
- `Fuels`: maps fuel codes to human-readable fuel descriptions  

## Next Steps
- Complete analysis and visualization scripts using the `clean_generation` table.  
- Explore trends in electricity generation by fuel type and state.  
- Potential enhancements: automated alerts for new API data, additional data quality checks, or integration with plotting libraries for interactive dashboards.

## Technologies
- Python  
- SQLite  
- EIA Open Data API
