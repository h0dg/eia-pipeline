# EIA Electricity Generation Pipeline

A Python command-line tool that downloads annual, plant-level U.S. electricity generation data from the EIA Open Data API into SQLite, totals it by year, state, and fuel, and plots the top 10 fuels for a year you choose.

## How it works

The pipeline has three steps. Each can be run on its own.

### 1. Ingest (`src/ingest/crawler.py`)

- Requests the EIA `electricity/facility-fuel` dataset (annual frequency, generation values) one page at a time, using the API's `offset` parameter.
- Keeps only rows where `primeMover` is `ALL`.
- Saves rows to the `raw_generation` table in `data/raw_gen_data.sqlite`. A unique constraint on `(period, plantCode, fuel2002)` means rows that are already stored are skipped.
- Saves its current offset to `crawl_metadata` as it goes and again when the program exits, including after Ctrl-C or a failed request. The next run continues from that offset.
- When a crawl reaches the end of the data, or stops because of duplicates, the saved offset is reset to 0. The next run then starts from the beginning and skips rows it already has.
- Stops once more than 10,000 duplicate rows have been seen in a single run (the `max_duplicates` default), on the assumption that it is re-reading data it already has.

### 2. Transform (`src/transform/clean.py`)

- Builds the `states`, `fuels`, and `units` lookup tables from the raw data. Existing lookup entries are not overwritten.
- Sums generation by year, state, and fuel code across all plants and writes the result to `clean_generation` in `data/clean_gen_data.sqlite`. If a row for that year, state, and fuel already exists, its generation value is updated.
- Converts the unit `megawatthours` to `MWh`. This is the only unit it handles. Any other unit, or mixed units within one group, raises an error.
- Cleans fuel descriptions (title case, `And` becomes `&`) and fixes one misspelling found in the source data ("Municiapl").

### 3. Visualize (`src/analysis/visualize.py`)

- Prompts for a year within the range found in the clean database.
- Prints the top 10 fuels by total generation for that year and shows them as a bar chart (matplotlib). The `ALL` fuel code is left out of the ranking.

`src/main.py` is the command-line entry point. Database paths, table names, and the EIA dataset are set in `config.yaml`. SQLite foreign key enforcement is turned on for every database connection.

## Setup

You need Python 3 and a free EIA API key (https://www.eia.gov/opendata/).

1. Create a `.env` file in the project root:

   ```
   EIA-API-KEY=<your_api_key>
   ```

2. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

## Usage

Run from the project root:

```
python3 -m src.main --ingest      # download records from the EIA API
python3 -m src.main --transform   # build lookup tables and the clean dataset
python3 -m src.main --visualize   # choose a year and plot the top 10 fuels
python3 -m src.main --all         # ingest, transform, then visualize
```

`--visualize` (and so `--all`) stops to ask for a year and opens a chart window.

## Database tables

**Raw database** (`data/raw_gen_data.sqlite`)

- `raw_generation`: EIA records (period, plant code and name, fuel, state, prime mover, generation, units, ingestion timestamp). Unique on `(period, plantCode, fuel2002)`.
- `crawl_metadata`: the last offset and timestamp for each pipeline, used to resume ingestion.

**Clean database** (`data/clean_gen_data.sqlite`)

- `clean_generation`: generation summed by `(year, state_code, fuel_code)`, with units and an update timestamp. Foreign keys point to the three lookup tables.
- `states`, `fuels`, `units`: lookup tables.

## Limitations

- Only the EIA `facility-fuel` dataset is configured, at annual frequency, and only rows with `primeMover` of `ALL` are kept.
- A failed API request stops the crawl. There are no retries or rate-limit handling, so rerun to continue.
- The transform step supports only the `megawatthours` unit.
- Ingestion is run manually. There is no scheduler.
- The visualization step produces one chart type and needs a display.

## Possible next steps

- Add retries for failed API requests.
- Support more EIA datasets and units.
- Add more charts, and a way to pass the year as a command-line argument.

## Built with

Python 3 (standard-library `urllib` for API calls), SQLite, matplotlib, numpy, PyYAML, python-dotenv.
