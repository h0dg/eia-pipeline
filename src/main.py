import argparse

from src.ingest.crawler import setup_ingest, crawl_eia_dataset
from src.transform.clean import (
    setup_transform,
    build_state_mapping,
    build_units_mapping,
    build_fuels_mapping,
    aggregate_generation,
)
from src.config import API_KEY


def run_ingest():
    raw_db, base_url = setup_ingest()
    crawl_eia_dataset(base_url, raw_db, API_KEY)


def run_transform():
    raw_db, clean_db = setup_transform()  # unpack tuple properly

    print('Generating mapping tables...')
    build_state_mapping(raw_db, clean_db)
    build_units_mapping(raw_db, clean_db)
    build_fuels_mapping(raw_db, clean_db)
    print('Mapping completed successfully.')

    print('Aggregating raw data into usable table...')
    aggregate_generation(raw_db, clean_db)
    print('Data aggregated successfully.')

    raw_db.close()
    clean_db.close()


def main():
    parser = argparse.ArgumentParser(description="EIA Generation Data Pipeline")

    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Pull raw data from the EIA API"
    )

    parser.add_argument(
        "--transform",
        action="store_true",
        help="Transform raw data into clean tables"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run ingest and transform steps"
    )

    args = parser.parse_args()

    if not (args.ingest or args.transform or args.all):
        parser.print_help()
        return

    if args.all or args.ingest:
        print("\n--- INGEST STEP ---")
        run_ingest()

    if args.all or args.transform:
        print("\n--- TRANSFORM STEP ---")
        run_transform()


if __name__ == "__main__":
    main()
