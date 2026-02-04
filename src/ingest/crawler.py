import json
import urllib.parse, urllib.request
from src.db import Database
from src.config import API_KEY, EIA_CONFIG ,get_dataset_url

def setup_ingest():
    """
    Perform setup for the EIA data ingest pipeline. Returns a DB connection.

    - Validates that the API key is present.
    - Initializes the raw database and its tables.
    - Constructs the dataset base URL.

    :return: Tuple containing:
        - raw_db (db.Database): Initialized database object for raw data.
        - base_url (str): Full URL for the EIA dataset.
    :raises ValueError: If API key is missing or invalid.
    """
    if len(API_KEY) < 40:
        raise ValueError('API key missing in .env')
    
    raw_db = Database("raw")
    raw_db.initialize_raw_tables()
    base_url = get_dataset_url(EIA_CONFIG, 'facility-fuel')

    return raw_db, base_url

def fetch_page(baseurl, offset, apikey):
    """
    Fetch a single page of data from the EIA API. offset is used for pagination of the API.

    :param baseurl: str - The base URL of the dataset endpoint.
    :param offset: int - The row offset for pagination.
    :param apikey: str - Your EIA API key.
    :return: Tuple containing:
        - success (bool) - True if the request succeeded and data was parsed.
        - js (dict or None) - Parsed JSON response if successful, None otherwise.
    """
    params = {
        'frequency' : 'annual',
        'data[0]' : 'generation',
        'offset' : offset,
        'api_key' : apikey
    }
    url = baseurl + '?' + urllib.parse.urlencode(params)
    try :
        handle = urllib.request.urlopen(url)
        if handle.getcode() != 200 :
            print(f'Error code={handle.getcode()} at {url}')
            return False, None
        js = json.loads(handle.read().decode())
        return True, js
    except Exception as e :
        print(f'Error fetching page {url}:', e)
        return False, None

def process_page(page):
    """
    Extract relevant raw generation entries from an API response page.

    Filters out any rows where 'primeMover' is not 'ALL'.

    :param page: dict - JSON response from the EIA API for a single page.
    :return: List[dict] - Each dict represents a cleaned raw row with keys:
        'period', 'plantCode', 'plantName', 'fuel2002', 'fuelTypeDescription',
        'state', 'stateDescription', 'primeMover', 'generation', 'units'.
    """
    pulled_data = []
    for line in page['response']['data']:
        if line['primeMover'] != 'ALL':
            continue
        entry = {
            'period': line['period'],
            'plantCode': line['plantCode'],
            'plantName': line['plantName'],
            'fuel2002': line['fuel2002'],
            'fuelTypeDescription': line['fuelTypeDescription'],
            'state': line['state'],
            'stateDescription': line['stateDescription'],
            'primeMover': line['primeMover'],
            'generation': line['generation'],
            'units': line['generation-units']
        }
        pulled_data.append(entry)
    return pulled_data

def update_pipeline_offset(db, pipeline, offset):
    """
    Update the last processed row offset for a specific pipeline in the database. This is to allow crawl to resume if interrupted.

    :param db: db.Database - Database instance for raw data.
    :param pipeline: str - Name of the pipeline (e.g., 'eia_generation').
    :param offset: int - The current row offset to store.
    :return: None
    """
    db.update_metadata(pipeline, offset)
    print(f'Updated {pipeline} offset to {offset}')

def crawl_eia_dataset(baseurl, db, api_key, batch_size=50, max_duplicates=10000):
    """
    Crawl the EIA dataset from the API and store results in the raw database.

    Handles pagination, duplicate detection, and metadata updates.

    :param baseurl: str - Full URL to the dataset endpoint.
    :param db: db.Database - Initialized raw database instance.
    :param api_key: str - Your EIA API key.
    :param batch_size: int, optional - Number of rows between metadata updates (default 50).
    :param max_duplicates: int, optional - Maximum allowed duplicate rows before stopping (default 10000).
    :return: None
    """
    offset = db.load_metadata('eia_generation')
    if offset == 0 :
        print('Starting a new crawl...')
    else :
        print(f'Resuming previous crawl from row {offset:,}')
    ignored_rows = 0
    try:
        while True:
            # Fetch a page of data
            success, page = fetch_page(baseurl, offset, api_key)
            if not success or not page:
                break
            # Total rows in API dataset
            totalRows = int(page['response']['total'])

            # If no data, we've reached the end
            if not page['response']['data']:
                print('Reached last page of available data. Crawl successful.')
                offset = 0
                break

            # Process page and filter relevant rows    
            pulled_data = process_page(page)
            
            # Save to DB and count duplicates
            if pulled_data:
                new_rows = db.save_raw_data(pulled_data)
                ignored_rows += len(pulled_data) - new_rows

            # Update offset for next page
            offset += len(page['response']['data'])

            # Log process
            print(f'Crawled through {offset:,} out of {totalRows:,} rows of data.')

            # Periodically save progress
            if offset % batch_size == 0:
                update_pipeline_offset(db, 'eia_generation', offset)

            # Stop crawl if too many duplicate rows because we're crawling old data.
            if ignored_rows > max_duplicates:
                print(f'{ignored_rows} rows of duplicate data crawled. Rerun program when new data is available.')
                offset = 0
                break

    except KeyboardInterrupt:
        print('\nProgram interrupted by User...')

    finally:
        update_pipeline_offset(db, 'eia_generation', offset)
        db.close()