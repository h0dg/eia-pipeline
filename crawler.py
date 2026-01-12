import json
import urllib.parse, urllib.request
import re
import sqlite3

# Database setup
conn = sqlite3.connect('eia_data.sqlite')
cur = conn.cursor()

cur.execute('''
    CREATE TABLE IF NOT EXISTS raw_generation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period TEXT,
        plantCode TEXT,
        plantName TEXT,
        fuel2002 TEXT,
        fuelTypeDescription TEXT,
        state TEXT,
        stateDescription TEXT,
        primeMover TEXT,
        generation REAL,
        units TEXT,
        ingestionTimestamp TIMESTAMP,
        UNIQUE (period, plantCode, fuel2002)
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS crawl_metadata (
        pipeline TEXT PRIMARY KEY,
        lastOffset INTEGER,
        lastTimestamp TIMESTAMP
    )
''')

# Read API Key from file
text = open('API-key.txt').read()
apikey = re.findall(r'Key:\s*(\S*)', text)[0]

# Check to make sure valid API key is in file
if len(apikey) < 40 :
    print('Please add your API Key to API-key.txt')
    quit()

# Check if metadata exists for this pipeline
cur.execute("SELECT lastOffset FROM crawl_metadata WHERE pipeline = 'eia_generation'")
row = cur.fetchone()

if row is None :
    # Initialize metadata on first run
    offset = 0
    cur.execute('''INSERT INTO crawl_metadata (pipeline, lastOffset, lastTimestamp)
        VALUES ( ?, ?, CURRENT_TIMESTAMP )''', ('eia_generation', offset))
else :
    offset = row[0] # Get saved offset value
    #offset = 717000
baseurl = 'https://api.eia.gov/v2/electricity/facility-fuel/data'

print('Program started...\nTarget dataset:', baseurl)
if offset == 0 :
    print('Starting a new crawl...')
else :
    print(f'Resuming previous crawl from row {offset:,}')

def apicall(baseurl, offset, apikey) :
    params = {
        'frequency' : 'annual',
        'data[0]' : 'generation',
        #'facets[primeMover][]' : 'ALL',
        'offset' : offset,
        'api_key' : apikey
    }
    url = baseurl + '?' + urllib.parse.urlencode(params)
    try :
        handle = urllib.request.urlopen(url)
        if handle.getcode() != 200 :
            print('Error code=', handle.getcode(), url)
            return False
        data = handle.read().decode()
        try :
            js = json.loads(data)
        except :
            js = None
        return js
    except Exception as e :
        print('Unable to retrieve or parse page', url)
        print('Error', e)
        return False

# Crawl loop
ignored = 0
while True :
    try :
        page = apicall(baseurl, offset, apikey)
        if page is False or page is None : break
        totalRows = int(page['response']['total'])
        if len(page['response']['data']) == 0 :
            print('Reached last page of available data. Crawl successful.')
            offset = 0
            break
        else :
            entries = page['response']['data']
            for line in entries :
                if line['primeMover'] != 'ALL' : # filter out any granular primeMover entries
                    offset = offset + 1
                    continue
                period = line['period']
                plantCode = line['plantCode']
                plantName = line['plantName']
                fuel2002 = line['fuel2002']
                fuelTypeDescription = line['fuelTypeDescription']
                state = line['state']
                stateDescription = line['stateDescription']
                primeMover = line['primeMover']
                generation = line['generation']
                units = line['generation-units']
                cur.execute('''INSERT OR IGNORE INTO raw_generation
                    (period, plantCode, plantName, fuel2002, fuelTypeDescription, state, stateDescription, primeMover, generation, units, ingestionTimestamp)
                    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''', 
                    (period, plantCode, plantName, fuel2002, fuelTypeDescription, state, stateDescription, primeMover, generation, units)
                )
                if cur.rowcount == 0 :
                    ignored = ignored + 1
                else :
                    ignored = 0
                offset = offset + 1
                if offset % 50 == 0 : 
                    cur.execute("UPDATE crawl_metadata SET lastOffset = ?, lastTimestamp = CURRENT_TIMESTAMP WHERE pipeline = 'eia_generation'", (offset,))
                    conn.commit() # commit changes to DB every 50 rows
            print(f'Crawled through {offset:,} out of {totalRows:,} rows of data.')
            if ignored > 10000 :
                print(ignored, 'rows of duplicate data crawled. Rerun program when new data is available.')
                offset = 0
                break
    except KeyboardInterrupt :
        print('')
        print('Program interrupted by User...')
        break
cur.execute("UPDATE crawl_metadata SET lastOffset = ?, lastTimestamp = CURRENT_TIMESTAMP WHERE pipeline = 'eia_generation'", (offset,))
conn.commit()
cur.close()