import sqlite3

conn = sqlite3.connect('eia_data.sqlite')
cur = conn.cursor()

# ----- Create tables ----------

cur.execute('DROP TABLE IF EXISTS States')
cur.execute('DROP TABLE IF EXISTS Units')
cur.execute('DROP TABLE IF EXISTS Fuels')
cur.execute('DROP TABLE IF EXISTS clean_generation')

# Enable FKs in SQLite
cur.execute('PRAGMA foreign_keys = ON')

cur.execute('''CREATE TABLE IF NOT EXISTS States (
    state_code TEXT PRIMARY KEY NOT NULL,
    state_desc TEXT)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Units (
    units_raw TEXT PRIMARY KEY,
    units_clean TEXT)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Fuels (
    fuel_code TEXT PRIMARY KEY,
    fuel_desc TEXT)''')

cur.execute('''CREATE TABLE IF NOT EXISTS clean_generation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER,
    state_code TEXT,
    fuel_code TEXT,
    generation REAL,
    units TEXT,
    updated_at TIMESTAMP,
    UNIQUE (year, state_code, fuel_code),
    FOREIGN KEY (state_code) REFERENCES States(state_code),
    FOREIGN KEY (fuel_code) REFERENCES Fuels(fuel_code),
    FOREIGN KEY(units) REFERENCES Units(units_raw)
    )
''')

# Create indexes on year, fuel + year, and state
cur.execute('''
    CREATE INDEX IF NOT EXISTS idx_clean_generation_year
    ON clean_generation(year)
''')

cur.execute('''
    CREATE INDEX IF NOT EXISTS idx_clean_generation_fuel_year
    ON clean_generation(fuel_code, year)     
''')

cur.execute('''
    CREATE INDEX IF NOT EXISTS idx_clean_generation_state           
    ON clean_generation(state_code)
''')

conn.commit()

# ----- Mapping -------

print('Generating mapping tables...')

# State mapping
states = dict()
cur.execute('SELECT state, stateDescription FROM raw_generation')
for state in cur :
    if state[0] == 'PR' : # state codes with NULL descriptions need intentional assignments.
        states[state[0]] = 'Puerto Rico'
    else :
        states[state[0]] = state[1]
for code in states :
    cur.execute('''INSERT OR IGNORE INTO States
        (state_code, state_desc) VALUES (?, ?)''', (code, states[code]))

# Units mapping
units = dict()
cur.execute('SELECT units FROM raw_generation')
for item in cur :
    if item[0] == 'megawatthours' :
        desc = 'MWh'
    else :
        print('New unit found. Please edit mapping.py with new unit.')
    units[item[0]] = desc
for code in units :
    cur.execute('''INSERT OR IGNORE INTO Units
        (units_raw, units_clean) VALUES (?, ?)''', (code, units[code]))

# Fuel mapping
fuelcodes = dict()
cur.execute('SELECT fuel2002, fuelTypeDescription FROM raw_generation')
for row in cur :
    desc = str.title(row[1])
    desc = desc.replace(' And ',' & ')
    desc = desc.replace('Municiapl', 'Municipal')
    fuelcodes[row[0]] = desc
for code in fuelcodes :
    cur.execute('''INSERT OR IGNORE INTO Fuels
        (fuel_code, fuel_desc) VALUES (?,?)''', (code, fuelcodes[code]))
conn.commit()

print('Mapping completed successfully.')

# ------- Load --------

print('Aggregating raw data into usable table...')

# create a dict() that has format { (year, state_code, fuel_code) , (generation, units) }
data = dict()
cur.execute('SELECT period, state, fuel2002, generation, units FROM raw_generation WHERE state IS NOT NULL')
for year, state_code, fuel_code, generation, units in cur :
    year = int(year)
    generation = float(generation)

    key = (year, state_code, fuel_code)

    if key not in data :
        data[key] = {
            'generation' : generation,
            'units' : units
        }
    else :
        if units != data[key]['units'] :
            raise ValueError(
                f'Unit mismatch for {key}: '
                f"{data[key]['units']} vs {units}"
            )
        data[key]['generation'] += generation

for (year, state_code, fuel_code), values in data.items() :
    cur.execute('''INSERT INTO clean_generation
        (year, state_code, fuel_code, generation, units, updated_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (year, state_code, fuel_code) DO UPDATE SET 
            generation = excluded.generation,
            updated_at = CURRENT_TIMESTAMP
        ''', (year, state_code, fuel_code, values['generation'], values['units']))

conn.commit()
cur.close()

print('Data aggregated successfully.')