import sqlite3
from src.config import DB_CONFIG

class Database:
    def __init__(self, db_type="raw"):
        """
        Initialize a Database object for interacting with either the raw or clean SQLite database.
        
        :param db_type: str, optional
            Type of database to connect to. Must be either "raw" or "clean".
            - "raw": database storing unprocessed API data and crawl metadata
            - "clean": database storing normalized/aggregated data and mapping tables
            Default is "raw".
        :raises ValueError: if db_type is not "raw" or "clean"
        """   
        cfg = DB_CONFIG[db_type]
        self.path = cfg["path"]
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cur = self.conn.cursor()
        self.table = cfg["table"]
        self.metadata_table = cfg.get("metadata_table")     # only for raw DB
        self.mapping_tables = cfg.get("mapping_tables")     # only for clean DB

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
    
    # ---- Raw DB Methods ----

    def initialize_raw_tables(self):
        self.cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table} (
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
        
        self.cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.metadata_table} (
            pipeline TEXT PRIMARY KEY,
            lastOffset INTEGER,
            lastTimestamp TIMESTAMP
            )
        ''')

        self.commit()

    def save_raw_data(self, records: list[dict]):
        """
        Insert raw API data into raw_generation table.
        
        :param records: list of dict
            Each dict must contain the following keys:
                -   "period", "plantCode", "plantName", "fuel2002", "fuelTypeDescription",
                    "state", "stateDescription", "primeMover", "generation", "units"
        :return: integer
        """
        inserted = 0
        for r in records:
            self.cur.execute(
                f"""
                INSERT OR IGNORE INTO {self.table}
                (period, plantCode, plantName, fuel2002, fuelTypeDescription, state, stateDescription, primeMover, generation, units, ingestionTimestamp)
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (r["period"], r["plantCode"], r["plantName"], r["fuel2002"], r["fuelTypeDescription"],
                r["state"], r["stateDescription"], r["primeMover"], r["generation"], r["units"])
            )
            inserted += self.cur.rowcount
        self.commit()
        return inserted

    def load_raw_data(self):
        """
        Load all raw rows from raw_generation table.

        :return: list of tuples
        """
        self.cur.execute(f"SELECT * FROM {self.table}")
        return self.cur.fetchall()
    
    def load_metadata(self, pipeline_name):
        """
        Load the last offset for a given pipeline from the metadata table.

        :param pipeline_name: str
            Name of the pipeline (e.g., 'eia_generation')
        :return: int
            Last offset stored, returns 0 if no metadata exists.
        """
        if not self.metadata_table:
            return 0

        self.cur.execute(
            f"SELECT lastOffset FROM {self.metadata_table} WHERE pipeline = ?",
            (pipeline_name,)
        )
        row = self.cur.fetchone()
        return row[0] if row else 0
    
    def update_metadata(self, pipeline_name, offset):
        """
        Update or insert the last offset for a given pipeline in the metadata table.
        
        :param pipeline_name: str
            Name of the pipeline (e.g., 'eia_generation')
        :param offset: int
            Offset value to store
        """
        self.cur.execute(f"SELECT 1 from {self.metadata_table} WHERE pipeline = ?", (pipeline_name,))
        row = self.cur.fetchone()
        if row is None:
            self.cur.execute(f"""INSERT INTO {self.metadata_table} (pipeline, lastOffset, lastTimestamp)
            VALUES (?, ?, CURRENT_TIMESTAMP)""", (pipeline_name, offset)
            )

        else:
            self.cur.execute(
                f"UPDATE {self.metadata_table} SET lastOffset = ?, lastTimestamp = CURRENT_TIMESTAMP WHERE pipeline = ?",
                (offset, pipeline_name)
            )
        self.commit()

    def get_raw_states(self):
        """
        Fetch distinct state codes and descriptions from raw data.
        """
        self.cur.execute(
            f"SELECT DISTINCT state, stateDescription FROM {self.table} WHERE state IS NOT NULL"
        )
        return self.cur.fetchall()
    
    def get_raw_units(self):
        """
        Fetch distinct units from raw data.
        """
        self.cur.execute(
            f"SELECT DISTINCT units FROM {self.table}"
        )
        return [row[0] for row in self.cur.fetchall()]
    
    def get_raw_fuels(self):
        """
        Fetch distinct fuel codes from raw data.
        """
        self.cur.execute(
            f"SELECT DISTINCT fuel2002, fuelTypeDescription FROM {self.table}"
        )
        return self.cur.fetchall()

    def get_raw_generation_rows(self):
        """
        Fetch raw generation rows used for aggregation.
        """
        self.cur.execute(f"""
            SELECT period, state, fuel2002, generation, units
            FROM {self.table}
            WHERE state IS NOT NULL
        """)
        return self.cur.fetchall()


    # ---- Clean DB Methods ----

    def initialize_clean_tables(self, reset=False):
        """
        Creates tables needed in clean database and ensures indexes exist. Set reset=True to delete all tables.
        
        :param reset: bool
            Set reset=True to delete all tables in clean DB and start fresh
            Default value is False
        """
        if reset is True:
            self.cur.execute(f"DROP TABLE IF EXISTS {self.table}")
            for t in self.mapping_tables:
                self.cur.execute(f"DROP TABLE IF EXISTS {t}")
        
        self.cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            state_code TEXT,
            fuel_code TEXT,
            generation REAL,
            units TEXT,
            updated_at TIMESTAMP,
            UNIQUE (year, state_code, fuel_code),
            FOREIGN KEY (state_code) REFERENCES states(state_code),
            FOREIGN KEY (fuel_code) REFERENCES fuels(fuel_code),
            FOREIGN KEY(units) REFERENCES units(units_raw)
            )
        ''')
        
        self.cur.execute('''CREATE TABLE IF NOT EXISTS states (
            state_code TEXT PRIMARY KEY NOT NULL,
            state_desc TEXT)''')
        
        self.cur.execute('''CREATE TABLE IF NOT EXISTS units (
            units_raw TEXT PRIMARY KEY,
            units_clean TEXT)''')
        
        self.cur.execute('''CREATE TABLE IF NOT EXISTS fuels (
            fuel_code TEXT PRIMARY KEY,
            fuel_desc TEXT)''')

        self.cur.execute(f'''CREATE INDEX IF NOT EXISTS idx_clean_generation_year
            ON {self.table}(year)''')
        
        self.cur.execute(f'''CREATE INDEX IF NOT EXISTS idx_clean_generation_fuel_year
            ON {self.table}(fuel_code, year)''')

        self.cur.execute(f'''CREATE INDEX IF NOT EXISTS idx_clean_generation_state           
            ON {self.table}(state_code)''')
        
        self.commit()


    def save_clean_data(self, records: list[dict]):
        """
        Insert clean data into clean_generation table.
        
        :param records: list of dict
            Each dict must contain the following keys"
                -   "year", "state_code", "fuel_code", "generation", "units"
        """
        for r in records:
            self.cur.execute(
                f"""
                INSERT INTO {self.table}
                (year, state_code, fuel_code, generation, units, updated_at)
                VALUES ( ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (year, state_code, fuel_code) DO UPDATE SET
                    generation = excluded.generation,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (r["year"], r["state_code"], r["fuel_code"], r["generation"], r["units"])
            )
        self.commit()

    def load_clean_data(self):
        """
        Loads all clean rows from clean_generation table

        :return: list of tuples
        """
        self.cur.execute(f"SELECT * FROM {self.table}")
        return self.cur.fetchall()
    

    def insert_states(self, states: dict):
        """
        Insert states mappings into states table.
        """
        for code, desc in states.items() :
            self.cur.execute('''INSERT OR IGNORE INTO states
                (state_code, state_desc) VALUES (?, ?)''', 
                (code, desc)
            )
        self.commit()
        
    def insert_units(self, units: dict):
        """
        Insert unit mappings into units table.
        """
        for raw, clean in units.items():
            self.cur.execute(
                """
                INSERT OR IGNORE INTO units (units_raw, units_clean)
                VALUES (?, ?)
                """,
                (raw, clean)
            )
        self.commit()

    def insert_fuels(self, fuels: dict):
        """
        Insert fuel mappings into fuels table.
        """
        for code, desc in fuels.items():
            self.cur.execute(
                """
                INSERT OR IGNORE INTO fuels (fuel_code, fuel_desc)
                VALUES (?, ?)
                """,
                (code, desc)
            )
        self.commit()
