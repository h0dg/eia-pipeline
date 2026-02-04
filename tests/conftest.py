import pytest
from src.db.repository import Database

# -------------------------------
# Fixtures for in-memory databases
# -------------------------------

@pytest.fixture
def in_memory_raw_db():
    """
    Provides an in-memory raw database instance for testing.
    """
    db = Database.__new__(Database)  # Bypass __init__ since we want custom DB
    db.path = ":memory:"
    import sqlite3
    db.conn = sqlite3.connect(db.path)
    db.conn.execute("PRAGMA foreign_keys = ON")
    db.cur = db.conn.cursor()
    db.table = "raw_generation"
    db.metadata_table = "crawl_metadata"
    
    # Create minimal raw tables
    db.cur.execute(f"""
        CREATE TABLE {db.table} (
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
            UNIQUE(period, plantCode, fuel2002)
        )
    """)
    db.cur.execute(f"""
        CREATE TABLE {db.metadata_table} (
            pipeline TEXT PRIMARY KEY,
            lastOffset INTEGER,
            lastTimestamp TIMESTAMP
        )
    """)
    db.commit = db.conn.commit
    db.close = lambda: db.conn.close()
    return db

@pytest.fixture
def in_memory_clean_db():
    """
    Provides an in-memory clean database instance for testing.
    """
    db = Database.__new__(Database)
    db.path = ":memory:"
    import sqlite3
    db.conn = sqlite3.connect(db.path)
    db.conn.execute("PRAGMA foreign_keys = ON")
    db.cur = db.conn.cursor()
    db.table = "clean_generation"
    db.mapping_tables = ["states", "fuels", "units"]

    # Create minimal clean tables
    db.cur.execute(f"""
        CREATE TABLE {db.table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            state_code TEXT,
            fuel_code TEXT,
            generation REAL,
            units TEXT,
            updated_at TIMESTAMP,
            UNIQUE(year, state_code, fuel_code)
        )
    """)
    db.commit = db.conn.commit
    db.close = lambda: db.conn.close()
    return db
