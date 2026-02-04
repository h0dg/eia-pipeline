import pytest
from datetime import datetime

# -------------------------------
# Raw DB tests
# -------------------------------

def test_save_and_load_raw_data(in_memory_raw_db):
    db = in_memory_raw_db

    records = [
        {
            "period": "2020",
            "plantCode": "001",
            "plantName": "Plant A",
            "fuel2002": "COL",
            "fuelTypeDescription": "Coal",
            "state": "TX",
            "stateDescription": "Texas",
            "primeMover": "ALL",
            "generation": 100,
            "units": "MWh"
        }
    ]

    inserted = db.cur.executemany(
        f"""
        INSERT OR IGNORE INTO {db.table} 
        (period, plantCode, plantName, fuel2002, fuelTypeDescription, state,
        stateDescription, primeMover, generation, units, ingestionTimestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (r["period"], r["plantCode"], r["plantName"], r["fuel2002"], r["fuelTypeDescription"],
             r["state"], r["stateDescription"], r["primeMover"], r["generation"], r["units"], datetime.now().isoformat(sep=" ", timespec="seconds")
)
            for r in records
        ]
    )
    db.commit()

    db.cur.execute(f"SELECT * FROM {db.table}")
    loaded = db.cur.fetchall()
    assert len(loaded) == 1
    assert loaded[0][3] == "Plant A"  # plantName column

def test_metadata(in_memory_raw_db):
    db = in_memory_raw_db
    pipeline = "test_pipeline"
    offset = 123

    # Insert metadata
    db.cur.execute(f"INSERT INTO {db.metadata_table} (pipeline, lastOffset, lastTimestamp) VALUES (?, ?, ?)",
                   (pipeline, offset, datetime.now().isoformat(sep=" ", timespec="seconds")
))
    db.commit()

    # Load metadata
    db.cur.execute(f"SELECT lastOffset FROM {db.metadata_table} WHERE pipeline = ?", (pipeline,))
    row = db.cur.fetchone()
    assert row[0] == offset
