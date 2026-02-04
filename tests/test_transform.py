import pytest
from datetime import datetime

def test_clean_db_save_and_load(in_memory_clean_db):
    db = in_memory_clean_db

    records = [
        {"year": 2020, "state_code": "TX", "fuel_code": "COL", "generation": 100, "units": "MWh"},
        {"year": 2020, "state_code": "CA", "fuel_code": "GAS", "generation": 50, "units": "MWh"}
    ]

    # Insert data
    for r in records:
        db.cur.execute(
            f"""INSERT INTO {db.table} (year, state_code, fuel_code, generation, units, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (r["year"], r["state_code"], r["fuel_code"], r["generation"], r["units"], datetime.now().isoformat(sep=" ", timespec="seconds")
)
        )
    db.commit()

    # Load data
    db.cur.execute(f"SELECT state_code, fuel_code, generation FROM {db.table}")
    loaded = db.cur.fetchall()
    assert len(loaded) == 2
    assert ("TX", "COL", 100) in loaded
    assert ("CA", "GAS", 50) in loaded
