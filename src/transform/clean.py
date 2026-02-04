from src.db import Database

def setup_transform():
    raw_db = Database("raw")
    clean_db = Database("clean")
    clean_db.initialize_clean_tables(False) # Set "True" to reset tables

    return raw_db, clean_db

# ----- Mapping -------

def build_state_mapping(raw_db, clean_db):
    states = {}
    for code, desc in raw_db.get_raw_states():
        if code == "PR":
            states[code] = 'Puerto Rico'
        else:
            states[code] = desc

    clean_db.insert_states(states)

def build_units_mapping(raw_db, clean_db):
    units = {}
    for unit in raw_db.get_raw_units():
        if unit == "megawatthours":
            units[unit] = "MWh"
        else:
            raise ValueError(f"Unknown unit: {unit}")
    
    clean_db.insert_units(units)

def build_fuels_mapping(raw_db, clean_db):
    fuels = {}
    for code, desc in raw_db.get_raw_fuels():
        clean_desc = (
            str.title(desc)
            .replace(" And ", " & ")
            .replace("Municiapl", "Municipal")
        )
        fuels[code] = clean_desc

    clean_db.insert_fuels(fuels)

# ------- Load --------

# create a dict() that has format { (year, state_code, fuel_code) , (generation, units) }

def aggregate_generation(raw_db, clean_db):
    data = {}
    for year, state_code, fuel_code, generation, units in raw_db.get_raw_generation_rows():
        year = int(year)
        generation = float(generation)

        key = (year, state_code, fuel_code)

        if key not in data:
            data[key] = {
                "generation": generation,
                "units": units
            }
        else:
            if units != data[key]["units"]:
                raise ValueError(
                    f'Unit mismatch for {key}: '
                    f"{data[key]['units']} vs {units}"
                )
            data[key]["generation"] += generation
    
    records = [
        {
            "year": y,
            "state_code": s,
            "fuel_code": f,
            "generation": v["generation"],
            "units": v["units"]
        }
        for (y,s,f), v in data.items()
    ]

    clean_db.save_clean_data(records)