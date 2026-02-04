# Minimal placeholder for crawler logic
def test_process_page_mock():
    data = [
        {"period": "2020", "plantCode": "001", "plantName": "Plant A",
         "fuel2002": "COL", "fuelTypeDescription": "Coal", "state": "TX",
         "stateDescription": "Texas", "primeMover": "ALL", "generation": 100,
         "generation-units": "MWh"}
    ]

    # Simulate filtering in process_page
    filtered = [row for row in data if row["primeMover"] == "ALL"]
    assert len(filtered) == 1
    assert filtered[0]["plantName"] == "Plant A"
