import numpy as np
import pytest

from src.analysis.visualize import create_arrays

class FakeCleanDB:
    def aggregate_generation(self, year):
        return [("COL", 150), ("GAS", 70), ("NUC", 50)]

def test_create_arrays_returns_arrays():
    clean_db = FakeCleanDB()
    fuel_codes, generation, top10 = create_arrays(clean_db, 2020)

    assert isinstance(fuel_codes, np.ndarray)
    assert isinstance(generation, np.ndarray)
    assert len(fuel_codes) == 3
    assert top10[0][0] == "COL"
    assert top10[0][1] == 150
