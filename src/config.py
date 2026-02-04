import yaml
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

API_KEY = os.getenv('EIA-API-KEY')

# Load config.yaml
CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'
with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

# Database configuration
DB_CONFIG = {
    "raw": {
        "path": cfg["database"]["raw"]["path"],
        "table": cfg["database"]["raw"]["table"],
        "metadata_table": cfg["database"]["raw"].get("metadata_table"),
    },
    "clean": {
        "path": cfg["database"]["clean"]["path"],
        "table": cfg["database"]["clean"]["table"],
        "mapping_tables": cfg["database"]["clean"].get("mapping_tables",[]),
    }
}

EIA_CONFIG = cfg["eia"]

def get_dataset_url(eia_cfg, dataset_name: str):
    """
    Returns the full URL for the dataset with the given name.

    :param cfg: dict loaded from config.yaml
    :param dataset_name: str, name of the dataset in cfg['eia']['datasets']
    :return: str, full URL
    :raises ValueError: if dataset_name not found
    """

    base_url = eia_cfg["base_url"]
    datasets = eia_cfg["datasets"]

    for d in datasets:
        if d["name"] == dataset_name:
            return base_url + d["path"]

