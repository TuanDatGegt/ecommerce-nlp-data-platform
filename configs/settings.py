## configs/settings.py
import os
from pathlib import Path

from dotenv import load_dotenv

# =====================================================================
# PROJECT ROOT & ENVIRONMENT
# =====================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)


# =====================================================================
# DATA LAKE & STORAGE PATHS
# =====================================================================

DATA_LAKE_PATH = PROJECT_ROOT / os.getenv(
    "DATA_LAKE_PATH",
    "data",
)

RAW_DATA_PATH = PROJECT_ROOT / os.getenv(
    "RAW_DATA_PATH",
    "data/raw",
)

SAMPLE_DATA_PATH = PROJECT_ROOT / os.getenv(
    "SAMPLE_DATA_PATH",
    "data/sample",
)

BRONZE_DATA_PATH = DATA_LAKE_PATH / "bronze"
SILVER_DATA_PATH = DATA_LAKE_PATH / "silver"
GOLD_DATA_PATH = DATA_LAKE_PATH / "gold"
DLQ_DATA_PATH = DATA_LAKE_PATH / "dlq"

METADATA_PATH = DATA_LAKE_PATH / "metadata"

MANIFEST_PATH = PROJECT_ROOT / os.getenv(
    "MANIFEST_PATH",
    "data/metadata/processed_files.json",
)


# =====================================================================
# CONFIGURATION FILES
# =====================================================================

YAML_CONFIG_PATH = PROJECT_ROOT / os.getenv(
    "YAML_CONFIG_PATH",
    "configs/config.yaml",
)


# =====================================================================
# EXECUTION PARAMETERS
# =====================================================================

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))

CHUNK_SIZE = 150_000

COMPRESSION = "snappy"


# =====================================================================
# BRONZE PROCESSING
# =====================================================================

CATEGORY_MAPPING = r"amazon_reviews_us_([A-Za-z0-9_-]+)_v"


# =====================================================================
# KAGGLE API
# =====================================================================

USERNAME_KAGGLE = os.getenv("USERNAME_KAGGLE")

API_KAGGLE_TOKEN_KEY = os.getenv("API_KAGGLE_TOKEN_KEY")

KAGGLE_DATASET = os.getenv(
    "KAGGLE_DATASET",
    "cynthiarempel/amazon-us-customer-reviews-dataset",
)
