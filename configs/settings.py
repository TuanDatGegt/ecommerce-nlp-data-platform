##configs/settings.py
import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
env_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=env_path)

# Data Lake and Repository Runtime Paths
DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "data")
SAMPLE_DATA_PATH = os.getenv("SAMPLE_DATA_PATH", "data/sample")
YAML_CONFIG_PATH = os.getenv("YAML_CONFIG_PATH", "configs/config.yaml")
MANIFEST_PATH = os.getenv("MANIFEST_PATH", "data/metadata/processed_files.json")

# Execution Parameters
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 3))
CHUNK_SIZE = 150_000
COMPRESSION = "snappy"
CATEGORY_MAPPING = r"amazon_reviews_us_([A-Za-z0-9_-]+)_v"

# Kaggle API Credentials
API_KAGGLE_TOKEN_KEY = os.getenv("API_KAGGLE_TOKEN_KEY")
USERNAME_KAGGLE = os.getenv("USERNAME_KAGGLE")
KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "caglaates/amazon-product-reviews-dataset")
