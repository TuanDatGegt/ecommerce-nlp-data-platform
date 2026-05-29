#config/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

DATA_LAKE_PATH = os.getenv('DATA_LAKE_PATH')
SAMPLE_DATA_PATH = os.getenv('SAMPLE_DATA_PATH')

yaml_config_path = os.getenv('YAML_CONFIG_PATH')

MANIFEST_PATH = os.getenv('MANIFEST_PATH')

MAX_WORKERS = int(os.getenv('MAX_WORKERS', 4))

CHUNK_SIZE = 200_000
COMPRESSION = 'snappy'

CATEGORY_MAPPING = r"amazon_reviews_us_([A-Za-z0-9_\-]+)_v"

#Kaggle API
API_KAGGLE_TOKEN_KEY = os.getenv('API_KAGGLE_TOKEN_KEY')
USERNAME_KAGGLE = os.getenv('USERNAME_KAGGLE')
KAGGLE_DATASET = os.getenv('KAGGLE_DATASET')
