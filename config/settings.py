import os
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = os.getenv('RAW_DATA_PATH')
PROCESSED_DATA_PATH = os.getenv('PROCESSED_DATA_PATH')

MAX_WORKERS = int(os.getenv('MAX_WORKERS', 4))

CHUNK_SIZE = 200_000
COMPRESSION = 'snappy'

CATEGORY_MAPPING = r"amazon_reviews_us_([A-Za-z0-9_\-]+)_v"
    