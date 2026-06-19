#pipeline/gold/storage/gold_storage.py

import os
import sys
from minio import Minio

from configs.settings import SILVER_BUCKET_NAME
from configs.settings import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, MINIO_SECURE

class GoldStorage():
    def __init__(self):
        self.storage = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )

    def read_silver_pipeline(self):
    
    def write_parquet_gold(self):
    
    def save_sample(self):
    
    def get_gold_path(self):

    
    