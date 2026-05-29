#pipeline/bronze/storage/bronze_storage.py

import os
import tempfile
from pipeline.bronze.storage.minio_client import MinioCLinent

from configs.settings import BRONZE_BUCKET_NAME

storage = MinioCLinent()

def upload_parquet_file(local_parquet_file, object_name):
    """
    Uploads a local Parquet file to the Minio bronze bucket.
    """
    storage.create_bucket(BRONZE_BUCKET_NAME)
    storage.upload_file(BRONZE_BUCKET_NAME, object_name, local_parquet_file)
    
    return object_name

def build_bronze_object_name(category, year, month, chunk_idx):
    return os.path.join("bronze", "reviews", f"year={year}",f"month={month}", f"category={category}", f"part_{chunk_idx:05d}.parquet")

def download_bronze_file(object_name):
    """Downloads bronze parquet temporarily. 
    Returns the local path of the downloaded file.
    """
    temp_dir = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, os.path.basename(object_name))

    storage.download_file(BRONZE_BUCKET_NAME, object_name, local_path)

    return local_path

def list_bronze_files():
    objects = storage.list_objects(BRONZE_BUCKET_NAME, prefix="bronze/reviews/")
    return [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]

