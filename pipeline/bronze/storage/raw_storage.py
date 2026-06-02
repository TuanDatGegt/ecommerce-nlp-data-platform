#pipeline/bronze/storage/raw_stogare.py

import os
import tempfile
from pipeline.bronze.storage.minio_client import MinioClient

from configs.settings import RAW_BUCKET_NAME



def upload_parquet_file(local_path, object_name):
    """
    Uploads all files from the local dataset directory to the Minio raw bucket.
    """
    storage = MinioClient()

    storage.upload_file(
        bucket_name=RAW_BUCKET_NAME, 
        object_name=object_name, 
        local_path=local_path
    )
    return object_name

def build_bronze_object_name(category, year, month, chunk_idx):
    return os.path.join(
        f"bronze/reviews/",
        f"year={year}/",
        f"month={month}/",
        f"category={category}/",
        f"part_{chunk_idx:05d}.parquet"
    )

def download_bronze_file(object_name):
    storage = MinioClient()

    temp_dir = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, os.path.basename(object_name))
    storage.download_file(RAW_BUCKET_NAME, object_name, local_path)
    return local_path

def list_raw_file():
    storage = MinioClient()
    objects = storage.list_objects(RAW_BUCKET_NAME, prefix="raw/amazone_review/")
    return [obj.object_name for obj in objects if obj.object_name.endswith(".tsv")]

