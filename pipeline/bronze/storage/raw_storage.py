#pipeline/bronze/storage/raw_stogare.py

import os
import tempfile
from pipeline.bronze.storage.minio_client import MinioStorage

from configs.settings import RAW_BUCKET_NAME

storage = MinioStorage()

def upload_parquet_file(local_path, object_name):
    """
    Uploads all files from the local dataset directory to the Minio raw bucket.
    """
    storage.create_bucket(RAW_BUCKET_NAME)
    storage.upload_file(
        bucket_name=RAW_BUCKET_NAME, 
        object_name=object_name, 
        local_path=local_path
    )
    return object_name

    print(f"Uploaded {len(uploaded_files)} raw TSV files")

    return uploaded_files

def build_bronze_object_name(category, year, month, chunk_idx):
    return os.path.join(
        "bronze",
        "reviews",
        f"year={year}",
        f"month={month}",
        f"category={category}",
        f"part_{chunk_idx:05d}.parquet"
    )

def download_bronze_file(object_name):
    temp_dir = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, os.path.basename(object_name))
    storage.download_file(RAW_BUCKET_NAME, object_name, local_path)
    return local_path

def list_raw_file():
    objects = storage.list_objects(RAW_BUCKET_NAME, prefix="raw/amazone_review/")
    return [obj.object_name for obj in objects if obj.object_name.endswith(".tsv")]





    