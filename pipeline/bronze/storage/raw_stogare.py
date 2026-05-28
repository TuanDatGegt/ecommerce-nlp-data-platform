#pipeline/bronze/storage/raw_stogare.py

import os
from pipeline.bronze.storage.minio_client import MinioStorage

from configs.settings import RAW_BUCKET_NAME

storage = MinioStorage()

def upload_raw_dataset(dataset_dir):
    """
    Uploads all files from the local dataset directory to the Minio raw bucket.
    """
    storage.create_bucket(RAW_BUCKET_NAME)
    uploaded_files = []

    for file_name in os.listtdir(dataset_dir):
        if not file_name.endswith(".tsv"):
            continue
        local_path = os.path.join(dataset_dir, file_name)
        object_name = os.path.join("raw", "amazone_review", file_name)
        storage.upload_file(RAW_BUCKET_NAME, object_name, local_path)

        uploaded_files.append(object_name)

    print(f"Uploaded {len(uploaded_files)} raw TSV files")

    return uploaded_files

def list_raw_file():
    objects = storage.list_objects(RAW_BUCKET_NAME, prefix="raw/amazone_review/")
    return [obj.object_name for obj in objects if obj.object_name.endswith(".tsv")]





    