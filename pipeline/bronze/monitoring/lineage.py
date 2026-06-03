#pipeline/bronze/monitoring/lineage.py

import os
import json
import tempfile
from datetime import datetime, timezone

from pipeline.bronze.storage.minio_client import MinioClient
from configs.settings import RAW_BUCKET_NAME, LINEAGE_OBJECT_NAME

# LINEAGA_OBJECT_NAME = "metadata/lineage/bronze_lineage.json"



def load_lineage():
    storage_client = MinioClient()
    temp_file = tempfile.mkdtemp()
    local_file = os.path.join(temp_file, "bronze_lineage.json")

    try:
        storage_client.download_file(
            bucket_name=RAW_BUCKET_NAME,
            object_name=LINEAGE_OBJECT_NAME,
            local_path=local_file
        )
        with open(local_file, "r", encoding="utf-8") as f:
            lineage_data =json.load(f)

    except Exception:
        lineage_data = []
    
    finally:
        if os.path.exists(local_file):
            os.remove(local_file)

    return lineage_data

def save_lineage(lineage_data):
    storage_client = MinioClient()
    temp_dir = tempfile.mkdtemp()
    local_file = os.path.join(temp_dir, "bronze_lineage.json")

    with open(local_file, "w", encoding="utf-8") as f:
        json.dump(lineage_data, f, indent=4)
    
    storage_client.upload_file(
        bucket_name=RAW_BUCKET_NAME,
        object_name=LINEAGE_OBJECT_NAME,
        local_path=local_file
    )
    if os.path.exists(local_file):
        os.remove(local_file)

def record_lineage(source_file, parquet_objects, category, row_count, chunk_idx):
    lineage_data = load_lineage()
    
    record = {
        "source_file": source_file,
        "output_parquet": parquet_objects,
        "category": category, 
        "row_count": row_count,
        "chunk_idx": chunk_idx,
        "processed_at": datetime.now(timezone.utc).isoformat()
    }

    lineage_data.append(record)
    save_lineage(lineage_data)

def list_lineage():
    return load_lineage()

