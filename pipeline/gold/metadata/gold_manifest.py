#pipeline/gold/metadata/gold_manifest.py

import os, json
from datetime import timezone, datetime
from configs.settings import GOLD_BUCKET_NAME
from pipeline.storage.pipeline_storage import StorageClient

MANIFEST_KEY = "metadata/gold_manifest.json"

def load_manifest():
    storage = StorageClient()
    try:
        data = storage.read_json_from_minio(GOLD_BUCKET_NAME, MANIFEST_KEY)
    except Exception:
        return []
    
def save_manifest(records):
    storage = StorageClient()
    storage.upload_json_to_minio(GOLD_BUCKET_NAME, MANIFEST_KEY, records)

def update_manifest(rows_input, rows_output, status="SUCCESS"):
    manifest = load_manifest()

    manifest.append(
        {
            "run-time": datetime.now(datetime).isoformat(),
            "row-input": int(rows_input),
            "rows-output": int(rows_output),
            "status": status
        }
    )

    save_manifest(manifest)

