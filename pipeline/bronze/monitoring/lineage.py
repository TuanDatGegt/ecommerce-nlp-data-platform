#pipeline/bronze/monitoring/lineage.py

import os
import json
import tempfile
from datetime import datetime, timezone

from configs.settings import RAW_BUCKET_NAME, LINEAGE_OBJECT_NAME

LOCAL_LINEAGE_PATH =os.path.join("logs", "bronze_local_lineage.json")



def load_lineage():

    if not os.path.join(LOCAL_LINEAGE_PATH):
        os.makedirs(os.path.dirname(LOCAL_LINEAGE_PATH), exist_ok=True)
        with open(LOCAL_LINEAGE_PATH, "w", encoding="utf-8") as f:
            json.dump([], f)
        return []
    
    try:
        with open(LOCAL_LINEAGE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_lineage(lineage_data):
    os.makedirs(os.path.dirname(LOCAL_LINEAGE_PATH), exist_ok=True)
    try:
        with open(LOCAL_LINEAGE_PATH, "w", encoding="utf-8") as f:
            json.dump(lineage_data, f, indent=4)
    except Exception as e:
        print(f"[LINEAGE ERROR] Khong the ghi file lineage local: {e}")


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

