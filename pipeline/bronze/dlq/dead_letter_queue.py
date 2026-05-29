#pipeline/bronze/dlq/dead_letter_queue.py

import os
import json
import tempfile
from datetime import datetime, timezone

from pipeline.bronze.storage.minio_client import MinioClient

from configs.settings import MINIO_BUCKET_NAME

storage = MinioClient()

def write_to_dlq(df, source_file, error_message):
    temp_dir = tempfile.mkdtemp()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    file_name =(f"{source_file}_{timestamp}.json")

    local_file = os.path.join(temp_dir, file_name)
    records = df.to_dict(orient='records')

    payload={
        "source_file":  source_file,
        "error_message": error_message,
        "failed_at": str(datetime.now(timezone.utc)),
        records: records
    }

    with open(local_file, "w")as f:
        json.dump(payload, f, indent=4, default=str)

    object_name = os.path.join('dlq', "bronze", file_name)

    storage.upload_file(bucket_name=MINIO_BUCKET_NAME, file_path=local_file, object_name=object_name)
    
    os.remove(local_file)
    return object_name
