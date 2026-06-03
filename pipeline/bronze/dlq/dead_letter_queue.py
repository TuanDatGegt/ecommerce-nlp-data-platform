#pipeline/bronze/dlq/dead_letter_queue.py

import os
import json
import tempfile
from datetime import datetime, timezone

from pipeline.bronze.storage.minio_client import MinioClient

from configs.settings import RAW_BUCKET_NAME

storage = MinioClient(auto_init=False)

def write_to_dlq(df, source_file, error_message):
    temp_dir = tempfile.mkdtemp()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    file_name =(f"{source_file}_{timestamp}.json")

    local_file = os.path.join(temp_dir, file_name)

    if df is None and hasattr(df, "to_dict"):
        records = df.to_dict(orient='records')
    else: 
        records = [{"raw_data_error": str(df)}]

    payload={
        "source_file":  source_file,
        "error_message": str(error_message),
        "failed_at": str(datetime.now(timezone.utc)),
        "records": records
    }

    with open(local_file, "w")as f:
        json.dump(payload, f, indent=4, default=str)

    object_name = f"dlq/bronze/{file_name}"

    storage.upload_file(
        bucket_name=RAW_BUCKET_NAME,
        file_path=local_file,
        object_name=object_name)
    
    os.remove(local_file)
    return object_name
