#pipeline/silver/storage/silver_storage.py

import os
import io
import pandas as pd
from pipeline.bronze.storage.minio_client import MinioClient
from configs.settings import BRONZE_BUCKET_NAME, SILVER_BUCKET_NAME

class SilverStorage:
    def __init__(self):
        self.client = MinioClient(auto_init=True)
        self._ensure_silver_bucket()

    def _ensure_silver_bucket(self):
        if not self.client.client.bucket_exists(SILVER_BUCKET_NAME):
            self.client.client.make_bucket(SILVER_BUCKET_NAME)

    def read_parquet_from_bronze(self, object_name: str) -> pd.DataFrame:
        response = self.client.client.get_object(BRONZE_BUCKET_NAME, object_name)
        try:
            return pd.read_parquet(io.BytesIO(response.read()))
        finally:
            response.close()
            response.release_conn()

    def write_parquet_to_silver(self, df: pd.DataFrame, object_name: str):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)

        self.client.client.put_object(
            bucket_name = SILVER_BUCKET_NAME,
            object_name = object_name,
            data = buffer,
            length = len(buffer.getvalue()),
            content_type = "application/parquet"
        )

        