# pipeline/storage/silver/silver_storage.py

import os
import io
import json
import pandas as pd
from minio import Minio
from pipeline.bronze.storage.minio_client import MinioClient
from configs.settings import (
    BRONZE_BUCKET_NAME,
    SILVER_BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)


class SilverStorage:
    def __init__(self, ensur_bucket: bool = False):
        # Khởi tạo trực tiếp từ thư viện gốc, đảm bảo nhận tham số 'endpoint'
        self.client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        if ensur_bucket:
            self._ensure_silver_bucket()

    def _ensure_silver_bucket(self):
        if not self.client.bucket_exists(SILVER_BUCKET_NAME):
            self.client.make_bucket(SILVER_BUCKET_NAME)

    def read_parquet_from_bronze(self, object_name: str) -> pd.DataFrame:
        response = self.client.get_object(BRONZE_BUCKET_NAME, object_name)
        try:
            return pd.read_parquet(io.BytesIO(response.read()))
        finally:
            response.close()
            response.release_conn()

    def write_parquet_to_silver(self, df: pd.DataFrame, object_name: str):
        if df.empty:
            return

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)

        self.client.put_object(
            bucket_name=SILVER_BUCKET_NAME,
            object_name=object_name,
            data=buffer,
            length=len(buffer.getvalue()),
            content_type="application/parquet",
        )

    def write_parquet_to_silver(self, df: pd.DataFrame, object_name: str):
        if df.empty:
            return

        buffer = io.BytesIO()

        df.to_parquet(buffer, index=False, compression="snappy")

        # === PERFORMANCE IMPROVEMENT ===
        # Tránh buffer.getvalue()
        # vì sẽ copy toàn bộ parquet vào RAM thêm 1 lần

        buffer.seek(0, os.SEEK_END)
        parquet_size = buffer.tell()
        buffer.seek(0)

        self.client.put_object(
            bucket_name=SILVER_BUCKET_NAME,
            object_name=object_name,
            data=buffer,
            length=parquet_size,
            content_type="application/parquet",
        )
