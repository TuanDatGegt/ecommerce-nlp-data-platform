#pipeline/storage/pipeline_storage.py

import io, os, tempfile, json
from typing import List
import pandas as pd

from minio import Minio
from minio.error import S3Error

from configs.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,

    BRONZE_BUCKET_NAME,
    SILVER_BUCKET_NAME,
    GOLD_BUCKET_NAME
)

class StorageClient():
    
    def __init__(self, auto_init: bool=True):
        self.storage = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )

        self.required_buckets=[
            BRONZE_BUCKET_NAME,
            SILVER_BUCKET_NAME,
            GOLD_BUCKET_NAME
        ]

        if auto_init:
            self._initialize_buckets()

    def _initialize_buckets(self):
        """Tự động khởi tạo toàn bộ các buckets hệ thống nếu chưa tồn tại."""
        try:
            print("======================MINIO CLIENT INITIALIZATION======================")
            for bucket in self.required_buckets:
                if not self.storage.make_bucket(bucket):
                    print(f"[CREATE] Bucket '{bucket}' created.")
                else:
                    print(f"[EXIST] Bucket '{bucket}' already exists.")
            print("=======================================================================")
        except S3Error as err:
            raise RuntimeError(f"Error inititializing MinIO buckets: {err}")
        except Exception as e:
            raise RuntimeError(f"Cannot connect to MinIO server: {e}")
        
    #=====================================================================================
    # Core I/O & Low-Level File Operation
    #=====================================================================================

    def write_parquet(self, bucket_name: str, object_name: str, df: pd.DataFrame):
        if df.empty:
            return
        
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression="snappy")
        
        buffer.seek(0, os.SEEK_END)
        parquet_size = buffer.tell()
        buffer.seek(0)

        self.storage.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
    
    def read_parquet(self, bucket_name: str, object_name:str) -> pd.DataFrame:
        response = self.storage.get_object(bucket_name, object_name)
        try:
            data = response.read()
            return pd.read_parquet(io.BytesIO(data))
        finally:
            response.close()
            response.release_conn()

    def upload_file(self, bucket_name: str, object_name: str, local_file_path: str):
        """Upload trực tiếp một file vật lý từ ổ đĩa cứng lên MinIO."""
        self.storage.fput_object(bucket_name, object_name, local_file_path)
        print(f"[UPLOAD] {local_file_path} -> {bucket_name}/{object_name}")

    def download_file(self, bucket_name: str, object_name: str, local_file_path: str):
        """Tải một object từ MinIO xuống một đường dẫn vật lý trên ổ đĩa."""
        self.storage.fget_object(bucket_name, object_name, local_file_path)
        print(f"[DOWNLOAD] {bucket_name}/{object_name} -> {local_file_path}")

    def object_exists(self, bucket_name: str, object_name:str) -> bool:
        """Kiểm tra sự tồn tại của một file/object trên Cloud Storage."""
        try:
            self.storage.stat_object(bucket_name, object_name)
            return True
        except S3Error:
            return False
        
    def delete_object(self, bucket_name:str, object_name: str):
        """Xóa một file chỉ định khỏi bucket."""
        self.storage.remove_objects(bucket_name, object_name)
        print(f"[DELETED] Object: {bucket_name}/{object_name}")

    def list_objects(self, bucket_name:str, prefix: str=""):
        """Liệt kê toàn bộ danh sách object dựa trên tiền tố thư mục."""
        return list(self.storage.list_objects(bucket_name, prefix, recursive=True))
    
    def read_all_parquet(self, bucket_name:str, prefix: str="")-> pd.DataFrame:
        """Quét toàn bộ thư mục và gộp tất cả các file Parquet thành 1 DataFrame lớn."""
        dfs = []
        objects = self.storage.list_objects(bucket_name, prefix=prefix, recursive=True)

        for obj in objects:
            if not obj.object_name.endswith(".parquet"):
                continue
            df = self.read_parquet(bucket_name, obj.object_name)
            dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    #=================================================
    #Silver Layer
    #=================================================
    def write_silver_parquet(self, df: pd.DataFrame, object_name: str):
        self.write_parquet(SILVER_BUCKET_NAME, object_name, df)
    
    def read_silver_parquet(self, object_name: str) -> pd.DataFrame:
        return self.read_parquet(SILVER_BUCKET_NAME, object_name)
    
    def read_all_silver(self, prefix: str = "") -> pd.DataFrame:
        return self.read_all_parquet(SILVER_BUCKET_NAME, prefix=prefix)
    
    #=================================================
    #Gold Layer
    #=================================================
    def write_gold_parquet(self, df: pd.DataFrame, object_name: str):
        self.write_parquet(GOLD_BUCKET_NAME, object_name, df)
    
    def read_gold_parquet(self, object_name: str) -> pd.DataFrame:
        return self.read_parquet(GOLD_BUCKET_NAME, object_name)
    
    def read_all_gold(self, prefix: str = "") -> pd.DataFrame:
        return self.read_all_parquet(GOLD_BUCKET_NAME, prefix=prefix)
    
    #=================================================
    #Bronze Layer & Raw Processing
    #=================================================
    def build_bronze_object_name(self, category: str, year: str|int, month: str|int, chunk_idx: int) -> str:
        """
        Xây dựng đường dẫn chuẩn hóa cho Data Lakehouse.
        Sử dụng phân tách '/' cố định để tránh lỗi dấu gạch chéo ngược '\' của Windows.
        """
        return f"bronze/reviews/year={year}/month={month}/category={category}/part_{chunk_idx:05d}.parquet"
    

    def write_bronze_parquet(self, df: pd.DataFrame, object_name: str):
        self.write_parquet(BRONZE_BUCKET_NAME, object_name, df)
    
    def read_bronze_parquet(self, object_name: str) -> pd.DataFrame:
        return self.read_parquet(BRONZE_BUCKET_NAME, object_name)
    
    def read_all_bronze(self, prefix: str="") -> pd.DataFrame:
        return self.read_all_parquet(BRONZE_BUCKET_NAME, prefix=prefix)
    
    def upload_parquet_to_bronze(self, local_parquet_file: str, object_name: str) -> str:
        """Đẩy trực tiếp file parquet cục bộ lên Bronze Bucket."""
        self.upload_file(BRONZE_BUCKET_NAME, object_name, local_parquet_file)
        return object_name
    
    def download_bronze_file(self, object_name: str) -> str:
        """Tải một file từ Bronze Bucket về thư mục Temp cục bộ và trả về đường dẫn."""
        tempdir = tempfile.mkdtemp()
        local_path = os.path.join(tempdir, os.path.basename(object_name))
        self.download_file(BRONZE_BUCKET_NAME, object_name, local_path)
        return local_path
    
    def list_bronze_file(self, prefix: str="bronze/reviews/") -> List[str]:
        objects = self.list_objects(BRONZE_BUCKET_NAME, prefix=prefix)
        return [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]
    
    def list_raw_files(self, prefix: str="raw/amazon_review/") -> List[str]:
        objects = self.list_objects(BRONZE_BUCKET_NAME, prefix=prefix)
        return [obj.object_name for obj in objects if obj.object_name.endswith(".tsv")]

    #=================================================
    #Utilities & Experimentation
    #=================================================

    def save_sample(self, bucket_name: str, object_name: str, df: pd.DataFrame, sample_size: int=100):
        sample_df = df.head(sample_size)
        self.write_parquet(bucket_name, object_name, sample_df)
        
    def get_object_info(self, bucket_name: str, object_name: str):
        return self.storage.stat_object(bucket_name, object_name)
    
    #I/O json

    def upload_json_to_minio(self, bucket_name: str, object_name: str, data_dict: dict):
        """Chuyển đổi một Python dict thành chuỗi JSON và upload trực tiếp lên minIO"""
        try:
            json_str = json.dumps(data_dict, ensure_ascii=False, indent=4)
            json_bytes = json_str.encode("utf-8")

            buffer = io.BytesIO(json_bytes)

            self.storage.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=buffer,
                length=len(json_bytes),
                content_type="application/json"
            )
            print(f"[MINIO] Uploaded JSON successfully: s3://{bucket_name}/{object_name}")
        except Exception as e:
            print(f"[ERROR] Failed to upload JSON to MinIO: {e}")
            raise e
        
    def read_json_from_minio(self, bucket_name: str, object_name: str) -> dict:
        """Đọc một file JSON từ MinIO và giải mã ngược lại thành Python dict"""
        response = None
        try:
            response = self.storage.get_object(bucket_name, object_name)
            data_bytes = response.read()
            json_data = json.load(data_bytes.decode("utf-8"))
            return json_data
        except Exception as e:
            print(f"[ERROR] Failed to read JSON file from MinIO: {e}")
            raise e
        finally:
            if response:
                response.close()
                response.release_conn()

