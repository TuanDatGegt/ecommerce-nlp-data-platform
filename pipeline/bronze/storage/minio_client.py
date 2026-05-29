#pipeline/bronze/storage/minio_client.py

from minio import Minio
from minio.error import S3Error

from configs.settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE

class MinioCLinent:
    def __init__(self):
        self.client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )

    def bucket_exists(self, bucket_name):
        return self.client.bucket_exists(bucket_name)
    
    def create_bucket(self, bucket_name):
        if not self.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")

    def upload_file(self, bucket_name, object_name, file_path):
        self.client.fput_object(bucket_name, object_name, file_path)
        print(f"Uploaded {file_path} to {bucket_name}/{object_name}")

    def download_file(self, bucket_name, object_name, file_path):
        self.client.fget_object(bucket_name, object_name, file_path)
        print(f"Downloaded {bucket_name}/{object_name} to {file_path}")

    def list_objects(self, bucket_name, prefix=None):
        return self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
    
    def remove_object(self, bucket_name, object_name):
        self.client.remove_object(bucket_name, object_name)
        print(f"Deleted object: {bucket_name}/{object_name}")
