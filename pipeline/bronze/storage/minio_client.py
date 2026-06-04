#pipeline/bronze/storage/minio_client.py

from minio import Minio
from minio.error import S3Error

from configs.settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE

class MinioClient:
    def __init__(self, auto_init=False):
        self.client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )

        self.required_buckets = [
            "ecommerce-reviews-bronze", 
            "ecommerce-reviews-silver", 
            "ecommerce-reviews-gold"
        ]
        if auto_init:
            self._auto_init_buckets()

    def _auto_init_buckets(self):
        """Function locally automatically creates the required buckets if they don't exist."""
        try:
            print(f"============== MINIO CLIENT INITIALIZATION ==============")
            for bucket_name in self.required_buckets:
                if not self.client.bucket_exists(bucket_name):
                    self.client.make_bucket(bucket_name)
                    print(f"[CREATED] Bucket '{bucket_name}' created.")
                else:
                    print(f"[EXISTS] Bucket '{bucket_name}' already exists.")
            print(f"==========================================================")

        except S3Error as e:
            raise RuntimeError(f"Error initializing MinIO client: {e}")
        except Exception as e:
            raise RuntimeError(f"Cannot connect to MinIO server: {e}")


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
