#pipeline/bronze/processing/writer.py
import os
import tempfile

from pipeline.bronze.storage.minio_client import MinioClient
from configs.settings import MINIO_BUCKET_NAME

storage = MinioClient()


def write_parquet_chunk(df, output_prefix, chunk_idx):

    temp_dir = tempfile.kwtempdir()
    local_file = os.path.join(temp_dir, f"part_{chunk_idx:05d}.parquet")
 
    df.to_parquet(local_file, index=False, compression='snappy')

    object_name = os.path.join(output_prefix, f"part_{chunk_idx:05d}.parquet")

    storage.upload_file(buket_name=MINIO_BUCKET_NAME, object_name=object_name, file_path=local_file)

    os.remove(local_file)

    return object_name



