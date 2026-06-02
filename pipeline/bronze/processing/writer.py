#pipeline/bronze/processing/writer.py
import os
import tempfile

from pipeline.bronze.storage.minio_client import MinioClient
#from configs.settings import MINIO_BUCKET_NAME

storage = MinioClient()


def write_parquet_chunk(df, output_prefix, chunk_idx):

    temp_dir = tempfile.mkdtemp()
    local_file = os.path.join(temp_dir, f"part_{chunk_idx:05d}.parquet") 
    df.to_parquet(local_file, index=False, compression='snappy')

    return local_file
