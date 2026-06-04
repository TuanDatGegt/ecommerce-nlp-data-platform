#pipeline/bronze/processing/writer.py
import os
import tempfile

from pipeline.bronze.storage.minio_client import MinioClient
from configs.settings import TEMPDIR_PATH

storage = MinioClient()


def write_parquet_chunk(df, output_prefix, chunk_idx):
    os.makedirs(TEMPDIR_PATH, exist_ok=True)

    temp_dir = tempfile.mkdtemp(dir=TEMPDIR_PATH, prefix=f"{output_prefix}_tmp_")
    local_file = os.path.join(temp_dir, f"part_{chunk_idx:05d}.parquet") 
    df.to_parquet(local_file, index=False, compression='snappy')

    return local_file
