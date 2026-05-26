#pipeline/bronze/writer.py
import os

def write_parquet_chunk(df, output_dir, chunk_index):
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"part_{chunk_index:05d}.parquet")

    df.to_parquet(file_path, index=False, compression='snappy')

    return file_path

