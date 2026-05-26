#pipeline/bronze/sample_writer.py

import os
import shutil

from configs.settings import SAMPLE_DATA_PATH

def save_sample_chunk(parquet_path, category):
    sample_dir = os.path.join(
        SAMPLE_DATA_PATH,
        'bronze',
        'reviews',
        f"category={category}"
    )

    os.makedirs(sample_dir, exist_ok=True)

    sample_path = os.path.join(
        sample_dir,
        "part-00000.parquet"
    )

    if not os.path.exists(sample_path):
        shutil.copy2(parquet_path, sample_path)

        