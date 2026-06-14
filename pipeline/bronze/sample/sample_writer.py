#pipeline/bronze/sample_writer.py

import os
import shutil
import logging

from configs.settings import SAMPLE_DATA_PATH

logger = logging.getLogger("bronze_pipeline")

def save_sample_chunk(parquet_path, category):
    sample_dir = os.path.join(
        SAMPLE_DATA_PATH,
        'bronze',
        'reviews',
        f"category={category}"
    )

    try:
        os.makedirs(sample_dir, exist_ok=True)
    except Exception as e:
        logger.debug(f"[SAMPLE DIR] Tranh chap quyen tao thu muc song song: {e}")
    

    sample_path = os.path.join(sample_dir, "part-00000.parquet")
    file_exits_and_valid = os.path.exists(sample_path) and os.path.getsize(sample_path) > 0


    if not file_exits_and_valid:
        try:
            if os.path.exists(parquet_path):
                shutil.copy2(parquet_path, sample_path)
                logger.info(f"[SAMPLE SUCCESS] Da luu tru du lieu mau local cho {category} -> {sample_path}")
        except Exception as copy_err:
            logger.warning(f"[SAMPLE ERROR] Khong the sao chep file mau: {str(copy_err)}")        

        