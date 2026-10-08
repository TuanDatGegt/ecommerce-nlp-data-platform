## pipeline/bronze/sample_writer.py
import os
import shutil
import logging
from configs.settings import SAMPLE_DATA_PATH

logger = logging.getLogger("bronze_sample_writer")


def save_sample_chunk(parquet_path, category):
    """
    Trích xuất và lưu giữ khối part-00000.parquet mẫu phục vụ testing.
    """
    if not os.path.exists(parquet_path):
        logger.warning(
            f"[SAMPLE] File nguồn không tồn tại để tạo sample: {parquet_path}"
        )
        return None

    sample_dir = os.path.join(
        SAMPLE_DATA_PATH, "bronze", "reviews", f"category={category}"
    )
    os.makedirs(sample_dir, exist_ok=True)

    sample_target_path = os.path.join(sample_dir, "part-00000.parquet")

    # Chỉ ghi nếu chưa có file sample
    if not os.path.exists(sample_target_path):
        try:
            shutil.copy2(parquet_path, sample_target_path)
            logger.info(
                f"[SAMPLE] Đã lưu mẫu dữ liệu cho category '{category}' tại: {sample_target_path}"
            )
        except Exception as e:
            logger.error(f"[SAMPLE ERROR] Không thể lưu file sample: {e}")

    return sample_target_path
