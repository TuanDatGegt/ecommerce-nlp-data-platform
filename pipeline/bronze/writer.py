## pipeline/bronze/writer.py
import os
import logging
from configs.settings import TEMPDIR_PATH, COMPRESSION

logger = logging.getLogger("bronze_writer")


def write_parquet_chunk(df, output_dir, chunk_index):
    """
    Xuất DataFrame ra file Parquet định dạngpart_{chunk_index:05d}.parquet
    với chuẩn nén Snappy.
    """
    try:
        # Ưu tiên tạo thư mục đích nếu chưa có
        os.makedirs(output_dir, exist_ok=True)

        file_name = f"part_{chunk_index:05d}.parquet"
        file_path = os.path.join(output_dir, file_name)

        # Ghi file Parquet tối ưu dung lượng
        df.to_parquet(file_path, index=False, compression=COMPRESSION)

        logger.info(f"[WRITER] Đã ghi thành công block: {file_path}")
        return file_path
    except Exception as e:
        logger.error(
            f"[WRITER ERROR] Lỗi khi ghi file Parquet tại chunk {chunk_index}: {e}"
        )
        raise e
