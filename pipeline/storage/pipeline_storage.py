## pipeline/storage/pipeline_storage.py
import os
import glob
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger("pipeline_storage")


class PipelineStorage:
    """
    Local File System Storage Abstraction Layer.
    Quản lý trực tiếp thư mục data/ trên đĩa local thay thế cho MinIO/S3 Client.
    """

    def __init__(self, base_dir: str = "data"):
        # Chuyển đổi về đường dẫn tuyệt đối dựa trên thư mục gốc dự án
        self.base_dir = Path(base_dir).resolve()
        self._init_directories()

    def _init_directories(self) -> None:
        """Khởi tạo cấu trúc các thư mục lưu trữ cơ bản nếu chưa tồn tại."""
        layers = ["bronze", "silver", "gold", "dlq", "metadata"]
        for layer in layers:
            (self.base_dir / layer).mkdir(parents=True, exist_ok=True)

    def get_full_path(self, relative_path: str) -> Path:
        """Chuyển relative path (vd: 'bronze/reviews/...') thành absolute Path."""
        clean_path = relative_path.lstrip("/").lstrip("\\")
        return self.base_dir / clean_path

    def save_parquet(
        self, df: pd.DataFrame, relative_path: str, compression: str = "snappy"
    ) -> Path:
        """Ghi DataFrame ra tệp Parquet tại thư mục chỉ định trong data/."""
        full_path = self.get_full_path(relative_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(full_path, index=False, compression=compression)
        logger.info(f"[STORAGE] Đã ghi Parquet: {full_path}")
        return full_path

    def read_parquet(self, relative_path_or_pattern: str) -> pd.DataFrame:
        """Đọc tệp Parquet hoặc wildcard pattern."""
        full_path_pattern = str(self.get_full_path(relative_path_or_pattern))
        matching_files = glob.glob(full_path_pattern, recursive=True)

        if not matching_files:
            raise FileNotFoundError(
                f"[STORAGE ERROR] Không tìm thấy file Parquet tại: {full_path_pattern}"
            )

        dataframes = [pd.read_parquet(file_path) for file_path in matching_files]
        return pd.concat(dataframes, ignore_index=True)

    def exists(self, relative_path: str) -> bool:
        """Kiểm tra sự tồn tại của file hoặc thư mục."""
        return self.get_full_path(relative_path).exists()

    def list_files(self, relative_path: str, pattern: str = "*.parquet") -> list[Path]:
        """Liệt kê danh sách các file khớp pattern trong thư mục tương đối."""
        target_dir = self.get_full_path(relative_path)
        if not target_dir.exists():
            return []
        return list(target_dir.glob(pattern))

    def delete_file(self, relative_path: str) -> bool:
        """Xoá một file trong data/ nếu tồn tại."""
        full_path = self.get_full_path(relative_path)
        if full_path.is_file():
            full_path.unlink()
            return True
        return False
