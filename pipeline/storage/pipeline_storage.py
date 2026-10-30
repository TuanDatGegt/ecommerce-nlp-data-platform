## pipeline/storage/pipeline_storage.py
import os
import glob
import logging
from pathlib import Path
import pandas as pd
from configs.settings import DATA_LAKE_PATH

logger = logging.getLogger("pipeline_storage")


class PipelineStorage:
    """
    Local File System Storage Abstraction Layer.
    Quản lý trực tiếp thư mục DATA_LAKE_PATH trên đĩa local.
    """

    def __init__(self, base_dir: str | Path | None = None):
        if base_dir is None:
            self.base_dir = Path(DATA_LAKE_PATH).resolve()
        else:
            self.base_dir = Path(base_dir).resolve()
        self._init_directories()

    def _init_directories(self) -> None:
        """Khởi tạo cấu trúc các thư mục lưu trữ cơ bản nếu chưa tồn tại."""
        layers = ["bronze", "silver", "gold", "dlq", "metadata"]
        for layer in layers:
            (self.base_dir / layer).mkdir(parents=True, exist_ok=True)

    def get_full_path(self, relative_path: str | Path) -> Path:
        """Chuyển relative path thành absolute Path an toàn."""
        clean_path = str(relative_path).lstrip("/").lstrip("\\")
        return self.base_dir / clean_path

    def save_parquet(
        self, df: pd.DataFrame, relative_path: str | Path, compression: str = "zstd"
    ) -> Path:
        """Ghi DataFrame ra tệp Parquet tại thư mục chỉ định."""
        full_path = self.get_full_path(relative_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(full_path, index=False, compression=compression)
        logger.info(f"[STORAGE] Đã ghi Parquet: {full_path}")
        return full_path

    def read_parquet(self, relative_path_or_pattern: str | Path) -> pd.DataFrame:
        """Đọc tệp Parquet hoặc wildcard pattern."""
        full_path_pattern = str(self.get_full_path(relative_path_or_pattern))
        matching_files = glob.glob(full_path_pattern, recursive=True)

        if not matching_files:
            raise FileNotFoundError(
                f"[STORAGE ERROR] Không tìm thấy file Parquet tại: {full_path_pattern}"
            )

        dataframes = [pd.read_parquet(file_path) for file_path in matching_files]
        return pd.concat(dataframes, ignore_index=True)

    def exists(self, relative_path: str | Path) -> bool:
        """Kiểm tra sự tồn tại của file hoặc thư mục."""
        return self.get_full_path(relative_path).exists()
