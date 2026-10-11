## pipeline/bronze/downloader.py
import os
import glob
import shutil
import logging
import kagglehub
from pathlib import Path
from configs.settings import (
    API_KAGGLE_TOKEN_KEY,
    USERNAME_KAGGLE,
    KAGGLE_DATASET,
    RAW_DATA_PATH,
)

logger = logging.getLogger("bronze_downloader")


def validate_kaggle_credentials() -> None:
    """Xác thực thông tin tài khoản Kaggle API từ cấu hình môi trường."""
    if not USERNAME_KAGGLE or not API_KAGGLE_TOKEN_KEY:
        raise ValueError(
            "[KAGGLE AUTH ERROR] Thiếu credentials (USERNAME/KEY) trong tệp .env"
        )

    os.environ["KAGGLE_USERNAME"] = USERNAME_KAGGLE
    os.environ["KAGGLE_KEY"] = API_KAGGLE_TOKEN_KEY
    logger.info("Kaggle API credentials validated successfully.")


def download_kaggle_dataset() -> str:
    """
    Tải dataset từ KaggleHub và lưu trữ/đồng bộ vào thư mục data/raw/ của dự án.
    Nếu data/raw/ đã có sẵn các tệp TSV, sử dụng trực tiếp (Cache Hit).
    """
    raw_dir = Path(RAW_DATA_PATH if RAW_DATA_PATH else "data/raw").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)

    # 1. Kiểm tra Cache Hit tại thư mục local data/raw/
    existing_tsv_files = glob.glob(os.path.join(raw_dir, "*.tsv"))
    if existing_tsv_files:
        logger.info(
            f"[CACHE HIT] Tìm thấy {len(existing_tsv_files)} tệp TSV có sẵn tại: {raw_dir}"
        )
        return str(raw_dir)

    # 2. Xử lý tải về qua KaggleHub nếu chưa có trong data/raw/
    validate_kaggle_credentials()
    logger.info(f"[DOWNLOAD] Đang tải dataset từ Kaggle: {KAGGLE_DATASET}")

    try:
        # KaggleHub tải về thư mục cache mặc định của OS
        cache_dataset_path = kagglehub.dataset_download(handle=KAGGLE_DATASET)
        logger.info(f"[DOWNLOAD SUCCESS] Dataset đã tải về cache: {cache_dataset_path}")

        # 3. Đồng bộ/Sao chép các tệp TSV từ KaggleHub cache về data/raw/
        downloaded_tsvs = glob.glob(os.path.join(cache_dataset_path, "*.tsv"))
        if not downloaded_tsvs:
            # Tìm kiếm đệ quy nếu tệp TSV nằm trong thư mục con
            downloaded_tsvs = glob.glob(
                os.path.join(cache_dataset_path, "**", "*.tsv"), recursive=True
            )

        for tsv_file in downloaded_tsvs:
            dest_file = raw_dir / os.path.basename(tsv_file)
            if not dest_file.exists():
                shutil.copy2(tsv_file, dest_file)
                logger.info(
                    f"[SYNC] Đã chuyển tệp {os.path.basename(tsv_file)} -&gt; {raw_dir}"
                )

        return str(raw_dir)

    except Exception as e:
        logger.error(f"[DOWNLOAD ERROR] Lỗi khi tải hoặc đồng bộ dataset: {e}")
        raise e


def discover_tsv_files(dataset_path: str) -> list[str]:
    """Quét và trả về danh sách các tệp TSV thô tại thư mục chỉ định (data/raw/)."""
    pattern = os.path.join(dataset_path, "*.tsv")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            f"[EXTRACTOR ERROR] Không tìm thấy tệp TSV nào trong thư mục: {dataset_path}"
        )

    logger.info(f"[DISCOVER] Tìm thấy {len(files)} tệp TSV thô tại {dataset_path}.")
    return files
