## pipeline/bronze/downloader.py
import os
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
    Tải và giải nén dataset trực tiếp vào RAW_DATA_PATH.
    Nếu RAW_DATA_PATH đã có TSV, sử dụng trực tiếp (Cache Hit).
    """
    raw_dir = Path(RAW_DATA_PATH).resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)

    # 1. Kiểm tra Cache Hit tại RAW_DATA_PATH
    existing_tsv_files = list(raw_dir.rglob("*.tsv"))
    if existing_tsv_files:
        logger.info(
            f"[CACHE HIT] Tìm thấy {len(existing_tsv_files)} tệp TSV có sẵn tại: {raw_dir}"
        )
        return str(raw_dir)

    validate_kaggle_credentials()
    logger.info(f"[DOWNLOAD] Đang tải dataset từ Kaggle: {KAGGLE_DATASET}")

    try:
        downloaded_path = kagglehub.dataset_download(
            handle=KAGGLE_DATASET,
            output_dir=str(raw_dir),
        )
        logger.info(
            f"[DOWNLOAD SUCCESS] Dataset đã tải và giải nén tại: {downloaded_path}"
        )

        downloaded_tsvs = list(raw_dir.rglob("*.tsv"))
        if not downloaded_tsvs:
            raise FileNotFoundError(
                f"[DOWNLOAD ERROR] Không tìm thấy tệp TSV sau khi tải vào: {raw_dir}"
            )

        return str(raw_dir)

    except Exception as e:
        logger.error(f"[DOWNLOAD ERROR] Lỗi khi tải hoặc đồng bộ dataset: {e}")
        raise


def discover_tsv_files(dataset_path: str) -> list[str]:
    """Quét và trả về danh sách các tệp TSV thô tại thư mục chỉ định."""
    files = [str(path) for path in Path(dataset_path).rglob("*.tsv")]

    if not files:
        raise FileNotFoundError(
            f"[EXTRACTOR ERROR] Không tìm thấy tệp TSV nào trong thư mục: {dataset_path}"
        )

    logger.info(f"[DISCOVER] Tìm thấy {len(files)} tệp TSV thô tại {dataset_path}.")
    return files
