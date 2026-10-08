## pipeline/bronze/downloader.py
import os
import glob
import logging
import kagglehub
from configs.settings import (
    API_KAGGLE_TOKEN_KEY,
    USERNAME_KAGGLE,
    KAGGLE_DATASET,
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
    """Tải dataset sử dụng cơ chế cache nội bộ mặc định của KaggleHub."""
    validate_kaggle_credentials()

    logger.info(f"[DOWNLOAD] Initiating dataset retrieval: {KAGGLE_DATASET}")
    try:
        # KaggleHub tự động quản lý cache theo đường dẫn chuẩn của OS mà không cần custom folder
        dataset_path = kagglehub.dataset_download(handle=KAGGLE_DATASET)
        logger.info(f"[DOWNLOAD SUCCESS] Dataset ready at: {dataset_path}")
        return dataset_path
    except Exception as e:
        logger.error(f"[DOWNLOAD ERROR] Failed to retrieve dataset: {e}")
        raise e


def discover_tsv_files(dataset_path: str) -> list[str]:
    """Quét và trả về danh sách các tệp TSV thô."""
    pattern = os.path.join(dataset_path, "*.tsv")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            f"[EXTRACTOR ERROR] No TSV files found in directory: {dataset_path}"
        )

    logger.info(f"[DISCOVER] Discovered {len(files)} raw TSV file(s).")
    return files
