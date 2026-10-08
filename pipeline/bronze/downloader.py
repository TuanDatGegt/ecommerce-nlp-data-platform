## pipeline/bronze/downloader.py
import os
import glob
import shutil
import logging
import kagglehub
from configs.settings import (
    API_KAGGLE_TOKEN_KEY,
    USERNAME_KAGGLE,
    KAGGLE_DATASET,
    KAGGLEHUB_CACHE_DIR,
)

logger = logging.getLogger("bronze_downloader")


def validate_kaggle_credentials() -> None:
    """Xác thực thông tin tài khoản Kaggle API từ cấu hình."""
    if not USERNAME_KAGGLE or not API_KAGGLE_TOKEN_KEY:
        raise ValueError(
            "[KAGGLE AUTH ERROR] Thiếu credentials (USERNAME/KEY) trong tệp .env"
        )

    os.environ["KAGGLE_USERNAME"] = USERNAME_KAGGLE
    os.environ["KAGGLE_KEY"] = API_KAGGLE_TOKEN_KEY


def download_kaggle_dataset() -> str:
    """Tải dataset từ Kaggle với cơ chế kiểm tra Cache Hit trên đĩa local."""
    validate_kaggle_credentials()

    target_path = os.path.abspath(KAGGLEHUB_CACHE_DIR)
    os.makedirs(target_path, exist_ok=True)

    # Kiểm tra các tệp TSV đã có sẵn
    existing_tsv_files = glob.glob(os.path.join(target_path, "*.tsv"))
    if existing_tsv_files:
        logger.info(
            f"[CACHE HIT] Tìm thấy {len(existing_tsv_files)} tệp TSV có sẵn tại: {target_path}"
        )
        return target_path

    logger.info(f"[DOWNLOAD] Bắt đầu tải dataset từ Kaggle: {KAGGLE_DATASET}")
    try:
        dataset_path = kagglehub.dataset_download(
            handle=KAGGLE_DATASET,
            output_dir=target_path,
            force_download=True,
        )
        logger.info(f"[DOWNLOAD SUCCESS] Đã tải về: {dataset_path}")
        return dataset_path
    except Exception as e:
        logger.error(f"[DOWNLOAD ERROR] Lỗi tải dataset: {e}")
        raise e


def discover_tsv_files(dataset_path: str) -> list[str]:
    """Quét và trả về danh sách đường dẫn các tệp TSV thô."""
    pattern = os.path.join(dataset_path, "*.tsv")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            f"[EXTRACTOR ERROR] Không tìm thấy tệp TSV nào tại: {dataset_path}"
        )

    logger.info(f"[DISCOVER] Đã phát hiện {len(files)} tệp TSV thô.")
    return files


def cleanup_files(path: str) -> None:
    """Dọn dẹp các tệp và thư mục đệm tạm thời sau khi hoàn tất pipeline."""
    if not os.path.exists(path):
        return

    for root, dirs, files in os.walk(path, topdown=False):
        for file in files:
            if file == ".gitkeep":
                continue
            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
            except Exception:
                pass

        for dir_name in dirs:
            if any(
                tmp_tag in dir_name for tmp_tag in ["tempdir", "temp_chunks", "_tmp_"]
            ):
                dir_path = os.path.join(root, dir_name)
                try:
                    shutil.rmtree(dir_path)
                except Exception:
                    pass
