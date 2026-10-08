## pipeline/bronze/validate.py
import os
import logging
from configs.schemas.review_schemas import REQUIRED_COLUMNS

logger = logging.getLogger("bronze_validate")


def validate_required_columns(df):
    """Kiểm tra sự tồn tại của các cột bắt buộc trong DataFrame."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Thiếu các cột bắt buộc trong dữ liệu: {missing}")
    return df


def validate_star_rating(df):
    """Lọc dữ liệu rating nằm trong khoảng từ 1 đến 5."""
    if "star_rating" in df.columns:
        return df[(df["star_rating"] >= 1) & (df["star_rating"] <= 5)]
    return df


def validate_helpful_votes(df):
    """Đảm bảo số votes hữu ích không vượt quá tổng số votes."""
    if "helpful_votes" in df.columns and "total_votes" in df.columns:
        return df[df["helpful_votes"] <= df["total_votes"]]
    return df


def write_to_dlq(df, filename):
    """Ghi nhận bản ghi lỗi vào thư mục Dead Letter Queue (DLQ) đệm."""
    try:
        dlq_dir = os.path.join("data", "dlq")
        os.makedirs(dlq_dir, exist_ok=True)

        file_path = os.path.join(dlq_dir, f"{filename}.json")

        if hasattr(df, "to_json"):
            df.to_json(file_path, orient="records", indent=4)
        else:
            import json

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump([{"raw_error": str(df)}], f, indent=4)

        logger.warning(f"[DLQ] Đã lưu bản ghi lỗi vào: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"[DLQ ERROR] Thất bại khi ghi DLQ cho file {filename}: {e}")
        return None
