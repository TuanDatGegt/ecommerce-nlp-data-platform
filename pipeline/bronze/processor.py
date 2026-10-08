## pipeline/bronze/processor.py
import re
import csv
import uuid
import logging
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from configs.settings import CHUNK_SIZE, COMPRESSION, CATEGORY_MAPPING
from configs.schemas.review_schemas import REQUIRED_COLUMNS
from pipeline.storage import PipelineStorage

logger = logging.getLogger("bronze_processor")


def read_tsv_in_chunks(input_file: str):
    """Đọc tệp TSV thô theo khối (chunking) để tối ưu RAM."""
    return pd.read_csv(
        input_file,
        sep="\t",
        chunksize=CHUNK_SIZE,
        engine="python",
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",
        dtype=str,
    )


def extract_category_from_filename(file_name: str) -> str:
    """Bóc tách tên ngành hàng từ tên tệp TSV nguồn."""
    match = re.search(CATEGORY_MAPPING, file_name)
    return match.group(1) if match else "Unknown"


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ép kiểu dữ liệu nhỏ gọn để tiết kiệm bộ nhớ RAM."""
    df = df.copy()

    # Ép kiểu Boolean
    bool_map = {"y": True, "n": False, "true": True, "false": False}
    for col in ["vine", "verified_purchase"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map(bool_map).fillna(False)

    # Ép kiểu Category
    for col in ["marketplace", "product_category"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # Ép kiểu Numeric
    for col in ["star_rating", "helpful_votes", "total_votes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Ép kiểu Datetime
    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")

    return df


def add_metadata_columns(
    df: pd.DataFrame, source_file: str, batch_id: str = None
) -> pd.DataFrame:
    """Bổ sung metadata dòng dữ liệu để truy vết lineage."""
    df["source_file"] = source_file
    df["ingest_time"] = datetime.now(timezone.utc).isoformat()
    df["batch_id"] = batch_id if batch_id else str(uuid.uuid4())
    return df


def validate_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Kiểm tra và lọc chất lượng dữ liệu cơ bản ở tầng Bronze."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if "star_rating" in df.columns:
        df = df[(df["star_rating"] >= 1) & (df["star_rating"] <= 5)]

    if "helpful_votes" in df.columns and "total_votes" in df.columns:
        df = df[df["helpful_votes"] <= df["total_votes"]]

    if "review_body" in df.columns:
        df = df[df["review_body"].notna()]

    if "review_id" in df.columns:
        df = df.drop_duplicates(subset=["review_id"])

    return df


def process_and_save_chunk(
    df: pd.DataFrame,
    storage: PipelineStorage,
    category: str,
    year: int,
    month: str,
    chunk_idx: int,
) -> Path:
    """Ghi trực tiếp DataFrame ra tệp Parquet phân vùng tại data/bronze/."""
    relative_path = f"bronze/reviews/year={year}/month={month}/category={category}/part_{chunk_idx:05d}.parquet"
    return storage.save_parquet(df, relative_path, compression=COMPRESSION)
