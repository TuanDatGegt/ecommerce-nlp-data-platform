## tests/test_bronze_pipeline.py
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch

from pipeline.storage import PipelineStorage
from pipeline.bronze.downloader import download_kaggle_dataset, discover_tsv_files
from pipeline.bronze.processor import (
    extract_category_from_filename,
    optimize_dataframe,
    validate_chunk,
    process_and_save_chunk,
)
from pipeline.bronze.pipeline import is_file_processed, mark_file_as_processed


def test_extract_category():
    filename = "amazon_reviews_us_Books_v1_00.tsv"
    category = extract_category_from_filename(filename)
    assert category == "Books"


def test_optimize_and_validate_chunk():
    raw_data = {
        "review_id": ["R1", "R2", "R2", "R3"],
        "customer_id": ["C1", "C2", "C2", "C3"],
        "product_id": ["P1", "P2", "P2", "P3"],
        "product_category": ["Books", "Books", "Books", "Books"],
        "review_headline": ["Great", "Okay", "Okay", "Bad"],
        "review_date": ["2026-01-01", "2026-01-02", "2026-01-02", "2026-01-03"],
        "star_rating": ["5", "3", "3", "99"],
        "helpful_votes": ["10", "2", "2", "0"],
        "total_votes": ["10", "5", "5", "0"],
        "verified_purchase": ["Y", "N", "N", "Y"],
        "review_body": ["Great book!", "Okay text", "Okay text", "Bad rating"],
    }
    df = pd.DataFrame(raw_data)

    df_opt = optimize_dataframe(df)
    assert df_opt["star_rating"].iloc[0] == 5

    df_valid = validate_chunk(df_opt)
    assert len(df_valid) == 2
    assert set(df_valid["review_id"]) == {"R1", "R2"}


def test_process_and_save_chunk(tmp_path: Path):
    storage = PipelineStorage(base_dir=str(tmp_path / "data"))
    df = pd.DataFrame(
        {
            "review_id": ["R1"],
            "star_rating": [1],
            "review_body": ["Sample review"],
        }
    )

    saved_path = process_and_save_chunk(
        df=df,
        storage=storage,
        category="Books",
        year=2026,
        month="10",
        chunk_idx=0,
    )

    assert saved_path.exists()
    assert (
        "data/bronze/reviews/year=2026/month=10/category=Books/part_00000.parquet"
        in str(saved_path)
    )


@patch("pipeline.bronze.downloader.kagglehub.dataset_download")
def test_download_kaggle_dataset_native(mock_download):
    mock_download.return_value = "/mock/kagglehub/cache/dataset"

    with patch.dict(
        "os.environ",
        {
            "USERNAME_KAGGLE": "test_user",
            "API_KAGGLE_TOKEN_KEY": "test_key",
        },
    ):
        dataset_path = download_kaggle_dataset()
        assert dataset_path == "/mock/kagglehub/cache/dataset"
        mock_download.assert_called_once()


def test_checkpoint_processed_files(tmp_path: Path):
    test_manifest = tmp_path / "data" / "metadata" / "processed_files.json"

    with patch("pipeline.bronze.pipeline.CHECKPOINT_PATH", test_manifest):
        file_name = "amazon_reviews_us_Electronics_v1_00.tsv"

        assert is_file_processed(file_name) is False
        mark_file_as_processed(file_name, record_count=1500)
        assert is_file_processed(file_name) is True
