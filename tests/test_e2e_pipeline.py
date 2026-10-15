## tests/test_e2e_pipeline.py
import os
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch

from pipeline.storage import PipelineStorage
from pipeline.bronze.processor import (
    optimize_dataframe,
    validate_chunk,
    process_and_save_chunk,
)
from pipeline.silver.cleaner import clean_review_dataframe
from pipeline.silver.validator import create_validation_views
from pipeline.gold.features import create_gold_feature_views
from pipeline.gold.aggregations import create_gold_aggregation_views
import duckdb


@pytest.fixture
def mock_pipeline_storage(tmp_path: Path) -> PipelineStorage:
    """Khởi tạo PipelineStorage trên đĩa tạm pytest."""
    base_dir = tmp_path / "data"
    return PipelineStorage(base_dir=str(base_dir))


def test_e2e_pipeline_flow(mock_pipeline_storage: PipelineStorage):
    storage = mock_pipeline_storage

    # =========================================================================
    # STEP 1: DỮ LIỆU ĐẦU VÀO THÔ (RAW TSV MOCK)
    # =========================================================================
    raw_data = {
        "marketplace": ["US", "US", "US", "US"],
        "customer_id": ["C101", "C102", "C102", "C103"],
        "review_id": ["R1", "R2", "R2", "R3_BAD"],  # R2 bị trùng, R3_BAD rating vi phạm
        "product_id": ["P001", "P001", "P001", "P002"],
        "product_parent": ["100", "100", "100", "200"],
        "product_title": [
            "Wireless Mouse",
            "Wireless Mouse",
            "Wireless Mouse",
            "Keyboard",
        ],
        "product_category": [
            "Electronics",
            "Electronics",
            "Electronics",
            "Electronics",
        ],
        "star_rating": [5, 4, 4, 99],  # 99 -> Invalid
        "helpful_votes": ["10", "2", "2", "0"],
        "total_votes": ["10", "5", "5", "0"],
        "vine": ["N", "N", "N", "N"],
        "verified_purchase": ["Y", "Y", "Y", "N"],
        "review_headline": ["<b>Great item!</b>", "Good value", "Good value", "Bad"],
        "review_body": [
            "Fits perfectly &amp; works fast.",
            "Decent mouse.",
            "Decent mouse.",
            "Error body",
        ],
        "review_date": ["2026-01-10", "2026-01-12", "2026-01-12", "2026-01-15"],
    }
    df_raw = pd.DataFrame(raw_data)

    # =========================================================================
    # STEP 2: XỬ LÝ TẦNG BRONZE
    # =========================================================================
    df_opt = optimize_dataframe(df_raw)
    df_bronze = validate_chunk(df_opt)
    df_bronze["source_file"] = "mock_reviews.tsv"
    df_bronze["category"] = "Electronics"

    bronze_path = process_and_save_chunk(
        df=df_bronze,
        storage=storage,
        category="Electronics",
        year=2026,
        month="01",
        chunk_idx=0,
    )
    assert bronze_path.exists()
    assert "data/bronze/reviews/year=2026/month=01/category=Electronics" in str(
        bronze_path
    )

    # =========================================================================
    # STEP 3: XỬ LÝ TẦNG SILVER (DUCKDB + CLEANER)
    # =========================================================================
    con = duckdb.connect(database=":memory:")
    con.execute(
        f"CREATE TABLE t_bronze AS SELECT * FROM read_parquet('{bronze_path}');"
    )

    create_validation_views(con, "t_bronze")

    valid_silver_df = con.execute("SELECT * FROM v_silver_valid").df()
    invalid_silver_df = con.execute("SELECT * FROM v_silver_invalid").df()

    # R3_BAD bị loại do rating 99 -&gt; 1 bản ghi lỗi
    assert len(invalid_silver_df) == 1
    assert invalid_silver_df["review_id"].iloc[0] == "R3_BAD"

    # R1 và R2 đạt chuẩn (R2 trùng bị khử) -&gt; 2 bản ghi hợp lệ
    assert len(valid_silver_df) == 2

    # ทำ sạch văn bản
    clean_silver_df = clean_review_dataframe(valid_silver_df)

    silver_path = storage.save_parquet(
        clean_silver_df,
        "silver/reviews/year=2026/month=01/category=Electronics/part_00000.parquet",
    )
    assert silver_path.exists()

    # =========================================================================
    # STEP 4: XỬ LÝ TẦNG GOLD (FEATURES + AGGREGATIONS)
    # =========================================================================
    con.execute(
        f"CREATE TABLE t_silver AS SELECT * FROM read_parquet('{silver_path}');"
    )

    create_gold_feature_views(con, "t_silver")
    create_gold_aggregation_views(con, "v_gold_fact_reviews")

    fact_df = con.execute("SELECT * FROM v_gold_fact_reviews").df()
    agg_prod_df = con.execute("SELECT * FROM v_gold_agg_product").df()

    # Kiểm tra Fact Features
    assert "review_length" in fact_df.columns
    assert "helpful_ratio" in fact_df.columns
    assert "rating_bucket" in fact_df.columns
    assert set(fact_df["rating_bucket"]) == {"Positive"}

    # Kiểm tra Aggregations
    assert len(agg_prod_df) == 1  # 1 sản phẩm P001
    assert agg_prod_df["total_reviews"].iloc[0] == 2
    assert agg_prod_df["avg_star_rating"].iloc[0] == 4.5

    con.close()
