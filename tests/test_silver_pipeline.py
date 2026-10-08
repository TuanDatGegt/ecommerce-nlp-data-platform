## tests/test_silver_pipeline.py
import duckdb
import pandas as pd
from pathlib import Path
from pipeline.storage import PipelineStorage
from pipeline.silver.cleaner import (
    strip_html_tags,
    normalize_text,
    clean_review_dataframe,
)
from pipeline.silver.validator import create_validation_views


def test_text_cleaner_functions():
    raw_html = "<p>This product is <b>AMAZING</b>! &amp; cheap.</p>"
    cleaned = normalize_text(raw_html)
    assert "<p>" not in cleaned
    assert "<b>" not in cleaned
    assert "&amp;" not in cleaned
    assert cleaned == "this product is amazing ! cheap."


def test_clean_review_dataframe():
    df = pd.DataFrame(
        {"review_headline": ["<b>Great!</b>"], "review_body": [" Very good value."]}
    )
    df_clean = clean_review_dataframe(df)
    assert df_clean["review_headline"].iloc[0] == "great!"
    assert "script" not in df_clean["review_body"].iloc[0]


def test_silver_duckdb_validator():
    con = duckdb.connect(database=":memory:")

    # Dataset thử nghiệm chứa cả bản ghi hợp lệ lẫn vi phạm
    test_data = pd.DataFrame(
        {
            "review_id": [
                "R1",
                "R2",
                "R2",
                "R3",
                "R4",
            ],  # R2 trùng, R3 rating lỗi, R4 body rỗng
            "product_id": ["P1", "P2", "P2", "P3", "P4"],
            "star_rating": [5, 4, 4, 99, 3],
            "helpful_votes": [2, 1, 1, 0, 0],
            "total_votes": [2, 1, 1, 0, 0],
            "review_body": ["Good product", "Nice item", "Nice item", "Bad rating", ""],
            "category": ["Books", "Books", "Books", "Books", "Books"],
            "review_date": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-01",
                "2026-01-01",
            ],
            "ingest_time": [
                "2026-10-01",
                "2026-10-01",
                "2026-10-02",
                "2026-10-01",
                "2026-10-01",
            ],
        }
    )

    con.execute("CREATE TABLE t_bronze AS SELECT * FROM test_data;")
    create_validation_views(con, "t_bronze")

    valid_count = con.execute("SELECT COUNT(*) FROM v_silver_valid").fetchone()[0]
    invalid_count = con.execute("SELECT COUNT(*) FROM v_silver_invalid").fetchone()[0]

    # R1 (Valid), R2 (Deduplicated -&gt; giữ 1), R3 (Invalid rating), R4 (Invalid body)
    assert valid_count == 2
    assert invalid_count == 2

    valid_ids = (
        con.execute("SELECT review_id FROM v_silver_valid").df()["review_id"].tolist()
    )
    assert set(valid_ids) == {"R1", "R2"}

    con.close()
