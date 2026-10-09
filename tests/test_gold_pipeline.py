## tests/test_gold_pipeline.py
import duckdb
import pandas as pd
from pipeline.gold.features import create_gold_feature_views
from pipeline.gold.aggregations import create_gold_aggregation_views


def test_gold_feature_views():
    con = duckdb.connect(database=":memory:")

    test_silver_data = pd.DataFrame(
        {
            "review_id": ["R1", "R2"],
            "customer_id": ["C1", "C2"],
            "product_id": ["P1", "P1"],
            "product_title": ["Phone Case", "Phone Case"],
            "product_category": ["Electronics", "Electronics"],
            "star_rating": [5, 2],
            "helpful_votes": [8, 1],
            "total_votes": [10, 2],
            "vine": [False, False],
            "verified_purchase": [True, True],
            "review_date": ["2026-01-15", "2026-01-20"],
            "review_headline": ["Awesome", "Bad"],
            "review_body": ["Fits perfectly on my phone!", "Broke after two days."],
            "category": ["Electronics", "Electronics"],
            "source_file": ["test.tsv", "test.tsv"],
            "batch_id": ["b1", "b1"],
            "ingest_time": ["2026-10-01", "2026-10-01"],
        }
    )

    con.execute("CREATE TABLE t_silver AS SELECT * FROM test_silver_data;")
    create_gold_feature_views(con, "t_silver")

    df_features = con.execute("SELECT * FROM v_gold_fact_reviews").df()

    assert len(df_features) == 2
    assert "review_length" in df_features.columns
    assert "helpful_ratio" in df_features.columns
    assert "rating_bucket" in df_features.columns

    # R1: 5 sao -&gt; Positive, helpful_ratio = 8/10 = 0.8
    r1 = df_features[df_features["review_id"] == "R1"].iloc[0]
    assert r1["rating_bucket"] == "Positive"
    assert abs(r1["helpful_ratio"] - 0.8) < 1e-4

    # R2: 2 sao -&gt; Negative, helpful_ratio = 1/2 = 0.5
    r2 = df_features[df_features["review_id"] == "R2"].iloc[0]
    assert r2["rating_bucket"] == "Negative"
    assert abs(r2["helpful_ratio"] - 0.5) < 1e-4

    con.close()


def test_gold_aggregation_views():
    con = duckdb.connect(database=":memory:")

    test_silver_data = pd.DataFrame(
        {
            "review_id": ["R1", "R2", "R3"],
            "customer_id": ["C1", "C2", "C3"],
            "product_id": ["P1", "P1", "P2"],
            "product_title": ["Item A", "Item A", "Item B"],
            "product_category": ["Books", "Books", "Books"],
            "star_rating": [5, 4, 1],
            "helpful_votes": [10, 5, 0],
            "total_votes": [10, 5, 2],
            "vine": [False, False, False],
            "verified_purchase": [True, True, False],
            "review_date": ["2026-01-10", "2026-01-15", "2026-01-20"],
            "review_headline": ["H1", "H2", "H3"],
            "review_body": ["Body 1", "Body 2", "Body 3"],
            "category": ["Books", "Books", "Books"],
            "source_file": ["f1", "f1", "f1"],
            "batch_id": ["b1", "b1", "b1"],
            "ingest_time": ["t1", "t1", "t1"],
        }
    )

    con.execute("CREATE TABLE t_silver AS SELECT * FROM test_silver_data;")
    create_gold_feature_views(con, "t_silver")
    create_gold_aggregation_views(con, "v_gold_fact_reviews")

    df_prod = con.execute(
        "SELECT * FROM v_gold_agg_product WHERE product_id = 'P1'"
    ).df()
    assert len(df_prod) == 1
    p1 = df_prod.iloc[0]
    assert p1["total_reviews"] == 2
    assert p1["avg_star_rating"] == 4.5
    assert p1["positive_reviews_count"] == 2

    con.close()
