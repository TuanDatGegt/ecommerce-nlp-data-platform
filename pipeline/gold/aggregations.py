## pipeline/gold/aggregations.py
import logging
import duckdb

logger = logging.getLogger("gold_aggregations")


def process_dashboard_aggregations_by_category(
    con: duckdb.DuckDBPyConnection,
    category_name: str,
    top_n_products: int = 1000,
):
    """
    Tạo các bảng thống kê Dashboard cho 1 Category từ bảng tạm `tmp_nlp_features`.
    """
    # 1. category_month_stats
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_cat_month_stats AS
        SELECT 
            category, year, month,
            COUNT(review_id) AS review_count,
            ROUND(AVG(star_rating), 2) AS avg_rating,
            COUNT(CASE WHEN star_rating = 1 THEN 1 END) AS star1,
            COUNT(CASE WHEN star_rating = 2 THEN 1 END) AS star2,
            COUNT(CASE WHEN star_rating = 3 THEN 1 END) AS star3,
            COUNT(CASE WHEN star_rating = 4 THEN 1 END) AS star4,
            COUNT(CASE WHEN star_rating = 5 THEN 1 END) AS star5,
            ROUND(COUNT(CASE WHEN verified_purchase = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS verified_pct,
            0.0 AS vine_pct,
            ROUND(AVG(helpful_votes), 2) AS avg_helpful_votes,
            ROUND(AVG(CASE WHEN total_votes > 0 THEN CAST(helpful_votes AS DOUBLE) / total_votes ELSE 0 END), 4) AS helpful_ratio,
            ROUND(AVG(body_len_chars), 1) AS avg_review_len,
            ROUND(AVG(LENGTH(clean_headline)), 1) AS avg_headline_len,
            COUNT(DISTINCT review_id) AS unique_reviews,
            COUNT(DISTINCT product_id) AS unique_products
        FROM tmp_nlp_features
        GROUP BY category, year, month;
    """)

    # 2. product_stats (Top N)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_product_stats AS
        WITH prod_agg AS (
            SELECT 
                category, product_id,
                COUNT(review_id) AS review_count,
                ROUND(AVG(star_rating), 2) AS avg_rating,
                ROUND(COUNT(CASE WHEN verified_purchase = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS verified_pct,
                MIN(year || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') || '-01') AS first_review_date,
                MAX(year || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') || '-28') AS last_review_date,
                ROW_NUMBER() OVER(ORDER BY COUNT(review_id) DESC) AS rank
            FROM tmp_nlp_features
            GROUP BY category, product_id
        )
        SELECT category, product_id, '' AS product_title, review_count, avg_rating, verified_pct, first_review_date, last_review_date
        FROM prod_agg
        WHERE rank <;= {top_n_products};
    """)

    # 3. customer_stats (Histogram)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_customer_stats AS
        WITH cust_counts AS (
            SELECT category, review_id, COUNT(*) AS user_rev_count
            FROM tmp_nlp_features
            GROUP BY category, review_id
        )
        SELECT 
            category,
            CASE 
                WHEN user_rev_count = 1 THEN '1_review'
                WHEN user_rev_count BETWEEN 2 AND 5 THEN '2-5_reviews'
                WHEN user_rev_count BETWEEN 6 AND 10 THEN '6-10_reviews'
                ELSE '>10_reviews'
            END AS review_bucket,
            COUNT(*) AS customer_count
        FROM cust_counts
        GROUP BY category, review_bucket;
    """)

    # 4. helpfulness_buckets
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_helpfulness_buckets AS
        SELECT 
            category,
            CASE 
                WHEN helpful_votes = 0 THEN '0_votes'
                WHEN helpful_votes BETWEEN 1 AND 5 THEN '1-5_votes'
                WHEN helpful_votes BETWEEN 6 AND 20 THEN '6-20_votes'
                ELSE '>20_votes'
            END AS helpful_bucket,
            COUNT(*) AS review_count
        FROM tmp_nlp_features
        GROUP BY category, helpful_bucket;
    """)

    # 5. rating_verified_compare
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_rating_verified_compare AS
        SELECT 
            category, verified_purchase, FALSE AS vine,
            COUNT(*) AS review_count,
            ROUND(AVG(star_rating), 2) AS avg_rating
        FROM tmp_nlp_features
        GROUP BY category, verified_purchase;
    """)


def consolidate_global_dashboard_tables(storage):
    """
    Gộp các bản phần của 37 category thành các file Parquet duy nhất cho Dashboard.
    """
    con = duckdb.connect(database=":memory:")
    try:
        dash_dir = storage.get_full_path("gold/dashboard")
        parts_dir = dash_dir / "parts"

        # 1. category_month_stats
        con.execute(
            f"CREATE OR REPLACE TABLE cat_month AS SELECT * FROM read_parquet('{parts_dir / 'cat_month_*.parquet'}');"
        )
        con.execute(
            f"COPY cat_month TO '{dash_dir / 'category_month_stats.parquet.tmp'}' (FORMAT PARQUET, COMPRESSION 'zstd');"
        )
        (dash_dir / "category_month_stats.parquet.tmp").replace(
            dash_dir / "category_month_stats.parquet"
        )

        # 2. category_year_stats
        con.execute("""
            CREATE OR REPLACE TABLE cat_year AS
            SELECT 
                category, year,
                SUM(review_count) AS review_count,
                ROUND(AVG(avg_rating), 2) AS avg_rating,
                SUM(star1) AS star1, SUM(star2) AS star2, SUM(star3) AS star3, SUM(star4) AS star4, SUM(star5) AS star5,
                ROUND(AVG(verified_pct), 2) AS verified_pct,
                ROUND(AVG(avg_helpful_votes), 2) AS avg_helpful_votes,
                ROUND(AVG(avg_review_len), 1) AS avg_review_len,
                SUM(unique_reviews) AS unique_reviews,
                SUM(unique_products) AS unique_products
            FROM cat_month
            GROUP BY category, year;
        """)
        con.execute(
            f"COPY cat_year TO '{dash_dir / 'category_year_stats.parquet.tmp'}' (FORMAT PARQUET, COMPRESSION 'zstd');"
        )
        (dash_dir / "category_year_stats.parquet.tmp").replace(
            dash_dir / "category_year_stats.parquet"
        )

    finally:
        con.close()
