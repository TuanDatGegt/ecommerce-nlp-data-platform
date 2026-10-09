# pipeline/gold/aggregations.py
import logging
import duckdb

logger = logging.getLogger("gold_aggregations")


def create_gold_aggregation_views(
    con: duckdb.DuckDBPyConnection, fact_view: str = "v_gold_fact_reviews"
):
    """
    Tạo các View tổng hợp trên DuckDB:
    1. v_gold_agg_product: Metrics tổng hợp theo sản phẩm (product_id, category)
    2. v_gold_agg_category_monthly: Xu hướng chỉ số theo ngành hàng và tháng
    """
    # 1. Tổng hợp theo Sản phẩm (Product Level)
    product_agg_query = f"""
        CREATE OR REPLACE VIEW v_gold_agg_product AS
        SELECT
            product_id,
            category,
            FIRST(product_title) AS product_title,
            COUNT(review_id) AS total_reviews,
            ROUND(AVG(star_rating), 2) AS avg_star_rating,
            SUM(helpful_votes) AS total_helpful_votes,
            SUM(total_votes) AS total_votes_cast,
            ROUND(AVG(helpful_ratio), 4) AS avg_helpful_ratio,
            ROUND(AVG(review_length), 1) AS avg_review_length,
            COUNT(CASE WHEN rating_bucket = 'Positive' THEN 1 END) AS positive_reviews_count,
            COUNT(CASE WHEN rating_bucket = 'Negative' THEN 1 END) AS negative_reviews_count,
            COUNT(CASE WHEN verified_purchase = TRUE THEN 1 END) AS verified_purchases_count,
            MAX(review_date) AS last_review_date
        FROM {fact_view}
        GROUP BY product_id, category;
    """
    con.execute(product_agg_query)

    # 2. Tổng hợp theo Ngành hàng &amp; Tháng (Category Monthly Trend)
    category_monthly_query = f"""
        CREATE OR REPLACE VIEW v_gold_agg_category_monthly AS
        SELECT
            category,
            review_year AS year,
            review_month AS month,
            COUNT(review_id) AS total_reviews,
            ROUND(AVG(star_rating), 2) AS avg_star_rating,
            ROUND(AVG(helpful_ratio), 4) AS avg_helpful_ratio,
            COUNT(CASE WHEN rating_bucket = 'Positive' THEN 1 END) AS positive_reviews,
            COUNT(CASE WHEN rating_bucket = 'Neutral' THEN 1 END) AS neutral_reviews,
            COUNT(CASE WHEN rating_bucket = 'Negative' THEN 1 END) AS negative_reviews
        FROM {fact_view}
        GROUP BY category, review_year, review_month;
    """
    con.execute(category_monthly_query)

    logger.info(
        "Đã tạo xong DuckDB aggregation views (v_gold_agg_product, v_gold_agg_category_monthly)."
    )
