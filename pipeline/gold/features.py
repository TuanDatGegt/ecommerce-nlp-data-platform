## pipeline/gold/features.py
import logging
import duckdb

logger = logging.getLogger("gold_features")


def create_gold_feature_views(con: duckdb.DuckDBPyConnection, silver_input_view: str):
    """
    Tạo SQL View 'v_gold_fact_reviews' trên DuckDB để tính toán các thuộc tính bổ sung:
    1. review_length: Độ dài đoạn văn đánh giá (ký tự)
    2. headline_length: Độ dài tiêu đề đánh giá
    3. helpful_ratio: Tỷ lệ đánh giá hữu ích (helpful_votes / total_votes)
    4. rating_bucket: Phân nhóm cảm xúc dựa trên số sao (Positive: 4-5*, Neutral: 3*, Negative: 1-2*)
    5. is_extreme_rating: Cờ nhận diện đánh giá cực đoan (1* hoặc 5*)
    """
    query = f"""
        CREATE OR REPLACE VIEW v_gold_fact_reviews AS
        SELECT
            review_id,
            customer_id,
            product_id,
            product_title,
            product_category,
            star_rating,
            helpful_votes,
            total_votes,
            vine,
            verified_purchase,
            review_date,
            review_headline,
            review_body,
            category,
            source_file,
            batch_id,
            ingest_time,
            
            -- Calculated Features
            LENGTH(COALESCE(review_body, '')) AS review_length,
            LENGTH(COALESCE(review_headline, '')) AS headline_length,
            
            CASE 
                WHEN total_votes > 0 THEN CAST(helpful_votes AS DOUBLE) / total_votes 
                ELSE 0.0 
            END AS helpful_ratio,
            
            CASE 
                WHEN star_rating >= 4 THEN 'Positive'
                WHEN star_rating = 3 THEN 'Neutral'
                ELSE 'Negative'
            END AS rating_bucket,
            
            CASE 
                WHEN star_rating = 1 OR star_rating = 5 THEN TRUE 
                ELSE FALSE 
            END AS is_extreme_rating,
            
            YEAR(COALESCE(TRY_CAST(review_date AS DATE), CURRENT_DATE)) AS review_year,
            MONTH(COALESCE(TRY_CAST(review_date AS DATE), CURRENT_DATE)) AS review_month
            
        FROM {silver_input_view};
    """
    con.execute(query)
    logger.info("Đã tạo xong DuckDB feature view: v_gold_fact_reviews.")
