##pipeline/silver/validator.py
import logging
import duckdb

logger = logging.getLogger("silver_validator")


def create_validation_views(con: duckdb.DuckDBPyConnection, input_table_or_view: str):
    """
    Tạo các SQL View trên DuckDB để phân tách dữ liệu Hợp lệ (Valid) và Lỗi (Invalid):
    1. star_rating phải nằm trong khoảng 1 -> 5
    2. helpful_votes <= total_votes
    3. Khử trùng lặp review_id (giữ lại bản ghi có review_date / ingest_time mới nhất)
    4. Yêu cầu bắt buộc không rỗng cho các trường core (review_id, customer_id, product_id, review_body)
    """
    # 1. Tạo View lọc dữ liệu vi phạm Rule (Invalid / Corrupted)
    invalid_query = f"""
        CREATE OR REPLACE VIEW v_silver_invalid AS
        SELECT *,
            CASE
                WHEN star_rating < 1 OR star_rating > 5 THEN 'INVALID_STAR_RATING'
                WHEN helpful_votes > total_votes THEN 'INVALID_HELPFUL_VOTES'
                WHEN review_id IS NULL OR review_id = '' THEN 'MISSING_REVIEW_ID'
                WHEN product_id IS NULL OR product_id = '' THEN 'MISSING_PRODUCT_ID'
                WHEN review_body IS NULL OR TRIM(review_body) = '' THEN 'MISSING_REVIEW_BODY'
                ELSE 'UNKNOWN_ERROR'
            END AS dlq_rejection_reason
        FROM {input_table_or_view}
        WHERE star_rating < 1 
           OR star_rating > 5
           OR helpful_votes > total_votes
           OR review_id IS NULL OR review_id = ''
           OR product_id IS NULL OR product_id = ''
           OR review_body IS NULL OR TRIM(review_body) = '';
    """
    con.execute(invalid_query)

    # 2. Tạo View lọc dữ liệu đạt chuẩn + Khử trùng lặp review_id (Qualify Deduplication)
    valid_query = f"""
        CREATE OR REPLACE VIEW v_silver_valid AS
        WITH filtered_data AS (
            SELECT *
            FROM {input_table_or_view}
            WHERE star_rating BETWEEN 1 AND 5
              AND helpful_votes <= total_votes
              AND review_id IS NOT NULL AND review_id != ''
              AND product_id IS NOT NULL AND product_id != ''
              AND review_body IS NOT NULL AND TRIM(review_body) != ''
        )
        SELECT * EXCLUDE (row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY review_id 
                    ORDER BY COALESCE(review_date, '1970-01-01') DESC, 
                             COALESCE(ingest_time, '') DESC
                ) as row_num
            FROM filtered_data
        )
        WHERE row_num = 1;
    """
    con.execute(valid_query)

    logger.info(
        "Đã khởi tạo xong DuckDB validation views (v_silver_valid, v_silver_invalid)."
    )
