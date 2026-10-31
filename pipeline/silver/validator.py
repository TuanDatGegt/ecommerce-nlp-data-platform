## pipeline/silver/validator.py
import logging
import duckdb

logger = logging.getLogger("silver_validator")

# `... IS TRUE` biến NULL thành FALSE => dòng có star_rating/helpful_votes/total_votes/... NULL
# sẽ rơi vào nhánh INVALID (trước đây rơi vào CẢ HAI view nên mất tích không dấu vết).
VALID_CONDITION = """(
        star_rating BETWEEN 1 AND 5
        AND helpful_votes <= total_votes
        AND review_id <> ''
        AND product_id <> ''
        AND TRIM(review_body) <> ''
    ) IS TRUE"""


def create_validation_views(con: duckdb.DuckDBPyConnection, input_table_or_view: str):
    """
    Tạo 2 view trên `input_table_or_view`:
      v_silver_invalid : dòng vi phạm rule (kể cả NULL) + lý do
      v_silver_valid   : dòng đạt chuẩn, đã khử trùng lặp review_id (giữ bản mới nhất)

    LƯU Ý: hàm này phải được gọi trên dữ liệu của MỘT category (view đã lọc sẵn),
    không phải toàn bộ Bronze, vì ROW_NUMBER() OVER (PARTITION BY review_id) là
    toán tử chặn (blocking) - nó phải xử lý hết input rồi mới trả dòng đầu tiên.
    """
    con.execute(f"""
        CREATE OR REPLACE VIEW v_silver_invalid AS
        SELECT *,
            CASE
                WHEN review_id IS NULL OR review_id = '' THEN 'MISSING_REVIEW_ID'
                WHEN product_id IS NULL OR product_id = '' THEN 'MISSING_PRODUCT_ID'
                WHEN review_body IS NULL OR TRIM(review_body) = '' THEN 'MISSING_REVIEW_BODY'
                WHEN star_rating IS NULL OR star_rating < 1 OR star_rating > 5 THEN 'INVALID_STAR_RATING'
                WHEN helpful_votes IS NULL OR total_votes IS NULL
                     OR helpful_votes > total_votes THEN 'INVALID_HELPFUL_VOTES'
                ELSE 'UNKNOWN_ERROR'
            END AS dlq_rejection_reason
        FROM {input_table_or_view}
        WHERE NOT {VALID_CONDITION};
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW v_silver_valid AS
        WITH filtered_data AS (
            SELECT * FROM {input_table_or_view}
            WHERE {VALID_CONDITION}
        )
        SELECT * EXCLUDE (row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY review_id
                    ORDER BY COALESCE(review_date, '1970-01-01') DESC,
                             COALESCE(ingest_time, '') DESC
                ) AS row_num
            FROM filtered_data
        )
        WHERE row_num = 1;
    """)
    logger.info(
        "Đã khởi tạo DuckDB validation views (v_silver_valid, v_silver_invalid)."
    )
