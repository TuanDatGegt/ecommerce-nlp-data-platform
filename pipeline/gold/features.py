## pipeline/gold/features.py
import logging
import duckdb

logger = logging.getLogger("gold_features")


def process_nlp_features_by_category(
    con: duckdb.DuckDBPyConnection,
    silver_path_pattern: str,
    category_name: str,
    output_parquet_path: str,
    min_body_len: int = 10,
    helpful_min_votes: int = 2,
    helpful_min_ratio: float = 0.6,
) -> int:
    """
    Đọc 1 category từ Silver, làm sạch text, tính toán thuộc tính NLP
    và xuất thẳng ra Parquet bằng DuckDB native export.
    """
    # Macro DuckDB làm sạch văn bản: xóa HTML tags, chuẩn hóa khoảng trắng
    con.execute("""
        CREATE OR REPLACE MACRO clean_text(str) AS 
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(str, ''), '<[^>]+>', ' ', 'g'), '\\s+', ' ', 'g'));
    """)

    query = f"""
        CREATE OR REPLACE TEMP TABLE tmp_nlp_features AS
        WITH raw_silver AS (
            SELECT 
                review_id,
                customer_id,
                product_id,
                product_title,
                star_rating,
                helpful_votes,
                total_votes,
                verified_purchase,
                review_headline,
                review_body,
                review_date,
                category,
                YEAR(TRY_CAST(review_date AS DATE)) AS year,
                MONTH(TRY_CAST(review_date AS DATE)) AS month,
                clean_text(review_headline) AS clean_headline,
                clean_text(review_body) AS clean_body
            FROM read_parquet('{silver_path_pattern}', hive_partitioning=true)
            WHERE category = '{category_name}'
        ),
        filtered AS (
            SELECT 
                review_id,
                product_id,
                category,
                COALESCE(year, 2020) AS year,
                COALESCE(month, 1) AS month,
                star_rating,
                verified_purchase,
                helpful_votes,
                total_votes,
                clean_headline,
                clean_body,
                LENGTH(clean_body) AS body_len_chars,
                ARRAY_LENGTH(STRING_SPLIT(clean_body, ' ')) AS body_len_words,
                CASE WHEN LENGTH(clean_headline) > 0 THEN TRUE ELSE FALSE END AS has_headline,
                CASE 
                    WHEN helpful_votes >= {helpful_min_votes} 
                         AND (CAST(helpful_votes AS DOUBLE) / NULLIF(total_votes, 0)) >= {helpful_min_ratio} 
                    THEN TRUE ELSE FALSE 
                END AS is_helpful,
                CASE 
                    WHEN star_rating <= 2 THEN 'negative'
                    WHEN star_rating = 3 THEN 'neutral'
                    ELSE 'positive'
                END AS sentiment_label_from_rating,
                COUNT(*) OVER(PARTITION BY clean_body) > 1 AS is_duplicate_text,
                ROW_NUMBER() OVER(PARTITION BY star_rating ORDER BY HASH(review_id)) AS sample_rank,
                ROW_NUMBER() OVER(PARTITION BY review_id ORDER BY review_date DESC) AS dup_review_rank
            FROM raw_silver
            WHERE LENGTH(clean_body) >= {min_body_len}
        )
        SELECT 
            review_id, product_id, category, year, month,
            star_rating, verified_purchase, helpful_votes, total_votes,
            clean_headline, clean_body, body_len_chars, body_len_words,
            has_headline, is_helpful, sentiment_label_from_rating, is_duplicate_text,
            CASE WHEN sample_rank <= 50000 THEN TRUE ELSE FALSE END AS sample_flag
        FROM filtered
        WHERE dup_review_rank = 1;
    """
    con.execute(query)

    # Xuất ra file .tmp phục vụ Atomic Write
    tmp_out = f"{output_parquet_path}.tmp"
    export_sql = (
        f"COPY tmp_nlp_features TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION 'zstd');"
    )
    con.execute(export_sql)

    count = con.execute("SELECT COUNT(*) FROM tmp_nlp_features").fetchone()[0]
    return count
