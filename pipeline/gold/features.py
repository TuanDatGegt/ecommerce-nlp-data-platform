## pipeline/gold/features.py
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger("gold_pipeline.features")


def _q(value) -> str:
    """Escape dấu nháy đơn để nhúng vào chuỗi SQL."""
    return str(value).replace("'", "''")


def _to_bool(col: str) -> str:
    return f"COALESCE(CAST({col} AS VARCHAR) IN ('Y','y','true','TRUE','True','t','1'), FALSE)"


def process_nlp_features_by_category(
    con: duckdb.DuckDBPyConnection,
    silver_path_pattern: str,
    category_name: str,
    output_parquet_path: str,
    min_body_len: int = 10,
    helpful_min_votes: int = 2,
    helpful_min_ratio: float = 0.6,
    sample_per_star: int = 50000,
) -> int:
    """
    Đọc 1 category từ Silver, làm sạch text, tính thuộc tính NLP, ghi Parquet (.tmp)
    theo kiểu stream. Tạo VIEW `tmp_nlp_features` trỏ vào file đó cho bước aggregations.
    `silver_path_pattern` phải là glob của ĐÚNG category (không quét toàn Silver).
    """
    tmp_out = Path(f"{output_parquet_path}.tmp")
    tmp_out.parent.mkdir(parents=True, exist_ok=True)
    tmp_out.unlink(missing_ok=True)

    con.execute("""
        CREATE OR REPLACE MACRO clean_text(str) AS
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(str, ''), '<[^>]+>', ' ', 'g'), '\\s+', ' ', 'g'));
    """)

    cat = _q(category_name)
    read_sql = f"read_parquet('{_q(silver_path_pattern)}', hive_partitioning = false)"

    # --- 1. Các tập phụ NHỎ, chỉ đọc 1 cột nên rất nhẹ ---------------------------------
    # review_id bị trùng (thường rỗng nếu Silver đã khử trùng)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_dup_ids AS
        SELECT review_id FROM {read_sql}
        WHERE review_id IS NOT NULL
        GROUP BY review_id HAVING COUNT(*) > 1
    """)
    n_dup_ids = con.execute("SELECT COUNT(*) FROM tmp_dup_ids").fetchone()[0]

    # hash nội dung bị lặp (thay cho window PARTITION BY cả chuỗi text)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_dup_hashes AS
        SELECT HASH(review_body) AS body_hash FROM {read_sql}
        WHERE review_body IS NOT NULL
        GROUP BY 1 HAVING COUNT(*) > 1
    """)

    # ngưỡng lấy mẫu theo số sao (thay cho ROW_NUMBER phải sort toàn bộ)
    star_counts = con.execute(
        f"SELECT star_rating, COUNT(*) FROM {read_sql} GROUP BY 1"
    ).fetchall()
    con.execute(
        "CREATE OR REPLACE TEMP TABLE tmp_star_rate (star_rating BIGINT, thr BIGINT)"
    )
    for star, n in star_counts:
        if star is None or not n:
            continue
        thr = int(1_000_000 * min(1.0, sample_per_star / n))
        con.execute("INSERT INTO tmp_star_rate VALUES (?, ?)", [int(star), thr])

    # --- 2. Truy vấn chính -------------------------------------------------------------
    src = f"""
        SELECT * FROM (
            SELECT
                review_id, customer_id, product_id, product_title,
                star_rating, helpful_votes, total_votes,
                {_to_bool('vine')}              AS vine,
                {_to_bool('verified_purchase')} AS verified_purchase,
                TRY_CAST(review_date AS DATE)   AS review_date,
                clean_text(review_headline)     AS clean_headline,
                clean_text(review_body)         AS clean_body,
                HASH(review_body)               AS body_hash
            FROM {read_sql}
        )
        WHERE LENGTH(clean_body) >= {int(min_body_len)} AND review_id IS NOT NULL
    """

    def _variant(extra: str = "", qualify: str = "") -> str:
        return f"{src} {extra} {qualify}"

    if n_dup_ids == 0:
        base = _variant()
    else:
        # window chỉ chạy trên tập review_id bị trùng (nhỏ), phần còn lại chạy stream
        base = (
            _variant("AND review_id NOT IN (SELECT review_id FROM tmp_dup_ids)")
            + " UNION ALL "
            + _variant(
                "AND review_id IN (SELECT review_id FROM tmp_dup_ids)",
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_date DESC) = 1",
            )
        )

    con.execute(f"""
        COPY (
            SELECT
                b.review_id, b.customer_id, b.product_id, b.product_title,
                '{cat}' AS category,
                b.review_date,
                YEAR(b.review_date)  AS year,
                MONTH(b.review_date) AS month,
                b.star_rating, b.verified_purchase, b.vine,
                b.helpful_votes, b.total_votes,
                b.clean_headline, b.clean_body,
                LENGTH(b.clean_body)     AS body_len_chars,
                LENGTH(b.clean_headline) AS headline_len_chars,
                ARRAY_LENGTH(STRING_SPLIT(b.clean_body, ' ')) AS body_len_words,
                LENGTH(b.clean_headline) > 0 AS has_headline,
                COALESCE(
                    b.helpful_votes >= {int(helpful_min_votes)}
                    AND (CAST(b.helpful_votes AS DOUBLE) / NULLIF(b.total_votes, 0)) >= {float(helpful_min_ratio)},
                    FALSE
                ) AS is_helpful,
                CASE
                    WHEN b.star_rating <= 2 THEN 'negative'
                    WHEN b.star_rating = 3 THEN 'neutral'
                    ELSE 'positive'
                END AS sentiment_label_from_rating,
                COALESCE(b.body_hash IN (SELECT body_hash FROM tmp_dup_hashes), FALSE) AS is_duplicate_text,
                COALESCE((HASH(b.review_id) % 1000000) < r.thr, FALSE) AS sample_flag
            FROM ({base}) b
            LEFT JOIN tmp_star_rate r ON CAST(b.star_rating AS BIGINT) = r.star_rating
        ) TO '{_q(str(tmp_out))}' (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    # --- 3. VIEW trên file vừa ghi: aggregations chỉ đọc các cột số, không nạp text ----
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW tmp_nlp_features AS "
        f"SELECT * FROM read_parquet('{_q(str(tmp_out))}')"
    )
    count, d_min, d_max, d_null = con.execute("""
        SELECT COUNT(*), MIN(review_date), MAX(review_date),
               COUNT(*) FILTER (WHERE review_date IS NULL)
        FROM tmp_nlp_features
    """).fetchone()
    logger.info(
        "[%s] features: %s dòng | review_date %s → %s | thiếu ngày: %s | review_id trùng: %s",
        category_name,
        f"{count:,}",
        d_min,
        d_max,
        f"{d_null:,}",
        f"{n_dup_ids:,}",
    )
    return count
