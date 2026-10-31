## pipeline/gold/aggregations.py
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger("gold_pipeline.aggregations")

# prefix file trong parts/  ->  bảng tạm  (tên file đầu ra toàn cục ở MERGED_TABLES)
PART_TABLES = {
    "cat_month": "tmp_cat_month_stats",
    "cat_year": "tmp_cat_year_stats",
    "product": "tmp_product_stats",
    "customer": "tmp_customer_stats",
    "helpful": "tmp_helpfulness_buckets",
    "rating_verified": "tmp_rating_verified_compare",
}
MERGED_TABLES = {
    "cat_month": "category_month_stats",
    "cat_year": "category_year_stats",
    "product": "product_stats",
    "customer": "customer_stats",
    "helpful": "helpfulness_buckets",
    "rating_verified": "rating_verified_compare",
}

_METRICS = """
    COUNT(*) AS review_count,
    ROUND(AVG(star_rating), 2) AS avg_rating,
    COUNT(*) FILTER (WHERE star_rating = 1) AS star1,
    COUNT(*) FILTER (WHERE star_rating = 2) AS star2,
    COUNT(*) FILTER (WHERE star_rating = 3) AS star3,
    COUNT(*) FILTER (WHERE star_rating = 4) AS star4,
    COUNT(*) FILTER (WHERE star_rating = 5) AS star5,
    ROUND(100.0 * COUNT(*) FILTER (WHERE verified_purchase) / NULLIF(COUNT(*), 0), 2) AS verified_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE vine) / NULLIF(COUNT(*), 0), 2) AS vine_pct,
    ROUND(AVG(helpful_votes), 2) AS avg_helpful_votes,
    ROUND(AVG(CASE WHEN total_votes > 0 THEN CAST(helpful_votes AS DOUBLE) / total_votes END), 4) AS helpful_ratio,
    ROUND(AVG(body_len_chars), 1) AS avg_review_len,
    ROUND(AVG(headline_len_chars), 1) AS avg_headline_len,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products
"""


def _q(value) -> str:
    return str(value).replace("'", "''")


def process_dashboard_aggregations_by_category(
    con: duckdb.DuckDBPyConnection,
    category_name: str,
    top_n_products: int = 1000,
):
    """Tạo các bảng tạm thống kê cho 1 category từ view `tmp_nlp_features`."""

    # 1-2. Thống kê theo tháng và theo năm (tính trực tiếp, không suy từ bảng tháng)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_cat_month_stats AS
        SELECT category, year, month, {_METRICS}
        FROM tmp_nlp_features GROUP BY category, year, month;
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_cat_year_stats AS
        SELECT category, year, {_METRICS}
        FROM tmp_nlp_features GROUP BY category, year;
    """)

    # 3. Top N sản phẩm: xếp hạng bằng cột hẹp, chỉ lấy tên cho N sản phẩm đó
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_product_stats AS
        WITH top AS (
            SELECT category, product_id,
                   COUNT(*) AS review_count,
                   ROUND(AVG(star_rating), 2) AS avg_rating,
                   ROUND(100.0 * COUNT(*) FILTER (WHERE verified_purchase) / NULLIF(COUNT(*), 0), 2) AS verified_pct,
                   MIN(review_date) AS first_review_date,
                   MAX(review_date) AS last_review_date
            FROM tmp_nlp_features
            GROUP BY category, product_id
            ORDER BY review_count DESC, product_id
            LIMIT {int(top_n_products)}
        ),
        titles AS (
            SELECT product_id, ANY_VALUE(product_title) AS product_title
            FROM tmp_nlp_features
            WHERE product_id IN (SELECT product_id FROM top)
            GROUP BY product_id
        )
        SELECT t.category, t.product_id, ti.product_title, t.review_count,
               t.avg_rating, t.verified_pct, t.first_review_date, t.last_review_date
        FROM top t LEFT JOIN titles ti USING (product_id)
        ORDER BY t.review_count DESC;
    """)

    # 4. Histogram số review / khách hàng (theo customer_id, không phải review_id)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_customer_stats AS
        WITH c AS (
            SELECT category, customer_id, COUNT(*) AS n
            FROM tmp_nlp_features
            WHERE customer_id IS NOT NULL
            GROUP BY category, customer_id
        )
        SELECT category,
               CASE WHEN n = 1 THEN '1_review'
                    WHEN n BETWEEN 2 AND 5 THEN '2-5_reviews'
                    WHEN n BETWEEN 6 AND 10 THEN '6-10_reviews'
                    ELSE '>10_reviews' END AS review_bucket,
               COUNT(*) AS customer_count
        FROM c GROUP BY category, review_bucket;
    """)

    # 5. Phân bố helpful_votes
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_helpfulness_buckets AS
        SELECT category,
               CASE WHEN helpful_votes = 0 THEN '0_votes'
                    WHEN helpful_votes BETWEEN 1 AND 5 THEN '1-5_votes'
                    WHEN helpful_votes BETWEEN 6 AND 20 THEN '6-20_votes'
                    ELSE '>20_votes' END AS helpful_bucket,
               COUNT(*) AS review_count
        FROM tmp_nlp_features GROUP BY category, helpful_bucket;
    """)

    # 6. Rating theo verified / vine
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_rating_verified_compare AS
        SELECT category, verified_purchase, vine,
               COUNT(*) AS review_count,
               ROUND(AVG(star_rating), 2) AS avg_rating
        FROM tmp_nlp_features GROUP BY category, verified_purchase, vine;
    """)


def write_dashboard_parts(
    con: duckdb.DuckDBPyConnection, category_name: str, parts_dir: Path
) -> list[tuple[Path, Path]]:
    """Ghi mỗi bảng tạm ra parts/category=<ngành>/<prefix>_<ngành>.parquet.tmp.
    Trả về danh sách (tmp, final) để commit (rename) sau khi mọi bước thành công."""
    cat_dir = parts_dir / f"category={category_name}"
    cat_dir.mkdir(parents=True, exist_ok=True)

    pairs = []
    for prefix, table in PART_TABLES.items():
        final = cat_dir / f"{prefix}_{category_name}.parquet"
        tmp = final.with_name(final.name + ".tmp")
        tmp.unlink(missing_ok=True)
        con.execute(
            f"COPY {table} TO '{_q(tmp)}' (FORMAT PARQUET, COMPRESSION 'zstd');"
        )
        pairs.append((tmp, final))
    return pairs


def consolidate_global_dashboard_tables(storage):
    """Gộp các bản phần của các category thành 6 file Parquet toàn cục cho Dashboard."""
    con = duckdb.connect(database=":memory:")
    try:
        dash_dir = storage.get_full_path("gold/dashboard")
        parts_dir = dash_dir / "parts"

        for prefix, out_name in MERGED_TABLES.items():
            files = list(parts_dir.glob(f"category=*/{prefix}_*.parquet"))
            if not files:
                raise FileNotFoundError(
                    f"Không có file '{prefix}_*.parquet' trong {parts_dir}/category=*/"
                )
            tmp = dash_dir / f"{out_name}.parquet.tmp"
            # hive_partitioning=false: cột `category` đã có sẵn trong file,
            # tránh trùng tên với cột partition suy ra từ tên thư mục
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{_q(parts_dir)}/category=*/{prefix}_*.parquet', "
                f"union_by_name = true, hive_partitioning = false)) "
                f"TO '{_q(tmp)}' (FORMAT PARQUET, COMPRESSION 'zstd');"
            )
            tmp.replace(dash_dir / f"{out_name}.parquet")
            logger.info("Đã gộp %s từ %d category", out_name, len(files))
    finally:
        con.close()
