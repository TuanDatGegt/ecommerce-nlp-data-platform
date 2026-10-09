## pipeline/gold/pipeline.py
import duckdb
import logging
from datetime import datetime, timezone

from pipeline.storage import PipelineStorage
from pipeline.gold.features import create_gold_feature_views
from pipeline.gold.aggregations import create_gold_aggregation_views
from utils.logger import setup_logger

logger = setup_logger("gold_pipeline", "logs/gold_pipeline.log")


def run_pipeline():
    logger.info("=============================================")
    logger.info("  STARTING GOLD PIPELINE (FEATURE &amp; AGGREGATE)")
    logger.info("=============================================")

    storage = PipelineStorage()

    silver_dir = storage.get_full_path("silver/reviews")
    if not silver_dir.exists():
        logger.warning(f"Thư mục nguồn Silver không tồn tại: {silver_dir}")
        return

    # Khởi tạo DuckDB In-Memory Connection với cấu hình Out-of-Core
    con = duckdb.connect(database=":memory:")

    temp_duckdb_dir = storage.get_full_path("metadata/duckdb_temp")
    temp_duckdb_dir.mkdir(parents=True, exist_ok=True)

    con.execute("PRAGMA memory_limit='6GB';")
    con.execute(f"PRAGMA temp_directory='{temp_duckdb_dir}';")
    con.execute("PRAGMA threads=3;")

    try:
        # 1. Đọc dữ liệu Silver Parquet
        silver_glob_pattern = str(silver_dir / "*" / "*" / "*" / "*.parquet")
        logger.info(f"[READ] Đọc dữ liệu Silver từ: {silver_glob_pattern}")

        con.execute(f"""
            CREATE OR REPLACE VIEW v_silver_raw AS 
            SELECT * FROM read_parquet('{silver_glob_pattern}');
        """)

        total_silver_count = con.execute("SELECT COUNT(*) FROM v_silver_raw").fetchone()
        logger.info(f"[SILVER DATASET] Tổng số bản ghi Silver: {total_silver_count:,}")

        if total_silver_count == 0:
            logger.warning("Không có dữ liệu trong Silver Layer. Dừng Gold Pipeline.")
            return

        # 2. Tạo Feature Views
        create_gold_feature_views(con, "v_silver_raw")

        # 3. Tạo Aggregation Views
        create_gold_aggregation_views(con, "v_gold_fact_reviews")

        now = datetime.now(timezone.utc)
        year_str = str(now.year)
        month_str = f"{now.month:02d}"

        # 4. Xuất Bảng Fact (Fact Reviews) phân vùng theo year/month/category
        categories_res = con.execute(
            "SELECT DISTINCT category FROM v_gold_fact_reviews"
        ).fetchall()
        categories = [row for row in categories_res if row]

        logger.info(
            f"[WRITE FACT] Xuất fact_reviews cho {len(categories)} ngành hàng..."
        )
        for cat in categories:
            fact_df = con.execute(
                "SELECT * FROM v_gold_fact_reviews WHERE category = ?", [cat]
            ).df()

            if not fact_df.empty:
                relative_path = f"gold/fact_reviews/year={year_str}/month={month_str}/category={cat}/part_00000.parquet"
                storage.save_parquet(fact_df, relative_path, compression="snappy")

        # 5. Xuất Bảng Aggregation: Product Level
        logger.info("[WRITE AGG] Xuất agg_product...")
        for cat in categories:
            agg_prod_df = con.execute(
                "SELECT * FROM v_gold_agg_product WHERE category = ?", [cat]
            ).df()

            if not agg_prod_df.empty:
                relative_path = f"gold/agg_product/category={cat}/part_00000.parquet"
                storage.save_parquet(agg_prod_df, relative_path, compression="snappy")

        # 6. Xuất Bảng Aggregation: Category Monthly
        logger.info("[WRITE AGG] Xuất agg_category_monthly...")
        agg_cat_df = con.execute("SELECT * FROM v_gold_agg_category_monthly").df()
        if not agg_cat_df.empty:
            relative_path = "gold/agg_category_monthly/part_00000.parquet"
            storage.save_parquet(agg_cat_df, relative_path, compression="snappy")

        logger.info("\n========= SILVER TO GOLD EXECUTION SUMMARY =========")
        logger.info(f"Tổng bản ghi Fact Reviews (Gold): {total_silver_count:,}")
        logger.info(f"Tổng số ngành hàng đã xử lý: {len(categories)}")
        logger.info("===================================================\n")

    except Exception as e:
        logger.error(f"[GOLD PIPELINE ERROR] Thất bại trong quá trình thực thi: {e}")
        raise e
    finally:
        con.close()

    logger.info("=== HOÀN THÀNH HOÀN TOÀN GOLD PIPELINE ===")


if __name__ == "__main__":
    run_pipeline()
