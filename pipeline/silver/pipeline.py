## pipeline/silver/silver_pipeline.py
import os
import duckdb
import logging
from pathlib import Path
from datetime import datetime, timezone

from pipeline.storage import PipelineStorage
from pipeline.dlq import DeadLetterQueue
from pipeline.silver.validator import create_validation_views
from pipeline.silver.cleaner import clean_review_dataframe
from utils.logger import setup_logger

logger = setup_logger("silver_pipeline", "logs/silver_pipeline.log")


def run_pipeline():
    logger.info("=============================================")
    logger.info("  STARTING SILVER PIPELINE (DUCKDB OUT-OF-CORE)")
    logger.info("=============================================")

    storage = PipelineStorage()
    dlq = DeadLetterQueue(storage=storage)

    bronze_dir = storage.get_full_path("bronze/reviews")
    if not bronze_dir.exists():
        logger.warning(f"Thư mục nguồn Bronze không tồn tại: {bronze_dir}")
        return

    # Khởi tạo DuckDB In-Memory Connection với cấu hình Out-of-Core
    con = duckdb.connect(database=":memory:")

    # Thiết lập giới hạn RAM 6GB theo Roadmap V7 &amp; kích hoạt Temp Directory nếu tràn RAM
    temp_duckdb_dir = storage.get_full_path("metadata/duckdb_temp")
    temp_duckdb_dir.mkdir(parents=True, exist_ok=True)

    con.execute("PRAGMA memory_limit='6GB';")
    con.execute(f"PRAGMA temp_directory='{temp_duckdb_dir}';")
    con.execute("PRAGMA threads=3;")  # Dành 1 core CPU cho OS/Airflow

    try:
        # 1. Nạp toàn bộ dữ liệu Parquet từ Bronze vào DuckDB View
        bronze_glob_pattern = str(bronze_dir / "*" / "*" / "*" / "*.parquet")
        logger.info(f"[READ] Đọc dữ liệu Bronze từ: {bronze_glob_pattern}")

        con.execute(f"""
            CREATE OR REPLACE VIEW v_bronze_raw AS 
            SELECT * FROM read_parquet('{bronze_glob_pattern}');
        """)

        total_bronze_count = con.execute(
            "SELECT COUNT(*) FROM v_bronze_raw"
        ).fetchone()[0]
        logger.info(f"[BRONZE DATASET] Tổng số bản ghi Bronze: {total_bronze_count:,}")

        if total_bronze_count == 0:
            logger.warning("Không có dữ liệu trong Bronze Layer. Dừng Silver Pipeline.")
            return

        # 2. Tạo Validation Views
        create_validation_views(con, "v_bronze_raw")

        valid_count = con.execute("SELECT COUNT(*) FROM v_silver_valid").fetchone()[0]
        invalid_count = con.execute("SELECT COUNT(*) FROM v_silver_invalid").fetchone()[
            0
        ]

        logger.info(
            f"[VALIDATION METRICS] Bản ghi hợp lệ: {valid_count:,} | Bản ghi lỗi: {invalid_count:,}"
        )

        # 3. Ghi các bản ghi không hợp lệ vào DLQ
        if invalid_count > 0:
            invalid_df = con.execute("SELECT * FROM v_silver_invalid LIMIT 10000").df()
            dlq.write_error(
                data=invalid_df,
                layer="silver",
                source_file="bronze_parquet_batch",
                error_message=f"Phát hiện {invalid_count} bản ghi không đạt quy chuẩn chất lượng Silver",
            )

        # 4. Xuất dữ liệu Hợp lệ theo từng Category &amp; Phân vùng ra data/silver/
        categories_res = con.execute(
            "SELECT DISTINCT category FROM v_silver_valid"
        ).fetchall()
        categories = [row[0] for row in categories_res if row[0]]

        logger.info(
            f"[TRANSFORM &amp; WRITE] Đang làm sạch văn bản &amp; xuất file cho {len(categories)} ngành hàng..."
        )

        now = datetime.now(timezone.utc)
        year_str = str(now.year)
        month_str = f"{now.month:02d}"

        total_written = 0
        for cat in categories:
            # Query dữ liệu của từng category từ DuckDB
            cat_df = con.execute(
                "SELECT * FROM v_silver_valid WHERE category = ?", [cat]
            ).df()

            if cat_df.empty:
                continue

            # Làm sạch văn bản (HTML, lowercase, ký tự đặc biệt)
            cat_df = clean_review_dataframe(cat_df)

            # Ghi file Parquet ra data/silver/
            relative_path = f"silver/reviews/year={year_str}/month={month_str}/category={cat}/part_00000.parquet"
            storage.save_parquet(cat_df, relative_path, compression="snappy")
            total_written += len(cat_df)

        logger.info("\n========= BRONZE TO SILVER EXECUTION SUMMARY =========")
        logger.info(f"Tổng bản ghi đầu vào (Bronze): {total_bronze_count:,}")
        logger.info(f"Tổng bản ghi xuất thành công (Silver): {total_written:,}")
        logger.info(f"Tổng bản ghi đẩy vào DLQ: {invalid_count:,}")
        logger.info("=====================================================\n")

    except Exception as e:
        logger.error(f"[SILVER PIPELINE ERROR] Thất bại trong quá trình thực thi: {e}")
        raise e
    finally:
        con.close()

    logger.info("=== HOÀN THÀNH HOÀN TOÀN SILVER PIPELINE ===")


if __name__ == "__main__":
    run_pipeline()
