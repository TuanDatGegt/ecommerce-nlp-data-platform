## pipeline/silver/silver_pipeline.py
import argparse
import os
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.storage import PipelineStorage
from pipeline.dlq import DeadLetterQueue
from pipeline.silver.validator import create_validation_views
from pipeline.silver.cleaner import clean_review_table
from utils.logger import setup_logger

logger = setup_logger("silver_pipeline", "logs/silver_pipeline.log")

BATCH_ROWS = 500_000  # số dòng mỗi lô khi stream DuckDB -> làm sạch -> ghi Parquet
DLQ_SAMPLE_PER_CATEGORY = 1_000

# Cột KHÔNG ghi ra Silver. Chỉ bỏ ở bước ghi: validator/dedup vẫn dùng được ingest_time, review_date.
# year/month/category đã nằm trong đường dẫn partition nên không cần lặp lại trong file;
# khi đọc Silver bằng hive_partitioning=true chúng tự xuất hiện lại từ đường dẫn.
DROP_COLUMNS = [
    "marketplace",
    "source_file",
    "ingest_time",
    "batch_id",
    "category",
    "year",
    "month",
]


def _arrow_reader(result):
    """DuckDB >= 1.4 dùng to_arrow_reader; bản cũ hơn dùng fetch_record_batch."""
    if hasattr(result, "to_arrow_reader"):
        return result.to_arrow_reader(BATCH_ROWS)
    return result.fetch_record_batch(BATCH_ROWS)


def discover_categories(bronze_dir):
    """Lấy danh sách category từ TÊN THƯ MỤC (không cần quét dữ liệu)."""
    return sorted(
        {
            p.parent.name.split("=", 1)[1]
            for p in bronze_dir.glob("*/*/category=*/*.parquet")
        }
    )


def process_category(con, storage, dlq, bronze_dir, cat, out_rel, force):
    out_path = storage.get_full_path(out_rel)
    if out_path.exists() and not force:
        logger.info(f"[SKIP] {cat}: đã có {out_path.name} (dùng --force để chạy lại)")
        return None

    pattern = str(bronze_dir / "*" / "*" / f"category={cat}" / "*.parquet").replace(
        "'", "''"
    )
    con.execute(f"""
        CREATE OR REPLACE VIEW v_bronze_cat AS
        SELECT * FROM read_parquet('{pattern}', hive_partitioning=false, union_by_name=true)
    """)
    bronze_count = con.execute("SELECT COUNT(*) FROM v_bronze_cat").fetchone()[
        0
    ]  # đọc từ metadata
    if bronze_count == 0:
        return None

    # Validation + dedup CHỈ trên dữ liệu của category này
    create_validation_views(con, "v_bronze_cat")
    invalid_count = con.execute("SELECT COUNT(*) FROM v_silver_invalid").fetchone()[0]
    if invalid_count:
        sample = con.execute(
            f"SELECT * FROM v_silver_invalid LIMIT {DLQ_SAMPLE_PER_CATEGORY}"
        ).df()
        dlq.write_error(
            data=sample,
            layer="silver",
            source_file=f"bronze/reviews/category={cat}",
            error_message=f"[{cat}] {invalid_count:,} bản ghi không đạt chuẩn Silver (mẫu {len(sample)})",
        )

    # Stream: DuckDB -> lô Arrow -> làm sạch -> ghi từng row group. RAM luôn ~ 1 lô.
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    all_cols = [r[0] for r in con.execute("DESCRIBE v_silver_valid").fetchall()]
    keep_cols = [c for c in all_cols if c not in DROP_COLUMNS]
    select_list = ", ".join('"{}"'.format(c.replace('"', '""')) for c in keep_cols)
    reader = _arrow_reader(con.execute(f"SELECT {select_list} FROM v_silver_valid"))
    writer, written = None, 0
    try:
        for batch in reader:
            table = clean_review_table(pa.Table.from_batches([batch]))
            if writer is None:
                writer = pq.ParquetWriter(
                    str(tmp_path), table.schema, compression="snappy"
                )
            writer.write_table(table)
            written += table.num_rows
    finally:
        if writer is not None:
            writer.close()
    if writer is not None:
        os.replace(
            tmp_path, out_path
        )  # chỉ đổi tên khi ghi xong -> không bao giờ có file dở dang
    elif tmp_path.exists():
        tmp_path.unlink()

    dup_removed = bronze_count - invalid_count - written
    logger.info(
        f"[{cat}] bronze={bronze_count:,} | invalid={invalid_count:,} | "
        f"trùng review_id={dup_removed:,} | silver={written:,}"
    )
    return {
        "bronze": bronze_count,
        "invalid": invalid_count,
        "dups": dup_removed,
        "written": written,
    }


def run_pipeline(partition=None, force=False, only=None):
    logger.info("=============================================")
    logger.info("  STARTING SILVER PIPELINE (PER-CATEGORY, STREAMING)")
    logger.info("=============================================")

    storage = PipelineStorage()
    dlq = DeadLetterQueue(storage=storage)

    bronze_dir = storage.get_full_path("bronze/reviews")
    if not bronze_dir.exists():
        logger.warning(f"Thư mục nguồn Bronze không tồn tại: {bronze_dir}")
        return

    # Cố định năm/tháng của partition đầu ra NGAY TỪ ĐẦU: chạy lại/resume qua nửa đêm
    # hay qua tháng mới sẽ không đẻ ra thư mục partition khác.
    if partition:
        year_str, month_str = partition.split("-")
    else:
        now = datetime.now(timezone.utc)
        year_str, month_str = str(now.year), f"{now.month:02d}"

    categories = discover_categories(bronze_dir)
    if only:
        categories = [c for c in categories if c in only]
    logger.info(
        f"[DISCOVER] {len(categories)} ngành hàng | partition đầu ra {year_str}-{month_str}"
    )

    temp_dir = storage.get_full_path("metadata/duckdb_temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA memory_limit='6GB';")
    con.execute(f"PRAGMA temp_directory='{temp_dir}';")
    con.execute("PRAGMA threads=3;")
    con.execute("SET preserve_insertion_order=false;")
    con.execute(
        "PRAGMA max_temp_directory_size='50GiB';"
    )  # chốt chặn: lỗi sớm thay vì đầy ổ đĩa

    totals = {"bronze": 0, "invalid": 0, "dups": 0, "written": 0}
    failed = []
    try:
        for i, cat in enumerate(categories, 1):
            logger.info(f"--- ({i}/{len(categories)}) {cat} ---")
            rel = f"silver/reviews/year={year_str}/month={month_str}/category={cat}/part_00000.parquet"
            try:
                res = process_category(con, storage, dlq, bronze_dir, cat, rel, force)
            except Exception as e:  # 1 ngành lỗi không làm hỏng 36 ngành còn lại
                logger.error(f"[{cat}] THẤT BẠI: {e}")
                failed.append(cat)
                continue
            if res:
                for k in totals:
                    totals[k] += res[k]
    finally:
        con.close()

    logger.info("\n========= BRONZE TO SILVER EXECUTION SUMMARY =========")
    logger.info(f"Bronze (các ngành đã xử lý) : {totals['bronze']:,}")
    logger.info(f"Không hợp lệ -> DLQ         : {totals['invalid']:,}")
    logger.info(f"Trùng review_id đã loại     : {totals['dups']:,}")
    logger.info(f"Silver ghi thành công       : {totals['written']:,}")
    logger.info("=====================================================\n")

    if failed:
        raise RuntimeError(
            f"Silver pipeline lỗi ở {len(failed)} ngành: {failed}. Chạy lại để resume."
        )
    logger.info("=== HOÀN THÀNH SILVER PIPELINE ===")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--partition",
        help="YYYY-MM của thư mục silver đầu ra (mặc định: tháng hiện tại UTC)",
    )
    ap.add_argument("--force", action="store_true", help="Ghi đè category đã có")
    ap.add_argument(
        "--only", help="Chỉ chạy các ngành, cách nhau bằng dấu phẩy: Apparel,Books"
    )
    a = ap.parse_args()
    run_pipeline(a.partition, a.force, set(a.only.split(",")) if a.only else None)
