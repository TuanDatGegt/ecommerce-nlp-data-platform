## pipeline/gold/pipeline.py
import argparse
import logging
import os
import time
from pathlib import Path

import duckdb
from pipeline.gold.aggregations import (
    consolidate_global_dashboard_tables,
    process_dashboard_aggregations_by_category,
)
from pipeline.gold.config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_TEMP_DIR,
    DUCKDB_THREADS,
    HELPFUL_MIN_RATIO,
    HELPFUL_MIN_VOTES,
    MIN_BODY_LEN,
    TOP_N_PRODUCTS,
    validate_environment,
)
from pipeline.gold.features import process_nlp_features_by_category
from pipeline.storage import PipelineStorage
from utils.checkpoint import load_checkpoint, save_checkpoint
from utils.logger import setup_logger

logger = setup_logger("gold_pipeline", "logs/gold_pipeline.log")


def discover_silver_categories(storage: PipelineStorage) -> list[str]:
    """Phát hiện các danh mục trong Silver layer."""
    silver_dir = storage.get_full_path("silver/reviews")
    categories = set()
    if not silver_dir.exists():
        return []
    for p in silver_dir.rglob("*.parquet"):
        for part in p.parts:
            if part.startswith("category="):
                categories.add(part.split("=")[1])
    return sorted(list(categories))


def run_category_pipeline(
    cat: str, storage: PipelineStorage, force: bool = False
) -> dict:
    start_time = time.time()
    silver_pattern = str(storage.get_full_path("silver/reviews/**/*.parquet"))

    nlp_out_dir = storage.get_full_path(f"gold/nlp_features/category={cat}")
    nlp_out_dir.mkdir(parents=True, exist_ok=True)
    nlp_file = nlp_out_dir / "part_00000.parquet"

    parts_dir = storage.get_full_path("gold/dashboard/parts")
    parts_dir.mkdir(parents=True, exist_ok=True)

    if nlp_file.exists() and not force:
        logger.info(f"[{cat}] Đã tồn tại trong Gold. Bỏ qua.")
        return {"status": "skipped", "category": cat}

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"PRAGMA temp_directory='{DUCKDB_TEMP_DIR}';")
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")

    try:
        # 1. Processing NLP Features
        rows = process_nlp_features_by_category(
            con,
            silver_path_pattern=silver_pattern,
            category_name=cat,
            output_parquet_path=str(nlp_file),
            min_body_len=MIN_BODY_LEN,
            helpful_min_votes=HELPFUL_MIN_VOTES,
            helpful_min_ratio=HELPFUL_MIN_RATIO,
        )

        tmp_nlp = Path(f"{nlp_file}.tmp")
        if tmp_nlp.exists():
            os.replace(tmp_nlp, nlp_file)

        # 2. Processing Dashboard Aggregations
        process_dashboard_aggregations_by_category(
            con, cat, top_n_products=TOP_N_PRODUCTS
        )

        con.execute(
            f"COPY tmp_cat_month_stats TO '{parts_dir / f'cat_month_{cat}.parquet.tmp'}' (FORMAT PARQUET, COMPRESSION 'zstd');"
        )
        os.replace(
            parts_dir / f"cat_month_{cat}.parquet.tmp",
            parts_dir / f"cat_month_{cat}.parquet",
        )

        elapsed = time.time() - start_time
        logger.info(f"[{cat}] Hoàn tất: {rows:,} dòng trong {elapsed:.2f}s")
        return {"status": "success", "category": cat, "rows": rows, "time": elapsed}

    except Exception as e:
        logger.error(f"[{cat}] LỖI: {e}", exc_info=True)
        return {"status": "failed", "category": cat, "error": str(e)}
    finally:
        con.close()


def run_pipeline(
    force: bool = False,
    only_category: str | None = None,
    table_name: str | None = None,
):
    validate_environment()
    storage = PipelineStorage()
    categories = discover_silver_categories(storage)

    if only_category:
        categories = [c for c in categories if c == only_category]

    logger.info(f"Bắt đầu Gold Pipeline cho {len(categories)} categories...")
    completed = load_checkpoint()

    failed_cats = []
    for i, cat in enumerate(categories, 1):
        logger.info(f"=== ({i}/{len(categories)}) Category: {cat} ===")
        res = run_category_pipeline(cat, storage, force=force)
        if res["status"] == "success":
            completed.setdefault("gold", {})[cat] = True
            save_checkpoint(completed)
        elif res["status"] == "failed":
            failed_cats.append(cat)

    if not failed_cats and not only_category:
        logger.info("Gộp các bảng Dashboard toàn cục...")
        consolidate_global_dashboard_tables(storage)
        logger.info("=== HOÀN THÀNH GOLD PIPELINE ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Layer Pipeline")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--category", type=str, default=None)
    parser.add_argument("--table", type=str, default=None)
    args = parser.parse_args()

    run_pipeline(force=args.force, only_category=args.category, table_name=args.table)
