## pipeline/gold/pipeline.py
import argparse
import logging
import os
import time
from pathlib import Path

import duckdb
from pipeline.gold.aggregations import (
    PART_TABLES,
    consolidate_global_dashboard_tables,
    process_dashboard_aggregations_by_category,
    write_dashboard_parts,
)
from pipeline.gold.config import (
    DUCKDB_MAX_TEMP_SIZE,
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
from utils.checkpoint import load_manifest, save_manifest
from utils.logger import setup_logger

logger = setup_logger("gold_pipeline", "logs/gold_pipeline.log")

MAX_SAME_ERROR = (
    3  # dừng nếu N ngành liên tiếp lỗi cùng một thông điệp (không tính OOM)
)


def discover_silver_categories(storage: PipelineStorage) -> list[str]:
    """Lấy danh sách category từ tên thư mục partition (không liệt kê hàng nghìn file)."""
    silver_dir = storage.get_full_path("silver/reviews")
    if not silver_dir.exists():
        return []
    return sorted(
        {p.name.split("=", 1)[1] for p in silver_dir.rglob("category=*") if p.is_dir()}
    )


def _expected_outputs(storage: PipelineStorage, cat: str) -> list[Path]:
    nlp_file = storage.get_full_path(
        f"gold/nlp_features/category={cat}/part_00000.parquet"
    )
    cat_parts_dir = storage.get_full_path(f"gold/dashboard/parts/category={cat}")
    return [nlp_file] + [
        cat_parts_dir / f"{prefix}_{cat}.parquet" for prefix in PART_TABLES
    ]


def _is_complete(files: list[Path]) -> bool:
    return all(f.exists() and f.stat().st_size > 0 for f in files)


def run_category_pipeline(
    cat: str, storage: PipelineStorage, force: bool = False
) -> dict:
    start = time.time()
    outputs = _expected_outputs(storage, cat)

    # Chỉ bỏ qua khi TẤT CẢ file đầu ra đã có (không chỉ file NLP)
    if not force and _is_complete(outputs):
        logger.info(f"[{cat}] Đã đủ file Gold. Bỏ qua.")
        return {"status": "skipped", "category": cat}

    nlp_file = outputs[0]
    nlp_file.parent.mkdir(parents=True, exist_ok=True)
    parts_dir = storage.get_full_path("gold/dashboard/parts")
    parts_dir.mkdir(parents=True, exist_ok=True)

    # Chỉ quét đúng thư mục của category này
    silver_pattern = str(
        storage.get_full_path(f"silver/reviews/year=*/month=*/category={cat}/*.parquet")
    )

    con = duckdb.connect(database=":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory='{DUCKDB_TEMP_DIR}';")
    con.execute(f"SET max_temp_directory_size='{DUCKDB_MAX_TEMP_SIZE}';")
    con.execute(f"SET threads={DUCKDB_THREADS};")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=false;")

    try:
        # 1. NLP features -> file .tmp + view tmp_nlp_features
        rows = process_nlp_features_by_category(
            con,
            silver_path_pattern=silver_pattern,
            category_name=cat,
            output_parquet_path=str(nlp_file),
            min_body_len=MIN_BODY_LEN,
            helpful_min_votes=HELPFUL_MIN_VOTES,
            helpful_min_ratio=HELPFUL_MIN_RATIO,
        )

        # 2. Aggregations -> 6 bảng tạm -> 6 file parts .tmp
        process_dashboard_aggregations_by_category(
            con, cat, top_n_products=TOP_N_PRODUCTS
        )
        pending = [(Path(f"{nlp_file}.tmp"), nlp_file)] + write_dashboard_parts(
            con, cat, parts_dir
        )

        # 3. Commit: chỉ đổi tên khi MỌI bước đã thành công
        for tmp, final in pending:
            os.replace(tmp, final)

        elapsed = time.time() - start
        logger.info(f"[{cat}] Hoàn tất: {rows:,} dòng trong {elapsed:.2f}s")
        return {"status": "success", "category": cat, "rows": rows, "time": elapsed}

    except Exception as e:
        logger.error(f"[{cat}] LỖI: {e}", exc_info=True)
        return {"status": "failed", "category": cat, "error": str(e)}
    finally:
        con.close()
        for f in outputs:  # dọn file .tmp dở dang
            Path(f"{f}.tmp").unlink(missing_ok=True)


def run_pipeline(
    force: bool = False,
    only_category: str | None = None,
    consolidate_only: bool = False,
):
    validate_environment()
    storage = PipelineStorage()

    if consolidate_only:
        consolidate_global_dashboard_tables(storage)
        return

    categories = discover_silver_categories(storage)
    if only_category:
        categories = [c for c in categories if c == only_category]
        if not categories:
            logger.error(f"Không thấy category '{only_category}' trong Silver.")
            return

    logger.info(f"Bắt đầu Gold Pipeline cho {len(categories)} categories...")
    completed = load_manifest()

    failed_cats: list[str] = []
    last_err, same_err = None, 0
    for i, cat in enumerate(categories, 1):
        logger.info(f"=== ({i}/{len(categories)}) Category: {cat} ===")
        res = run_category_pipeline(cat, storage, force=force)

        if res["status"] == "success":
            completed.setdefault("gold", {})[cat] = True
            save_manifest(completed)
            last_err, same_err = None, 0
        elif res["status"] == "failed":
            failed_cats.append(cat)
            key = res["error"].splitlines()[0][:120]
            if "Out of Memory" in key:
                continue
            same_err = same_err + 1 if key == last_err else 1
            last_err = key
            if same_err >= MAX_SAME_ERROR:
                logger.error(
                    f"Dừng sớm: {same_err} ngành liên tiếp lỗi giống nhau: {key}"
                )
                break

    if failed_cats:
        logger.warning(f"Còn {len(failed_cats)} ngành lỗi: {', '.join(failed_cats)}")
    elif not only_category and all(
        _is_complete(_expected_outputs(storage, c)) for c in categories
    ):
        logger.info("Gộp các bảng Dashboard toàn cục...")
        consolidate_global_dashboard_tables(storage)
        logger.info("=== HOÀN THÀNH GOLD PIPELINE ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Layer Pipeline")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--category", type=str, default=None)
    parser.add_argument(
        "--consolidate-only", action="store_true", help="Chỉ gộp parts, không xử lý lại"
    )
    args = parser.parse_args()

    run_pipeline(
        force=args.force,
        only_category=args.category,
        consolidate_only=args.consolidate_only,
    )
