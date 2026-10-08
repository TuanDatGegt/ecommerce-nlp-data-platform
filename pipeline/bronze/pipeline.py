## pipeline/bronze/pipeline.py
import os
import json
import uuid
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from tqdm import tqdm

from configs.settings import MAX_WORKERS, MANIFEST_PATH
from pipeline.storage import PipelineStorage
from pipeline.dlq import DeadLetterQueue
from pipeline.bronze.downloader import download_kaggle_dataset, discover_tsv_files
from pipeline.bronze.processor import (
    read_tsv_in_chunks,
    extract_category_from_filename,
    optimize_dataframe,
    add_metadata_columns,
    validate_chunk,
    process_and_save_chunk,
)
from utils.logger import setup_logger

logger = setup_logger("bronze_pipeline", "logs/bronze_pipeline.log")

CHECKPOINT_PATH = Path(MANIFEST_PATH)


def load_processed_files() -> dict:
    """Đọc tệp manifest theo dõi các file đã xử lý thành công."""
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read checkpoint manifest: {e}")
            return {}
    return {}


def mark_file_as_processed(file_name: str, record_count: int) -> None:
    """Ghi nhận một file đã hoàn tất vào data/metadata/processed_files.json."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    manifest = load_processed_files()

    manifest[file_name] = {
        "status": "COMPLETED",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "total_records": record_count,
    }

    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)


def is_file_processed(file_name: str) -> bool:
    """Kiểm tra xem file TSV đã được xử lý thành công trước đó chưa."""
    manifest = load_processed_files()
    return manifest.get(file_name, {}).get("status") == "COMPLETED"


def process_single_file(args):
    input_file, shared_metrics_queue = args
    file_name = os.path.basename(input_file)
    now = datetime.now(timezone.utc)
    year, month = now.year, f"{now.month:02d}"

    category = extract_category_from_filename(file_name)

    if is_file_processed(file_name):
        logger.info(f"[SKIP] Previously processed: {file_name}")
        return

    storage = PipelineStorage()
    dlq = DeadLetterQueue(storage=storage)
    batch_id = str(uuid.uuid4())
    chunk = None
    total_file_rows = 0

    try:
        logger.info(f"[START] Processing file: {file_name}")
        reader = read_tsv_in_chunks(input_file)

        for i, chunk in enumerate(reader):
            chunk = optimize_dataframe(chunk)
            chunk = add_metadata_columns(chunk, file_name, batch_id)
            chunk = validate_chunk(chunk)

            saved_path = process_and_save_chunk(
                df=chunk,
                storage=storage,
                category=category,
                year=year,
                month=month,
                chunk_idx=i,
            )

            chunk_rows = len(chunk)
            total_file_rows += chunk_rows
            shared_metrics_queue.put(("rows", chunk_rows))
            shared_metrics_queue.put(("chunks", 1))

        mark_file_as_processed(file_name, total_file_rows)
        shared_metrics_queue.put(("files", 1))
        logger.info(f"[SUCCESS] Completed: {file_name} ({total_file_rows:,} rows)")

    except Exception as e:
        error_msg = f"Error processing file {file_name}: {str(e)}"
        logger.error(error_msg)
        dlq.write_error(
            data=chunk if chunk is not None else {"file": file_name},
            layer="bronze",
            source_file=file_name,
            error_message=str(e),
        )
        if chunk is not None:
            shared_metrics_queue.put(("failed_rows", len(chunk)))


def run_pipeline():
    logger.info("=============================================")
    logger.info("  STARTING BRONZE PIPELINE (RUNTIME: DATA/)  ")
    logger.info("=============================================")

    storage = PipelineStorage()

    dataset_path = download_kaggle_dataset()
    input_files = discover_tsv_files(dataset_path)

    manager = multiprocessing.Manager()
    shared_metrics_queue = manager.Queue()
    task_args = [(file, shared_metrics_queue) for file in input_files]

    workers = min(MAX_WORKERS, 3)
    logger.info(f"Executing ProcessPoolExecutor with {workers} workers...")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        list(tqdm(executor.map(process_single_file, task_args), total=len(input_files)))

    total_rows = 0
    total_chunks = 0
    total_files = 0
    failed_rows = 0

    while not shared_metrics_queue.empty():
        m_type, val = shared_metrics_queue.get()
        if m_type == "rows":
            total_rows += val
        elif m_type == "chunks":
            total_chunks += val
        elif m_type == "files":
            total_files += val
        elif m_type == "failed_rows":
            failed_rows += val

    logger.info("\n========= BRONZE PIPELINE EXECUTION SUMMARY =========")
    logger.info(f"Files completed: {total_files}/{len(input_files)}")
    logger.info(f"Chunks written: {total_chunks}")
    logger.info(f"Total rows succeeded: {total_rows:,}")
    logger.info(f"Total rows failed: {failed_rows:,}")
    logger.info("=====================================================\n")


if __name__ == "__main__":
    run_pipeline()
