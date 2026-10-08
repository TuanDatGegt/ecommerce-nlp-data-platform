## pipeline/bronze/pipeline.py
import os
import json
import uuid
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from tqdm import tqdm

from pipeline.storage import PipelineStorage
from pipeline.dlq import DeadLetterQueue
from pipeline.bronze.downloader import (
    download_kaggle_dataset,
    discover_tsv_files,
    cleanup_files,
)
from pipeline.bronze.processor import (
    read_tsv_in_chunks,
    extract_category_from_filename,
    optimize_dataframe,
    add_metadata_columns,
    validate_chunk,
    process_and_save_chunk,
    save_sample_chunk,
)
from utils.logger import setup_logger
from configs.settings import MAX_WORKERS

logger = setup_logger("bronze_pipeline", "logs/bronze_pipeline.log")

PROCESSED_FILES_PATH = Path("data/metadata/processed_files.json")


# --- Checkpoint Management ---
def load_processed_files() -> dict:
    """Đọc tệp manifest theo dõi các file đã xử lý thành công."""
    if PROCESSED_FILES_PATH.exists():
        try:
            with open(PROCESSED_FILES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Không thể đọc checkpoint manifest: {e}")
            return {}
    return {}


def mark_file_as_processed(file_name: str, record_count: int) -> None:
    """Ghi nhận một file đã hoàn tất vào data/metadata/processed_files.json."""
    PROCESSED_FILES_PATH.parent.mkdir(parents=True, exist_ok=True)
    manifest = load_processed_files()

    manifest[file_name] = {
        "status": "COMPLETED",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "total_records": record_count,
    }

    with open(PROCESSED_FILES_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)


def is_file_processed(file_name: str) -> bool:
    """Kiểm tra xem file TSV đã được xử lý thành công trước đó chưa."""
    manifest = load_processed_files()
    return manifest.get(file_name, {}).get("status") == "COMPLETED"


# --- Worker Task ---
def process_single_file(args):
    input_file, shared_metrics_queue = args
    file_name = os.path.basename(input_file)
    now = datetime.now(timezone.utc)
    year, month = now.year, f"{now.month:02d}"

    category = extract_category_from_filename(file_name)

    if is_file_processed(file_name):
        logger.info(f"[SKIP] Đã xử lý thành công từ trước: {file_name}")
        return

    storage = PipelineStorage()
    dlq = DeadLetterQueue(storage=storage)
    batch_id = str(uuid.uuid4())
    chunk = None
    total_file_rows = 0

    try:
        logger.info(f"[START] Tiến hành xử lý tệp: {file_name}")
        reader = read_tsv_in_chunks(input_file)

        for i, chunk in enumerate(reader):
            # 1. Tối ưu RAM &amp; Validate
            chunk = optimize_dataframe(chunk)
            chunk = add_metadata_columns(chunk, file_name, batch_id)
            chunk = validate_chunk(chunk)

            # 2. Ghi trực tiếp ra data/bronze/ qua PipelineStorage
            saved_path = process_and_save_chunk(
                df=chunk,
                storage=storage,
                category=category,
                year=year,
                month=month,
                chunk_idx=i,
            )

            # 3. Trích xuất file mẫu nếu là chunk đầu tiên
            if i == 0:
                save_sample_chunk(df=chunk, storage=storage, category=category)

            chunk_rows = len(chunk)
            total_file_rows += chunk_rows
            shared_metrics_queue.put(("rows", chunk_rows))
            shared_metrics_queue.put(("chunks", 1))

        # 4. Đánh dấu hoàn tất
        mark_file_as_processed(file_name, total_file_rows)
        shared_metrics_queue.put(("files", 1))
        logger.info(f"[SUCCESS] Hoàn thành: {file_name} ({total_file_rows:,} dòng)")

    except Exception as e:
        error_msg = f"Lỗi xử lý file {file_name}: {str(e)}"
        logger.error(error_msg)
        dlq.write_error(
            data=chunk if chunk is not None else {"file": file_name},
            layer="bronze",
            source_file=file_name,
            error_message=str(e),
        )
        if chunk is not None:
            shared_metrics_queue.put(("failed_rows", len(chunk)))


# --- Entrypoint ---
def run_pipeline():
    logger.info("=============================================")
    logger.info("  BẮT ĐẦU RUNNING BRONZE PIPELINE (LOCAL)   ")
    logger.info("=============================================")

    # Khởi tạo Storage
    storage = PipelineStorage()

    # Tải &amp; Phát hiện danh sách tệp TSV
    dataset_path = download_kaggle_dataset()
    input_files = discover_tsv_files(dataset_path)

    # Khởi tạo Queue thu gom Metrics giữa các tiến trình
    manager = multiprocessing.Manager()
    shared_metrics_queue = manager.Queue()
    task_args = [(file, shared_metrics_queue) for file in input_files]

    # Giới hạn tối đa 3 workers để dành 1 core cho OS / Airflow (máy local 4-core)
    workers = min(MAX_WORKERS, 3)
    logger.info(f"Khởi chạy ProcessPoolExecutor với {workers} workers...")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        list(tqdm(executor.map(process_single_file, task_args), total=len(input_files)))

    # Tổng hợp kết quả Metrics
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

    logger.info("\n========= KẾT QUẢ THỰC THI BRONZE LAYER =========")
    logger.info(f"Số file hoàn tất: {total_files}/{len(input_files)}")
    logger.info(f"Số chunks đã ghi: {total_chunks}")
    logger.info(f"Tổng số dòng thành công: {total_rows:,}")
    logger.info(f"Tổng số dòng thất bại: {failed_rows:,}")
    logger.info("=================================================\n")

    cleanup_files(dataset_path)
    logger.info("=== HOÀN THÀNH HOÀN TOÀN BRONZE PIPELINE ===")


if __name__ == "__main__":
    run_pipeline()
