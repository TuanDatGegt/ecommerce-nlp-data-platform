#pipeline/bronze/orchestration/bronze_pipeline.py

import os
import sys

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from tqdm import tqdm
import multiprocessing

from pipeline.bronze.kaggle.downloader import download_kaggle_dataset
from pipeline.bronze.storage.raw_storage import upload_parquet_file, build_bronze_object_name
from pipeline.bronze.storage.minio_client import MinioClient
from pipeline.bronze.kaggle.extractor import discover_tsv_files
from pipeline.bronze.kaggle.cleanup import cleanup_files
from pipeline.bronze.processing.reader import read_tsv_in_chunks
from pipeline.bronze.processing.transform import optimize_dataframe, add_metadata_columns
from pipeline.bronze.processing.validate import (
    validate_required_columns,
    validate_rating_range,
    validate_helpful_votes,
    removed_null_reviews,
    remove_duplicates
)

from pipeline.bronze.processing.metadata import (
    extract_category_from_filename,
    build_partition_prefix
)

from pipeline.bronze.processing.writer import write_parquet_chunk
from pipeline.bronze.state.checkpoint import is_processed, mark_processed
from pipeline.bronze.monitoring.metrics import PipelineMetrics
from pipeline.bronze.monitoring.lineage import record_lineage
from pipeline.bronze.dlq.dead_letter_queue import write_to_dlq
from utils.logger import setup_logger
from configs.settings import MAX_WORKERS

logger = setup_logger(
    "bronze_pipeline",
    "logs/bronze_pipeline.log"
)

def process_single_file(args):
    input_file, shared_metrics_queue = args
    file_name = os.path.basename(input_file)

    now = datetime.now(timezone.utc)
    year = now.year
    month = f"{now.month:02d}"

    category = extract_category_from_filename(file_name)
   

    if is_processed(file_name):
        logger.info(
            f"SKIPPED: {file_name}"
        )
        return
    
    chunk = None
    try:
        logger.info(f"START PROCESSING: {file_name}")
        reader = read_tsv_in_chunks(input_file)

        for i, chunk in enumerate(reader):
            chunk = optimize_dataframe(chunk)
            chunk = add_metadata_columns(chunk, file_name)
            validate_required_columns(chunk)
            chunk = validate_rating_range(chunk)
            chunk = validate_helpful_votes(chunk)
            chunk = removed_null_reviews(chunk)
            chunk = remove_duplicates(chunk)

            local_file_path = write_parquet_chunk(df=chunk, chunk_idx=i, output_prefix=category)

            object_name=build_bronze_object_name(
                category=category,
                year=year,
                month=month,
                chunk_idx=i
            )

            parquet_object = upload_parquet_file(local_file_path, object_name)

            if os.path.exists(local_file_path):
                os.remove(local_file_path)

            parquet_obj_str = str(parquet_object) if isinstance(parquet_object, list) else parquet_object

            record_lineage(
                source_file=file_name,
                parquet_objects=parquet_object,
                category=category,
                row_count=len(chunk),
                chunk_idx=i
            )

            shared_metrics_queue.put(("rows", len(chunk)))
            shared_metrics_queue.put(("chunks", 1))
            logger.info(f"SUCCESS: {parquet_object}")

        mark_processed(file_name)
        shared_metrics_queue.put(("files", 1))
        logger.info(f"COMPLETED FILE: {file_name}")

    except Exception as e:
        error_message = f"CRITICAL ERROR in file {file_name}: {str(e)}"
        logger.error(error_message)
        
        safe_df = chunk
        if isinstance(chunk, list):
            import pandas as pd
            try:
                safe_df = pd.DataFrame(chunk)
            except Exception:
                safe_df = pd.DataFrame([{"raw_data_error": str(chunk)}])

        write_to_dlq(
            df=chunk,
            source_file=file_name,
            error_message=str(e)
        )

        if chunk is not None:
            try:
                shared_metrics_queue.put(("failed_rows", len(chunk)))
            except Exception:
                shared_metrics_queue.put(("failed_rows", 1))

def run_pipeline():
    """Function to run the system Data Pipeline in layer Bronze"""
    logger.info("==========================================")
    logger.info("   STARTING AMAZON REVIEWS BRONZE PIPELINE ")
    logger.info("==========================================")

    logger.info("Checking Storage Infrastructure (MinIO)...")
    try:
        print("[INFO] Kích hoạt tiến trình chính - Kiểm tra hạ tầng lưu trữ...")
        MinioClient(auto_init=True)
        logger.info("MinIO Infrastructure is ready.")
    except Exception as e:
        logger.critical(f"Pipeline has Stopped. MinIO Initialization failed: {str(e)}")
        print(f"Không thể chạy pipeline do lỗi kết nối với MinIO Server. Hãy chắc chắn đã bật MinIO trên Bash")
        return
    
    dataset_path = download_kaggle_dataset()

    input_files = discover_tsv_files(dataset_path)
    if not input_files:
        logger.warning("No TSV files found in the dataset. Exiting pipeline.")
        return
    
    manager = multiprocessing.Manager()
    shared_metrics_queue = manager.Queue()

    task_args = [(file, shared_metrics_queue) for file in input_files]

    logger.info(f"Processing {len(input_files)} files with {MAX_WORKERS} workers...")
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(executor.map(process_single_file, task_args), total=len(input_files)))

    metrics = PipelineMetrics()

    while not shared_metrics_queue.empty():
        metric_type, value = shared_metrics_queue.get()
        if metric_type == "rows":
            metrics.add_rows(value)
        elif metric_type == "chunks":
            for _ in range(value):
                metrics.add_processed_chunk()
        elif metric_type == "files":
            for _ in range(value):
                metrics.add_processed_file()
        elif metric_type == "failed_rows":
            metrics.add_failed_rows(value)

    logger.info("Start cleaning up local middleware files...")
    cleanup_files(dataset_path)

    metrics.print_summary()
    logger.info("==========================================")
    logger.info("        PIPELINE COMPLETED SUCCESSFULLY   ")
    logger.info("==========================================")

if __name__ == "__main__":
    run_pipeline()

