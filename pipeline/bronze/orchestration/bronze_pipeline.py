#pipeline/bronze/orchestration/bronze_pipeline.py

import os

from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

from pipeline.bronze.kaggle.downloader import download_kaggle_dataset
from pipeline.bronze.storage.raw_storage import upload_raw_dataset
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
from pipeline.bronze.monitoring.metrics import PinelineMetrics
from pipeline.bronze.monitoring.lineage import record_lineage
from pipeline.bronze.dlq.dead_letter_queue import write_to_dlq
from utils.logger import setup_logger
from configs.settings import MAX_WORKERS

logger = setup_logger(
    "bronze_pipeline",
    "logs/bronze_pipeline.log"
)

def process_single_file(input_file):
    file_name = os.path.basename(input_file)
    category = extract_category_from_filename(file_name)
    output_prefix = build_partition_prefix(category)
    
    local_metrics = {
        "rows":0,
        "chunks":0,
        "files":0
    }

    if is_processed(file_name):
        logger.info(
            f"SKIPPED: {file_name}"
        )
        return
    
    chunk = None
    try:
        reader = read_tsv_in_chunks(input_file)

        for i, chunk in enumerate(reader):
            chunk = optimize_dataframe(chunk)
            chunk = add_metadata_columns(chunk, file_name)
            validate_required_columns(chunk)
            chunk = validate_rating_range(chunk)
            chunk = validate_helpful_votes(chunk)
            chunk = removed_null_reviews(chunk)
            chunk = remove_duplicates(chunk)

            parquet_object=(
                write_parquet_chunk(
                    df=chunk,
                    output_prefix=output_prefix,
                    chunk_idx=i
                )
            )

            record_lineage(
                source_file=file_name,
                parquet_objects=parquet_object,
                category=category,
                row_count=len(chunk),
                chunk_idx=i
            )

            local_metrics["rows"] += len(chunk)
            local_metrics["chunks"] += 1
            logger.info(
                f"SUCCESS: {parquet_object}"
            )

        local_metrics["files"] += 1
        mark_processed(file_name)

    except Exception as e:
        logger.error(
            f"FAILED: {file_name} {str(e)}"
        )

        write_to_dlq(
            df=chunk,
            source_file=file_name,
            error_message=str(e)
        )
    return local_metrics



def run_pipeline():
    logger.info(
        "STARTING BRONZE PIPELINE"
    )

    dataset_path = download_kaggle_dataset()
    upload_raw_dataset(dataset_path)
    input_files = discover_tsv_files(dataset_path)

    metrics = PinelineMetrics()

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results=list(tqdm(executor.map(process_single_file, input_files), total=len(input_files)))

        for res in results:
            if res:
                metrics.add_rows(res["rows"])
                for _ in range(res["chunks"]): metrics.add_processed_chunk(res["chunks"])
                for _ in range(res["files"]): metrics.add_processed_file(res["files"])

        cleanup_files(dataset_path)
        metrics.print_summary()
        logger.info("BRONZE PIPELINE COMPLETED")