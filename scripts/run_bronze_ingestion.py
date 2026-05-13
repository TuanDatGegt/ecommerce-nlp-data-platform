#scritps/run_bronze_ingestion.py

import glob
import os

from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

from configs.settings import (
    RAW_DATA_PATH,
    DATA_LAKE_PATH,
    MAX_WORKERS
)

from pipeline.bronze.ingrest import read_tsv_in_chunks

from pipeline.bronze.transform import (
    optimize_dataframe,
    add_metadata_columns
)

from pipeline.bronze.sample_writer import save_sample_chunk

from pipeline.bronze.validate import (
    validate_required_columns,
    write_to_dlq
)

from pipeline.bronze.writer import (
    write_parquet_chunk
)

from pipeline.bronze.metadata import (
    extract_category_from_filename,
    build_partition_path
)

from utils.logger import setup_logger

from utils.checkpoint import (
    is_processed,
    mark_processed
)

from utils.metrics import PinelineMetrics

metrics = PinelineMetrics()



logger = setup_logger(
    "bronze_ingestion",
    "logs/bronze_ingestion.log")

def process_single_file(input_file):
    file_name = os.path.basename(input_file)
    category = extract_category_from_filename(file_name)

    output_dir = build_partition_path(
        category
    )

    if is_processed(file_name):
        logger.info(
            f"SKIPPED {file_name}"
        )
        return category

    try: 
        reader = read_tsv_in_chunks(input_file)
        for i, chunk in enumerate(reader):
            chunk = optimize_dataframe(chunk)
            chunk = add_metadata_columns(
                chunk, 
                category
            )

            validate_required_columns(chunk)
            metrics.add_rows(len(chunk))
            path = write_parquet_chunk(
                chunk,
                output_dir,
                i
            )
            if i == 0:
                save_sample_chunk(parquet_path=path, category=category)

            logger.info(
                f"Processed {path}"
            )
        metrics.add_file()
        mark_processed(file_name)

    except Exception as e:
        logger.error(
            f"FAILED {file_name} | {str(e)}"
        )
        write_to_dlq(chunk, file_name)
    
    return category

if __name__ == "__main__":

    input_files = glob.glob(
        os.path.join(RAW_DATA_PATH, "*.tsv")
    )

    with ProcessPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:
        list(tqdm(
            executor.map(process_single_file, input_files),
            total=len(input_files)
        ))

