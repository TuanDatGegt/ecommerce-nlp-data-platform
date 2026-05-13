#scritps/run_bronze_ingestion.py

import glob
import os

from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

from configs.settings import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    MAX_WORKERS
)

from pipeline.bronze.ingrest import read_tsv_in_chunks

from pipeline.bronze.transform import (
    optimize_dataframe,
    add_metadata_columns
)

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

logger = setup_logger(
    "bronze_ingestion",
    "logs/bronze_ingestion.log")

def process_single_file(input_file):
    file_name = os.path.basename(input_file)
    category = extract_category_from_filename(file_name)

    output_dir = build_partition_path(
        PROCESSED_DATA_PATH,
        category
    )

    try: 
        reader = read_tsv_in_chunks(input_file)
        for i, chunk in enumerate(reader):
            chunk = optimize_dataframe(chunk)
            chunk = add_metadata_columns(
                chunk, 
                category
            )

        validate_required_columns(chunk)
        path = write_parquet_chunk(
            chunk,
            output_dir,
            i
        )
        logger.info(
            f"Processed {path}"
        )

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

