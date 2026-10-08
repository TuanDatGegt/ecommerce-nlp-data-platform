#pipeline/silver/orchestration/silver_pipeline.py

import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

#Storage
from pipeline.silver.storage.silver_storage import SilverStorage

#Processing
from pipeline.silver.processing.cleaner import process_silver_cleaning
from pipeline.silver.processing.enricher import enrich_feature, enrich_sentiment
from pipeline.silver.processing.validator import validate_silver_quality

#Logger
from utils.logger import setup_logger

#Environment
from configs.settings import BRONZE_BUCKET_NAME, BRONZE_DATASET_PREFIX, SILVER_BUCKET_NAME, MAX_WORKERS

logger = setup_logger('silver_pipeline', 'logs/silver_pipeline.log')

def transform_single_file(object_name: str):
    try:
        storage = SilverStorage(ensur_bucket=False)
        logger.info(f"[PROCESS] Processing file from Bronze: {object_name}")

        df_raw_storage = storage.read_parquet_from_bronze(object_name)
        if df_raw_storage.empty:
            logger.warning(f"[SKIP] File {object_name} is empty.")
            return True
        
        df_cleaned, df_invalid_lang = process_silver_cleaning(df_raw_storage)

        df_valid, df_invalid_rules = validate_silver_quality(df_cleaned)

        df_all_errors = pd.concat([df_invalid_rules, df_invalid_lang], ignore_index=True)
        if not df_all_errors.empty:
            storage.write_to_dlq_json(df_all_errors, object_name)
            logger.info(f"[DLQ] Dumped {len(df_all_errors)} invalid record to Dead Letter Queue from {object_name}")

        if not df_valid.empty:
            df_valid = enrich_sentiment(df_valid)
            df_valid = enrich_feature(df_valid)
            
            partition_path =  object_name.replace(BRONZE_DATASET_PREFIX, "").lstrip("/")
            silver_object_name = f"cleaned_amazon_reviews/{partition_path}"

            logger.info(f"[WRITE] Uploading clean data to Silver Bucket: {SILVER_BUCKET_NAME}")
            storage.write_parquet_to_silver(df_valid, silver_object_name)
            logger.info(f"[SUCCESS] Transformed and loaded {len(df_valid)} clean records to Silver: {silver_object_name}")
        else:
            logger.warning(f"[SKIP] No clean English records left in file {object_name} after filtering.")

        return True
    except Exception as e:
        logger.error(f"[ERROR] Failed to process file {object_name}: {str(e)}")
        return False
    
def run_silver_pipeline():
    logger.info(f"[START] Initializing Silver Pipeline...")

    init_storage = SilverStorage(ensur_bucket=True)
    
    logger.info(f"[CONFIG] Target scan -> Bucket: {BRONZE_BUCKET_NAME} | Prefix: {BRONZE_DATASET_PREFIX}")
    print(f"[CONSOLE] Scanning data based on prefixes. '{BRONZE_DATASET_PREFIX}' inside bucket '{BRONZE_BUCKET_NAME}'")

    try:
        objects = init_storage.client.list_objects(
            bucket_name=BRONZE_BUCKET_NAME,
            prefix=BRONZE_DATASET_PREFIX,
            recursive=True
        )

        bronze_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
    except Exception as e:
        logger.error(f"[CRITICAL] Cannot list objects from Bronze Bucket: {str(e)}")
        return
    
    del init_storage
    
    if not bronze_files:
        logger.info(f"[STOP] No new data found in Bronze Layer.")
        return
    
    logger.info(f"[INFO] Found {len(bronze_files)} files to process. Starting ProcessPoolExecutor...")
    print(f"[CONSOLE] Found {len(bronze_files)} parquet file. Processing...")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(transform_single_file, bronze_files))

    success_count = sum(1 for r in results if r)
    logger.info(f"[COMPLETE] Silver Pipeline finished. Success: {success_count}/{len(bronze_files)} files.")
    print(f"[CONSOLE] Pipeline executed completely. Success: {success_count}/{len(bronze_files)}")


if __name__ == "__main__":
    import multiprocessing                          
    multiprocessing.freeze_support()

    print(f"ACTIVATED FUNCTION RUN_SILVER_PIPELINE")
    run_silver_pipeline()

