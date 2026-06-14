#pipeline/silver/orchestration/silver_pipeline.py

import os
from concurrent.futures import ProcessPoolExecutor
from pipeline.silver.storage.silver_storage import SilverStorage
from pipeline.silver.processing.cleaner import process_silver_cleaning
from pipeline.silver.processing.enricher import enrich_feature, enrich_sentiment
from utils.logger import setup_logger

logger = setup_logger('silver_pipeline', 'logs/silver_pipeline.log')

def transform_single_file(object_name: str):
    try:
        storage = SilverStorage()
        logger.info("[PROCESS] Processing file from Bronze: {object_name}")

        df = storage.read_parquet_from_bronze(object_name)
        if df.empty:
            logger.warning(f"[SKIP] File {object_name} is empty.")
            return True
        
        df = process_silver_cleaning(df)
        df = enrich_sentiment(df)
        df = enrich_feature(df)

        silver_object_name = object_name.replace("bronze/", "silver/") if "bronze/" in object_name else f"silver/{object_name}"
        storage.write_parquet_to_silver(df, silver_object_name)

        logger.info(f"[SUCCESS] Transform and load to Silver: {silver_object_name}")
        return True
    except Exception as e:
        logger.error(f"[ERROR] Failed to process file {object_name}: {str(e)}")
        return False
    
def run_silver_pipeline():
    logger.info(f"[START] Initializing Silver Pipeline...")
    storage = SilverStorage()

    try:
        objects = storage.client.client.list_objects(storage.client.bucket_name, recursive=True)
        bronze_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
    except Exception as e:
        logger.error(f"[CRITICAL] Cannot list objects from Bronze Bucket: {str(e)}")
        return
    if not bronze_files:
        logger.info(f"[STOP] No new data found in Bronze Layer.")
        return
    logger.info(f"[INFO] Found {len(bronze_files)} files to process. Starting ProcessPoolExecutor...")

    max_workers = min(os.cpu_count() or 2, 4)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(transform_single_file, bronze_files))

    success_count = sum(1 for r in results if r)
    logger.info(f"[COMPLETE] Silver Pipeline finished. Success: {success_count}/{len(bronze_files)} files.")


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    run_silver_pipeline()

