#pipeline/bronze/kaggle/downloader.py

import os, glob
import kagglehub

from configs.settings import KAGGLE_DATASET, KAGGLEHUB_CACHE_DIR
from pipeline.bronze.kaggle.auth import validate_kaggle_credentials

def download_kaggle_dataset():

    validate_kaggle_credentials()

    print("==========================================")
    print(f"   STARTING DOWNLOAD: {KAGGLE_DATASET}")
    print("==========================================")

    target_path = os.path.abspath(KAGGLEHUB_CACHE_DIR)
    print(f"[*] Target Directory: {target_path}")

    if os.path.exists(target_path):
        existing_tsv_files = glob.glob(os.path.join(target_path, "*.tsv"))
        if existing_tsv_files:
            print(f"[✓] Cache Hit! Phát hiện {len(existing_tsv_files)} file TSV đang nằm an toàn ở ổ E.")
            print(f"[*] Tự động bỏ qua bước tải mạng, tái sử dụng tài nguyên local tại: {target_path}")
            print("==========================================")
            return target_path
    try:
        dataset_path = kagglehub.dataset_download(
            handle=KAGGLE_DATASET,
            output_dir=target_path,
            force_download=True
            )
        
        print(f"[✓] Success: Dataset downloaded and extracted to -> {dataset_path}")
        print("==========================================")
        return dataset_path
    except Exception as e:
        print(f"[❌] CRITICAL ERROR during dataset download: {str(e)}")
        raise e
    
if __name__ == "__main__":
    download_kaggle_dataset()

