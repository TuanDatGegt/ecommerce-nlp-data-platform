#pipeline/bronze/kaggle/downloader.py

import os
import kagglehub

from configs.settings import KAGGLE_DATASET

from pipeline.bronze.kaggle.auth import validate_kaggle_credentials

def download_kaggle_dataset():

    validate_kaggle_credentials()

    print(
        f"Downloading dataset {KAGGLE_DATASET}"
    )

    dataset_path = kagglehub.dataset_download(KAGGLE_DATASET)

    print(
        f"Dataset downloaded to {dataset_path}"
    )

    return dataset_path

