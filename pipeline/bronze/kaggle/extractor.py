#pipeline/bronze/kaggle/extractor.py

import glob
import os
import shutil

def discover_tsv_files(dataset_path):
    pattern = os.path.join(
        dataset_path,
        "*.tsv"
    )

    files = glob.glob(pattern)
    print(f"Found {len(files)} TSV files")

    if not files:
        raise FileNotFoundError(f"No TSV files found in {dataset_path}")
    
    return files

def copy_tsv_to_staging(input_files, staging_dir):
    """
    Copy TSV files into temporary staging directory for processing.
    """
    os.makedirs(staging_dir, exist_ok=True)

    staged_files = []

    for file_path in input_files:
        destination = os.path.join(staging_dir, os.path.basename(file_path))
        shutil.copy2(file_path, destination)
        staged_files.append(destination)

    print(f"Copied {len(staged_files)} TSV files to staging directory")

    return staged_files

