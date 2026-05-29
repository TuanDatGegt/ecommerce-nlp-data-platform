#pipeline/bronze/kaggle/cleanup.py

import os
import shutil

def cleanup_directory(path):
    """Remove temporary directory safely"""

    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Deleted temporary directory: {path}")

def cleanup_files(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted file: {file_path}")