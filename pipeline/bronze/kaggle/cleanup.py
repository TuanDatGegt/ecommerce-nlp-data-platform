#pipeline/bronze/kaggle/cleanup.py

import os
import shutil

def cleanup_files(path):
    """Remove temporary directory safely"""

    if not os.path.exists(path):
        print(f"[Dọn dẹp] Thư mục không tồn tại: {path}")
        return

    print("==========================================")
    print("[*] TIẾN HÀNH DỌN DẸP BỘ NHỚ ĐỆM LOCAL...")
    print("==========================================")

    for root, dirs, files in os.walk(path):
        for file in files:
            if file==".gitkeep":
                continue
            
            file_path = os.path.join(root, file)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"[Delete file] -> {file}")
            except Exception as e:
                print(f"[Error] Can not delete file {file_path}: {e}")

        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if "tempdir" in dir_name or "temp_chunks" in dir_name:
                try:
                    shutil.rmtree(dir_path)
                    print(f"[Delete temp file] -> {dir_name}")
                except Exception as e:
                    print(f"[ERROR] Can not delete file {dir_path}: {e}")
    print("Complete infrastructure cleaner middleware local. Completely!")
    print("=============================================================")

