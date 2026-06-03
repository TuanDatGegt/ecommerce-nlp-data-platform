#config/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

DATA_LAKE_PATH = os.getenv('DATA_LAKE_PATH')
SAMPLE_DATA_PATH = os.getenv('SAMPLE_DATA_PATH')

yaml_config_path = os.getenv('YAML_CONFIG_PATH')

MANIFEST_PATH = os.getenv('MANIFEST_PATH')

MAX_WORKERS = int(os.getenv('MAX_WORKERS', 4))

CHUNK_SIZE = 200_000
COMPRESSION = 'snappy'

CATEGORY_MAPPING = r"amazon_reviews_us_([A-Za-z0-9_\-]+)_v"

#Kaggle API
API_KAGGLE_TOKEN_KEY = os.getenv('API_KAGGLE_TOKEN_KEY')
USERNAME_KAGGLE = os.getenv('USERNAME_KAGGLE')
KAGGLE_DATASET = os.getenv('KAGGLE_DATASET')

#MinIO configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'False').lower() == 'true'

RAW_BUCKET_NAME=os.getenv("RAW_BUCKET_NAME")
CHECKPOINT_OBJECT = os.getenv("CHECKPOINT_OBJECT")
LINEAGE_OBJECT_NAME=os.getenv("LINEAGE_OBJECT_NAME")

#Lấy dia chi luu data trong repo
KAGGLEHUB_CACHE_DIR = os.getenv("KAGGLEHUB_CACHE_DIR")

os.environ["KAGGLEHUB_CACHE_DIR"] = KAGGLEHUB_CACHE_DIR

def init_repo_directory(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        print(f"[SYSTEM] Inititalized path dir temp: '{dir_path}'")
    gitkeep_path = os.path.join(dir_path, ".gitkeep")
    if os.path.exists(gitkeep_path):
        with open(gitkeep_path, "w", encoding='utf-8') as f:
            f.write("# File dùng để giữ chỗ thư mục trên Git Repo. Không xóa file này.\n")
        print(f" [Hệ thống] Đã tạo file giữ chỗ '{gitkeep_path}' thành công.")

init_repo_directory(KAGGLEHUB_CACHE_DIR)
