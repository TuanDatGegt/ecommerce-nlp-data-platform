## pipeline/gold/config.py
import os
from pathlib import Path
from configs.settings import (
    PROJECT_ROOT,
    DATA_LAKE_PATH,
    SILVER_DATA_PATH,
    GOLD_DATA_PATH,
    METADATA_PATH,
)

# Đường dẫn con của Gold Layer
GOLD_DASHBOARD_DIR = GOLD_DATA_PATH / "dashboard"
GOLD_NLP_DIR = GOLD_DATA_PATH / "nlp_features"
# cho phép đặt thư mục temp bằng .env, nên trỏ vào ổ nhiều dung lượng
DUCKDB_TEMP_DIR = Path(os.getenv("DUCKDB_TEMP_DIR", str(METADATA_PATH / "duckdb_temp")))
DUCKDB_MAX_TEMP_SIZE = os.getenv("DUCKDB_MAX_TEMP_SIZE", "200GB")

# Ngưỡng cấu hình xử lý
MIN_BODY_LEN = int(os.getenv("GOLD_MIN_BODY_LEN", "10"))
TOP_N_PRODUCTS = int(os.getenv("GOLD_TOP_N_PRODUCTS", "1000"))
HELPFUL_MIN_VOTES = int(os.getenv("GOLD_HELPFUL_MIN_VOTES", "2"))
HELPFUL_MIN_RATIO = float(os.getenv("GOLD_HELPFUL_MIN_RATIO", "0.6"))

# Cấu hình tài nguyên DuckDB
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")
DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", "3"))

# File mapping phân nhóm ngành
CATEGORY_GROUPS_CSV = PROJECT_ROOT / "configs" / "category_groups.csv"


def validate_environment():
    """Kiểm tra sự tồn tại của thư mục nguồn Silver và khởi tạo thư mục đích."""
    if not SILVER_DATA_PATH.exists():
        raise FileNotFoundError(
            f"[GOLD CONFIG ERROR] Thư mục Silver không tồn tại: {SILVER_DATA_PATH}"
        )
    GOLD_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    GOLD_NLP_DIR.mkdir(parents=True, exist_ok=True)
    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)
