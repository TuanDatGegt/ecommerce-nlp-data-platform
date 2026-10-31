## configs/__init__.py
"""
Configurations Package
Quản lý các thông số cấu hình hệ thống, đường dẫn Data Lake và DB connections.
"""

from configs.settings import (
    BRONZE_DATA_PATH,
    DATA_LAKE_PATH,
    DLQ_DATA_PATH,
    GOLD_DATA_PATH,
    MANIFEST_PATH,
    METADATA_PATH,
    PROJECT_ROOT,
    RAW_DATA_PATH,
    SAMPLE_DATA_PATH,
    SILVER_DATA_PATH,
)

__all__ = [
    "PROJECT_ROOT",
    "DATA_LAKE_PATH",
    "RAW_DATA_PATH",
    "SAMPLE_DATA_PATH",
    "BRONZE_DATA_PATH",
    "SILVER_DATA_PATH",
    "GOLD_DATA_PATH",
    "DLQ_DATA_PATH",
    "METADATA_PATH",
    "MANIFEST_PATH",
]
