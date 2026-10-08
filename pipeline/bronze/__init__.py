## pipeline/bronze/__init__.py
"""
Bronze Pipeline Package
Cung cấp giao diện chạy pipeline xử lý tầng Bronze chuẩn hóa.
"""

from pipeline.bronze.pipeline import run_pipeline

__all__ = ["run_pipeline"]
