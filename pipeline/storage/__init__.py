## pipeline/storage/__init__.py
"""
Storage Abstraction Package
Quản lý tương tác lưu trữ Parquet / File System trên Data Lake.
"""

from pipeline.storage.pipeline_storage import PipelineStorage

__all__ = ["PipelineStorage"]
