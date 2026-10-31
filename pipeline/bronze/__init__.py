## pipeline/bronze/__init__.py
"""
Bronze Layer Pipeline Package
Quản lý việc nạp dữ liệu thô từ Kaggle API và lưu trữ dưới dạng Parquet.
"""

__all__ = ["run_pipeline"]


def __getattr__(name):
    # Lazy import: tránh RuntimeWarning khi chạy `python -m pipeline.bronze.pipeline`
    if name == "run_pipeline":
        from pipeline.bronze.pipeline import run_pipeline

        return run_pipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals()) + __all__)
