## pipeline/silver/__init__.py
"""
Silver Layer Pipeline Package
Làm sạch, validate và loại bỏ trùng lặp dữ liệu từ tầng Bronze.
"""

__all__ = ["run_pipeline"]


def __getattr__(name):
    # Lazy import: tránh RuntimeWarning khi chạy `python -m pipeline.silver.pipeline`
    if name == "run_pipeline":
        from pipeline.silver.pipeline import run_pipeline

        return run_pipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals()) + __all__)
