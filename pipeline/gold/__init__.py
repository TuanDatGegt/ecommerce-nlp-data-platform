## pipeline/gold/__init__.py
"""
Gold Layer Pipeline Package
Cung cấp giao diện chạy pipeline tổng hợp và trích xuất thuộc tính tầng Gold.
"""

__all__ = ["run_pipeline"]


def __getattr__(name):
    # Lazy import: tránh RuntimeWarning khi chạy `python -m pipeline.gold.pipeline`
    if name == "run_pipeline":
        from pipeline.gold.pipeline import run_pipeline

        return run_pipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals()) + __all__)
