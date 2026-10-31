## pipeline/gold/__init__.py
"""
Gold Pipeline Package
Cung cấp giao diện chạy pipeline tổng hợp và trích xuất thuộc tính tầng Gold.
"""

__all__ = ["run_pipeline"]


def __getattr__(name):
    # Chỉ nạp pipeline.gold.pipeline khi thực sự cần,
    # tránh cảnh báo RuntimeWarning khi chạy `python -m pipeline.gold.pipeline`.
    if name == "run_pipeline":
        from pipeline.gold.pipeline import run_pipeline

        return run_pipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
