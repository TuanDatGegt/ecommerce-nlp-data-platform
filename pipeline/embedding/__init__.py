## pipeline/embedding/__init__.py
"""
Text Embedding Pipeline Package
Vector hóa văn bản bài review và nạp vào CSDL Vector ChromaDB.
"""

__all__ = ["TextEmbedder", "run_pipeline"]


def __getattr__(name):
    # Lazy import: không nạp thư viện embedding/ChromaDB nặng khi chỉ import package
    if name in __all__:
        from pipeline.embedding import embedder

        return getattr(embedder, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals()) + __all__)
