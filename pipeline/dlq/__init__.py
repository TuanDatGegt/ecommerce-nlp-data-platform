"""
Dead Letter Queue (DLQ) Package
Ghi nhận và cô lập các bản ghi lỗi dữ liệu trong quá trình xử lý pipeline.
"""

from pipeline.dlq.pipeline_dlq import DeadLetterQueue

__all__ = ["DeadLetterQueue"]
