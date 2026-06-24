#pipeline/gold/metadata/quality_report.py

import os, json
from configs.settings import GOLD_BUCKET_NAME
from pipeline.storage.pipeline_storage import StorageClient

def generate_quality_report(df):
    """Tạo báo cáo và lưu lên MinIO"""
    report={
        "total_rows": int(len(df)),
        "avg_review_length": float(df["review_length"].mean()),
        "avg_word_count": float(df["word_count"].mean()),
        "avg_helpful_ratio": float(df["helpful_ratio"].mean()),
        "positive_reviews": int((df["sentiment_proxy"]=="positive").sum()),
        "neural_reviews": int((df["sentiment_proxy"]=="neutral").sum()),
        "negative_reviews": int((df["sentiment_proxy"]=="negative").sum())
    }

    storage = StorageClient()
    object_name = "metadata/gold_quality_report.json"

    storage.upload_json_to_minio(GOLD_BUCKET_NAME, object_name, report)
    
    return report

