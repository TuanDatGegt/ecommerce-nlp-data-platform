#pipeline/bronze/transform.py

import pandas as pd
from datetime import datetime, timezone
import uuid

def optimize_dataframe(df):
    """
    Optimize the DataFrame by converting data types to reduce memory usage.
    """
    bool_mapping = {"Y": True, "N": False}

    for col in ["vine", "verified_purchase"]:
        if col in df.columns:
            df[col] = df[col].map(bool_mapping)
    
    cate_columns = [
        "marketplace",
        "product_category"
    ]

    for col in cate_columns:
        if col in df.columns:
            df[col] = df[col].astype('category')

    int_columns = [
        'star_rating',
        'helpful_votes',
        'total_votes'
    ]

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

    if "review_date" in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')

    return df


def add_metadata_columns(df, source_file):
    """
    Add metadata columns to the DataFrame.
    """

    df['source_file'] = source_file
    df['ingest_time'] = datetime.now(timezone.utc)

    batch_id = str(uuid.uuid4())
    df["batch_id"]=batch_id
    
    return df