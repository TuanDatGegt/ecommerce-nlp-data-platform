#pipeline/bronze/processing/validate.py
import os

from configs.schemas.review_schemas import REQUIRED_COLUMNS

def validate_required_columns(df):
    missing = [
        col for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing:
        raise ValueError(f"missing columns: {missing}")
    
def validate_rating_range(df):
    if "star_rating" not in df.columns:
        return df
    
    return df[
        (df["star_rating"] >1) & (df["star_rating"] <=5)
    ]

def validate_helpful_votes(df):
    if ("helpful_votes" not in df.columns
        or "total_votes" not in df.columns
    ):
        return df
    
    return df[
        (df["helpful_votes"] <= df["total_votes"])
    ]

def removed_null_reviews(df):

    required_text_columns = ["review_body", "review_title"]
    for col in required_text_columns:
        if col in df.columns:
            df = df[df[col].notna()]
    
    return df


def remove_duplicates(df):
    if "review_id" not in df.columns:
        return df
    
    return df.drop_duplicates(subset=["review_id"])


