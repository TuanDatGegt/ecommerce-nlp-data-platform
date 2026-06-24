#pipeline/gold/processing/aggregations.py

import pandas as pd

def aggregate_product_metrics(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby("product_id")
        .agg(
            avg_rating=("star_rating", "mean"),
            review_count=("review_id", "count"),
            avg_helpful_ratio=("helpful_ratio", "mean")
        ).reset_index()
    )
    return result


def aggregate_category_metrics(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby("product_category")
        .agg(
            avg_rating=("star_rating", "mean"),
            review_count=("review_id", "count")
        ).reset_index()
    )
    return result

def aggregate_time_metrics(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()

    tmp["review_date"] = pd.to_datetime(
        tmp["review_date"]
    )

    tmp["year"] = tmp["review_date"].dt.year
    tmp["month"] = tmp["review_date"].dt.month

    result = (
        tmp.groupby(["year", "month"])
        .agg(
            avg_rating=("star_rating", "mean"),
            review_count=("review_id", "count")
        ).reset_index()
    )

    return result



