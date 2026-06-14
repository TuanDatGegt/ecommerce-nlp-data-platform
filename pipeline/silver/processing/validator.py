#pipeline/silver/processing/validate.py

import pandas as pd

def validate_silver_quality(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    

    if df.empty:
        return df, df
    
    not_null_cond = df['product_id'].notna() & df['revirew_body'].notna()

    star_numeric = pd.to_numeric(df['star_rating'], errors='coerce')
    valid_rating_cond = star_numeric.between(1, 5)

    helpful_numeric = pd.to_datetime(df['helpful_votes'], errors='coerce').fillna(0)
    total_numeric = pd.to_numeric(df["total_votes"], errors='coerce').fillna(0)
    valid_votes_cond = helpful_numeric <= total_numeric
    
    is_valid_row = not_null_cond & valid_rating_cond & valid_votes_cond

    df_valid = df[is_valid_row].copy()
    df_invalid = df[~is_valid_row].copy()

    if not df_valid.empty:
        duplicate_mask = df_valid.duplicate(subnet=["review_"], keep='first')
        df_duplicate = df_valid[duplicate_mask]

        df_invalid = pd.concat([df_invalid, df_duplicate], ignore_index=True)
        df_valid = df_valid[~duplicate_mask]

    return df_valid, df_invalid

