#pipeline/silver/processing/validator.py

import pandas as pd

def validate_silver_quality(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    

    if df.empty:
        return df, df
    
    not_null_cond = (df['product_id'].notna() & df['review_body'].notna())

    valid_rating_cond = df['star_rating'].between(1, 5)

    valid_votes_cond = (df['helpful_votes'] <= df['total_votes'])
    
    valid_mask = (not_null_cond & valid_rating_cond & valid_votes_cond)

    df_valid = df.loc[valid_mask]
    df_invalid = df.loc[~valid_mask]

    if not df_valid.empty:
        duplicate_mask = df_valid.duplicated(subset=["review_id"], keep='first')
        if duplicate_mask.any():
            df_duplicate = df_valid.loc[duplicate_mask]

            df_invalid = pd.concat([df_invalid, df_duplicate], ignore_index=True)
            df_valid = df_valid.loc[~duplicate_mask]

    return (df_valid.copy(), df_invalid.copy())

