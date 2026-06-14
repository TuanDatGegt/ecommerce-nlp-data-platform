#pipeline/silver/processing/enricher.py

import pandas as pd

def enrich_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "star_rating" in df.columns:
        df['sentiment'] = df['star_rating'].apply(
            lambda x: 'positive' if x >=4 else ('negative' if x <= 2 else 'neutral')
        )
    else:
        df['sentiment'] = 'unknown'
    return df

def enrich_feature(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'helpful_votes' in df.columns and 'total_votes' in df.columns:
        df['helpful_ratio'] = df.apply(
            lambda row: row['helpful_votes']/row['total_votes'] if row['total_votes'] > 0 else 0.0,
            axis=1
        )
    return df

