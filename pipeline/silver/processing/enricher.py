#pipeline/silver/processing/enricher.py

import pandas as pd
import numpy as np

def enrich_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    if "star_rating" in df.columns:
        df['sentiment'] = pd.Series(pd.NA, index=df.index, dtype="object")
        
        df.loc[df['star_rating'] >= 4, 'sentiment'] = 'positive'
        df.loc[df['star_rating'] <=2, 'sentiment'] = 'negative'
        df['sentiment'] = df['sentiment'].fillna('neutral')
    else:
        df['sentiment'] = 'unknown'
    
    return df

def enrich_feature(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    if ('helpful_votes' in df.columns and 'total_votes' in df.columns):
        df["helpful_ratio"] = np.where(
            df["total_votes"] > 0,
            df["helpful_votes"] / df["total_votes"],
            0.0
        )

    return df

