#pipeline/silver/processing/enricher.py

import pandas as pd

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
        df['helpful_ratio'] = (
            df['helpful_votes'].div(df['total_votes'].replace(0, pd.NA)).fillna(0.0)
        )

    return df

