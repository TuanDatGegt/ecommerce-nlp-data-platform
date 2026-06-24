#pipeline/gold/processing/feature_joiner.py

import pandas as pd

def build_gold_feature_dataset(structured_df, text_df, sentiment_df):
    df = structured_df.copy()

    merge_cols = ["review_id"]
    text_cols = ["review_id", "token_count", "avg_word_length", "punctuation_count", "rag_context"]
    sentiment_cols = ['review_id', "sentiment_proxy"]

    df = df.merge(text_df[text_cols], on=merge_cols, how="left")
    df = df.merge(sentiment_df[sentiment_cols], on=merge_cols, how="left")

    return df

