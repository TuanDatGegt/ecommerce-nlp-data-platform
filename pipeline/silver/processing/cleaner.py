#pipeline/silver/processing/cleaner.py

import re
import pandas as pd
from langdetect import detect, LangDetectException

def clean_text_field(text: str) -> str:
    if pd.isna(text) or not isinstance(text, str):
        return ""
    
    #Loại bỏ các thẻ HTML giả lập hoặc thẻ thừa phát sinh khi crawl (<br />, <div>)
    text = re.sub(r'<[^>]+>', ' ', text)

    #Thay thế kí tự xuống dòng thành khoảng trắng đơn
    text = re.sub(r'\s+', ' ', text)

    #Loại bỏ các ký tự đặc biệt thừa nhưng giữ nguyên dấu câu quan trọng
    text = re.sub(r'[^\w\s\.,!\?]', '', text)

    return text.strip()

def is_english(text: str) -> bool:
    if not text or len(text.strip()) < 5:
        return False
    try:
        return detect(text) == "en"
    except LangDetectException:
        return False

def process_silver_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df, df
    
    df = df.copy()

    string_cols = ['marketplace', 'customer_id', 'review_id', 'product_id',
                   'product_parent', 'product_title', 'product_category']
    
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    #Encoding Numeric
    if 'star_rating' in df.columns:
        df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce').fillna(0).astype(int)
    if 'helpful_votes' in df.columns:
        df['helpful_votes'] = pd.to_numeric(df['helpful_votes'], errors='coerce').fillna(0).astype(int)
    if 'total_votes' in df.columns:
        df['total_votes'] = pd.to_numeric(df['total_votes'], errors='coerce').fillna(0).astype(int)
    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')

    if 'vine' in df.columns:
        df['vine'] = df['vine'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
    if 'verified_purchased' in df.columns:
        df['verified_purchased'] = df['verified_purchased'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)

    if 'review_headline' in df.columns:
        df['cleaned_review_headline'] = df['review_headline'].apply(clean_text_field)
    if 'review_body' in df.columns:
        df['cleaned_review_body'] = df['review_body'].apply(clean_text_field)

    df['is_en'] = df['cleaned_review_body'].apply(is_english)

    df_english = df[df['is_en'] == True].drop(columns=['is_en']).copy()
    df_wrong_lang = df[df['is_en']==False].drop(columns=['is_en']).copy()
    
    return df_english, df_wrong_lang

