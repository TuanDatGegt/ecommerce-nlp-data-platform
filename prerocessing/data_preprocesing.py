import pandas as pd
import csv
import os
from .utils import data_extract_category
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, CHUNK_SIZE


def optimize_csv(df):
    """
    Optimize the DataFrame by converting data types to reduce memory usage.
    """
    bool_mapping = {"Y": True, "N": False}

    for col in ["vine", "verified_purchase"]:
        if col in df.columns:
            df[col] = df[col].map(bool_mapping)

    cat_columns = [
        "marketplace",
        "product_category"
    ]

    for col in cat_columns:
        if col in df.columns:
            df[col] = df[col].astype('category')

    int_columns = [
        "star_rating",
        "helpful_votes",
        "total_votes"
    ]

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
    
    return df

def process_tsv_file(input_file=RAW_DATA_PATH, output_base = PROCESSED_DATA_PATH):
    """
    This function reads a TSV file in chunks, optimizes the data types, 
    and writes the precessed data to a new Parquet file.
    """
    filename = os.path.basename(input_file)
    category = data_extract_category(filename)

    output_dir = os.path.join(
        output_base,
        "raw",
        "amazon_reviews",
        f"category={category}"
    )

    os.makedirs(output_dir, exist_ok=True)
    
    render = pd.read_csv(
        input_file,
        sep='\t',
        chunksize=CHUNK_SIZE,
        engine='python',
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip")
    
    for i, chunk in enumerate(render):
        chunk = optimize_csv(chunk)

        file_path = os.path.join(output_dir, f"part_{i:05d}.parquet")
        
        chunk.to_parquet(file_path, index=False, compression='snappy')

        print(f"Processed chunk {i+1} and saved to {file_path}")




if __name__ == "__main__":
    intput_file = "E:/e-commerce-dataset/archive/amazon_reviews_us_Baby_v1_00.tsv"
    output_base = "E:/ecommerce-data-lake/"

    process_tsv_file(
        input_file=intput_file,
        output_base=output_base
    )
    
