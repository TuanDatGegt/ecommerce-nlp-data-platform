#pipeline/bronze/validate.py
import os

from configs.schemas.review_schemas import REQUIRED_COLUMNS

def validate_required_columns(df):
    missing = [
        col for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing:
        raise ValueError(f"missing columns: {missing}")
    
def write_to_dlq(df, filename):
    os.makedirs('dlq', exist_ok=True)

    path = f'data/dlq/{filename}.json'

    df.to_json(path, orient='records')


