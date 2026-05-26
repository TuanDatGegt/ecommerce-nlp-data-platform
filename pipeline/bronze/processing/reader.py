#pipeline/bronze/ingrest.py

import pandas as pd
import csv

from configs.settings import CHUNK_SIZE

def read_tsv_in_chunks(input_file):
    return pd.read_csv(
        input_file,
        sep="\t",
        chunksize=CHUNK_SIZE,
        engine='python',
        quoting=csv.QUOTE_NONE,
        on_bad_lines='skip'
    )