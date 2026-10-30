# E-COMMERCE AMAZONE REVIEW CHATBOX

This project analyzes then sentiments for customer and use the Amazon review dataset on Kaggle for analyst and I using the review customer for Natural Language Processing (NLP) for training the chatbox using RAG module to import with the this Repository. The second Repository I will using for training that NLP and connection the dataset in this pipeline. 

## Dataset and Architecture
- Kaggle Dataset
- MinIO
- PosgreSQL
- RAG
- Airflow
- Streamlit
- ChromeDB
- Fast API

## Key Objectives
- Analyze rating vs popularity for any product
- Explore product in best choice
- Support customer to choose a product at good
- Suggestion product for customer

## Data storage configuration

Set `DATA_LAKE_PATH`, `RAW_DATA_PATH`, `SAMPLE_DATA_PATH`, and `MANIFEST_PATH`
in `.env` to the mounted dataset drive. Bronze, Silver, Gold, DLQ, and metadata
use `DATA_LAKE_PATH` as their storage root.

The Bronze downloader uses TSV files already present in `RAW_DATA_PATH`.
Otherwise, KaggleHub downloads and extracts the dataset directly into
`RAW_DATA_PATH`, bypassing its default OS cache. When running with Docker
Compose, the configured data lake is mounted into Airflow at `/opt/airflow/data`.
