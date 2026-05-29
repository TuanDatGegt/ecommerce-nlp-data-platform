#pipeline/bronze/kaggle/auth.py
import os
from configs.settings import API_KAGGLE_TOKEN_KEY, USERNAME_KAGGLE

def validate_kaggle_credentials():
    username = USERNAME_KAGGLE
    api_key = API_KAGGLE_TOKEN_KEY

    if not username or not api_key:
        raise ValueError(
            "Missing Kaggle API Credentials in .env"
        )
    
    #Exporting credentials env for kagglehub
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = api_key

    print("Kaggle credentials validated and set in environment variables.")

    