import pandas as pd
import urllib.parse
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_db_connection():
    """
    Try to connect to the database using environment variables. If it fails, return None.
    use the following environment variables:
    - DB_USER
    - DB_PASSWORD
    - DB_HOST
    - DB_PORT
    - DB_NAME
    """
    try:
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        driver = os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")

        password_encoded = urllib.parse.quote_plus(password)

        connection_string = (
            f"mssql+pyodbc://{user}:{password_encoded}"
            f"@{host}:{port}/{db_name}"
            f"?driver={driver.replace(' ', '+')}"
            f"&TrustServerCertificate=yes"
            f"&Encrypt=no"
        )

        engine = create_engine(connection_string)

        return engine
    
    except Exception as e:
        print(f"Lỗi kết nối cơ sở dữ liệu: {e}")
        return None
    