## script/load_gold_to_postgres.py
import os
import duckdb
from dotenv import load_dotenv
from configs.settings import GOLD_DATA_PATH

load_dotenv()


def load_dashboard_to_postgres():
    """
    Đọc toàn bộ các file Parquet từ `gold/dashboard/` và nạp vào PostgreSQL.
    """
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "ecommerce_db")

    pg_conn_str = (
        f"host={host} port={port} dbname={dbname} user={user} password={password}"
    )

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{pg_conn_str}' AS pg (TYPE POSTGRES);")

        dash_dir = GOLD_DATA_PATH / "dashboard"

        # 1. Nạp category_month_stats
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS pg.gold_category_month_stats AS 
            SELECT * FROM read_parquet('{dash_dir / "category_month_stats.parquet"}') LIMIT 0;
            DELETE FROM pg.gold_category_month_stats;
            INSERT INTO pg.gold_category_month_stats 
            SELECT * FROM read_parquet('{dash_dir / "category_month_stats.parquet"}');
        """)

        # 2. Nạp category_year_stats
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS pg.gold_category_year_stats AS 
            SELECT * FROM read_parquet('{dash_dir / "category_year_stats.parquet"}') LIMIT 0;
            DELETE FROM pg.gold_category_year_stats;
            INSERT INTO pg.gold_category_year_stats 
            SELECT * FROM read_parquet('{dash_dir / "category_year_stats.parquet"}');
        """)

        print("Đã nạp toàn bộ dữ liệu Dashboard Parquet vào PostgreSQL thành công!")
    finally:
        con.close()


if __name__ == "__main__":
    load_dashboard_to_postgres()
