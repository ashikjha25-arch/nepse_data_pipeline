import psycopg2
import time
from app.config.settings import (
    POSTGRES_HOST,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_PORT
)

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                port=POSTGRES_PORT
            )

            cur = conn.cursor()

            cur.execute("SELECT version();")
            db_version = cur.fetchone()
            print(f"Connected to: {db_version}")

            return conn, cur

        except Exception as error:
            print("Postgres not ready, retrying...", error)
            time.sleep(5)