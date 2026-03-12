import psycopg2
import pandas as pd
from io import StringIO

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="enterprise_data_local",
        user="your_user_name",
        password="your_password",
        sslmode="require"
    )
    with conn.cursor() as cur:
        cur.execute("SET search_path TO enterprise_data_local")
    conn.commit()
    return conn

def copy_from_df(conn, df: pd.DataFrame, table: str):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} FROM STDIN WITH CSV",
            buffer
        )
    conn.commit()
