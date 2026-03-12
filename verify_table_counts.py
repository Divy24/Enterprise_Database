import psycopg2

def get_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="enterprise_data_local",
            user="your_user_name",
            password="your_password",
            sslmode="require"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def check_table_counts():
    conn = get_connection()
    if not conn:
        return

    # Get all tables from the schema
    schema = "enterprise_data_local"
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cur.fetchall()]
        tables.sort()
    except Exception as e:
        print(f"Error fetching existing tables: {e}")
        conn.close()
        return

    # Exclude table_embeddings
    if "table_embeddings" in tables:
        tables.remove("table_embeddings")
    
    empty_tables = []
    
    print(f"Checking table counts in schema '{schema}' (found {len(tables)} tables)...\n")
    
    for table in tables:
        try:
            cur.execute(f"SELECT count(*) FROM {schema}.{table}")
            count = cur.fetchone()[0]
            print(f"{table}: {count}")
            if count == 0:
                empty_tables.append(table)
        except Exception as e:
            print(f"Error checking {table}: {e}")
            # Identify if table exists or not
            conn.rollback() 

    conn.close()
    
    print("\n--------------------------------")
    if empty_tables:
        print(f"Found {len(empty_tables)} empty tables:")
        for t in empty_tables:
            print(f"- {t}")
    else:
        print("All tables have data.")

if __name__ == "__main__":
    check_table_counts()
