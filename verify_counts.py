import psycopg2
from db import get_connection

def main():
    conn = get_connection()
    cur = conn.cursor()
    
    tables_to_check = ["organizations", "users", "sessions", "enterprise_data.events"]
    
    print("--- Row Counts ---")
    for t in tables_to_check:
        try:
            # Handle potential schema prefix in query
            t_clean = t.split('.')[-1]
            cur.execute(f"SELECT count(*) FROM {t}")
            count = cur.fetchone()[0]
            print(f"{t_clean}: {count}")
        except Exception as e:
            print(f"{t}: Error {e}")
            conn.rollback() 
            
    conn.close()

if __name__ == "__main__":
    main()
