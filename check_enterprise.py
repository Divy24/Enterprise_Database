import psycopg2
from db import get_connection

def main():
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Verify current search path
        cur.execute("SHOW search_path;")
        print(f"Current search_path: {cur.fetchone()[0]}")
        
        # List tables in current schema
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'Enterprise_Data';
        """)
        tables = cur.fetchall()
        
        if not tables:
            print("No tables found in 'Enterprise_Data' schema.")
        else:
            print(f"Found {len(tables)} tables in 'Enterprise_Data':")
            for t in tables:
                print(f"- {t[0]}")
                
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
