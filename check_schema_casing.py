import psycopg2
from db import get_connection

def main():
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # List ALL schemas
        print("--- Existing Schemas ---")
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = cur.fetchall()
        for s in schemas:
            print(f"- {s[0]}")
            
        # Check tables in 'enterprise_data' (lowercase)
        print("\n--- Tables in 'enterprise_data' ---")
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'enterprise_data';
        """)
        tables = cur.fetchall()
        
        if not tables:
            print("No tables found in 'enterprise_data'.")
        else:
            print(f"Found {len(tables)} tables:")
            # Print first 10
            for t in tables[:10]:
                print(f"- {t[0]}")
            if len(tables) > 10:
                print(f"... and {len(tables)-10} more.")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
