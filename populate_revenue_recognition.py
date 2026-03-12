from db import get_connection, copy_from_df
from generators.revenue import generate_revenue_recognition
import pandas as pd

def populate_revenue_recognition():
    conn = get_connection()
    if not conn:
        print("Failed to connect to DB.")
        return

    print("Fetching invoices...")
    try:
        # Fetch invoices into a DataFrame
        query = "SELECT * FROM enterprise_data_local.invoices"
        invoices_df = pd.read_sql(query, conn)
        
        if invoices_df.empty:
            print("No invoices found to generate revenue from.")
            conn.close()
            return

        print(f"Found {len(invoices_df)} invoices. Generating revenue recognition entries...")
        
        # Generate revenue recognition data
        revenue_df = generate_revenue_recognition(invoices_df)
        
        if revenue_df.empty:
            print("No revenue recognition entries generated.")
        else:
            print(f"Generated {len(revenue_df)} revenue recognition entries.")
            copy_from_df(conn, revenue_df, "enterprise_data_local.revenue_recognition")
            print("Successfully populated enterprise_data_local.revenue_recognition.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    populate_revenue_recognition()
