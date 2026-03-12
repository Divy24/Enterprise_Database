import pandas as pd
from datetime import timedelta

def generate_revenue_recognition(invoices_df):
    rows = []
    rev_id_counter = 1
    for idx, inv in invoices_df.iterrows():
        # Spread revenue across 3 months
        monthly = inv["total_amount"] / 3
        for i in range(3):
            rows.append({
                "rev_rec_id": rev_id_counter,
                "invoice_id": inv["invoice_id"],
                "recognition_date": inv["issued_at"].date() + timedelta(days=30*i),
                "amount": monthly,
                "status": "recognized"
            })
            rev_id_counter += 1
    return pd.DataFrame(rows)
