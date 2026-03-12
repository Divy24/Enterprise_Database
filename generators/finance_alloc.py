import pandas as pd
import random

COST_CENTERS = [
    ("Sales", "Revenue Generation"),
    ("Support", "Customer Operations"),
    ("Engineering", "Product Development"),
    ("Marketing", "Growth"),
    ("Finance", "Corporate"),
    ("HR", "Corporate")
]

def generate_cost_centers():
    rows = []
    for i, (name, dept) in enumerate(COST_CENTERS):
        rows.append({
            "cost_center_id": i + 1,
            "name": name,
            "department": dept
        })
    return pd.DataFrame(rows)

def generate_chart_of_accounts():
    rows = [
        {"account_code": "1000", "account_name": "Cash", "account_type": "asset", "parent_account": None},
        {"account_code": "1200", "account_name": "Accounts Receivable", "account_type": "asset", "parent_account": None},
        {"account_code": "4000", "account_name": "Product Revenue", "account_type": "revenue", "parent_account": None},
        {"account_code": "6000", "account_name": "Operating Expense", "account_type": "expense", "parent_account": None},
    ]
    return pd.DataFrame(rows)

def generate_general_ledger(payments_df):
    rows = []
    entry_id = 1
    
    for _, pay in payments_df.iterrows():
        # Debit Cash 1000
        rows.append({
            "entry_id": entry_id,
            "account_code": "1000",
            "debit": pay["amount"],
            "credit": 0,
            "reference_type": "payment",
            "reference_id": pay["payment_id"],
            "entry_date": pay["paid_at"].date(),
            "created_at": pay["paid_at"]
        })
        entry_id += 1
        
        # Credit Revenue 4000
        rows.append({
            "entry_id": entry_id,
            "account_code": "4000",
            "debit": 0,
            "credit": pay["amount"],
            "reference_type": "payment",
            "reference_id": pay["payment_id"],
            "entry_date": pay["paid_at"].date(),
            "created_at": pay["paid_at"]
        })
        entry_id += 1
        
    if not rows:
        return pd.DataFrame(columns=["entry_id", "account_code", "debit", "credit", "reference_type", "reference_id", "entry_date", "created_at"])
    return pd.DataFrame(rows)

def generate_gl_allocations(gl_df, cost_center_count):
    rows = []
    if gl_df.empty:
        return pd.DataFrame(columns=["entry_id", "cost_center_id", "allocation_pct"])
        
    # Only allocate revenue/expenses (4000, 6000)
    target_entries = gl_df[gl_df["account_code"].isin(["4000", "6000"])]
    
    # Simple logic: 100% to one random cost center
    for _, entry in target_entries.iterrows():
        cc_id = random.randint(1, cost_center_count)
        rows.append({
            "entry_id": entry["entry_id"],
            "cost_center_id": cc_id,
            "allocation_pct": 100.00
        })
    return pd.DataFrame(rows)
