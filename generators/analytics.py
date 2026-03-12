import pandas as pd
import random
from datetime import timedelta
from generators.time_utils import random_date

def generate_fact_revenue(invoices_df):
    rows = {}
    for _, inv in invoices_df.iterrows():
        d = inv["issued_at"].date()
        rows.setdefault(d, 0)
        rows[d] += inv["total_amount"]

    data = []
    for d, amt in rows.items():
        data.append({
            "revenue_date": d,
            "org_id": random.randint(1, 500),
            "total_revenue": amt,
            "total_refunds": amt * random.uniform(0, 0.1),
            "net_revenue": amt * random.uniform(0.9, 1.0)
        })
    return pd.DataFrame(data)

def generate_fact_churn(subs_df):
    rows = []
    for _, s in subs_df.iterrows():
        if s["status"] == "cancelled":
            rows.append({
                "date": s["end_date"],
                "plan_id": s["plan_id"],
                "churned_users": 1
            })
    return pd.DataFrame(rows)

def generate_fact_subscription_metrics(subs_df):
    rows = []
    for _, s in subs_df.iterrows():
        rows.append({
            "metric_date": s["end_date"],
            "plan_id": s["plan_id"],
            "active_subs": 1,
            "new_subs": 0,
            "churned_subs": 0,
            # MRR estimate
            "mrr": random.randint(500, 5000)
        })
    
    df = pd.DataFrame(rows)
    if df.empty:
        return df
        
    # Aggregate by date and plan_id to ensure unique keys
    df = df.groupby(["metric_date", "plan_id"], as_index=False).sum()
    return df

def generate_snapshot_users(users_df, subs_df):
    rows = []
    for _, u in users_df.iterrows():
        rows.append({
            "snapshot_date": random_date().date(),
            "user_id": u["user_id"],
            "is_active": random.choice([True, False]),
            "plan_id": random.randint(1, subs_df["plan_id"].max()),
            "mrr": random.randint(500, 5000),
            "churn_risk": round(random.uniform(0, 1), 3)
        })
    return pd.DataFrame(rows)

def generate_snapshot_org_monthly(org_count):
    rows = []
    # No snapshot_id in schema
    for i in range(1, org_count + 1):
        rows.append({
            "snapshot_month": random_date().replace(day=1).date(),
            "org_id": i,
            "active_users": random.randint(5, 50),
            "total_revenue": random.randint(1000, 50000),
            "churn_rate": round(random.uniform(0, 0.1), 3)
        })
    return pd.DataFrame(rows)

def generate_data_quality_issues():
    rows = []
    check_id_counter = 1
    for _ in range(200):
        rows.append({
            "check_id": check_id_counter,
            "table_name": random.choice(["events", "payments", "subscriptions", "invoices"]),
            "rule": random.choice(["null_rate", "duplicate_rate", "late_arrival"]),
            "failure_rate": round(random.uniform(0, 0.2), 3),
            "checked_at": random_date()
        })
        check_id_counter += 1
    return pd.DataFrame(rows)

def generate_anomalies():
    rows = []
    anomaly_id_counter = 1
    for _ in range(300):
        rows.append({
            "anomaly_id": anomaly_id_counter,
            "entity_type": "revenue",
            "entity_id": random.randint(1, 500),
            "metric_name": "daily_revenue",
            "metric_value": random.randint(10000, 1000000),
            "expected_range": "{20000,500000}",
            "detected_at": random_date()
        })
        anomaly_id_counter += 1
    return pd.DataFrame(rows)
