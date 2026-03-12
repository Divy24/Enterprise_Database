import pandas as pd
import random
from datetime import timedelta
from generators.time_utils import random_date

PIPELINES = [
    ("user_ingestion", "users"),
    ("event_ingestion", "events"),
    ("billing_etl", "payments"),
    ("revenue_aggregation", "fact_revenue_daily"),
    ("churn_snapshot", "snapshot_user_daily"),
    ("fraud_scoring", "risk_scores"),
    ("feature_generation", "feature_store"),
]

def generate_ingestion_jobs():
    rows = []
    for i, (name, table) in enumerate(PIPELINES):
        rows.append({
            "job_id": i + 1,
            "source_system": name,
            "target_table": table,
            "schedule": random.choice(["hourly", "daily", "weekly"])
        })
    return pd.DataFrame(rows)

def generate_pipeline_runs(jobs_df, runs_per_job=120):
    rows = []
    run_id_counter = 1
    for job_id in range(1, len(jobs_df) + 1):
        for _ in range(runs_per_job):
            start = random_date()
            duration = timedelta(minutes=random.randint(5, 180))
            status = random.choices(
                ["success", "failed", "delayed"],
                weights=[0.85, 0.08, 0.07]
            )[0]

            rows.append({
                "run_id": run_id_counter,
                "job_id": job_id,
                "start_time": start,
                "end_time": start + duration,
                "status": status,
                "rows_processed": random.randint(1000, 500000)
            })
            run_id_counter += 1
    return pd.DataFrame(rows)

def generate_data_quality_checks():
    rules = ["null_rate", "duplicate_rate", "freshness", "referential_integrity"]
    tables = ["events", "payments", "subscriptions", "fact_revenue_daily", "snapshot_user_daily"]

    rows = []
    check_id_counter = 1
    rows.append({
        "check_id": check_id_counter,
        "table_name": random.choice(tables),
        "rule": random.choice(rules),
        "failure_rate": round(random.uniform(0, 0.25), 3),
        "checked_at": random_date()
    })
    check_id_counter += 1
    return pd.DataFrame(rows)

def generate_anomaly_flags():
    metrics = ["daily_revenue", "churn_rate", "fraud_rate", "session_count", "conversion_rate"]
    rows = []
    anomaly_id_counter = 1

    for _ in range(400):
        low = random.randint(1000, 10000)
        high = random.randint(50000, 200000)

        rows.append({
            "anomaly_id": anomaly_id_counter,
            "entity_type": "metric",
            "entity_id": random.randint(1, 1000),
            "metric_name": random.choice(metrics),
            "metric_value": random.randint(low, high),
            "expected_range": "{%d,%d}" % (low, high),
            "detected_at": random_date()
        })
        anomaly_id_counter += 1
    return pd.DataFrame(rows)
