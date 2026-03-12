import pandas as pd
import random
import json
from datetime import timedelta
from generators.time_utils import random_date

def generate_feature_store(user_count, versions=3):
    rows = []
    feature_id_counter = 1
    for user_id in range(1, user_count + 1):
        for v in range(1, versions + 1):
            rows.append({
                "feature_id": feature_id_counter,
                "entity_type": "user",
                "entity_id": user_id,
                "feature_set": json.dumps({
                    "avg_session_time": random.uniform(30, 900),
                    "events_last_7d": random.randint(1, 200),
                    "payments_last_30d": random.randint(0, 10),
                    "refund_ratio": round(random.uniform(0, 0.3), 3),
                    "device_entropy": round(random.uniform(0.1, 1.0), 3),
                    "campaign_touch_count": random.randint(0, 8),
                }),
                "version": v,
                "generated_at": random_date()
            })
            feature_id_counter += 1
    return pd.DataFrame(rows)

def generate_model_registry():
    rows = [
        {
            "model_id": 1,
            "model_name": "churn_model",
            "version": "v1",
            "metrics": json.dumps({"auc": 0.78, "precision": 0.65, "recall": 0.6}),
            "trained_at": random_date()
        },
        {
            "model_id": 2,
            "model_name": "fraud_model",
            "version": "v1",
            "metrics": json.dumps({"auc": 0.91, "precision": 0.83, "recall": 0.8}),
            "trained_at": random_date()
        },
        {
            "model_id": 3,
            "model_name": "ltv_model",
            "version": "v1",
            "metrics": json.dumps({"rmse": 420.5, "r2": 0.71}),
            "trained_at": random_date()
        }
    ]
    return pd.DataFrame(rows)

def generate_model_predictions(user_count, model_count):
    rows = []
    pred_id_counter = 1
    for model_id in range(1, model_count + 1):
        for user_id in range(1, user_count + 1):
            rows.append({
                "prediction_id": pred_id_counter,
                "model_id": model_id,
                "entity_type": "user",
                "entity_id": user_id,
                "score": round(random.uniform(0, 1), 4),
                "predicted_at": random_date()
            })
            pred_id_counter += 1
    return pd.DataFrame(rows)

def generate_drift_metrics(model_count):
    rows = []
    drift_id_counter = 1
    for model_id in range(1, model_count + 1):
        for _ in range(200):
            rows.append({
                "drift_id": drift_id_counter,
                "model_id": model_id,
                "metric_name": "feature_drift_psi",
                "metric_value": round(random.uniform(0, 0.5), 4),
                "measured_at": random_date()
            })
            drift_id_counter += 1
    return pd.DataFrame(rows)
