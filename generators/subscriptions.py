import pandas as pd
import random
from datetime import timedelta
from generators.time_utils import random_date
from config import SCALE

def generate_subscriptions(user_count, plan_count):
    rows = []
    sub_id_counter = 1
    for user_id in range(1, user_count + 1):
        if random.random() < 0.6:  # 60% conversion to paid
            start = random_date()
            duration_days = random.randint(30, 720)
            end = start + timedelta(days=duration_days)
            status = "active" if end > random_date() else "cancelled"

            rows.append({
                "subscription_id": sub_id_counter,
                "user_id": user_id,
                "plan_id": random.randint(1, plan_count),
                "status": status,
                "start_date": start.date(),
                "end_date": end.date(),
                "created_at": random_date()
            })
            sub_id_counter += 1
    return pd.DataFrame(rows)

def generate_subscription_events(subs_df):
    rows = []
    event_id_counter = 1
    import json
    
    for _, sub in subs_df.iterrows():
        # Created event
        rows.append({
            "event_id": event_id_counter,
            "subscription_id": sub["subscription_id"],
            "event_type": "created",
            "event_time": sub["created_at"],
            "metadata": json.dumps({"source": "web"})
        })
        event_id_counter += 1
        
        # Status change event if applicable
        if sub["status"] in ["cancelled", "expired"]:
            rows.append({
                "event_id": event_id_counter,
                "subscription_id": sub["subscription_id"],
                "event_type": sub["status"],
                "event_time": sub["end_date"],
                "metadata": json.dumps({"reason": "user_request"})
            })
            event_id_counter += 1
            
    return pd.DataFrame(rows)

def generate_usage_records(subs_df):
    rows = []
    usage_id_counter = 1
    
    for _, sub in subs_df.iterrows():
        if sub["status"] == "active":
            rows.append({
                "usage_id": usage_id_counter,
                "subscription_id": sub["subscription_id"],
                "metric_name": "api_calls",
                "metric_value": random.randint(100, 50000),
                "recorded_at": random_date()
            })
            usage_id_counter += 1
            
            rows.append({
                "usage_id": usage_id_counter,
                "subscription_id": sub["subscription_id"],
                "metric_name": "storage_gb",
                "metric_value": random.randint(1, 1000),
                "recorded_at": random_date()
            })
            usage_id_counter += 1
            
    return pd.DataFrame(rows)
