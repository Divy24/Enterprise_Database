import pandas as pd
import random
from faker import Faker
from datetime import timedelta
from generators.time_utils import random_date

fake = Faker()

def generate_devices(user_count, device_share_prob=0.15):
    devices = []
    device_id_counter = 1
    shared_device_pool = []

    for user_id in range(1, user_count + 1):
        if shared_device_pool and random.random() < device_share_prob:
            device = random.choice(shared_device_pool)
            device["user_id"] = user_id
            # device_id should be unique for the record, or should it replicate? 
            # Schema says device_id is PK, so it must be unique per row, even if fingerprint matches.
            new_device_entry = device.copy()
            new_device_entry["device_id"] = device_id_counter
            devices.append(new_device_entry)
        else:
            d = {
                "device_id": device_id_counter,
                "user_id": user_id,
                "fingerprint": fake.sha256(),
                "first_seen": random_date(),
                "last_seen": random_date()
            }
            devices.append(d)
            if random.random() < 0.05:
                shared_device_pool.append(d)
        device_id_counter += 1

    return pd.DataFrame(devices)

def generate_ip_intel(ip_count=20000):
    rows = []
    for _ in range(ip_count):
        rows.append({
            "ip_address": fake.ipv4(),
            "country": random.choice(["India", "US", "UK", "Germany", "Singapore"]),
            "risk_level": random.choice(["low", "medium", "high"])
        })
    return pd.DataFrame(rows)

def generate_risk_scores(user_count):
    rows = []
    score_id_counter = 1
    for user_id in range(1, user_count + 1):
        base = random.uniform(0, 1)
        drift = random.uniform(-0.2, 0.2)
        score = min(max(base + drift, 0), 1)

        rows.append({
            "score_id": score_id_counter,
            "entity_type": "user",
            "entity_id": user_id,
            "score": round(score, 3),
            "calculated_at": random_date()
        })
        score_id_counter += 1
    return pd.DataFrame(rows)

def generate_fraud_cases(risk_df, fraud_threshold=0.85):
    rows = []
    case_id_counter = 1
    for _, r in risk_df.iterrows():
        if r["score"] > fraud_threshold:
            rows.append({
                "case_id": case_id_counter,
                "entity_type": r["entity_type"],
                "entity_id": r["entity_id"],
                "label": True,
                "created_at": r["calculated_at"] + timedelta(days=random.randint(1,10))
            })
            case_id_counter += 1
    return pd.DataFrame(rows)
