import pandas as pd
import random
import uuid
import json
from datetime import timedelta
from faker import Faker
from generators.time_utils import random_date
from config import SCALE

fake = Faker()

EVENT_TYPES = [
    "page_view", "login", "feature_use", "search",
    "checkout_start", "payment_attempt", "subscription_upgrade",
    "error", "logout", "support_click"
]

def generate_sessions(user_count, session_count=200_000):
    rows = []
    session_id_counter = 1
    for i in range(session_count):
        user_id = random.randint(1, user_count)
        start = random_date()
        duration = timedelta(minutes=random.randint(2, 90))
        rows.append({
            "session_id": session_id_counter,
            "user_id": user_id,
            "started_at": start,
            "ended_at": start + duration,
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "ip_address": fake.ipv4()
        })
        session_id_counter += 1
    return pd.DataFrame(rows)

def generate_events(sessions_df, events_per_session_avg=5):
    rows = []
    event_id_counter = 1
    for idx, sess in sessions_df.iterrows():
        n_events = max(1, int(random.gauss(events_per_session_avg, 2)))
        base_time = sess["started_at"]

        for i in range(n_events):
            etype = random.choice(EVENT_TYPES)
            event_time = base_time + timedelta(minutes=random.randint(0, 60))

            rows.append({
                "event_id": event_id_counter,
                "session_id": sess["session_id"],
                "user_id": sess["user_id"],
                "event_type": etype,
                "event_time": event_time,
                "properties": json.dumps({
                    "url": fake.uri_path(),
                    "button": random.choice(["buy", "upgrade", "cancel", "search", "none"]),
                    "latency_ms": random.randint(50, 2000),
                    "experiment_variant": random.choice(["A", "B", "C"]),
                    "error_code": random.choice([None, "500", "403", "timeout"])
                })
            })
            event_id_counter += 1
    return pd.DataFrame(rows)
