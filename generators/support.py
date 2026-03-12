import pandas as pd
import random
from datetime import timedelta
from faker import Faker
from generators.time_utils import random_date

fake = Faker()

PRIORITIES = ["low", "medium", "high", "critical"]
STATUSES = ["open", "in_progress", "resolved", "closed"]

def generate_sla_policies():
    rows = [
        {"policy_id": 1, "name": "Standard", "response_time_hrs": 24, "resolution_time_hrs": 72},
        {"policy_id": 2, "name": "Premium", "response_time_hrs": 4, "resolution_time_hrs": 24},
        {"policy_id": 3, "name": "Enterprise", "response_time_hrs": 1, "resolution_time_hrs": 8},
    ]
    return pd.DataFrame(rows)

def generate_tickets(user_count, ticket_count=40000):
    rows = []
    ticket_id_counter = 1
    for _ in range(ticket_count):
        created = random_date()
        priority = random.choice(PRIORITIES)
        status = random.choice(STATUSES)

        rows.append({
            "ticket_id": ticket_id_counter,
            "user_id": random.randint(1, user_count),
            "subject": fake.sentence(nb_words=6),
            "priority": priority,
            "status": status,
            "created_at": created
        })
        ticket_id_counter += 1
    return pd.DataFrame(rows)

def generate_ticket_events(tickets_df):
    rows = []
    event_id_counter = 1
    for idx, t in tickets_df.iterrows():
        base = t["created_at"]
        steps = random.randint(2, 6)

        for i in range(steps):
            rows.append({
                "event_id": event_id_counter,
                "ticket_id": idx + 1,
                "event_type": random.choice(["created", "assigned", "responded", "updated", "resolved", "closed"]),
                "event_time": base + timedelta(hours=random.randint(1, 72))
            })
            event_id_counter += 1
    return pd.DataFrame(rows)

def generate_sla_metrics(tickets_df, sla_df):
    rows = []
    metric_id_counter = 1
    for idx, t in tickets_df.iterrows():
        policy = random.randint(1, len(sla_df))
        response_delay = random.randint(0, 48)
        resolution_delay = random.randint(1, 120)

        breach = (
            response_delay > sla_df.iloc[policy - 1]["response_time_hrs"] or
            resolution_delay > sla_df.iloc[policy - 1]["resolution_time_hrs"]
        )

        rows.append({
            "metric_id": metric_id_counter,
            "ticket_id": idx + 1,
            "sla_id": policy,
            "response_time_hrs": response_delay,
            "resolution_time_hrs": resolution_delay,
            "breach_flag": breach
        })
        metric_id_counter += 1
    return pd.DataFrame(rows)
