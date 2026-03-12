import pandas as pd
import random
from datetime import timedelta

FUNNEL_STEPS = ["visit", "signup", "trial_start", "subscribe", "upgrade"]

def generate_funnel(events_df):
    rows = []
    for step_order, step in enumerate(FUNNEL_STEPS):
        rows.append({
            "funnel_id": step_order + 1,
            "step_name": step,
            "step_order": step_order + 1,
            "user_count": int(len(events_df) * random.uniform(0.2, 0.9)),
            "computed_at": events_df["event_time"].max()
        })
    return pd.DataFrame(rows)
