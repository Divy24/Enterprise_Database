import pandas as pd
import random
from faker import Faker
from generators.time_utils import random_date
from config import SCALE

fake = Faker()

def generate_campaigns():
    rows = []
    for i in range(SCALE["campaigns"]):
        rows.append({
            "campaign_id": i + 1,
            "name": fake.catch_phrase(),
            "channel": fake.word(),
            "start_date": random_date(),
            "end_date": random_date()
        })
    return pd.DataFrame(rows)

def generate_ad_spend(campaigns_df):
    rows = []
    spend_id = 1
    
    for _, camp in campaigns_df.iterrows():
        # Daily spend for duration
        # Simplify: just 1-5 entries per campaign
        for _ in range(random.randint(1, 5)):
            rows.append({
                "spend_id": spend_id,
                "campaign_id": camp["campaign_id"],
                "spend_date": random_date().date(),
                "amount": round(random.uniform(100.0, 5000.0), 2)
            })
            spend_id += 1
            
    return pd.DataFrame(rows)

def generate_attribution_events(campaigns_df, users_df, count=None):
    rows = []
    attr_id = 1
    
    # Randomly attribute some users to campaigns
    camp_ids = campaigns_df["campaign_id"].tolist()
    user_ids = users_df["user_id"].tolist()
    
    # If count is specified, generate exactly that many
    target_count = count if count else int(len(users_df) * 0.4)
    
    for _ in range(target_count):
        rows.append({
            "attribution_id": attr_id,
            "user_id": random.choice(user_ids),
            "campaign_id": random.choice(camp_ids),
            "touch_type": random.choice(["first_touch", "last_touch", "assist"]),
            "event_time": random_date()
        })
        attr_id += 1
            
    return pd.DataFrame(rows)
