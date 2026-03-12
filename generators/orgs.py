import pandas as pd
import random
from faker import Faker
from config import SCALE
from generators.time_utils import random_date

fake = Faker()

def generate_organizations(count=None):
    if count is None:
        count = SCALE["organizations"]
    
    rows = []
    for i in range(count):
        rows.append({
            "org_id": i + 1,
            "org_name": fake.company(),
            "industry": random.choice(["SaaS", "FinTech", "Ecommerce", "EdTech", "HealthTech"]),
            "country": random.choice(["India", "US", "UK", "Germany", "Singapore"]),
            "created_at": random_date()
        })
    return pd.DataFrame(rows)

def generate_org_hierarchy(org_df):
    rows = []
    org_ids = list(range(1, len(org_df)+1))
    for child in org_ids:
        if random.random() < 0.3:
            parent = random.choice(org_ids)
            if parent != child:
                rows.append({
                    "parent_org_id": parent,
                    "child_org_id": child,
                    "depth": 1
                })
    return pd.DataFrame(rows)
