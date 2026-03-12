import pandas as pd
import random
from faker import Faker
from config import SCALE
from generators.time_utils import random_date

fake = Faker()

def generate_products():
    rows = []
    for i in range(SCALE["products"]):
        rows.append({
            "product_id": i + 1,
            "product_name": fake.bs().title(),
            "category": random.choice(["Analytics", "Payments", "CRM", "Security"]),
            "is_active": True,
            "created_at": random_date()
        })
    return pd.DataFrame(rows)

def generate_plans(product_count):
    rows = []
    plan_id_counter = 1
    for pid in range(1, product_count + 1):
        for _ in range(SCALE["plans_per_product"]):
            rows.append({
                "plan_id": plan_id_counter,
                "product_id": pid,
                "plan_name": random.choice(["Free", "Basic", "Pro", "Enterprise"]),
                "billing_cycle": random.choice(["monthly", "quarterly", "yearly"]),
                "created_at": random_date()
            })
            plan_id_counter += 1
    return pd.DataFrame(rows)

def generate_plan_pricing_history(plans_df):
    rows = []
    pricing_id_counter = 1
    
    for _, plan in plans_df.iterrows():
        # Simple logic: Just one current price for now
        price = random.randint(500, 10000)
        rows.append({
            "pricing_id": pricing_id_counter,
            "plan_id": plan["plan_id"],
            "price": price,
            "currency": "INR",
            "valid_from": plan["created_at"].date(),
            "valid_to": None,
            "is_current": True
        })
        pricing_id_counter += 1
        
    return pd.DataFrame(rows)
