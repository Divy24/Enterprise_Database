import pandas as pd
import random
from datetime import timedelta
from faker import Faker
from generators.time_utils import random_date

fake = Faker()

STAGES = [
    ("Prospecting", 1),
    ("Qualified", 2),
    ("Demo", 3),
    ("Proposal", 4),
    ("Negotiation", 5),
    ("Closed Won", 6),
    ("Closed Lost", 7),
]

INDUSTRY_DEAL_SIZE = {
    "SaaS": (5000, 50000),
    "FinTech": (10000, 150000),
    "Ecommerce": (3000, 40000),
    "EdTech": (2000, 30000),
    "HealthTech": (8000, 120000),
}

def generate_accounts(org_df):
    rows = []
    for i in range(len(org_df)):
        rows.append({
            "account_id": i + 1,
            "org_id": i + 1,
            "account_name": org_df.iloc[i]["org_name"],
            "industry": org_df.iloc[i]["industry"],
            "country": org_df.iloc[i]["country"],
            "created_at": random_date()
        })
    return pd.DataFrame(rows)

def generate_contacts(accounts_df, avg_contacts=3):
    rows = []
    contact_id_counter = 1
    for idx, acc in accounts_df.iterrows():
        for _ in range(random.randint(1, avg_contacts)):
            rows.append({
                "contact_id": contact_id_counter,
                "account_id": idx + 1,
                "email": fake.email(),
                "full_name": fake.name(),
                "role_title": random.choice(["CTO", "CFO", "VP Engineering", "Head of Ops", "Manager"]),
                "created_at": random_date()
            })
            contact_id_counter += 1
    return pd.DataFrame(rows)

def generate_leads(campaign_count, lead_count=15000):
    rows = []
    lead_id_counter = 1
    for _ in range(lead_count):
        rows.append({
            "lead_id": lead_id_counter,
            "source": random.choice(["web", "email", "partner", "event", "ads"]),
            "campaign_id": random.randint(1, campaign_count),
            "status": random.choice(["new", "qualified", "disqualified", "converted"]),
            "created_at": random_date()
        })
        lead_id_counter += 1
    return pd.DataFrame(rows)

def generate_opportunities(accounts_df, leads_df):
    rows = []
    opp_id_counter = 1
    for _, lead in leads_df.iterrows():
        if lead["status"] in ["qualified", "converted"] and random.random() < 0.4:
            acc_id = random.randint(1, len(accounts_df))
            industry = accounts_df.iloc[acc_id - 1]["industry"]
            low, high = INDUSTRY_DEAL_SIZE[industry]

            rows.append({
                "opp_id": opp_id_counter,
                "account_id": acc_id,
                "estimated_value": random.randint(low, high),
                "probability": round(random.uniform(0.2, 0.9), 2),
                "close_date": random_date().date(),
                "created_at": random_date()
            })
            opp_id_counter += 1
    return pd.DataFrame(rows)

def generate_opportunity_stages():
    return pd.DataFrame([{"stage_id": o, "stage_name": s, "stage_order": o} for s, o in STAGES])

def generate_opportunity_history(opps_df):
    rows = []
    for idx, opp in opps_df.iterrows():
        start = opp["created_at"]
        current_time = start

        for stage_name, order in STAGES:
            duration = timedelta(days=random.randint(5, 25))
            end_time = current_time + duration

            rows.append({
                "opp_id": idx + 1,
                "stage_id": order,
                "valid_from": current_time,
                "valid_to": end_time
            })

            current_time = end_time

            if stage_name in ["Closed Won", "Closed Lost"]:
                break

    return pd.DataFrame(rows)
