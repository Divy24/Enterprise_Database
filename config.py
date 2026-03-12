from datetime import datetime

SEED = 42

START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2025, 12, 31)

SCALE = {
    "organizations": 400,
    "users": 40000,
    "teams_per_org_avg": 2,
    "employees_per_team_avg": 3,
    "products": 20,
    "plans_per_product": 3,
    "campaigns": 50,
    "leads_daily": 150, # Approx
    "sessions_daily": 20000, # To hit ~140k in 7 days
}

NOISE = {
    "inactive_users_pct": 0.12,
    "duplicate_emails_pct": 0.002,
    "price_change_prob_per_quarter": 0.3
}
