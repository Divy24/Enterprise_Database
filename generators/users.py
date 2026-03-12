import pandas as pd
import random
from faker import Faker
from config import SCALE, NOISE
from generators.time_utils import random_date

fake = Faker()

def generate_users(org_count, count=None):
    if count is None:
        count = SCALE["users"]
        
    rows = []
    for i in range(count):
        rows.append({
            "user_id": i + 1,
            "org_id": random.randint(1, org_count),
            "email": f"{i+1}_{fake.email()}",
            "full_name": fake.name(),
            "status": random.choice(["active", "inactive", "suspended"]),
            "created_at": random_date()
        })
    return pd.DataFrame(rows)

def generate_roles():
    roles = ["Admin", "User", "Viewer", "Editor", "Manager"]
    rows = [{"role_id": i+1, "role_name": r} for i, r in enumerate(roles)]
    return pd.DataFrame(rows)

def generate_user_roles(users_df, roles_df):
    rows = []
    role_ids = roles_df["role_id"].tolist()
    
    for _, user in users_df.iterrows():
        # Assign 1-2 random roles
        assigned_roles = random.sample(role_ids, k=random.randint(1, 2))
        for rid in assigned_roles:
            rows.append({
                "user_id": user["user_id"],
                "role_id": rid,
                "assigned_at": user["created_at"]
            })
    return pd.DataFrame(rows)

def generate_teams(org_count):
    rows = []
    team_id_counter = 1
    # Simple logic: 2 teams per org for now
    for org_id in range(1, org_count + 1):
        for tname in ["Engineering", "Sales", "Marketing"]:
            rows.append({
                "team_id": team_id_counter,
                "org_id": org_id,
                "team_name": tname
            })
            team_id_counter += 1
    return pd.DataFrame(rows)

def generate_team_members(users_df, teams_df):
    rows = []
    # Map users to teams within their org
    # Optimization: To avoid slow lookups, valid teams for an org would be needed.
    # For speed/simplicity in generator, we'll assume valid map or simplify.
    # Strategy: Just assign random users to random teams for now, enforcing FK logic if passed.
    # For exactness: Group teams by org_id.
    
    teams_by_org = teams_df.groupby("org_id")["team_id"].apply(list).to_dict()
    
    for _, user in users_df.iterrows():
        oid = user["org_id"]
        if oid in teams_by_org:
            valid_teams = teams_by_org[oid]
            if valid_teams:
                # Join 1 team
                tid = random.choice(valid_teams)
                rows.append({
                    "team_id": tid,
                    "user_id": user["user_id"],
                    "joined_at": user["created_at"]
                })
    return pd.DataFrame(rows)

def generate_employee_profiles(users_df):
    rows = []
    # 30% of users are employees
    potential_managers = []
    
    for _, user in users_df.iterrows():
        if random.random() < 0.3:
            mgr_id = random.choice(potential_managers) if potential_managers else None
            
            rows.append({
                "user_id": user["user_id"],
                "manager_id": mgr_id,
                "title": fake.job(),
                "level": random.choice(["L1", "L2", "Senior", "Principal"]),
                "hired_at": user["created_at"].date()
            })
            
            if random.random() < 0.2: # Is a manager
                potential_managers.append(user["user_id"])
                
    df = pd.DataFrame(rows)
    if not df.empty and "manager_id" in df.columns:
        df["manager_id"] = df["manager_id"].astype("Int64")
    return df
