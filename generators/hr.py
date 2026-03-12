import pandas as pd
import random
from faker import Faker
from generators.time_utils import random_date

fake = Faker()

DEPARTMENTS = ["Sales", "Support", "Engineering", "Marketing", "Finance", "HR"]
LEVELS = ["L1", "L2", "L3", "Manager", "Director"]
RATINGS = ["Poor", "Average", "Good", "Excellent"]

def generate_departments():
    rows = []
    for i, d in enumerate(DEPARTMENTS):
        rows.append({
            "dept_id": i + 1,
            "name": d
        })
    return pd.DataFrame(rows)

def generate_employees(user_count, dept_count):
    rows = []
    emp_id_counter = 1
    for user_id in range(1, user_count + 1):
        if random.random() < 0.25:  # 25% of users are employees
            rows.append({
                "employee_id": emp_id_counter,
                "user_id": user_id,
                "dept_id": random.randint(1, dept_count),
                "title": random.choice(["Engineer", "Analyst", "Manager", "Executive"]),
                "level": random.choice(LEVELS),
                "hired_at": random_date()
            })
            emp_id_counter += 1
    return pd.DataFrame(rows)

def generate_payroll(employees_df):
    rows = []
    payroll_id_counter = 1
    for idx, emp in employees_df.iterrows():
        base = random.randint(30000, 200000)
        bonus = random.randint(0, 30000)
        rows.append({
            "payroll_id": payroll_id_counter,
            "employee_id": idx + 1,
            "base_salary": base,
            "bonus": bonus,
            "currency": "INR",
            "paid_at": random_date()
        })
        payroll_id_counter += 1
    return pd.DataFrame(rows)

def generate_performance_reviews(employees_df):
    rows = []
    review_id_counter = 1
    for idx, emp in employees_df.iterrows():
        rows.append({
            "review_id": review_id_counter,
            "employee_id": idx + 1,
            "rating": random.choice(RATINGS),
            "review_date": random_date(),
            "manager_comments": fake.sentence()
        })
        review_id_counter += 1
    return pd.DataFrame(rows)
