import psycopg2
from db import get_connection

tables = [
    "organizations", "users", "products", "plans", "campaigns", "org_hierarchy",
    "subscriptions", "orders", "invoices", "payments", "revenue_recognition",
    "sessions", "events", "fact_funnel", "devices", "ip_intel", "risk_scores", "fraud_cases",
    "feature_store", "model_registry", "model_predictions", "drift_metrics",
    "fact_revenue_daily", "fact_churn", "fact_subscription_metrics", "snapshot_user_daily", 
    "data_quality_checks", "anomaly_flags",
    "accounts", "contacts", "leads", "opportunity_stages", "opportunities", "opportunity_history",
    "sla_policies", "tickets", "ticket_events", "sla_metrics",
    "departments", "employees", "payroll", "performance_reviews",
    "cost_centers", "gl_allocations",
    "ingestion_jobs", "pipeline_runs"
]

def main():
    conn = get_connection()
    cur = conn.cursor()
    
    print(f"--- Verifying {len(tables)} tables in 'enterprise_data' ---")
    
    empty_tables = []
    error_tables = []
    
    for t in tables:
        try:
            # Force schema qualification to be safe
            full_name = f"enterprise_data.{t}"
            cur.execute(f"SELECT count(*) FROM {full_name}")
            count = cur.fetchone()[0]
            print(f"{t}: {count}")
            if count == 0:
                empty_tables.append(t)
        except Exception as e:
            print(f"{t}: Error {e}")
            error_tables.append(t)
            conn.rollback()
            
    print("\n--- Summary ---")
    if empty_tables:
        print(f"WARNING: {len(empty_tables)} empty tables: {', '.join(empty_tables)}")
    if error_tables:
        print(f"ERROR: {len(error_tables)} tables with errors: {', '.join(error_tables)}")
    
    if not empty_tables and not error_tables:
        print("SUCCESS: All tables populated.")
        
    conn.close()

if __name__ == "__main__":
    main()
