import psycopg2
from db import get_connection

def main():
    conn = get_connection()
    cur = conn.cursor()
    
    tables = [
        "organizations", "users", "products", "plans", "campaigns", "org_hierarchy", 
        "subscriptions", "orders", "invoices", "payments", "revenue_recognition",
        "sessions", "events", "fact_funnel", "devices", "ip_intel", "risk_scores", "fraud_cases",
        "feature_store", "model_registry", "model_predictions", "drift_metrics",
        "fact_revenue_daily", "fact_churn", "snapshot_user_daily", "data_quality_checks", "anomaly_flags",
        "accounts", "contacts", "leads", "opportunity_stages", "opportunities", "opportunity_history",
        "sla_policies", "tickets", "ticket_events", "sla_metrics",
        "departments", "employees", "payroll", "performance_reviews",
        "cost_centers", "gl_allocations",
        "ingestion_jobs", "pipeline_runs"
    ]
    
    with open("schema_utf8.txt", "w", encoding="utf-8") as f:
        for table in tables:
            f.write(f"--- Schema for {table} ---\n")
            cur.execute(f"""
                SELECT column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_name = '{table}'
                ORDER BY ordinal_position;
            """)
            rows = cur.fetchall()
            for row in rows:
                f.write(str(row) + "\n")
            f.write("\n")
        
    conn.close()

if __name__ == "__main__":
    main()
