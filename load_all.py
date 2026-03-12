from db import get_connection, copy_from_df
from config import SCALE
from generators.orgs import generate_organizations, generate_org_hierarchy
from generators.users import generate_users, generate_roles, generate_user_roles, generate_teams, generate_team_members, generate_employee_profiles
from generators.crm import generate_accounts, generate_contacts, generate_leads, generate_opportunities, generate_opportunity_stages, generate_opportunity_history
from generators.products import generate_products, generate_plans, generate_plan_pricing_history
from generators.subscriptions import generate_subscriptions, generate_subscription_events, generate_usage_records
from generators.billing import generate_orders, generate_order_items, generate_invoices, generate_payments, generate_refunds
from generators.finance_alloc import generate_chart_of_accounts, generate_cost_centers, generate_general_ledger, generate_gl_allocations
from generators.marketing import generate_campaigns, generate_ad_spend, generate_attribution_events
from generators.sessions_events import generate_sessions, generate_events
from generators.funnel import generate_funnel
from generators.fraud import generate_devices, generate_ip_intel, generate_risk_scores, generate_fraud_cases
from generators.ml import generate_feature_store, generate_model_registry, generate_model_predictions, generate_drift_metrics
from generators.data_eng import generate_ingestion_jobs, generate_pipeline_runs, generate_data_quality_checks, generate_anomaly_flags
from generators.analytics import generate_fact_revenue, generate_fact_churn, generate_fact_subscription_metrics, generate_snapshot_users, generate_snapshot_org_monthly
import pandas as pd

def main():
    conn = get_connection()
    
    # Tables to truncate (Phase 1-6)
    '''tables = [
        "organizations", "org_hierarchy", 
        "users", "roles", "user_roles", 
        "teams", "team_members", "employee_profiles",
        "accounts", "contacts", "leads", "opportunities", "opportunity_stages", "opportunity_history",
        "products", "plans", "plan_pricing_history",
        "subscriptions", "subscription_events", "usage_records",
        "orders", "order_items", "invoices", "payments", "refunds",
        "chart_of_accounts", "general_ledger", "cost_centers", "gl_allocations",
        "sessions", "events", "fact_funnel",
        "campaigns", "ad_spend", "attribution_events",
        "devices", "ip_intel", "risk_scores", "fraud_cases",
        "feature_store", "model_registry", "model_predictions", "drift_metrics",
        "ingestion_jobs", "pipeline_runs", "data_quality_checks", "anomaly_flags",
        "fact_revenue_daily", "fact_subscription_metrics", "snapshot_user_daily", "snapshot_org_monthly"
    ]
    
    print("Truncating ALL tables...")
    with conn.cursor() as cur:
        qualified_tables = [f"enterprise_data.{t}" for t in tables]
        cur.execute(f"TRUNCATE {', '.join(qualified_tables)} CASCADE;")
    conn.commit()'''

    print("--- Phase 1: Orgs & Users ---")
    
    # 1. Orgs
    print(f"Generating organizations ({SCALE['organizations']})...")
    orgs = generate_organizations(count=SCALE["organizations"])
    copy_from_df(conn, orgs, "enterprise_data_local.organizations")
    
    print("Generating org hierarchy...")
    hierarchy = generate_org_hierarchy(orgs)
    if not hierarchy.empty:
        copy_from_df(conn, hierarchy, "enterprise_data_local.org_hierarchy")

    # 3. Users
    print(f"Generating users ({SCALE['users']})...")
    users = generate_users(org_count=len(orgs), count=SCALE["users"])
    copy_from_df(conn, users, "enterprise_data_local.users")

    # 4. Roles
    print("Generating roles...")
    roles = generate_roles()
    copy_from_df(conn, roles, "enterprise_data_local.roles")

    # 5. User Roles
    print("Generating user roles...")
    user_roles = generate_user_roles(users, roles)
    copy_from_df(conn, user_roles, "enterprise_data_local.user_roles")
    
    # 6. Teams
    print("Generating teams...")
    teams = generate_teams(org_count=len(orgs))
    copy_from_df(conn, teams, "enterprise_data_local.teams")

    # 7. Team Members
    print("Generating team members...")
    team_members = generate_team_members(users, teams)
    copy_from_df(conn, team_members, "enterprise_data_local.team_members")

    # 8. Employee Profiles
    print("Generating employee profiles...")
    profiles = generate_employee_profiles(users)
    copy_from_df(conn, profiles, "enterprise_data_local.employee_profiles")
    
    print("--- Phase 2: CRM ---")
    
    # 9. Accounts
    print("Generating accounts...")
    accounts = generate_accounts(org_df=orgs)
    copy_from_df(conn, accounts, "enterprise_data_local.accounts")
    
    # 10. Contacts
    print("Generating contacts...")
    contacts = generate_contacts(accounts, avg_contacts=2)
    copy_from_df(conn, contacts, "enterprise_data_local.contacts")
    
    # 11. Leads
    print(f"Generating leads ({SCALE['leads_daily']} daily approx)...")
    # Scale leads based on campaign/daily setting if available, or just fixed large number?
    # Config has leads_daily. Let's assume some duration or just use a fixed count related to scale.
    # For now, let's look at config.py `leads_daily`. 
    # generate_leads takes lead_count.
    # We will generate enough for "some time". Say 30 days * leads_daily.
    leads = generate_leads(campaign_count=SCALE["campaigns"], lead_count=SCALE["leads_daily"] * 30)
    copy_from_df(conn, leads, "enterprise_data_local.leads")
    
    # 12. Stages
    print("Generating opportunity stages...")
    stages = generate_opportunity_stages()
    copy_from_df(conn, stages, "enterprise_data_local.opportunity_stages")
    
    # 13. Opportunities
    print("Generating opportunities...")
    opps = generate_opportunities(accounts, leads)
    if not opps.empty:
        copy_from_df(conn, opps, "enterprise_data_local.opportunities")
        
        # 14. History
        print("Generating opportunity history...")
        opp_hist = generate_opportunity_history(opps)
        copy_from_df(conn, opp_hist, "enterprise_data_local.opportunity_history")
    else:
        print("Skipped opportunities (none generated)")

    print("--- Phase 3: Billing ---")

    # 15. Products
    print(f"Generating products ({SCALE['products']})...")
    products = generate_products()
    copy_from_df(conn, products, "enterprise_data_local.products")

    # 16. Plans
    print("Generating plans...")
    plans = generate_plans(product_count=len(products))
    copy_from_df(conn, plans, "enterprise_data_local.plans")

    # 17. Plan Pricing
    print("Generating plan pricing...")
    pricing = generate_plan_pricing_history(plans)
    copy_from_df(conn, pricing, "enterprise_data_local.plan_pricing_history")

    # 18. Subscriptions
    print("Generating subscriptions...")
    subs = generate_subscriptions(user_count=len(users), plan_count=len(plans))
    copy_from_df(conn, subs, "enterprise_data_local.subscriptions")

    # 19. Subscription Events
    print("Generating subscription events...")
    sub_events = generate_subscription_events(subs)
    copy_from_df(conn, sub_events, "enterprise_data_local.subscription_events")

    # 20. Usage Records
    print("Generating usage records...")
    usage = generate_usage_records(subs)
    copy_from_df(conn, usage, "enterprise_data_local.usage_records")

    # 21. Orders
    print("Generating orders...")
    orders = generate_orders(subs)
    copy_from_df(conn, orders, "enterprise_data_local.orders")

    # 22. Order Items
    print("Generating order items...")
    order_items = generate_order_items(orders, products)
    copy_from_df(conn, order_items, "enterprise_data_local.order_items")

    # 23. Invoices
    print("Generating invoices...")
    invoices = generate_invoices(orders)
    copy_from_df(conn, invoices, "enterprise_data_local.invoices")

    # 24. Payments
    print("Generating payments...")
    payments = generate_payments(invoices)
    copy_from_df(conn, payments, "enterprise_data_local.payments")

    # 25. Refunds
    print("Generating refunds...")
    refunds = generate_refunds(payments)
    if not refunds.empty:
        copy_from_df(conn, refunds, "enterprise_data_local.refunds")
    else:
        print("Skipped refunds (none generated)")

    print("--- Phase 4: Finance ---")
    
    # 26. Chart of Accounts
    print("Generating chart of accounts...")
    coa = generate_chart_of_accounts()
    copy_from_df(conn, coa, "enterprise_data_local.chart_of_accounts")
    
    # 27. General Ledger
    print("Generating general ledger...")
    gl = generate_general_ledger(payments)
    copy_from_df(conn, gl, "enterprise_data_local.general_ledger")
    
    # 28. Cost Centers
    print("Generating cost centers...")
    cost_centers = generate_cost_centers()
    copy_from_df(conn, cost_centers, "enterprise_data_local.cost_centers")
    
    # 29. Allocations
    print("Generating GL allocations...")
    allocs = generate_gl_allocations(gl, cost_center_count=len(cost_centers))
    copy_from_df(conn, allocs, "enterprise_data_local.gl_allocations")

    print("--- Phase 5: Analytics & Marketing ---")
    
    # 30. Campaigns
    print(f"Generating campaigns ({SCALE['campaigns']})...")
    campaigns = generate_campaigns()
    copy_from_df(conn, campaigns, "enterprise_data_local.campaigns")
    
    # 31. Ad Spend
    print("Generating ad spend...")
    spend = generate_ad_spend(campaigns)
    copy_from_df(conn, spend, "enterprise_data_local.ad_spend")
    
    # 32. Attribution
    print("Generating attribution events...")
    attr = generate_attribution_events(campaigns, users, count=100000)
    copy_from_df(conn, attr, "enterprise_data_local.attribution_events")
    
    # 33. Sessions
    print(f"Generating sessions ({SCALE['sessions_daily']} daily approx)...")
    # Scale sessions: roughly user_count * x or daily * 30?
    # Config has sessions_daily.
    sessions = generate_sessions(user_count=len(users), session_count=SCALE["sessions_daily"] * 7) # 1 week
    copy_from_df(conn, sessions, "enterprise_data_local.sessions")
    
    # 34. Events
    print("Generating events...")
    events = generate_events(sessions)
    copy_from_df(conn, events, "enterprise_data_local.events")
    
    # 35. Funnel
    print("Generating funnel metrics...")
    funnel = generate_funnel(events)
    copy_from_df(conn, funnel, "enterprise_data_local.fact_funnel")

    print("--- Phase 6: ML, Fraud, Data Eng ---")
    
    # 36. Devices
    print("Generating devices...")
    devices = generate_devices(user_count=len(users))
    copy_from_df(conn, devices, "enterprise_data_local.devices")
    
    # 37. IP Intel
    print("Generating IP intel...")
    ip_intel = generate_ip_intel(ip_count=1000) 
    copy_from_df(conn, ip_intel, "enterprise_data_local.ip_intel")
    
    # 38. Risk Scores
    print("Generating risk scores...")
    risk_scores = generate_risk_scores(user_count=len(users))
    copy_from_df(conn, risk_scores, "enterprise_data_local.risk_scores")
    
    # 39. Fraud Cases
    print("Generating fraud cases...")
    fraud_cases = generate_fraud_cases(risk_scores)
    if not fraud_cases.empty:
        copy_from_df(conn, fraud_cases, "enterprise_data_local.fraud_cases")
    
    # 40. Feature Store
    print("Generating feature store...")
    features = generate_feature_store(user_count=len(users), versions=3)
    copy_from_df(conn, features, "enterprise_data_local.feature_store")
    
    # 41. Model Registry
    print("Generating model registry...")
    registry = generate_model_registry()
    copy_from_df(conn, registry, "enterprise_data_local.model_registry")
    
    # 42. predictions
    print("Generating model predictions...")
    preds = generate_model_predictions(user_count=len(users), model_count=len(registry))
    copy_from_df(conn, preds, "enterprise_data_local.model_predictions")
    
    # 43. Drift
    print("Generating drift metrics...")
    drift = generate_drift_metrics(model_count=len(registry))
    copy_from_df(conn, drift, "enterprise_data_local.drift_metrics")
    
    # 44. Data Eng - Jobs
    print("Generating ingestion jobs...")
    jobs = generate_ingestion_jobs()
    copy_from_df(conn, jobs, "enterprise_data_local.ingestion_jobs")
    
    # 45. Runs
    print("Generating pipeline runs...")
    runs = generate_pipeline_runs(jobs, runs_per_job=10)
    copy_from_df(conn, runs, "enterprise_data_local.pipeline_runs")
    
    # 46. DQ Checks
    print("Generating DQ checks...")
    dq = generate_data_quality_checks()
    copy_from_df(conn, dq, "enterprise_data_local.data_quality_checks")
    
    # 47. Anomalies
    print("Generating anomalies...")
    anomalies = generate_anomaly_flags()
    copy_from_df(conn, anomalies, "enterprise_data_local.anomaly_flags")
    
    # 48. Analytics Facts
    print("Generating analytics facts...")
    fact_rev = generate_fact_revenue(invoices)
    copy_from_df(conn, fact_rev, "enterprise_data_local.fact_revenue_daily")
    
    fact_subs = generate_fact_subscription_metrics(subs)
    copy_from_df(conn, fact_subs, "enterprise_data_local.fact_subscription_metrics")
    
    snap_users = generate_snapshot_users(users, subs)
    copy_from_df(conn, snap_users, "enterprise_data_local.snapshot_user_daily")
    
    snap_orgs = generate_snapshot_org_monthly(org_count=len(orgs))
    copy_from_df(conn, snap_orgs, "enterprise_data_local.snapshot_org_monthly")
    
    conn.close()
    print("FULL LOAD COMPLETE. All 48 tables generated and populated.")

if __name__ == "__main__":
    main()
