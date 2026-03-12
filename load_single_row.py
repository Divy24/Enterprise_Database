from db import get_connection, copy_from_df
from generators.orgs import generate_organizations, generate_org_hierarchy
from generators.users import generate_users, generate_roles, generate_user_roles, generate_teams, generate_team_members, generate_employee_profiles
import pandas as pd

def main():
    conn = get_connection()
    
    # Tables to truncate (Phase 1, 2, 3)
    tables = [
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
    
    print("Truncating Phase 1-6 tables...")
    with conn.cursor() as cur:
        qualified_tables = [f"enterprise_data.{t}" for t in tables]
        cur.execute(f"TRUNCATE {', '.join(qualified_tables)} CASCADE;")
    conn.commit()

    print("--- Phase 1: Orgs & Users ---")
    
    # 1. Orgs - Generate 2 to match hierarchy requirement
    print("Generating organizations (2)...")
    orgs = generate_organizations(count=2)
    copy_from_df(conn, orgs, "enterprise_data.organizations")
    
    print("Generating org hierarchy...")
    hierarchy = generate_org_hierarchy(orgs)
    if not hierarchy.empty:
        copy_from_df(conn, hierarchy, "enterprise_data.org_hierarchy")
    else:
        print("Skipped hierarchy (empty)")

    # 3. Users
    print("Generating users (2)...")
    users = generate_users(org_count=len(orgs), count=2)
    copy_from_df(conn, users, "enterprise_data.users")

    # 4. Roles
    print("Generating roles...")
    roles = generate_roles()
    copy_from_df(conn, roles, "enterprise_data.roles")

    # 5. User Roles
    print("Generating user roles...")
    user_roles = generate_user_roles(users, roles)
    copy_from_df(conn, user_roles, "enterprise_data.user_roles")
    
    # 6. Teams
    print("Generating teams...")
    teams = generate_teams(org_count=len(orgs))
    copy_from_df(conn, teams, "enterprise_data.teams")

    # 7. Team Members
    print("Generating team members...")
    team_members = generate_team_members(users, teams)
    copy_from_df(conn, team_members, "enterprise_data.team_members")

    # 8. Employee Profiles
    print("Generating employee profiles...")
    profiles = generate_employee_profiles(users)
    copy_from_df(conn, profiles, "enterprise_data.employee_profiles")
    
    print("--- Phase 2: CRM ---")
    from generators.crm import generate_accounts, generate_contacts, generate_leads, generate_opportunities, generate_opportunity_stages, generate_opportunity_history
    
    # 9. Accounts
    print("Generating accounts...")
    accounts = generate_accounts(org_df=orgs)
    copy_from_df(conn, accounts, "enterprise_data.accounts")
    
    # 10. Contacts
    print("Generating contacts...")
    contacts = generate_contacts(accounts, avg_contacts=1)
    copy_from_df(conn, contacts, "enterprise_data.contacts")
    
    # 11. Leads
    print("Generating leads...")
    leads = generate_leads(campaign_count=1, lead_count=5)
    copy_from_df(conn, leads, "enterprise_data.leads")
    
    # 12. Stages
    print("Generating opportunity stages...")
    stages = generate_opportunity_stages()
    copy_from_df(conn, stages, "enterprise_data.opportunity_stages")
    
    # 13. Opportunities
    print("Generating opportunities...")
    opps = generate_opportunities(accounts, leads)
    if not opps.empty:
        copy_from_df(conn, opps, "enterprise_data.opportunities")
        
        # 14. History
        print("Generating opportunity history...")
        opp_hist = generate_opportunity_history(opps)
        copy_from_df(conn, opp_hist, "enterprise_data.opportunity_history")
    else:
        print("Skipped opportunities (none generated)")

    print("--- Phase 3: Billing ---")
    from generators.products import generate_products, generate_plans, generate_plan_pricing_history
    from generators.subscriptions import generate_subscriptions, generate_subscription_events, generate_usage_records
    from generators.billing import generate_orders, generate_order_items, generate_invoices, generate_payments, generate_refunds

    # 15. Products
    print("Generating products...")
    products = generate_products()
    copy_from_df(conn, products, "enterprise_data.products")

    # 16. Plans
    print("Generating plans...")
    plans = generate_plans(product_count=len(products))
    copy_from_df(conn, plans, "enterprise_data.plans")

    # 17. Plan Pricing
    print("Generating plan pricing...")
    pricing = generate_plan_pricing_history(plans)
    copy_from_df(conn, pricing, "enterprise_data.plan_pricing_history")

    # 18. Subscriptions
    print("Generating subscriptions...")
    subs = generate_subscriptions(user_count=len(users), plan_count=len(plans))
    copy_from_df(conn, subs, "enterprise_data.subscriptions")

    # 19. Subscription Events
    print("Generating subscription events...")
    sub_events = generate_subscription_events(subs)
    copy_from_df(conn, sub_events, "enterprise_data.subscription_events")

    # 20. Usage Records
    print("Generating usage records...")
    usage = generate_usage_records(subs)
    copy_from_df(conn, usage, "enterprise_data.usage_records")

    # 21. Orders
    print("Generating orders...")
    orders = generate_orders(subs)
    copy_from_df(conn, orders, "enterprise_data.orders")

    # 22. Order Items
    print("Generating order items...")
    order_items = generate_order_items(orders, products)
    copy_from_df(conn, order_items, "enterprise_data.order_items")

    # 23. Invoices
    print("Generating invoices...")
    invoices = generate_invoices(orders)
    copy_from_df(conn, invoices, "enterprise_data.invoices")

    # 24. Payments
    print("Generating payments...")
    payments = generate_payments(invoices)
    copy_from_df(conn, payments, "enterprise_data.payments")

    # 25. Refunds
    print("Generating refunds...")
    refunds = generate_refunds(payments)
    if not refunds.empty:
        copy_from_df(conn, refunds, "enterprise_data.refunds")
    else:
        print("Skipped refunds (none generated)")

    print("--- Phase 4: Finance ---")
    from generators.finance_alloc import generate_chart_of_accounts, generate_cost_centers, generate_general_ledger, generate_gl_allocations
    
    # 26. Chart of Accounts
    print("Generating chart of accounts...")
    coa = generate_chart_of_accounts()
    copy_from_df(conn, coa, "enterprise_data.chart_of_accounts")
    
    # 27. General Ledger
    print("Generating general ledger...")
    # Derive from payments
    gl = generate_general_ledger(payments)
    copy_from_df(conn, gl, "enterprise_data.general_ledger")
    
    # 28. Cost Centers
    print("Generating cost centers...")
    cost_centers = generate_cost_centers()
    copy_from_df(conn, cost_centers, "enterprise_data.cost_centers")
    
    # 29. Allocations
    print("Generating GL allocations...")
    allocs = generate_gl_allocations(gl, cost_center_count=len(cost_centers))
    copy_from_df(conn, allocs, "enterprise_data.gl_allocations")

    print("--- Phase 5: Analytics & Marketing ---")
    from generators.marketing import generate_campaigns, generate_ad_spend, generate_attribution_events
    from generators.sessions_events import generate_sessions, generate_events
    from generators.funnel import generate_funnel
    
    # 30. Campaigns
    print("Generating campaigns...")
    campaigns = generate_campaigns()
    copy_from_df(conn, campaigns, "enterprise_data.campaigns")
    
    # 31. Ad Spend
    print("Generating ad spend...")
    spend = generate_ad_spend(campaigns)
    copy_from_df(conn, spend, "enterprise_data.ad_spend")
    
    # 32. Attribution
    print("Generating attribution events...")
    attr = generate_attribution_events(campaigns, users)
    copy_from_df(conn, attr, "enterprise_data.attribution_events")
    
    # 33. Sessions
    print("Generating sessions...")
    # Generate minimal sessions
    sessions = generate_sessions(user_count=len(users), session_count=10)
    copy_from_df(conn, sessions, "enterprise_data.sessions")
    
    # 34. Events
    print("Generating events...")
    events = generate_events(sessions)
    copy_from_df(conn, events, "enterprise_data.events")
    
    # 35. Funnel
    print("Generating funnel metrics...")
    funnel = generate_funnel(events)
    copy_from_df(conn, funnel, "enterprise_data.fact_funnel")

    print("--- Phase 6: ML, Fraud, Data Eng ---")
    from generators.fraud import generate_devices, generate_ip_intel, generate_risk_scores, generate_fraud_cases
    from generators.ml import generate_feature_store, generate_model_registry, generate_model_predictions, generate_drift_metrics
    from generators.data_eng import generate_ingestion_jobs, generate_pipeline_runs, generate_data_quality_checks, generate_anomaly_flags
    from generators.analytics import generate_fact_revenue, generate_fact_churn, generate_fact_subscription_metrics, generate_snapshot_users, generate_snapshot_org_monthly
    
    # 36. Devices
    print("Generating devices...")
    devices = generate_devices(user_count=len(users))
    copy_from_df(conn, devices, "enterprise_data.devices")
    
    # 37. IP Intel
    print("Generating IP intel...")
    ip_intel = generate_ip_intel(ip_count=50) # Small scale
    copy_from_df(conn, ip_intel, "enterprise_data.ip_intel")
    
    # 38. Risk Scores
    print("Generating risk scores...")
    risk_scores = generate_risk_scores(user_count=len(users))
    copy_from_df(conn, risk_scores, "enterprise_data.risk_scores")
    
    # 39. Fraud Cases
    print("Generating fraud cases...")
    fraud_cases = generate_fraud_cases(risk_scores)
    if not fraud_cases.empty:
        copy_from_df(conn, fraud_cases, "enterprise_data.fraud_cases")
    
    # 40. Feature Store
    print("Generating feature store...")
    features = generate_feature_store(user_count=len(users), versions=1)
    copy_from_df(conn, features, "enterprise_data.feature_store")
    
    # 41. Model Registry
    print("Generating model registry...")
    registry = generate_model_registry()
    copy_from_df(conn, registry, "enterprise_data.model_registry")
    
    # 42. predictions
    print("Generating model predictions...")
    preds = generate_model_predictions(user_count=len(users), model_count=len(registry))
    copy_from_df(conn, preds, "enterprise_data.model_predictions")
    
    # 43. Drift
    print("Generating drift metrics...")
    drift = generate_drift_metrics(model_count=len(registry))
    copy_from_df(conn, drift, "enterprise_data.drift_metrics")
    
    # 44. Data Eng - Jobs
    print("Generating ingestion jobs...")
    jobs = generate_ingestion_jobs()
    copy_from_df(conn, jobs, "enterprise_data.ingestion_jobs")
    
    # 45. Runs
    print("Generating pipeline runs...")
    runs = generate_pipeline_runs(jobs, runs_per_job=2)
    copy_from_df(conn, runs, "enterprise_data.pipeline_runs")
    
    # 46. DQ Checks
    print("Generating DQ checks...")
    dq = generate_data_quality_checks()
    copy_from_df(conn, dq, "enterprise_data.data_quality_checks")
    
    # 47. Anomalies
    print("Generating anomalies...")
    anomalies = generate_anomaly_flags()
    copy_from_df(conn, anomalies, "enterprise_data.anomaly_flags")
    
    # 48. Analytics Facts
    print("Generating analytics facts...")
    fact_rev = generate_fact_revenue(invoices)
    copy_from_df(conn, fact_rev, "enterprise_data.fact_revenue_daily")
    
    fact_subs = generate_fact_subscription_metrics(subs)
    copy_from_df(conn, fact_subs, "enterprise_data.fact_subscription_metrics")
    
    snap_users = generate_snapshot_users(users, subs)
    copy_from_df(conn, snap_users, "enterprise_data.snapshot_user_daily")
    
    snap_orgs = generate_snapshot_org_monthly(org_count=len(orgs))
    copy_from_df(conn, snap_orgs, "enterprise_data.snapshot_org_monthly")
    
    conn.close()
    print("Phase 1-6 Complete. ALL TABLES LOADED.")

if __name__ == "__main__":
    main()
