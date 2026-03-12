SET search_path TO "enterprise_data";

CREATE TABLE organizations (
    org_id          BIGSERIAL PRIMARY KEY,
    org_name        TEXT NOT NULL,
    industry        TEXT,
    country         TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE org_hierarchy (
    parent_org_id   BIGINT REFERENCES organizations(org_id),
    child_org_id    BIGINT REFERENCES organizations(org_id),
    depth           INT NOT NULL,
    PRIMARY KEY (parent_org_id, child_org_id)
);

CREATE INDEX idx_org_hierarchy_child ON org_hierarchy(child_org_id);
CREATE INDEX idx_org_hierarchy_parent ON org_hierarchy(parent_org_id);
CREATE TABLE users (
    user_id     BIGSERIAL PRIMARY KEY,
    org_id      BIGINT REFERENCES organizations(org_id),
    email       TEXT UNIQUE NOT NULL,
    full_name   TEXT,
    status      TEXT CHECK (status IN ('active','inactive','suspended')),
    created_at  TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_users_org ON users(org_id);

CREATE TABLE roles (
    role_id     BIGSERIAL PRIMARY KEY,
    role_name   TEXT UNIQUE NOT NULL
);

CREATE TABLE user_roles (
    user_id     BIGINT REFERENCES users(user_id),
    role_id     BIGINT REFERENCES roles(role_id),
    assigned_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (user_id, role_id)
);
CREATE TABLE teams (
    team_id     BIGSERIAL PRIMARY KEY,
    org_id      BIGINT REFERENCES organizations(org_id),
    team_name   TEXT NOT NULL
);

CREATE TABLE team_members (
    team_id     BIGINT REFERENCES teams(team_id),
    user_id     BIGINT REFERENCES users(user_id),
    joined_at   TIMESTAMP DEFAULT now(),
    PRIMARY KEY (team_id, user_id)
);

CREATE TABLE employee_profiles (
    user_id     BIGINT PRIMARY KEY REFERENCES users(user_id),
    manager_id  BIGINT REFERENCES users(user_id),
    title       TEXT,
    level       TEXT,
    hired_at    DATE
);

CREATE INDEX idx_employee_manager ON employee_profiles(manager_id);
CREATE TABLE accounts (
    account_id     BIGSERIAL PRIMARY KEY,
    org_id         BIGINT REFERENCES organizations(org_id),
    account_name   TEXT NOT NULL,
    industry       TEXT,
    country        TEXT,
    created_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_accounts_org ON accounts(org_id);

CREATE TABLE contacts (
    contact_id     BIGSERIAL PRIMARY KEY,
    account_id     BIGINT REFERENCES accounts(account_id),
    email          TEXT,
    full_name      TEXT,
    role_title     TEXT,
    created_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_contacts_account ON contacts(account_id);
CREATE TABLE leads (
    lead_id        BIGSERIAL PRIMARY KEY,
    source         TEXT,
    campaign_id    BIGINT,
    status         TEXT CHECK (status IN ('new','qualified','disqualified','converted')),
    created_at     TIMESTAMP DEFAULT now()
);

CREATE TABLE opportunities (
    opp_id         BIGSERIAL PRIMARY KEY,
    account_id    BIGINT REFERENCES accounts(account_id),
    estimated_value NUMERIC(12,2),
    probability    FLOAT,
    close_date     DATE,
    created_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_opportunities_account ON opportunities(account_id);
CREATE TABLE opportunity_stages (
    stage_id     BIGSERIAL PRIMARY KEY,
    stage_name   TEXT UNIQUE NOT NULL,
    stage_order  INT
);

CREATE TABLE opportunity_history (
    opp_id       BIGINT REFERENCES opportunities(opp_id),
    stage_id     BIGINT REFERENCES opportunity_stages(stage_id),
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP,
    PRIMARY KEY (opp_id, valid_from)
);

CREATE INDEX idx_opp_history_stage ON opportunity_history(stage_id);
CREATE TABLE products (
    product_id     BIGSERIAL PRIMARY KEY,
    product_name   TEXT NOT NULL,
    category       TEXT,
    is_active      BOOLEAN DEFAULT true,
    created_at     TIMESTAMP DEFAULT now()
);

CREATE TABLE plans (
    plan_id        BIGSERIAL PRIMARY KEY,
    product_id    BIGINT REFERENCES products(product_id),
    plan_name     TEXT NOT NULL,
    billing_cycle TEXT CHECK (billing_cycle IN ('monthly','quarterly','yearly')),
    created_at    TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_plans_product ON plans(product_id);
CREATE TABLE plan_pricing_history (
    pricing_id    BIGSERIAL PRIMARY KEY,
    plan_id       BIGINT REFERENCES plans(plan_id),
    price         NUMERIC(10,2) NOT NULL,
    currency      TEXT DEFAULT 'INR',
    valid_from    DATE NOT NULL,
    valid_to      DATE,
    is_current    BOOLEAN DEFAULT true
);

CREATE INDEX idx_pricing_plan ON plan_pricing_history(plan_id);
CREATE INDEX idx_pricing_current ON plan_pricing_history(is_current);
CREATE TABLE subscriptions (
    subscription_id BIGSERIAL PRIMARY KEY,
    user_id         BIGINT REFERENCES users(user_id),
    plan_id         BIGINT REFERENCES plans(plan_id),
    status          TEXT CHECK (status IN ('active','paused','cancelled','expired')),
    start_date      DATE,
    end_date        DATE,
    created_at      TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_subscriptions_user ON subscriptions(user_id);
CREATE INDEX idx_subscriptions_plan ON subscriptions(plan_id);

CREATE TABLE subscription_events (
    event_id        BIGSERIAL PRIMARY KEY,
    subscription_id BIGINT REFERENCES subscriptions(subscription_id),
    event_type      TEXT,
    event_time      TIMESTAMP DEFAULT now(),
    metadata        JSONB
);

CREATE INDEX idx_sub_events_sub ON subscription_events(subscription_id);
CREATE TABLE usage_records (
    usage_id        BIGSERIAL PRIMARY KEY,
    subscription_id BIGINT REFERENCES subscriptions(subscription_id),
    metric_name     TEXT,
    metric_value    NUMERIC,
    recorded_at     TIMESTAMP NOT NULL
);

CREATE INDEX idx_usage_sub_time ON usage_records(subscription_id, recorded_at);
CREATE TABLE orders (
    order_id     BIGSERIAL PRIMARY KEY,
    user_id      BIGINT REFERENCES users(user_id),
    order_status TEXT CHECK (order_status IN ('created','paid','cancelled','refunded')),
    order_time   TIMESTAMP DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT REFERENCES orders(order_id),
    product_id   BIGINT REFERENCES products(product_id),
    quantity     INT,
    unit_price   NUMERIC(10,2)
);

CREATE TABLE invoices (
    invoice_id    BIGSERIAL PRIMARY KEY,
    order_id      BIGINT REFERENCES orders(order_id),
    total_amount  NUMERIC(12,2),
    issued_at     TIMESTAMP DEFAULT now()
);

CREATE TABLE payments (
    payment_id    BIGSERIAL PRIMARY KEY,
    invoice_id    BIGINT REFERENCES invoices(invoice_id),
    amount        NUMERIC(12,2),
    method        TEXT,
    paid_at       TIMESTAMP DEFAULT now()
);

CREATE TABLE refunds (
    refund_id     BIGSERIAL PRIMARY KEY,
    payment_id   BIGINT REFERENCES payments(payment_id),
    amount       NUMERIC(12,2),
    refunded_at  TIMESTAMP DEFAULT now()
);
CREATE TABLE chart_of_accounts (
    account_code   VARCHAR(20) PRIMARY KEY,
    account_name   TEXT NOT NULL,
    account_type   TEXT CHECK (account_type IN ('asset','liability','equity','revenue','expense')),
    parent_account VARCHAR(20) REFERENCES chart_of_accounts(account_code)
);
CREATE TABLE general_ledger (
    entry_id       BIGSERIAL PRIMARY KEY,
    account_code   VARCHAR(20) REFERENCES chart_of_accounts(account_code),
    debit          NUMERIC(14,2) DEFAULT 0,
    credit         NUMERIC(14,2) DEFAULT 0,
    reference_type TEXT,
    reference_id   BIGINT,
    entry_date     DATE NOT NULL,
    created_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_gl_account_date ON general_ledger(account_code, entry_date);
CREATE TABLE cost_centers (
    cost_center_id BIGSERIAL PRIMARY KEY,
    name           TEXT,
    department     TEXT
);

CREATE TABLE gl_allocations (
    entry_id       BIGINT REFERENCES general_ledger(entry_id),
    cost_center_id BIGINT REFERENCES cost_centers(cost_center_id),
    allocation_pct NUMERIC(5,2),
    PRIMARY KEY (entry_id, cost_center_id)
);
CREATE TABLE revenue_recognition (
    rev_rec_id     BIGSERIAL PRIMARY KEY,
    invoice_id     BIGINT REFERENCES invoices(invoice_id),
    recognition_date DATE,
    amount         NUMERIC(12,2),
    status         TEXT CHECK (status IN ('recognized','deferred','reversed'))
);

CREATE INDEX idx_revrec_invoice ON revenue_recognition(invoice_id);
CREATE INDEX idx_revrec_date ON revenue_recognition(recognition_date);
CREATE TABLE sessions (
    session_id   BIGSERIAL PRIMARY KEY,
    user_id      BIGINT REFERENCES users(user_id),
    started_at   TIMESTAMP,
    ended_at     TIMESTAMP,
    device_type  TEXT,
    ip_address   INET
);

CREATE INDEX idx_sessions_user_time ON sessions(user_id, started_at);

CREATE TABLE events (
    event_id     BIGSERIAL PRIMARY KEY,
    session_id   BIGINT REFERENCES sessions(session_id),
    user_id      BIGINT REFERENCES users(user_id),
    event_type   TEXT,
    event_time   TIMESTAMP NOT NULL,
    properties   JSONB
);

CREATE INDEX idx_events_time ON events(event_time);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_props_gin ON events USING GIN (properties);
CREATE TABLE campaigns (
    campaign_id  BIGSERIAL PRIMARY KEY,
    name         TEXT,
    channel      TEXT,
    start_date   DATE,
    end_date     DATE
);

CREATE TABLE ad_spend (
    spend_id     BIGSERIAL PRIMARY KEY,
    campaign_id  BIGINT REFERENCES campaigns(campaign_id),
    spend_date   DATE,
    amount       NUMERIC(12,2)
);

CREATE TABLE attribution_events (
    attribution_id BIGSERIAL PRIMARY KEY,
    user_id        BIGINT REFERENCES users(user_id),
    campaign_id    BIGINT REFERENCES campaigns(campaign_id),
    touch_type     TEXT CHECK (touch_type IN ('first_touch','last_touch','assist')),
    event_time     TIMESTAMP
);

CREATE INDEX idx_attr_user_time ON attribution_events(user_id, event_time);
CREATE TABLE devices (
    device_id    BIGSERIAL PRIMARY KEY,
    user_id      BIGINT REFERENCES users(user_id),
    fingerprint  TEXT,
    first_seen   TIMESTAMP,
    last_seen    TIMESTAMP
);

CREATE TABLE ip_intel (
    ip_address   INET PRIMARY KEY,
    country      TEXT,
    risk_level   TEXT
);

CREATE TABLE risk_scores (
    score_id     BIGSERIAL PRIMARY KEY,
    entity_type  TEXT,
    entity_id    BIGINT,
    score        NUMERIC(5,2),
    calculated_at TIMESTAMP
);

CREATE TABLE fraud_cases (
    case_id      BIGSERIAL PRIMARY KEY,
    entity_type  TEXT,
    entity_id    BIGINT,
    label        BOOLEAN,
    created_at   TIMESTAMP
);
CREATE TABLE feature_store (
    feature_id   BIGSERIAL PRIMARY KEY,
    entity_type  TEXT,
    entity_id    BIGINT,
    feature_set  JSONB,
    version      INT,
    generated_at TIMESTAMP
);

CREATE INDEX idx_feature_entity_time ON feature_store(entity_type, entity_id, generated_at);
CREATE TABLE model_registry (
    model_id     BIGSERIAL PRIMARY KEY,
    model_name   TEXT,
    version      TEXT,
    metrics      JSONB,
    trained_at   TIMESTAMP
);

CREATE TABLE model_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    model_id      BIGINT REFERENCES model_registry(model_id),
    entity_type   TEXT,
    entity_id     BIGINT,
    score         NUMERIC,
    predicted_at  TIMESTAMP
);
CREATE TABLE fact_revenue_daily (
    revenue_date   DATE,
    org_id         BIGINT,
    total_revenue  NUMERIC(14,2),
    total_refunds  NUMERIC(14,2),
    net_revenue    NUMERIC(14,2),
    PRIMARY KEY (revenue_date, org_id)
);

CREATE TABLE fact_subscription_metrics (
    metric_date     DATE,
    plan_id         BIGINT,
    active_subs     INT,
    new_subs        INT,
    churned_subs    INT,
    mrr             NUMERIC(14,2),
    PRIMARY KEY (metric_date, plan_id)
);

CREATE TABLE fact_funnel (
    funnel_id     BIGSERIAL PRIMARY KEY,
    step_name     TEXT,
    step_order    INT,
    user_count    INT,
    computed_at   TIMESTAMP
);
CREATE TABLE snapshot_user_daily (
    snapshot_date DATE,
    user_id       BIGINT,
    is_active     BOOLEAN,
    plan_id       BIGINT,
    mrr           NUMERIC(10,2),
    churn_risk    NUMERIC(5,2),
    PRIMARY KEY (snapshot_date, user_id)
);

CREATE TABLE snapshot_org_monthly (
    snapshot_month DATE,
    org_id         BIGINT,
    active_users   INT,
    total_revenue  NUMERIC(14,2),
    churn_rate     NUMERIC(5,2),
    PRIMARY KEY (snapshot_month, org_id)
);
CREATE TABLE ingestion_jobs (
    job_id        BIGSERIAL PRIMARY KEY,
    source_system TEXT,
    target_table  TEXT,
    schedule      TEXT
);

CREATE TABLE pipeline_runs (
    run_id        BIGSERIAL PRIMARY KEY,
    job_id        BIGINT REFERENCES ingestion_jobs(job_id),
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    status        TEXT,
    rows_processed INT
);

CREATE TABLE data_quality_checks (
    check_id      BIGSERIAL PRIMARY KEY,
    table_name    TEXT,
    rule          TEXT,
    failure_rate  NUMERIC(5,2),
    checked_at    TIMESTAMP
);

SET search_path TO "enterprise_data";

CREATE TABLE drift_metrics (
    drift_id     BIGSERIAL PRIMARY KEY,
    model_id     BIGINT REFERENCES model_registry(model_id),
    metric_name  TEXT,
    metric_value NUMERIC,
    measured_at  TIMESTAMP
);


CREATE TABLE anomaly_flags (
    anomaly_id    BIGSERIAL PRIMARY KEY,
    entity_type   TEXT,
    entity_id     BIGINT,
    metric_name   TEXT,
    metric_value  NUMERIC,
    expected_range NUMERIC[],
    detected_at   TIMESTAMP
);
CREATE MATERIALIZED VIEW mv_monthly_revenue AS
SELECT 
    date_trunc('month', recognition_date) AS month,
    SUM(amount) AS recognized_revenue
FROM revenue_recognition
WHERE status = 'recognized'
GROUP BY 1;

CREATE MATERIALIZED VIEW mv_user_ltv AS
SELECT 
    u.user_id,
    SUM(p.amount) AS lifetime_value
FROM users u
JOIN subscriptions s ON u.user_id = s.user_id
JOIN orders o ON o.user_id = u.user_id
JOIN invoices i ON i.order_id = o.order_id
JOIN payments p ON p.invoice_id = i.invoice_id
GROUP BY u.user_id;
