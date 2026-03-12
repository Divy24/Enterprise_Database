import psycopg2
import re
from db import get_connection

SCHEMA_FILE = "schema_utf8.txt"
TARGET_SCHEMA = "enterprise_data"

# Hardcoded definitions for tables missing in schema file
MISSING_TABLES_DDL = {
    "fact_churn": """
        date DATE,
        plan_id BIGINT,
        churned_users INTEGER
    """,
    "drift_metrics": """
        drift_id BIGINT PRIMARY KEY,
        model_id BIGINT,
        metric_name TEXT,
        metric_value NUMERIC,
        measured_at TIMESTAMP
    """,
    "sla_policies": """
        policy_id BIGINT PRIMARY KEY,
        name TEXT,
        response_time_hrs INTEGER,
        resolution_time_hrs INTEGER
    """,
    "tickets": """
        ticket_id BIGINT PRIMARY KEY,
        user_id BIGINT,
        subject TEXT,
        priority TEXT,
        status TEXT,
        created_at TIMESTAMP
    """,
    "ticket_events": """
        event_id BIGINT PRIMARY KEY,
        ticket_id BIGINT,
        event_type TEXT,
        event_time TIMESTAMP
    """,
    "sla_metrics": """
        metric_id BIGINT PRIMARY KEY,
        ticket_id BIGINT,
        sla_id BIGINT,
        response_time_hrs INTEGER,
        resolution_time_hrs INTEGER,
        breach_flag BOOLEAN
    """,
    "departments": """
        dept_id BIGINT PRIMARY KEY,
        name TEXT
    """,
    "employees": """
        employee_id BIGINT PRIMARY KEY,
        user_id BIGINT,
        dept_id BIGINT,
        title TEXT,
        level TEXT,
        hired_at TIMESTAMP
    """,
    "payroll": """
        payroll_id BIGINT PRIMARY KEY,
        employee_id BIGINT,
        base_salary INTEGER,
        bonus INTEGER,
        currency TEXT,
        paid_at TIMESTAMP
    """,
    "performance_reviews": """
        review_id BIGINT PRIMARY KEY,
        employee_id BIGINT,
        rating TEXT,
        review_date TIMESTAMP,
        manager_comments TEXT
    """,
    # Also need to check if any others were empty in schema file
    # fact_subscription_metrics wasn't in list I inspected manually but let's check parse
}

def parse_schema_file():
    tables = {}
    current_table = None
    
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # Match Header: --- Schema for table_name ---
            match_header = re.match(r"^--- Schema for (.+) ---$", line)
            if match_header:
                current_table = match_header.group(1)
                tables[current_table] = []
                continue
            
            # Match Column: ('col_name', 'type', idx)
            # Input format example: ('org_id', 'bigint', 1)
            # Use strict regex to avoid partial matches
            if line.startswith("('") and line.endswith(")"):
                # Clean up tuple string tokens
                parts = line[1:-1].split(", ")
                if len(parts) >= 2:
                    col_name = parts[0].strip("'")
                    col_type = parts[1].strip("'")
                    
                    # Fix Types for Postgres
                    if col_type == "timestamp without time zone":
                        col_type = "TIMESTAMP"
                    elif col_type == "double precision":
                        col_type = "DOUBLE PRECISION"
                    
                    tables[current_table].append(f"{col_name} {col_type}")
    
    return tables

def main():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Parse existing schema file
    parsed_tables = parse_schema_file()
    
    # 2. Merge with missing tables
    # If a table in parsed_tables has 0 columns, use MISSING_TABLES_DDL if available
    final_tables = {}
    
    all_table_names = set(parsed_tables.keys()) | set(MISSING_TABLES_DDL.keys())
    
    for t in all_table_names:
        ddl_cols = []
        
        # Check parsed
        if t in parsed_tables and parsed_tables[t]:
            ddl_cols = parsed_tables[t]
        
        # Check missing fallback
        if not ddl_cols and t in MISSING_TABLES_DDL:
            # removing newlines and cleanup
            raw_ddl = MISSING_TABLES_DDL[t].strip().split(",")
            ddl_cols = [c.strip() for c in raw_ddl if c.strip()]
            
        final_tables[t] = ddl_cols
        
    print(f"Prepared DDL for {len(final_tables)} tables.")
    
    # 3. Create Schema and Tables
    try:
        # Create schema if not exists
        print(f"Creating schema '{TARGET_SCHEMA}'...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
        cur.execute(f"SET search_path TO {TARGET_SCHEMA};")
        
        for table, columns in final_tables.items():
            if not columns:
                print(f"Skipping {table} (No columns defined)")
                continue
                
            col_def_str = ",\n".join(columns)
            
            # Special fix for Arrays if any (parsed as 'ARRAY' string?)
            # In schema file: ('expected_range', 'ARRAY', 6)
            # Postgres needs 'integer[]' or similar. 
            # 'expected_range' in anomaly_flags is {int, int}. So INT[].
            col_def_str = col_def_str.replace(" ARRAY", " INTEGER[]")
            
            create_query = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {col_def_str}
                );
            """
            print(f"Creating table {table}...")
            # print(create_query)
            cur.execute(create_query)
            
        conn.commit()
        print("Schema recreation complete.")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
