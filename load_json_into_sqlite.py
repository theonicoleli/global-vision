import json
import sqlite3
import pandas as pd
import os

DB_PATH = "/app/my_database.db"

def create_tables(cursor):
    bronze_accounts = """
        CREATE TABLE IF NOT EXISTS accounts (
            account_sfid TEXT PRIMARY KEY,
            account_name TEXT,
            account_created_date TEXT,
            account_country TEXT,
            account_industry TEXT
        );
    """
    bronze_cases = """
        CREATE TABLE IF NOT EXISTS support_cases (
            case_sfid TEXT PRIMARY KEY,
            account_sfid TEXT,
            case_number INTEGER,
            case_contact_name TEXT,
            case_product TEXT,
            case_status TEXT,
            case_resolution TEXT,
            case_details TEXT,
            case_closure TEXT,
            case_priority TEXT,
            case_severity TEXT,
            case_reason TEXT,
            case_type TEXT,
            case_category TEXT,
            case_created_date TEXT,
            case_closed_date TEXT,
            FOREIGN KEY (account_sfid) REFERENCES accounts (account_sfid)
        );
    """
    cursor.execute(bronze_accounts)
    cursor.execute(bronze_cases)

def create_silver_table(cursor):
    silver_accounts_support_cases = """
        CREATE TABLE IF NOT EXISTS silver_accounts_support_cases AS
        SELECT
            a.account_sfid,
            a.account_name,
            a.account_created_date,
            a.account_country,
            a.account_industry,
            c.case_sfid,
            c.case_number,
            c.case_contact_name,
            c.case_product,
            c.case_status,
            c.case_resolution,
            c.case_details,
            c.case_closure,
            c.case_priority,
            c.case_severity,
            c.case_reason,
            c.case_type,
            c.case_category,
            c.case_created_date AS case_created_date,
            c.case_closed_date
        FROM accounts AS a
        JOIN support_cases AS c
            ON a.account_sfid = c.account_sfid;
    """
    cursor.execute(silver_accounts_support_cases)

def insert_accounts(cursor, accounts_data):
    insert_sql = """
        INSERT OR IGNORE INTO accounts (
            account_sfid, 
            account_name, 
            account_created_date, 
            account_country, 
            account_industry
        )
        VALUES (?, ?, ?, ?, ?);
    """
    for acct in accounts_data:
        cursor.execute(insert_sql, (
            acct["account_sfid"],
            acct["account_name"],
            acct["account_created_date"],
            acct["account_country"],
            acct["account_industry"]
        ))

def insert_support_cases(cursor, cases_data):
    insert_sql = """
        INSERT OR IGNORE INTO support_cases (
            case_sfid,
            account_sfid,
            case_number,
            case_contact_name,
            case_product,
            case_status,
            case_resolution,
            case_details,
            case_closure,
            case_priority,
            case_severity,
            case_reason,
            case_type,
            case_category,
            case_created_date,
            case_closed_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for case in cases_data:
        cursor.execute(insert_sql, (
            case["case_sfid"],
            case["account_sfid"],
            case["case_number"],
            case["case_contact_name"],
            case["case_product"],
            case["case_status"],
            case["case_resolution"],
            case["case_details"],
            case["case_closure"],
            case["case_priority"],
            case["case_severity"],
            case["case_reason"],
            case["case_type"],
            case["case_category"],
            case["case_created_date"],
            case["case_closed_date"]
        ))

def derive_metrics(cursor, conn):
    query = "SELECT * FROM silver_accounts_support_cases"
    silver_df = pd.read_sql_query(query, conn)

    silver_df["case_created_date"] = pd.to_datetime(silver_df["case_created_date"], errors='coerce')
    silver_df["case_closed_date"] = pd.to_datetime(silver_df["case_closed_date"], errors='coerce')
    silver_df["resolution_time_days"] = (silver_df["case_closed_date"] - silver_df["case_created_date"]).dt.days

    cases_per_account = silver_df.groupby("account_sfid")["case_sfid"].count().reset_index()
    cases_per_account.columns = ["account_sfid", "num_cases"]

    avg_resolution_time = silver_df.groupby("account_sfid")["resolution_time_days"].mean().reset_index()
    avg_resolution_time.columns = ["account_sfid", "avg_resolution_time_days"]

    priority_counts = silver_df["case_priority"].value_counts().reset_index()
    priority_counts.columns = ["case_priority", "count"]

    pending_cases = silver_df[silver_df["case_status"] == "Pending"].groupby("account_sfid")["case_sfid"].count().reset_index()
    pending_cases.columns = ["account_sfid", "pending_cases"]

    metrics = pd.merge(cases_per_account, avg_resolution_time, on="account_sfid", how="left")
    metrics = pd.merge(metrics, pending_cases, on="account_sfid", how="left")
    metrics["pending_cases"] = metrics["pending_cases"].fillna(0)

    return metrics, priority_counts

def main():
    try:

        db_dir = os.path.dirname(DB_PATH)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir) 

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        create_tables(cursor)

        with open("/app/accounts_anonymized.json", "r", encoding="utf-8") as f:
            accounts_data = json.load(f)
        insert_accounts(cursor, accounts_data)

        with open("/app/support_cases_anonymized.json", "r", encoding="utf-8") as f:
            cases_data = json.load(f)
        insert_support_cases(cursor, cases_data)

        conn.commit()

        create_silver_table(cursor)
        conn.commit()

        metrics, priority_counts = derive_metrics(cursor, conn)
        print("Derived Metrics by Account:")
        print(metrics.head())

        print("Case Priority Counts:")
        print(priority_counts)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
