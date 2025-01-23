import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_NAME = "../my_database.db"

if not os.path.exists(DB_NAME):
    raise FileNotFoundError(f"The database '{DB_NAME}' was not found.")

conn = sqlite3.connect(DB_NAME)

priority_counts = pd.DataFrame({
    "case_priority": ["Normal", "Urgent", "High", "not_priority"],
    "count": [8218, 152, 2, 35]
})

metrics = pd.DataFrame({
    "account_sfid": ["0001", "0002", "0003", "0004", "0005"],
    "avg_resolution_time_days": [4.5, 10.2, 6.7, 12.4, 9.1],
    "pending_cases": [3, 0, 1, 2, 4]
})

cases_df = pd.DataFrame({
    "account_sfid": ["0001", "0002", "0003", "0004", "0005"],
    "case_count": [5, 10, 15, 20, 25]
})

def fetch_priority_distribution():
    query = "SELECT case_priority, COUNT(*) AS count FROM support_cases GROUP BY case_priority"
    return pd.read_sql_query(query, conn)

def plot_priority_distribution(priority_counts):
    plt.figure(figsize=(8, 6))
    sns.barplot(data=priority_counts, x='case_priority', y='count', color='blue')
    plt.title("Distribution of Cases by Priority")
    plt.xlabel("Priority")
    plt.ylabel("Number of Cases")
    plt.show()

def fetch_metrics():
    query = """
        SELECT 
            account_sfid, 
            AVG(julianday(case_closed_date) - julianday(case_created_date)) AS avg_resolution_time_days,
            SUM(CASE WHEN case_status = 'Pending' THEN 1 ELSE 0 END) AS pending_cases
        FROM silver_accounts_support_cases
        GROUP BY account_sfid
    """
    return pd.read_sql_query(query, conn)

def plot_avg_resolution_time(metrics):
    plt.figure(figsize=(10, 6))
    sns.histplot(metrics['avg_resolution_time_days'].dropna(), kde=True, bins=30, color='blue')
    plt.title("Average Resolution Time (in Days)")
    plt.xlabel("Days")
    plt.ylabel("Frequency")
    plt.show()

def plot_pending_cases(metrics):
    print("\n=== Debugging Pending Cases Data ===")
    print(metrics.head()) 
    print(metrics.describe()) 
    
    if metrics.empty:
        print("No data available for plotting pending cases.")
        return
    
    top_accounts = metrics[metrics['pending_cases'] > 0].sort_values(by='pending_cases', ascending=False).head(10)
    
    if top_accounts.empty:
        print("No accounts have pending cases.")
        return
    
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top_accounts, x='account_sfid', y='pending_cases', palette='coolwarm')
    plt.title("Top 10 Accounts with Most Pending Cases")
    plt.xlabel("Account ID")
    plt.ylabel("Pending Cases")
    plt.xticks(rotation=45, ha='right')
    plt.show()

def plot_resolution_time_by_account(metrics):
    metrics_sorted = metrics.sort_values(by='avg_resolution_time_days', ascending=False).head(10)
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=metrics_sorted, x='account_sfid', y='avg_resolution_time_days', marker='o', color='purple')
    plt.title("Average Resolution Time by Account (Top 10)", pad=20)
    plt.xlabel("Account ID")
    plt.ylabel("Average Resolution Time (Days)")
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.subplots_adjust(bottom=0.2)
    plt.show()

def plot_priority_proportion(priority_counts):
    plt.figure(figsize=(8, 8)) 
    plt.pie(
        priority_counts['count'],
        labels=priority_counts['case_priority'],
        autopct='%1.1f%%',
        startangle=140,
        colors=sns.color_palette("viridis", len(priority_counts))
    )
    plt.title("Proportion of Cases by Priority")
    plt.show()

def fetch_cases_per_account():
    query = "SELECT account_sfid, COUNT(*) AS case_count FROM support_cases GROUP BY account_sfid"
    return pd.read_sql_query(query, conn)

def plot_cases_per_account(cases_df):
    plt.figure(figsize=(10, 6))
    sns.histplot(cases_df['case_count'], kde=False, bins=20, color='green')
    plt.title("Distribution of Cases per Account")
    plt.xlabel("Number of Cases")
    plt.ylabel("Frequency")
    plt.show()

def fetch_resolution_time_by_priority():
    query = """
        SELECT case_priority, 
               AVG(julianday(case_closed_date) - julianday(case_created_date)) AS avg_resolution_time_days
        FROM support_cases
        GROUP BY case_priority
    """
    return pd.read_sql_query(query, conn)

def check_nulls(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    nulls = df.isnull().sum()
    return nulls

def count_ids(table_name):
    query = f"SELECT COUNT(*) AS count_ids FROM {table_name}"
    result = pd.read_sql_query(query, conn)
    return result.iloc[0, 0]

def unique_ids(table_name, id_column):
    query = f"SELECT {id_column} FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    unique_count = df[id_column].nunique()
    return unique_count

def cases_per_account():
    query = "SELECT account_sfid, COUNT(*) AS case_count FROM support_cases GROUP BY account_sfid"
    df = pd.read_sql_query(query, conn)
    return df

def get_dtypes(table_name):
    query = f"SELECT * FROM {table_name} LIMIT 1"
    df = pd.read_sql_query(query, conn)
    dtypes = df.dtypes
    return dtypes

def analyze_data_types():
    tables = ["accounts", "support_cases"]
    for table in tables:
        query = f"SELECT * FROM {table} LIMIT 1"
        df = pd.read_sql_query(query, conn)
        dtypes = df.dtypes
        print(f"Tabela: {table}")
        print(dtypes)

def table_size():
    query = """
    SELECT 
        tbl_name AS table_name,
        (page_count * page_size) / (1024 * 1024) AS size_mb
    FROM 
        pragma_page_count(), 
        pragma_page_size(), 
        sqlite_master
    WHERE 
        type='table';
    """
    df = pd.read_sql_query(query, conn)
    return df

def unique_values(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    unique_counts = {column: df[column].nunique() for column in df.columns}
    return unique_counts

def visualize_case_priority(priority_counts):
    plt.figure(figsize=(8, 6))
    sns.barplot(data=priority_counts, x='case_priority', y='count', color='blue')
    plt.title("Case Priority Distribution")
    plt.xlabel("Case Priority")
    plt.ylabel("Number of Cases")
    plt.show()

def visualize_avg_resolution_time(metrics):
    plt.figure(figsize=(10, 6))
    sns.histplot(metrics["avg_resolution_time_days"], kde=True, bins=10, color="blue")
    plt.title("Average Resolution Time (in Days)")
    plt.xlabel("Days")
    plt.ylabel("Frequency")
    plt.show()

def visualize_avg_resolution_time(metrics):
    plt.figure(figsize=(10, 6))
    sns.histplot(metrics["avg_resolution_time_days"], kde=True, bins=10, color="blue")
    plt.title("Average Resolution Time (in Days)")
    plt.xlabel("Days")
    plt.ylabel("Frequency")
    plt.show()

def visualize_case_distribution(cases_df):
    plt.figure(figsize=(10, 6))
    sns.barplot(data=cases_df, x="account_sfid", y="case_count", color="green")
    plt.title("Case Distribution per Account")
    plt.xlabel("Account ID")
    plt.ylabel("Number of Cases")
    plt.xticks(rotation=45)
    plt.show()

def visualize_pending_cases_proportion(metrics):
    pending_cases = metrics[metrics["pending_cases"] > 0]
    
    plt.figure(figsize=(8, 8))
    plt.pie(
        pending_cases["pending_cases"],
        labels=pending_cases["account_sfid"],
        autopct='%1.1f%%',
        startangle=140,
        colors=sns.color_palette("coolwarm", len(pending_cases))
    )
    plt.title("Proportion of Pending Cases by Account")
    plt.show()

def visualize_total_cases_by_account(cases_df):
    sorted_cases = cases_df.sort_values(by="case_count", ascending=False)
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=sorted_cases, x="account_sfid", y="case_count", marker="o", color="orange")
    plt.title("Total Cases by Account")
    plt.xlabel("Account ID")
    plt.ylabel("Number of Cases")
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.show()

def print_insights(priority_counts, metrics, cases_df):
    print("=== Summary Insights ===")
    
    most_common_priority = priority_counts.sort_values(by="count", ascending=False).iloc[0]
    print(f"1. The most common case priority is '{most_common_priority['case_priority']}' with {most_common_priority['count']} cases.")
    
    avg_time = metrics["avg_resolution_time_days"].mean()
    print(f"2. The average resolution time across all accounts is {avg_time:.2f} days.")
    
    total_pending_cases = metrics["pending_cases"].sum()
    print(f"3. There are a total of {total_pending_cases} pending cases across all accounts.")
    
    total_cases = cases_df["case_count"].sum()
    print(f"4. A total of {total_cases} cases are distributed across {len(cases_df)} accounts.")