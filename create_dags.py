import psycopg2
import os

# --- Config ---
DB_CONFIG = {
    "host": "sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com",
    "dbname": "postgres",
    "user": "stpostgres",
    "password": "stocktrader",
    "port": 5432,
}


TEMPLATE_FILE = "paper_trading_dag.py"

def get_users(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, alpaca_api_key, alpaca_secret_key, schema_name FROM master.user WHERE dag_name IS NULL and schema_name IS NOT NULL")
        return cur.fetchall()

def generate_and_upload_dag(user_id, api_key, secret_key, schema_name):
    dag_name = f"paper_trading_dag_multi_{user_id}"
    output_filename = f"{dag_name}.py"

    return dag_name

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    users = get_users(conn)

    print(f"Found {len(users)} users needing DAGs...")

    for user in users:
        user_id, api_key, secret_key, schema_name = user
        print(f"🔧 Generating DAG for user {user_id}")
        dag_name = generate_and_upload_dag(user_id, api_key, secret_key, schema_name)


    conn.close()
    print(" All DAGs generated and uploaded.")

if __name__ == "__main__":
    main()
