import boto3
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

S3_BUCKET = "sp-classifier-mwaa-2"
S3_PREFIX = "dags/war_dag/"
TEMPLATE_FILE = "paper_trading_dag.py"

# AWS credentials from env
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

def get_users(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, alpaca_api_key, alpaca_secret_key, schema_name FROM master.user WHERE dag_name IS NULL and schema_name IS NOT NULL")
        return cur.fetchall()

def update_dag_name(conn, user_id, dag_name):
    with conn.cursor() as cur:
        cur.execute("UPDATE master.user SET dag_name = %s WHERE id = %s", (dag_name, user_id))
    conn.commit()

def generate_and_upload_dag(user_id, api_key, secret_key, schema_name):
    dag_name = f"paper_trading_dag_multi_{user_id}"
    output_filename = f"{dag_name}.py"

    with open(TEMPLATE_FILE, "r") as f:
        dag_code = f.read()

    # Replace credentials and schema
    dag_code = dag_code.replace('api_key="PKEXRCBONN22KAM81303"', f'api_key="{api_key}"')
    dag_code = dag_code.replace('secret_key="0wpcbFv9fXQdVTxe8PI5Gop561TU4zvVilIHl14c"', f'secret_key="{secret_key}"')
    dag_code = dag_code.replace('schema_name_global = "paper_trading_test"', f'schema_name_global = "{schema_name}"')
    dag_code = dag_code.replace("dag_id='paper_trading_dag'", f"dag_id='{dag_name}'")

    # Save locally
    with open(output_filename, "w") as f:
        f.write(dag_code)

    # Upload to S3
    s3.upload_file(output_filename, S3_BUCKET, f"{S3_PREFIX}{output_filename}")
    print(f"✅ Uploaded {output_filename} to s3://{S3_BUCKET}/{S3_PREFIX}{output_filename}")

    return dag_name

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    users = get_users(conn)

    print(f"Found {len(users)} users needing DAGs...")

    for user in users:
        user_id, api_key, secret_key, schema_name = user
        print(f"🔧 Generating DAG for user {user_id}")
        dag_name = generate_and_upload_dag(user_id, api_key, secret_key, schema_name)
        update_dag_name(conn, user_id, dag_name)
        print(f"✅ DAG created and DB updated for user {user_id}: {dag_name}")

    conn.close()
    print("🎉 All DAGs generated and uploaded.")

if __name__ == "__main__":
    main()
