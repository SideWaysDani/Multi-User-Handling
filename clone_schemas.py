import psycopg2
import sys
import os

os.environ["PYTHONUNBUFFERED"] = "1"
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)  # Line buffered



DB_CONFIG = {
    "host": "sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com",
    "dbname": "postgres",
    "user": "stpostgres",
    "password": "stocktrader",
    "port": 5432,
}

TEMPLATE_SCHEMA = 'paper_trading_multi_clean'
MASTER_SCHEMA = 'master'
USER_TABLE = 'user'

def log(msg):
    print(msg, flush=True)

def clone_schema(conn, user_id):
    new_schema = f"paper_trading_multi_{user_id}"
    with conn.cursor() as cur:
        # Check if schema already exists
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = %s
        """, (new_schema,))
        exists = cur.fetchone()

        if exists:
            log(f"⚠️ Schema {new_schema} already exists for user {user_id}. Skipping.")
            return

        # Create schema
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {new_schema}')
        log(f"Created schema {new_schema} for user {user_id}")

        # Clone tables from template schema
        cur.execute(f"""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = %s
        """, (TEMPLATE_SCHEMA,))
        tables = cur.fetchall()

        for (table,) in tables:
            # 1. Create table with structure
            cur.execute(f"""
                CREATE TABLE {new_schema}.{table}
                (LIKE {TEMPLATE_SCHEMA}.{table} INCLUDING ALL)
            """)

            log(f"Created table {new_schema}.{table} with structure from {TEMPLATE_SCHEMA}.{table}")

            # 2. Copy data from template
            cur.execute(f"""
                INSERT INTO {new_schema}.{table}
                SELECT * FROM {TEMPLATE_SCHEMA}.{table}
            """)

            log(f"Copied data to {new_schema}.{table} from {TEMPLATE_SCHEMA}.{table}")

        # Update user table
        cur.execute(f"""
            UPDATE {MASTER_SCHEMA}.{USER_TABLE}
            SET schema_name = %s
            WHERE id = %s
        """, (new_schema, user_id))
        log(f"Cloned schema for user {user_id} → {new_schema}")

    conn.commit()

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id FROM {MASTER_SCHEMA}.{USER_TABLE}
            WHERE schema_name IS NULL
        """)
        users = cur.fetchall()

    for (user_id,) in users:
        clone_schema(conn, user_id)

    conn.close()

if __name__ == "__main__":
    main()
