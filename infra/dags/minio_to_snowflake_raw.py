import os
import json
from datetime import datetime, timedelta

import boto3
from botocore.client import Config
import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator


# -----------------------
# Environment variables
# -----------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "real-time-stocks-raw")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "RT_STOCKS_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "REAL_TIME_STOCKS")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "BRONZE")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "STOCK_QUOTES_RAW")


# -----------------------
# Helper: Snowflake connect
# -----------------------
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


# -----------------------
# Task 1: List MinIO objects
# -----------------------
def list_minio_objects(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    response = s3.list_objects_v2(Bucket=MINIO_BUCKET)
    keys = [obj["Key"] for obj in response.get("Contents", [])]

    context["ti"].xcom_push(key="minio_keys", value=keys)
    print(f"Found {len(keys)} objects in MinIO bucket")


# -----------------------
# Task 2: Load new files into Snowflake
# -----------------------
def load_new_files_to_snowflake(**context):
    keys = context["ti"].xcom_pull(task_ids="list_minio_objects", key="minio_keys")
    if not keys:
        print("No files found in MinIO.")
        return

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    conn = get_snowflake_conn()
    cur = conn.cursor()

    # Find already loaded files
    cur.execute(
        f"SELECT SOURCE_KEY FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.MINIO_LOAD_LOG"
    )
    loaded_keys = {row[0] for row in cur.fetchall()}

    new_keys = [k for k in keys if k not in loaded_keys]

    if not new_keys:
        print("No new files to load.")
        cur.close()
        conn.close()
        return

    run_id = context["run_id"]
    inserted = 0

    try:
        for key in new_keys:
            obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
            payload = json.loads(obj["Body"].read().decode("utf-8"))

            cur.execute(
                f"""
                INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
                  (RAW, SOURCE, SOURCE_KEY, LOAD_RUN_ID)
                SELECT
                  PARSE_JSON(%s),
                  'minio',
                  %s,
                  %s
                """,
                (json.dumps(payload), key, run_id),
            )

            cur.execute(
                f"""
                INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.MINIO_LOAD_LOG
                (SOURCE_KEY)
                VALUES (%s)
                """,
                (key,),
            )

            inserted += 1

        print(f"Loaded {inserted} new records into Snowflake.")

    finally:
        cur.close()
        conn.close()


# -----------------------
# DAG definition
# -----------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="minio_to_snowflake_raw",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1,
) as dag:

    list_minio = PythonOperator(
        task_id="list_minio_objects",
        python_callable=list_minio_objects,
    )

    load_snowflake = PythonOperator(
        task_id="load_new_files_to_snowflake",
        python_callable=load_new_files_to_snowflake,
    )

    list_minio >> load_snowflake
