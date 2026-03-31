from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from datetime import datetime, timedelta
import duckdb
import json
import os
import logging

DUCKDB_PATH = "/usr/local/airflow/duckdb_result/support_calls.duckdb"
TELEPHONY_FOLDER = "/usr/local/airflow/telephony_data/"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="hourly_support_call_enrichment",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1
)

# task 1: detect new calls from MySQL and push to XCom


def detect_new_calls(**context):
    mysql_hook = MySqlHook(mysql_conn_id="mysql_support_db")
    last_watermark = Variable.get(
        "last_loaded_call_time", default_var="1970-01-01 00:00:00")

    query = f"""
        SELECT * FROM calls
        WHERE call_time > '{last_watermark}'
        ORDER BY call_time ASC
    """

    records = mysql_hook.get_records(query)

    if not records:
        logging.info("No new calls found.")
        context['ti'].xcom_push(key='new_calls', value=[])
        context['ti'].xcom_push(key='new_call_ids', value=[])
        context['ti'].xcom_push(key='new_watermark', value=last_watermark)
        return []

    # convert call_time to string for XCom serialization
    serializable_records = [
        (r[0], r[1], str(r[2]), r[3], r[4], r[5])
        for r in records
    ]

    call_ids = [r[0] for r in records]
    max_call_time = str(max(r[2] for r in records))

    context['ti'].xcom_push(key='new_calls', value=serializable_records)
    context['ti'].xcom_push(key='new_call_ids', value=call_ids)
    context['ti'].xcom_push(key='new_watermark', value=max_call_time)

    logging.info(f"Detected {len(call_ids)} new calls.")
    return call_ids

# task 2: load from json and push valid records to XCom


def load_telephony_details(**context):
    call_ids = context['ti'].xcom_pull(
        task_ids='detect_new_calls', key='new_call_ids') or []

    telephony_records = []
    rejected = 0

    for call_id in call_ids:
        file_path = os.path.join(TELEPHONY_FOLDER, f"call_{call_id}.json")

        if not os.path.exists(file_path):
            logging.warning(f"Missing JSON for call_id={call_id}")
            rejected += 1
            continue

        try:
            with open(file_path) as f:
                data = json.load(f)

            # Validate required fields
            if not all(k in data for k in ("call_id", "duration_sec", "short_description")):
                raise ValueError("Missing required fields")

            if data["duration_sec"] < 0:
                raise ValueError("Invalid duration")

            telephony_records.append(data)

        except Exception as e:
            logging.error(f"Invalid JSON for call_id={call_id}: {e}")
            rejected += 1

    logging.info(f"Valid telephony records: {len(telephony_records)}")
    logging.info(f"Rejected telephony records: {rejected}")

    context['ti'].xcom_push(key='telephony_data', value=telephony_records)

# task 3: transform and load into DuckDB


def transform_and_load_duckdb(**context):
    mysql_hook = MySqlHook(mysql_conn_id="mysql_support_db")

    calls = context['ti'].xcom_pull(
        task_ids='detect_new_calls', key='new_calls')
    telephony = context['ti'].xcom_pull(
        task_ids='load_telephony_details', key='telephony_data')
    watermark = context['ti'].xcom_pull(
        task_ids='detect_new_calls', key='new_watermark')

    if not calls:
        logging.info("No calls to process.")
        return

    conn = duckdb.connect(DUCKDB_PATH)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS support_call_enriched (
        call_id INTEGER PRIMARY KEY,
        employee_id INTEGER,
        full_name VARCHAR,
        team VARCHAR,
        call_time TIMESTAMP,
        phone VARCHAR,
        direction VARCHAR,
        status VARCHAR,
        duration_sec INTEGER,
        short_description VARCHAR
    )
    """)

    for call in calls:
        call_id, employee_id, call_time, phone, direction, status = call

        employee = mysql_hook.get_first(
            f"SELECT full_name, team FROM employees WHERE employee_id = {employee_id}"
        )

        telephony_match = next(
            (t for t in telephony if t["call_id"] == call_id), None)

        if not employee or not telephony_match:
            logging.warning(
                f"Skipping call_id={call_id} due to missing join data.")
            continue

        conn.execute("""
            INSERT OR REPLACE INTO support_call_enriched
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            call_id,
            employee_id,
            employee[0],
            employee[1],
            call_time,
            phone,
            direction,
            status,
            telephony_match["duration_sec"],
            telephony_match["short_description"]
        ))

    Variable.set("last_loaded_call_time", watermark)

    logging.info("DuckDB load complete.")
    conn.close()


t1 = PythonOperator(
    task_id="detect_new_calls",
    python_callable=detect_new_calls,
    dag=dag
)

t2 = PythonOperator(
    task_id="load_telephony_details",
    python_callable=load_telephony_details,
    dag=dag
)

t3 = PythonOperator(
    task_id="transform_and_load_duckdb",
    python_callable=transform_and_load_duckdb,
    dag=dag
)

t1 >> t2 >> t3
