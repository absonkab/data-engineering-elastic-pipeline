from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

# -------------------------------
# Default arguments for the DAG
# -------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,   # don't wait for previous DAG runs
    "retries": 1,               # retry once if failed
}

# -------------------------------
# DAG definition
# -------------------------------
dag = DAG(
    dag_id="index_hourly_metrics_to_elasticsearch",
    description="Read hourly_metrics from Postgres and index into Elasticsearch",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # schedule_interval="@hourly",  # every hour
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,                # do not backfill
    tags=["postgres", "elasticsearch", "analytics"],
)

# -------------------------------
# Function to fetch data from Postgres
# -------------------------------
def fetch_hourly_metrics():
    """
    Connect to Postgres and fetch all records from hourly_metrics view
    Returns a list of tuples
    """
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    records = pg_hook.get_records("SELECT * FROM hourly_metrics;")
    print(f"Fetched {len(records)} records from Postgres")
    return records

# -------------------------------
# Function to index data into Elasticsearch
# -------------------------------
def index_to_elasticsearch(**kwargs):
    """
    Reads hourly metrics from Postgres (via XCom) and indexes into Elasticsearch
    """
    # Fetch data pushed via XCom
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='fetch_hourly_metrics')

    # Initialize Elasticsearch client
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])  # Docker service

    # Prepare bulk actions for Elasticsearch
    actions = [
        {
            "_index": "hourly_metrics",
            "_id": r[0].isoformat(),  # use hour as unique ID
            "_source": {
                "@timestamp": datetime.utcnow().isoformat(),
                "hour": r[0].isoformat(),
                "transaction_count": r[1],
                "total_amount": float(r[2]),
                "avg_amount": float(r[3]),
            },
        }
        for r in records
    ]

    # Bulk index into ES
    if actions:
        helpers.bulk(es, actions)
        print(f"Indexed {len(actions)} hourly metrics into Elasticsearch")
    else:
        print("No records to index")

# -------------------------------
# Tasks
# -------------------------------
fetch_task = PythonOperator(
    task_id='fetch_hourly_metrics',
    python_callable=fetch_hourly_metrics,
    dag=dag,
)

index_task = PythonOperator(
    task_id='index_to_elasticsearch',
    python_callable=index_to_elasticsearch,
    provide_context=True,  # allows access to XCom
    dag=dag,
)

# -------------------------------
# Task dependencies
# -------------------------------
fetch_task >> index_task
