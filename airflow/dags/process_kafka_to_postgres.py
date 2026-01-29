from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ------------------------
# Default args
# ------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------
# DAG definition
# ------------------------
with DAG(
    dag_id="process_kafka_to_postgres",
    description="Consume Kafka events, clean data and build analytics tables",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # schedule_interval="@hourly",    # run every hour
    # schedule_interval="* * * * *",   # run every min
    schedule_interval="*/5 * * * *",   # run every 5 min
    catchup=False,
    template_searchpath="/opt/airflow/sql",
    tags=["kafka", "postgres", "streaming", "etl"],
) as dag:

    # 1️ Consume Kafka and write to Postgres (raw ingestion)
    consume_kafka = BashOperator(
        task_id="consume_kafka",
        bash_command="python /opt/airflow/consumer/kafka_consumer.py",
    )

    # 2️ Run SQL transformations (raw → clean)
    run_transformations = PostgresOperator(
        task_id="run_transformations",
        postgres_conn_id="postgres_default",
        # sql=open("/opt/airflow/sql/transformations.sql").read(), # test if the file is readable
        sql="transformations.sql",
    )

    # 3️ Refresh analytics views
    refresh_views = PostgresOperator(
        task_id="refresh_views",
        postgres_conn_id="postgres_default",
        sql="views.sql",
    )

    # ------------------------
    # Task dependencies
    # ------------------------
    consume_kafka >> run_transformations >> refresh_views
