from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    "db_ingestion", start_date=days_ago(1), schedule_interval="@once"
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = DummyOperator(task_id="validate")
    prepare = PostgresOperator(
        task_id="prepare", postgres_conn_id="ml_conn", sql=""
    )
    load = DummyOperator(task_id="load")
    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> validate >> prepare >> load >> end_workflow
