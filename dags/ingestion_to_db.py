from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

with DAG("db_ingestion") as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = DummyOperator(task_id="validate")
    prepare = DummyOperator(task_id="prepare")
    load = DummyOperator(task_id="load")
    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> validate >> prepare >> load >> end_workflow
