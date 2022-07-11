from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

S3_BUCKET_NAME = "bootcamp-project-assets"
S3_KEY_NAME = "datasets/monthly-charts.csv"


def ingest_data(postgres_conn_id: str):
    hook = PostgresHook(postgres_conn_id)
    hook.insert_rows(
        table="chart_data",
        rows=[
            [
                "Jan 2000",
                1,
                "Rob Thomas & Santana",
                "Smooth",
                3911.953,
                1.0,
                None,
                44.0,
                None,
                None,
                5.0,
            ]
        ],
    )


with DAG(dag_id="test", start_date=days_ago(1)) as dag:
    start_pipeline = DummyOperator(task_id="start_pipeline")
    end_pipeline = DummyOperator(task_id="end_pipeline")

    # varify_key = S3KeySensor(
    #     task_id="verify_key", bucket_key=S3_KEY_NAME, bucket_name=S3_BUCKET_NAME
    # )

    create_entity = PostgresOperator(
        task_id="create_entity",
        postgres_conn_id="ml_conn",
        sql=f"""
            CREATE TABLE IF NOT EXISTS chart_data (
                month VARCHAR(10),
                position INTEGER,
                artist VARCHAR(100),
                song VARCHAR(100),
                indicativerevenue NUMERIC,
                us INTEGER,
                uk INTEGER,
                de INTEGER,
                fr INTEGER,
                ca INTEGER,
                au INTEGER
            )
        """,
    )

    delete_records = PostgresOperator(
        task_id="delete_records",
        postgres_conn_id="ml_conn",
        sql=f"""
            DELETE FROM chart_data;
        """,
    )

    continue_process = DummyOperator(task_id="continue_process")

    _ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
        op_kwargs={"postgres_conn_id": "ml_conn"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    validate_records = BranchSQLOperator(
        task_id="validate_records",
        conn_id="ml_conn",
        sql="select count(*) from chart_data",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[delete_records.task_id],
    )

    (
        start_pipeline
        >> create_entity
        >> validate_records
        >> [continue_process, delete_records]
        >> _ingest_data
        >> end_pipeline
    )
