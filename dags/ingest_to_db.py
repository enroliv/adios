"""Database Ingestion Workflow

Author: Enrique Olivares <enrique.olivares@wizeline.com>

Description: Ingests the data from an S3 bucket into a postgres table.
"""

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# General constants
DAG_ID = "database_ingestion_workflow"

# AWS constants
AWS_CONN_ID = "aws_default"
S3_BUCKET_NAME = ""
S3_KEY_NAME = "datasets/iris.csv"

# Postgres constants
POSTGRES_CONN_ID = "postgres_default"
POSTGRES_TABLE_NAME = "iris"


def ingest_data_from_s3(
    s3_bucket: str,
    s3_key: str,
    postgres_table: str,
    aws_conn_id: str = "aws_default",
    postgres_conn_id: str = "postgres_default",
):
    """Ingest data from an S3 location into a postgres table.

    Args:
        s3_bucket (str): Name of the s3 bucket.
        s3_key (str): Name of the s3 key.
        postgres_table (str): Name of the postgres table.
        aws_conn_id (str): Name of the aws connection ID.
        postgres_conn_id (str): Name of the postgres connection ID.
    """
    import tempfile

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)
    with tempfile.NamedTemporaryFile() as tmp:
        s3_hook.download_file(
            key=s3_key, bucket_name=s3_bucket, local_path=tmp.name
        )
        psql_hook.bulk_load(table=postgres_table, tmp_file=tmp.name)


with DAG(dag_id=DAG_ID, schedule_interval="@once") as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    verify_key_existence = S3KeySensor(
        task_id="verify_key_existence",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME,
        bucket_key=S3_KEY_NAME,
    )

    create_table_entity = PostgresOperator(
        task_id="create_table_entity",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_NAME} (
                sepal_length NUMERIC,
                sepal_width NUMERIC,
                petal_length NUMERIC,
                petal_width NUMERIC,
                class varchar(50)
            )
        """,
    )

    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_from_s3,
        op_kwargs={
            "aws_conn_id": AWS_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "s3_bucket": S3_BUCKET_NAME,
            "s3_key": S3_KEY_NAME,
            "postgres_table": POSTGRES_TABLE_NAME,
        },
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    (
        start_workflow
        >> verify_key_existence
        >> create_table_entity
        >> ingest_data
        >> end_workflow
    )

    dag.doc_md = __doc__
