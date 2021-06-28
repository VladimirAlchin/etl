from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.xcom import XCom
from airflow.operators.subdag_operator import SubDagOperator
from airflow.task import task_runner
from airflow.hooks.base_hook import BaseHook
from airflow.models import variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
import csv

DAG_NAME = 'create_core'
DEFAULT_ARGS = {
    "owner": "AlchinVS",
    "start_date": datetime(2021, 6, 14),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
        dag_id=DAG_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval="@once",
        catchup=False) as dag:
    start = DummyOperator(
        task_id='start',
    )

    create_schema_core = PostgresOperator(
        task_id="create_schema_core",
        postgres_conn_id="post_target",
        sql="""CREATE SCHEMA IF NOT EXISTS core""",
    )

    create_table_core = PostgresOperator(
        task_id="create_table_core",
        postgres_conn_id="post_target",
        sql="dss_core.sql"
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> create_schema_core >> create_table_core >> end
