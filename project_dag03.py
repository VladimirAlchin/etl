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

DAG_NAME = 'copy_source_to_stage'
DEFAULT_ARGS = {
    "owner": "AlchinVS",
    "start_date": datetime(2021, 6, 14),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

def get_data():
    sql_table = "select table_name from information_schema.tables where table_schema='public'"
    pg_hook = PostgresHook(postgres_conn_id='post_source', schema="my_database")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_table)
    source = cursor.fetchall()
    cursor.close()
    connection.close()
    source = ['Orders','Products','Suppliers','OrderDetails','ProductSuppl']
    for table in source:
        pg_hook = PostgresHook(postgres_conn_id='post_source', schema="my_database")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        # get_sql = f"select * from my_database.public.{str(table[0])}"
        get_sql = f"select * from my_database.public.{str(table)}"
        cursor.execute(get_sql)
        table_data = cursor.fetchall()
        pg_hook2 = PostgresHook(postgres_conn_id='post_target', schema="my_database")
        pg_hook2.insert_rows(table=f'my_database.stage.{str(table)}', rows=table_data, commit_every=10000)
        # pg_hook2.insert_rows(table=f'my_database.stage.{str(table[0])}', rows=table_data, commit_every=10000)
        cursor.close()
        connection.close()

with DAG(
        dag_id=DAG_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval="@once",
        catchup=False) as dag:
    start = DummyOperator(
        task_id='start',
    )
    write_in_stage = PythonOperator(
        task_id='write_in_stage',
        python_callable=get_data

    )

    end = DummyOperator(
        task_id='end',
    )

    start >>  write_in_stage >> end
