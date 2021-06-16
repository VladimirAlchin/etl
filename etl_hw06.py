from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

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

DAG_NAME = 'testim_dagi'
DEFAULT_ARGS = {
    "owner": "AlchinVS",
    "start_date": datetime(2021, 6, 14),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


def subdag(parent_dag_name, child_dag_name, args, ll):
    print(type(ll))

    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        # start_date=days_ago(2),
        schedule_interval="@daily",
    )

    for i in range(5):
        DummyOperator(
            task_id=f'{child_dag_name}-task-{i + 1}',
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


def get_data(table_name: str):
    # sql_table = "select table_name from information_schema.tables where table_schema='public'"
    # pg_hook = PostgresHook(postgres_conn_id='postgres_target', schema="my_database")
    # connection = pg_hook.get_conn()
    # cursor = connection.cursor()
    # cursor.execute(sql_table)
    # source = cursor.fetchall()
    # cursor.close()
    # connection.close()
    # for table in list_table:
    pg_hook = PostgresHook(postgres_conn_id='postgres_target', schema="my_database")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    get_sql = f"select * from my_database.public.{table_name}"
    print(get_sql)
    cursor.execute(get_sql)
    table_data = cursor.fetchall()
    pg_hook2 = PostgresHook(postgres_conn_id='postgres_source', schema="my_database")
    pg_hook2.insert_rows(table=f'my_database.public.{table_name}', rows=table_data, commit_every=50000)
    cursor.close()
    connection.close()


# TODO получаем список таблиц и передаем в xcom
def get_table():
    sql_table = "select table_name from information_schema.tables where table_schema='public'"
    pg_hook = PostgresHook(postgres_conn_id='postgres_target', schema="my_database")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_table)
    source = cursor.fetchall()
    list_table = list()
    cursor.close()
    connection.close()
    for i in source:
        list_table.append(i[0])
    return list_table


# TODO добавить проверку наличия таблиц и пересоздание из ddl файла
def check_table():
    return ('check_table')


with DAG(
        # dag_id="test_customer2",
        dag_id=DAG_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval="@once",
        catchup=False) as dag:
    start = DummyOperator(
        task_id='start',
    )
    get_table_task = PythonOperator(
        task_id='get_table_list',
        python_callable=get_table,
        dag=dag,
    )

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag(DAG_NAME, 'section-1', DEFAULT_ARGS, XCom.task_id('get_table_task')),
    )

    insert_my_data = PythonOperator(
        task_id='hook_test',
        python_callable=get_data
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> get_table_task >> section_1 >> end
