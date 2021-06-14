from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
import csv

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 6, 14),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


def get_data():
    # sql = "select r_regionkey, r_name, r_comment from my_database.public.region"
    sql_table = "select table_name from information_schema.tables where table_schema='public'"
    pg_hook = PostgresHook(postgres_conn_id='postgres_target', schema="my_database")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_table)
    source = cursor.fetchall()
    cursor.close()
    connection.close()
    for table in source:
        pg_hook = PostgresHook(postgres_conn_id='postgres_target', schema="my_database")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        get_sql = f"select * from my_database.public.{str(table[0])}"
        print(get_sql)
        cursor.execute(get_sql)
        table_data = cursor.fetchall()
        pg_hook2 = PostgresHook(postgres_conn_id='postgres_source', schema="my_database")
        pg_hook2.insert_rows(table=f'my_database.public.{str(table[0])}', rows=table_data, commit_every=10000)
        cursor.close()
        connection.close()




    # with open('test.csv', 'w', encoding='utf-8') as f:
    #     for table in source:
    #         get_sql = f"select r_regionkey, r_name, r_comment from my_database.public.{str(table[0])}"
    #         f.write(get_sql + '\n')



    # pg_hook.insert_rows(table='my_database.public.region2', rows=source, commit_every=0)
    # return source


with DAG(
        dag_id="test_customer2",
        default_args=DEFAULT_ARGS,
        schedule_interval="@once",
        catchup=False) as dag:
    insert_my_data = PythonOperator(
        task_id='hook_test',
        python_callable=get_data
        # postgres_conn_id='postgres_target',
        # sql="select r_regionkey, r_name, r_comment from my_database.public.region"
    )

    # src = PostgresHook(postgres_conn_id='postgres_target')
    # src_conn = src.get_conn()
    # cursor = src_conn.cursor()
    # cursor.execute("select r_regionkey, r_name, r_comment from my_database.public.region")
