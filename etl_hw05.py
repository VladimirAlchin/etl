import psycopg2
import pandas as pd
import os

# Параметры соединения
conn_string = "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"  # source
conn_string2 = "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"  # target
os.system("pg_dump -d my_database -h localhost -p 54320 -U root -W postgres -t 'public.customer'")
# Создаем соединение (оно поддерживает контекстный менеджер, рекомендую пользоваться им)
# Создаем курсор - это специальный объект который делает запросы и получает их результаты
# with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
#     # query = 'select * from customer limit 1' # запрос к БД
#     query = "select table_name from information_schema.tables where table_schema='public'"  # запрос к БД
#     cursor.execute(query)  # выполнение запроса
#     result = cursor.fetchall()  # получение результата
# for i in result:
#     a = i[0]
#     with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
#         q = f"COPY {a} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
#         with open(f'{a}.csv', 'w') as f:
#             cursor.copy_expert(q, f)
#     print(f'Выгрузка базы данных {a} на диск закончена')
#     print(pd.read_csv(f'{a}.csv'))
#     with psycopg2.connect(conn_string2) as conn, conn.cursor() as cursor:
#         q = f"COPY {a} from STDIN WITH DELIMITER ',' CSV HEADER;"
#         with open(f'{a}.csv', 'r') as f:
#             cursor.copy_expert(q, f)
#     print(f'Файл {a} загружен в новую базу')