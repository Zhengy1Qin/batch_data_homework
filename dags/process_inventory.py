import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

from shared import normalize_csv, load_csv_to_postgres

import process_inventory_sqls as sqls

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

with DAG(
    dag_id="process_inventory",
    start_date=datetime.datetime(2019, 1, 1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:

    check_stg_products_csv_readiness = BashSensor(
        task_id="check_stg_inventory_csv_readiness",
        bash_command="""
            ls /data/raw/inventory_2019-01-01.csv
        """,
    )

    create_stg_products_table = PostgresOperator(
        task_id="create_stg_inventory_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_stg_inventory_sql,
    )

    normalize_products_csv = PythonOperator(
        task_id='normalize_inventory_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/inventory_2019-01-01.csv",
            'target': "/data/stg/inventory_{{ ds }}.csv"
        }
    )

    load_products_to_stg_products_table = PythonOperator(
        task_id='load_inventory_to_stg_inventory_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/inventory_{{ ds }}.csv",
            'table_name': 'stg_inventory',
            'connection_id': connection_id
        },
    )

    create_fact_inventory_table = PostgresOperator(
        task_id="create_fact_inventory_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_fact_inventory_sql,
    )

    transform_fact_inventory_table = PostgresOperator(
        task_id="transform_fact_inventory_table",
        postgres_conn_id=connection_id,
        sql=sqls.transform_fact_inventory_sql,
    )



    check_stg_products_csv_readiness >> normalize_products_csv >> create_stg_products_table >> load_products_to_stg_products_table >> create_fact_inventory_table >> transform_fact_inventory_table