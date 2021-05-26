import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

default_args = {"owner": "airflow"}
connection_id = 'dwh'

create_stg_products_sql = """
CREATE TABLE IF NOT EXISTS stg_products (
    id VARCHAR NOT NULL UNIQUE,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time timestamp
);

truncate stg_products;
"""

create_dim_products_sql = """
CREATE TABLE IF NOT EXISTS dim_products (
    id VARCHAR NOT NULL,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time timestamp,
    start_time timestamp,
    end_time timestamp,
UNIQUE(id, start_time)
);
"""

transform_dim_products_sql = """
UPDATE dim_products
    SET end_time = '{{ ts }}'
FROM stg_products
WHERE 
    stg_products.id = dim_products.id
    AND '{{ ts }}' >= dim_products.start_time AND '{{ ts }}' < dim_products.end_time
    AND (dim_products.title <> stg_products.title OR dim_products.category <> stg_products.category OR dim_products.price <> stg_products.price);

WITH sc as (
    SELECT * FROM dim_products
    WHERE '{{ ts }}' >= dim_products.start_time and '{{ ts }}' < dim_products.end_time
)
INSERT INTO dim_products(id, title, category, price, processed_time, start_time, end_time)
SELECT stg_products.id as id,
    stg_products.title,
    stg_products.category,
    stg_products.price,
    '{{ ts }}' AS processed_time,
    '{{ ts }}' AS start_time,
    '9999-12-31 23:59:59' AS end_time
FROM stg_products
WHERE stg_products.id NOT IN (select id from sc);
"""

create_stg_orders_sql = """
CREATE TABLE IF NOT EXISTS stg_orders (
    id VARCHAR NOT NULL,
    productId VARCHAR,
    amount DECIMAL,
    totalPrice DECIMAL,
    status VARCHAR,
    eventTime timestamp,
    processed_time timestamp
);

truncate stg_orders;
"""

create_dim_orders_sql = """
CREATE TABLE IF NOT EXISTS dim_orders (
    order_id VARCHAR NOT NULL,
    status VARCHAR,
    eventTime timestamp,
    processed_time timestamp,
    start_time timestamp,
    end_time timestamp,
    UNIQUE(order_id, start_time)
);
"""

create_orders_sql = """
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR NOT NULL,
    product_id VARCHAR,
    amount DECIMAL,
    totalPrice DECIMAL,
    processed_time timestamp,
    UNIQUE(order_id)
);
"""

transform_dim_orders_sql = """
WITH stg_orders_with_row_number AS (
     SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY id ORDER BY eventtime) AS rn
      FROM stg_orders
), earliest_orders AS (
    SELECT * FROM stg_orders_with_row_number WHERE rn = 1
)
UPDATE dim_orders
SET end_time = '{{ ts }}'
FROM earliest_orders
WHERE earliest_orders.id = dim_orders.order_id
AND '{{ ts }}' >= dim_orders.start_time AND '{{ ts }}' < dim_orders.end_time
AND (earliest_orders.status <> dim_orders.status);

WITH ordered_stg_orders as (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id,status ORDER BY eventtime) rn,
    LAST_VALUE(eventtime) OVER(PARTITION BY id,status ORDER BY eventtime) last_eventtime
    FROM stg_orders
    order by id, eventTime
), distinct_stg_orders as (
    select id, status, eventTime, 
    eventTime as startTime,
    last_eventtime as endTime,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY eventtime) rn
    from ordered_stg_orders where ordered_stg_orders.rn = 1
) ,new_records as (select 
    current_orders.id,
    current_orders.status,
    current_orders.eventTime,
    current_orders.eventTime as startTime,
    coalesce(next_orders.eventTime, '9999-12-31 23:59:59') as endTime
from distinct_stg_orders current_orders left join distinct_stg_orders next_orders
on current_orders.id = next_orders.id and current_orders.rn = next_orders.rn -  1
) 
INSERT INTO dim_orders(order_id, status, eventTime, processed_time, start_time, end_time)
SELECT id AS order_id,
    status,
    eventTime,
    '{{ ts }}',
    startTime,
    endTime
FROM new_records
"""

def normalize_csv(ts, **kwargs):
    import csv
    source_filename = kwargs['source']
    target_filename = kwargs['target']
    header_skipped = False
    with open(source_filename, newline='') as source_file:
        with open(target_filename, "w", newline='') as target_file:
            reader = csv.reader(source_file, delimiter=',')
            writer = csv.writer(target_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if not header_skipped:
                    header_skipped = True
                    continue
                row.append(ts)
                writer.writerow(row)
    return target_filename

def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.bulk_load(table_name, csv_filepath)
    return table_name

with DAG(
    dag_id="process_orders",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    check_stg_products_csv_readiness = BashSensor(
        task_id="check_stg_products_csv_readiness",
        bash_command="""
            ls /data/raw/products_{{ ds }}.csv
        """,
    )
    create_stg_products_table = PostgresOperator(
        task_id="create_stg_products_table",
        postgres_conn_id=connection_id,
        sql=create_stg_products_sql,
    )

    normalize_products_csv = PythonOperator(
        task_id='normalize_products_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/products_{{ ds }}.csv",
            'target': "/data/stg/products_{{ ds }}.csv"
        }
    )

    load_products_to_stg_products_table = PythonOperator(
        task_id='load_products_to_stg_products_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/products_{{ ds }}.csv",
            'table_name': 'stg_products'
        },
    )

    check_stg_products_csv_readiness >> normalize_products_csv >> create_stg_products_table >> load_products_to_stg_products_table

    create_dim_products_table = PostgresOperator(
        task_id="create_dim_products_table",
        postgres_conn_id=connection_id,
        sql=create_dim_products_sql,
    )

    transform_dim_products_table = PostgresOperator(
        task_id="load_dim_products_table",
        postgres_conn_id=connection_id,
        sql=transform_dim_products_sql,
    )

    load_products_to_stg_products_table >> create_dim_products_table >> transform_dim_products_table

    # orders
    check_stg_orders_csv_readiness = BashSensor(
        task_id="check_stg_orders_csv_readiness",
        bash_command="""
            ls /data/raw/orders_{{ ds }}.csv
        """,
    )

    normalize_orders_csv = PythonOperator(
        task_id='normalize_orders_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/orders_{{ ds }}.csv",
            'target': "/data/stg/orders_{{ ds }}.csv"
        }
    )

    create_stg_orders_table = PostgresOperator(
        task_id="create_stg_orders_table",
        postgres_conn_id=connection_id,
        sql=create_stg_orders_sql,
    )

    load_orders_to_stg_orders_table = PythonOperator(
        task_id='load_orders_to_stg_orders_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/orders_{{ ds }}.csv",
            'table_name': 'stg_orders'
        },
    )

    create_dim_orders_table = PostgresOperator(
        task_id="create_dim_orders_table",
        postgres_conn_id=connection_id,
        sql=create_dim_orders_sql,
    )

    create_orders_table = PostgresOperator(
        task_id="create_orders_table",
        postgres_conn_id=connection_id,
        sql=create_orders_sql,
    )

    transform_dim_orders_table = PostgresOperator(
        task_id="transform_dim_orders_table",
        postgres_conn_id=connection_id,
        sql=transform_dim_orders_sql,
    )

    check_stg_orders_csv_readiness >> normalize_orders_csv >> create_stg_orders_table >> load_orders_to_stg_orders_table >> [create_dim_orders_table, create_orders_table]
    create_dim_orders_table >> transform_dim_orders_table
