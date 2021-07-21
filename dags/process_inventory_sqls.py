create_stg_inventory_sql = """
drop table IF exists stg_inventory;
CREATE TABLE IF NOT EXISTS stg_inventory (
    product_id VARCHAR,
    amount int,
    date timestamp,
    sys_time timestamp
);
"""

create_fact_inventory_sql = """
drop table IF exists fact_inventory;
CREATE TABLE IF NOT EXISTS fact_inventory (
    product_id VARCHAR NOT NULL,
    product_name VARCHAR,
    amount DECIMAL,
    date timestamp,
    year int,
    month int,
    processed_time timestamp
);
"""

transform_fact_inventory_sql = """
truncate fact_inventory;
INSERT INTO fact_inventory(product_id, product_name,amount, date,month,year, processed_time)
SELECT product_id,
    title,
    amount,
    date,
    EXTRACT(MONTH FROM a.date) AS month,
    EXTRACT(YEAR FROM a.date) AS year,
    '{{ ts }}'
FROM stg_inventory a
INNER JOIN dim_products b on a.product_id = b.id
"""