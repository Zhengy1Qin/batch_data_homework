create_stg_inventory_sql = """
drop table IF exists stg_inventory;
CREATE TABLE IF NOT EXISTS stg_inventory (
    productId VARCHAR,
    amount int,
    date timestamp,
    sys_time timestamp
);

truncate stg_products;
"""