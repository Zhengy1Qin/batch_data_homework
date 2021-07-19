create_stg_inventory_sql = """
CREATE TABLE IF NOT EXISTS stg_inventory (
    productId VARCHAR NOT NULL UNIQUE,
    amount int,
    `date` timestamp
);

truncate stg_products;
"""