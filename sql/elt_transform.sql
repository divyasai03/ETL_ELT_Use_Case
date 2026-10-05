USE airflow_etl_db;

-- ============================================================
-- ELT TRANSFORMATION
-- Transform data from staging tables into final tables
-- ============================================================

START TRANSACTION;

-- ============================================================
-- 1. TRANSFORM CUSTOMERS
-- ============================================================

INSERT INTO customers (
    customer_id,
    customer_name,
    email,
    city
)
SELECT
    TRIM(customer_id),
    TRIM(customer_name),
    LOWER(TRIM(email)),
    TRIM(city)
FROM staging_customers
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    email = VALUES(email),
    city = VALUES(city);


-- ============================================================
-- 2. TRANSFORM ORDERS
-- ============================================================

INSERT INTO orders (
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount
)
SELECT
    order_id,
    TRIM(customer_id),
    order_date,
    UPPER(TRIM(order_status)),
    total_amount
FROM staging_orders
WHERE customer_id IN (
    SELECT customer_id
    FROM staging_customers
)
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    order_date = VALUES(order_date),
    order_status = VALUES(order_status),
    total_amount = VALUES(total_amount);


-- ============================================================
-- 3. TRANSFORM ORDER ITEMS
-- ============================================================

INSERT INTO order_items (
    order_item_id,
    order_id,
    product_name,
    quantity,
    unit_price
)
SELECT
    order_item_id,
    order_id,
    TRIM(product_name),
    quantity,
    unit_price
FROM staging_order_items
WHERE order_id IN (
    SELECT order_id
    FROM staging_orders
)
ON DUPLICATE KEY UPDATE
    order_id = VALUES(order_id),
    product_name = VALUES(product_name),
    quantity = VALUES(quantity),
    unit_price = VALUES(unit_price);


-- ============================================================
-- 4. COMMIT
-- ============================================================

COMMIT;


-- ============================================================
-- 5. VERIFY FINAL TABLES
-- ============================================================

SELECT
    'customers' AS table_name,
    COUNT(*) AS record_count
FROM customers

UNION ALL

SELECT
    'orders',
    COUNT(*)
FROM orders

UNION ALL

SELECT
    'order_items',
    COUNT(*)
FROM order_items;