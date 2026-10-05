from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"


# =========================================================
# MYSQL CONNECTION DETAILS
# =========================================================

MYSQL_USER = "root"
MYSQL_PASSWORD = "mysql@123"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "airflow_etl_db"


# =========================================================
# CREATE MYSQL CONNECTION
# =========================================================

connection_url = URL.create(
    drivername="mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE,
)

engine = create_engine(connection_url)


# =========================================================
# INPUT FILES
# =========================================================

CUSTOMERS_FILE = (
    PROCESSED_DATA_DIR / "customers_transformed.csv"
)

ORDERS_FILE = (
    PROCESSED_DATA_DIR / "orders_transformed.csv"
)

ORDER_ITEMS_FILE = (
    PROCESSED_DATA_DIR / "order_items_transformed.csv"
)


# =========================================================
# LOAD CUSTOMERS
# =========================================================

def load_customers(customers_df):

    print("\nLoading customers...")

    sql = text("""
        INSERT INTO customers (
            customer_id,
            customer_name,
            email,
            city
        )
        VALUES (
            :customer_id,
            :customer_name,
            :email,
            :city
        )
        ON DUPLICATE KEY UPDATE
            customer_name = VALUES(customer_name),
            email = VALUES(email),
            city = VALUES(city)
    """)

    records = customers_df[
        [
            "customer_id",
            "customer_name",
            "email",
            "city"
        ]
    ].to_dict(orient="records")

    with engine.begin() as connection:
        connection.execute(sql, records)

    print(
        "Customers loaded:",
        len(records)
    )


# =========================================================
# LOAD ORDERS
# =========================================================

def load_orders(orders_df):

    print("\nLoading orders...")

    sql = text("""
        INSERT INTO orders (
            order_id,
            customer_id,
            order_date,
            order_status,
            total_amount
        )
        VALUES (
            :order_id,
            :customer_id,
            :order_date,
            :order_status,
            :total_amount
        )
        ON DUPLICATE KEY UPDATE
            customer_id = VALUES(customer_id),
            order_date = VALUES(order_date),
            order_status = VALUES(order_status),
            total_amount = VALUES(total_amount)
    """)

    records = orders_df[
        [
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "total_amount"
        ]
    ].to_dict(orient="records")

    with engine.begin() as connection:
        connection.execute(sql, records)

    print(
        "Orders loaded:",
        len(records)
    )


# =========================================================
# LOAD ORDER ITEMS
# =========================================================

def load_order_items(order_items_df):

    print("\nLoading order items...")

    sql = text("""
        INSERT INTO order_items (
            order_item_id,
            order_id,
            product_name,
            quantity,
            unit_price
        )
        VALUES (
            :order_item_id,
            :order_id,
            :product_name,
            :quantity,
            :unit_price
        )
        ON DUPLICATE KEY UPDATE
            order_id = VALUES(order_id),
            product_name = VALUES(product_name),
            quantity = VALUES(quantity),
            unit_price = VALUES(unit_price)
    """)

    records = order_items_df[
        [
            "order_item_id",
            "order_id",
            "product_name",
            "quantity",
            "unit_price"
        ]
    ].to_dict(orient="records")

    with engine.begin() as connection:
        connection.execute(sql, records)

    print(
        "Order items loaded:",
        len(records)
    )


# =========================================================
# VERIFY MYSQL DATA
# =========================================================

def verify_loaded_data():

    print("\nVerifying MySQL data...")

    queries = {
        "customers": "SELECT COUNT(*) FROM customers",
        "orders": "SELECT COUNT(*) FROM orders",
        "order_items": "SELECT COUNT(*) FROM order_items",
    }

    with engine.connect() as connection:

        for table_name, query in queries.items():

            result = connection.execute(
                text(query)
            )

            count = result.scalar()

            print(
                f"{table_name}: {count} records"
            )


# =========================================================
# MAIN LOAD PROCESS
# =========================================================

def load_data():

    print("======================================")
    print("Starting MySQL load...")
    print("======================================")

    # -----------------------------------------------------
    # Check input files
    # -----------------------------------------------------

    required_files = [
        CUSTOMERS_FILE,
        ORDERS_FILE,
        ORDER_ITEMS_FILE
    ]

    for file_path in required_files:

        if not file_path.exists():

            raise FileNotFoundError(
                f"Required file not found: {file_path}"
            )

    # -----------------------------------------------------
    # Read transformed data
    # -----------------------------------------------------

    customers_df = pd.read_csv(
        CUSTOMERS_FILE
    )

    orders_df = pd.read_csv(
        ORDERS_FILE
    )

    order_items_df = pd.read_csv(
        ORDER_ITEMS_FILE
    )

    print("\nTransformed data loaded from CSV files.")

    print(
        "Customers:",
        len(customers_df)
    )

    print(
        "Orders:",
        len(orders_df)
    )

    print(
        "Order items:",
        len(order_items_df)
    )

    # -----------------------------------------------------
    # Load data into MySQL
    # -----------------------------------------------------

    load_customers(
        customers_df
    )

    load_orders(
        orders_df
    )

    load_order_items(
        order_items_df
    )

    # -----------------------------------------------------
    # Verify
    # -----------------------------------------------------

    verify_loaded_data()

    print("\n======================================")
    print("MySQL load completed successfully!")
    print("======================================")


# =========================================================
# PROGRAM ENTRY POINT
# =========================================================

if __name__ == "__main__":
    load_data()