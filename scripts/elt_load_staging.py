import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# CONFIGURATION
# ============================================================

from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")


# ============================================================
# CREATE MYSQL CONNECTION
# ============================================================

connection_url = URL.create(
    drivername="mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE
)

engine = create_engine(connection_url)


# ============================================================
# FILE PATHS
# ============================================================

CUSTOMERS_FILE = "data/raw/customers.csv"
ORDERS_FILE = "data/raw/orders.csv"
ORDER_ITEMS_FILE = "data/raw/order_items.csv"


# ============================================================
# LOAD CSV FILES
# ============================================================

print("=" * 50)
print("STARTING ELT STAGING LOAD")
print("=" * 50)

print("\nReading CSV files...")

customers_df = pd.read_csv(CUSTOMERS_FILE)
orders_df = pd.read_csv(ORDERS_FILE)
order_items_df = pd.read_csv(ORDER_ITEMS_FILE)

print(f"Customers read: {len(customers_df)}")
print(f"Orders read: {len(orders_df)}")
print(f"Order items read: {len(order_items_df)}")


# ============================================================
# LOAD DATA INTO STAGING TABLES
# ============================================================

try:

    with engine.begin() as connection:

        print("\nClearing existing staging data...")

        connection.execute(
            text("TRUNCATE TABLE staging_order_items")
        )

        connection.execute(
            text("TRUNCATE TABLE staging_orders")
        )

        connection.execute(
            text("TRUNCATE TABLE staging_customers")
        )

        print("Staging tables cleared.")

    print("\nLoading customers into staging_customers...")

    customers_df.to_sql(
        name="staging_customers",
        con=engine,
        if_exists="append",
        index=False
    )

    print(
        f"Customers loaded into staging: "
        f"{len(customers_df)}"
    )

    print("\nLoading orders into staging_orders...")

    orders_df.to_sql(
        name="staging_orders",
        con=engine,
        if_exists="append",
        index=False
    )

    print(
        f"Orders loaded into staging: "
        f"{len(orders_df)}"
    )

    print("\nLoading order items into staging_order_items...")

    order_items_df.to_sql(
        name="staging_order_items",
        con=engine,
        if_exists="append",
        index=False
    )

    print(
        f"Order items loaded into staging: "
        f"{len(order_items_df)}"
    )


    # ========================================================
    # VERIFY STAGING DATA
    # ========================================================

    print("\nVerifying staging tables...")

    with engine.connect() as connection:

        customers_count = connection.execute(
            text("SELECT COUNT(*) FROM staging_customers")
        ).scalar()

        orders_count = connection.execute(
            text("SELECT COUNT(*) FROM staging_orders")
        ).scalar()

        order_items_count = connection.execute(
            text("SELECT COUNT(*) FROM staging_order_items")
        ).scalar()

    print(f"staging_customers: {customers_count} records")
    print(f"staging_orders: {orders_count} records")
    print(
        f"staging_order_items: "
        f"{order_items_count} records"
    )

    print("\n" + "=" * 50)
    print("ELT STAGING LOAD COMPLETED SUCCESSFULLY")
    print("=" * 50)


except Exception as error:

    print("\n" + "=" * 50)
    print("ELT STAGING LOAD FAILED")
    print("=" * 50)

    print(f"Error: {error}")

    raise