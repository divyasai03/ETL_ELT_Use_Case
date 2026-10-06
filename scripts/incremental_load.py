from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db_config import get_engine


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"


# =========================================================
# MYSQL CONNECTION
# =========================================================


# =========================================================
# CREATE MYSQL CONNECTION
# =========================================================

engine = get_engine()


# =========================================================
# FILE
# =========================================================

ORDERS_FILE = (
    PROCESSED_DATA_DIR / "orders_transformed.csv"
)


# =========================================================
# PIPELINE NAME
# =========================================================

PIPELINE_NAME = "ecommerce_etl_pipeline"


# =========================================================
# GET LAST PROCESSED ORDER ID
# =========================================================

def get_last_processed_order_id():

    query = text("""
        SELECT last_processed_order_id
        FROM etl_control
        WHERE pipeline_name = :pipeline_name
    """)

    with engine.connect() as connection:

        result = connection.execute(
            query,
            {
                "pipeline_name": PIPELINE_NAME
            }
        )

        row = result.fetchone()

        if row is None:
            return 0

        return row[0] or 0


# =========================================================
# UPDATE CONTROL TABLE
# =========================================================

def update_control_table(
    last_order_id,
    status
):

    query = text("""
        UPDATE etl_control
        SET
            last_processed_order_id = :last_order_id,
            last_run_time = NOW(),
            last_status = :status
        WHERE pipeline_name = :pipeline_name
    """)

    with engine.begin() as connection:

        connection.execute(
            query,
            {
                "last_order_id": last_order_id,
                "status": status,
                "pipeline_name": PIPELINE_NAME
            }
        )


# =========================================================
# LOAD INCREMENTAL ORDERS
# =========================================================

def load_incremental_orders(
    orders_df
):

    last_processed_id = (
        get_last_processed_order_id()
    )

    print(
        "\nLast processed order ID:",
        last_processed_id
    )

    # -----------------------------------------------------
    # Select only new records
    # -----------------------------------------------------

    new_orders_df = orders_df[
        orders_df["order_id"] > last_processed_id
    ].copy()

    print(
        "Total records in source:",
        len(orders_df)
    )

    print(
        "New records to process:",
        len(new_orders_df)
    )

    # -----------------------------------------------------
    # No new records
    # -----------------------------------------------------

    if new_orders_df.empty:

        print(
            "\nNo new orders found."
        )

        update_control_table(
            last_processed_id,
            "SUCCESS_NO_NEW_DATA"
        )

        return 0

    # -----------------------------------------------------
    # Load new orders
    # -----------------------------------------------------

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

    records = new_orders_df[
        [
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "total_amount"
        ]
    ].to_dict(
        orient="records"
    )

    with engine.begin() as connection:

        connection.execute(
            sql,
            records
        )

    # -----------------------------------------------------
    # Find latest order ID
    # -----------------------------------------------------

    latest_order_id = int(
        new_orders_df["order_id"].max()
    )

    # -----------------------------------------------------
    # Update control table
    # -----------------------------------------------------

    update_control_table(
        latest_order_id,
        "SUCCESS"
    )

    print(
        "\nIncremental load completed."
    )

    print(
        "Records loaded:",
        len(records)
    )

    print(
        "New last processed order ID:",
        latest_order_id
    )

    return len(records)


# =========================================================
# MAIN
# =========================================================

def main():

    print("======================================")
    print("Starting incremental load...")
    print("======================================")

    # -----------------------------------------------------
    # Check source file
    # -----------------------------------------------------

    if not ORDERS_FILE.exists():

        raise FileNotFoundError(
            f"File not found: {ORDERS_FILE}"
        )

    # -----------------------------------------------------
    # Read transformed orders
    # -----------------------------------------------------

    orders_df = pd.read_csv(
        ORDERS_FILE
    )

    # -----------------------------------------------------
    # Run incremental load
    # -----------------------------------------------------

    load_incremental_orders(
        orders_df
    )

    print("\n======================================")
    print("Incremental process finished.")
    print("======================================")


# =========================================================
# PROGRAM ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()

