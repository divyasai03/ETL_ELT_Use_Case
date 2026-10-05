import os
import uuid
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# =========================================================
# CONFIGURATION
# =========================================================

MYSQL_USER = "root"
MYSQL_PASSWORD = "mysql@123"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "airflow_etl_db"

PIPELINE_NAME = "ecommerce_etl_pipeline"

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

RAW_DIR = os.path.join(
    BASE_DIR,
    "data",
    "raw"
)

PROCESSED_DIR = os.path.join(
    BASE_DIR,
    "data",
    "processed"
)

ERROR_DIR = os.path.join(
    BASE_DIR,
    "data",
    "error"
)


# =========================================================
# MYSQL CONNECTION
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
# CREATE RUN ID
# =========================================================

def create_run_id():

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    unique_id = uuid.uuid4().hex[:8]

    return (
        f"{PIPELINE_NAME}_"
        f"{timestamp}_"
        f"{unique_id}"
    )


# =========================================================
# AUDIT LOGGING
# =========================================================

def insert_audit_log(
    run_id,
    start_time,
    end_time,
    records_read,
    records_valid,
    records_rejected,
    records_loaded,
    status,
    error_message=None
):

    query = text("""
        INSERT INTO etl_audit_log (
            pipeline_name,
            run_id,
            start_time,
            end_time,
            records_read,
            records_valid,
            records_rejected,
            records_loaded,
            status,
            error_message
        )
        VALUES (
            :pipeline_name,
            :run_id,
            :start_time,
            :end_time,
            :records_read,
            :records_valid,
            :records_rejected,
            :records_loaded,
            :status,
            :error_message
        )
    """)

    with engine.begin() as connection:

        connection.execute(
            query,
            {
                "pipeline_name": PIPELINE_NAME,
                "run_id": run_id,
                "start_time": start_time,
                "end_time": end_time,
                "records_read": records_read,
                "records_valid": records_valid,
                "records_rejected": records_rejected,
                "records_loaded": records_loaded,
                "status": status,
                "error_message": error_message
            }
        )


# =========================================================
# ERROR LOGGING
# =========================================================

def insert_error_log(
    run_id,
    record_id,
    error_type,
    error_message
):

    query = text("""
        INSERT INTO etl_error_log (
            pipeline_name,
            run_id,
            record_id,
            error_type,
            error_message
        )
        VALUES (
            :pipeline_name,
            :run_id,
            :record_id,
            :error_type,
            :error_message
        )
    """)

    with engine.begin() as connection:

        connection.execute(
            query,
            {
                "pipeline_name": PIPELINE_NAME,
                "run_id": run_id,
                "record_id": str(record_id),
                "error_type": error_type,
                "error_message": error_message
            }
        )


# =========================================================
# STEP 1 — EXTRACT
# =========================================================

def extract_data():

    print("\nSTEP 1: EXTRACT")
    print("--------------------------------------")

    customers_file = os.path.join(
        RAW_DIR,
        "customers.csv"
    )

    orders_file = os.path.join(
        RAW_DIR,
        "orders.csv"
    )

    order_items_file = os.path.join(
        RAW_DIR,
        "order_items.csv"
    )

    customers_df = pd.read_csv(
        customers_file
    )

    orders_df = pd.read_csv(
        orders_file
    )

    order_items_df = pd.read_csv(
        order_items_file
    )

    print(
        f"Customers extracted: "
        f"{len(customers_df)}"
    )

    print(
        f"Orders extracted: "
        f"{len(orders_df)}"
    )

    print(
        f"Order items extracted: "
        f"{len(order_items_df)}"
    )

    return (
        customers_df,
        orders_df,
        order_items_df
    )


# =========================================================
# STEP 2 — VALIDATION
# =========================================================

def validate_data(
    customers_df,
    orders_df,
    order_items_df,
    run_id
):

    print("\nSTEP 2: VALIDATION")
    print("--------------------------------------")

    validation_errors = []

    # -----------------------------------------------------
    # CUSTOMER REQUIRED COLUMNS
    # -----------------------------------------------------

    required_customer_columns = [
        "customer_id",
        "customer_name",
        "email",
        "city"
    ]

    for column in required_customer_columns:

        if column not in customers_df.columns:

            validation_errors.append(
                (
                    "customers",
                    "MISSING_COLUMN",
                    f"Missing customer column: {column}"
                )
            )

    # -----------------------------------------------------
    # CUSTOMER NULL CHECK
    # -----------------------------------------------------

    if "customer_id" in customers_df.columns:

        if customers_df["customer_id"].isna().any():

            validation_errors.append(
                (
                    "customers",
                    "NULL_CUSTOMER_ID",
                    "Customer ID contains NULL values"
                )
            )

    # -----------------------------------------------------
    # CUSTOMER DUPLICATE CHECK
    # -----------------------------------------------------

    if "customer_id" in customers_df.columns:

        duplicate_customers = (
            customers_df["customer_id"]
            .duplicated()
        )

        if duplicate_customers.any():

            duplicate_ids = (
                customers_df.loc[
                    duplicate_customers,
                    "customer_id"
                ]
                .astype(str)
                .tolist()
            )

            for duplicate_id in duplicate_ids:

                validation_errors.append(
                    (
                        duplicate_id,
                        "DUPLICATE_CUSTOMER_ID",
                        f"Duplicate customer ID: {duplicate_id}"
                    )
                )

    # -----------------------------------------------------
    # ORDER REQUIRED COLUMNS
    # -----------------------------------------------------

    required_order_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "total_amount"
    ]

    for column in required_order_columns:

        if column not in orders_df.columns:

            validation_errors.append(
                (
                    "orders",
                    "MISSING_COLUMN",
                    f"Missing order column: {column}"
                )
            )

    # -----------------------------------------------------
    # ORDER NULL CHECK
    # -----------------------------------------------------

    if "order_id" in orders_df.columns:

        if orders_df["order_id"].isna().any():

            validation_errors.append(
                (
                    "orders",
                    "NULL_ORDER_ID",
                    "Order ID contains NULL values"
                )
            )

    # -----------------------------------------------------
    # ORDER DUPLICATE CHECK
    # -----------------------------------------------------

    if "order_id" in orders_df.columns:

        duplicate_orders = (
            orders_df["order_id"]
            .duplicated()
        )

        if duplicate_orders.any():

            duplicate_ids = (
                orders_df.loc[
                    duplicate_orders,
                    "order_id"
                ]
                .astype(str)
                .tolist()
            )

            for duplicate_id in duplicate_ids:

                validation_errors.append(
                    (
                        duplicate_id,
                        "DUPLICATE_ORDER_ID",
                        f"Duplicate order ID: {duplicate_id}"
                    )
                )

    # -----------------------------------------------------
    # CUSTOMER REFERENCE CHECK
    # -----------------------------------------------------

    if (
        "customer_id" in customers_df.columns
        and "customer_id" in orders_df.columns
        and "order_id" in orders_df.columns
    ):

        valid_customer_ids = set(
            customers_df["customer_id"]
        )

        invalid_customer_orders = orders_df[
            ~orders_df["customer_id"].isin(
                valid_customer_ids
            )
        ]

        for _, row in invalid_customer_orders.iterrows():

            validation_errors.append(
                (
                    str(row["order_id"]),
                    "INVALID_CUSTOMER_REFERENCE",
                    (
                        f"Customer ID "
                        f"{row['customer_id']} "
                        f"does not exist"
                    )
                )
            )

    # -----------------------------------------------------
    # ORDER DATE CHECK
    # -----------------------------------------------------

    if "order_date" in orders_df.columns:

        invalid_dates = pd.to_datetime(
            orders_df["order_date"],
            errors="coerce"
        ).isna()

        invalid_date_rows = orders_df[
            invalid_dates
        ]

        for _, row in invalid_date_rows.iterrows():

            validation_errors.append(
                (
                    str(row["order_id"]),
                    "INVALID_ORDER_DATE",
                    (
                        f"Invalid order date: "
                        f"{row['order_date']}"
                    )
                )
            )

    # -----------------------------------------------------
    # TOTAL AMOUNT CHECK
    # -----------------------------------------------------

    if "total_amount" in orders_df.columns:

        numeric_amount = pd.to_numeric(
            orders_df["total_amount"],
            errors="coerce"
        )

        invalid_amount_rows = orders_df[
            numeric_amount.isna()
        ]

        for _, row in invalid_amount_rows.iterrows():

            validation_errors.append(
                (
                    str(row["order_id"]),
                    "INVALID_TOTAL_AMOUNT",
                    (
                        f"Invalid total amount: "
                        f"{row['total_amount']}"
                    )
                )
            )

        negative_amount_rows = orders_df[
            numeric_amount < 0
        ]

        for _, row in negative_amount_rows.iterrows():

            validation_errors.append(
                (
                    str(row["order_id"]),
                    "NEGATIVE_TOTAL_AMOUNT",
                    (
                        f"Negative total amount: "
                        f"{row['total_amount']}"
                    )
                )
            )

    # -----------------------------------------------------
    # ORDER ITEM REQUIRED COLUMNS
    # -----------------------------------------------------

    required_item_columns = [
        "order_item_id",
        "order_id",
        "product_name",
        "quantity",
        "unit_price"
    ]

    for column in required_item_columns:

        if column not in order_items_df.columns:

            validation_errors.append(
                (
                    "order_items",
                    "MISSING_COLUMN",
                    f"Missing order item column: {column}"
                )
            )

    # -----------------------------------------------------
    # ORDER ITEM NULL CHECK
    # -----------------------------------------------------

    if "order_item_id" in order_items_df.columns:

        if order_items_df["order_item_id"].isna().any():

            validation_errors.append(
                (
                    "order_items",
                    "NULL_ORDER_ITEM_ID",
                    "Order item ID contains NULL values"
                )
            )

    # -----------------------------------------------------
    # ORDER ITEM DUPLICATE CHECK
    # -----------------------------------------------------

    if "order_item_id" in order_items_df.columns:

        duplicate_items = (
            order_items_df["order_item_id"]
            .duplicated()
        )

        if duplicate_items.any():

            duplicate_ids = (
                order_items_df.loc[
                    duplicate_items,
                    "order_item_id"
                ]
                .astype(str)
                .tolist()
            )

            for duplicate_id in duplicate_ids:

                validation_errors.append(
                    (
                        duplicate_id,
                        "DUPLICATE_ORDER_ITEM_ID",
                        (
                            f"Duplicate order item ID: "
                            f"{duplicate_id}"
                        )
                    )
                )

    # -----------------------------------------------------
    # ORDER ITEM → ORDER REFERENCE
    # -----------------------------------------------------

    if (
        "order_id" in orders_df.columns
        and "order_id" in order_items_df.columns
        and "order_item_id" in order_items_df.columns
    ):

        valid_order_ids = set(
            orders_df["order_id"]
        )

        invalid_item_orders = order_items_df[
            ~order_items_df["order_id"].isin(
                valid_order_ids
            )
        ]

        for _, row in invalid_item_orders.iterrows():

            validation_errors.append(
                (
                    str(row["order_item_id"]),
                    "INVALID_ORDER_REFERENCE",
                    (
                        f"Order ID "
                        f"{row['order_id']} "
                        f"does not exist"
                    )
                )
            )

    # -----------------------------------------------------
    # QUANTITY CHECK
    # -----------------------------------------------------

    if "quantity" in order_items_df.columns:

        numeric_quantity = pd.to_numeric(
            order_items_df["quantity"],
            errors="coerce"
        )

        invalid_quantity_rows = order_items_df[
            numeric_quantity.isna()
        ]

        for _, row in invalid_quantity_rows.iterrows():

            validation_errors.append(
                (
                    str(row["order_item_id"]),
                    "INVALID_QUANTITY",
                    (
                        f"Invalid quantity: "
                        f"{row['quantity']}"
                    )
                )
            )

        invalid_quantity_rows = order_items_df[
            numeric_quantity <= 0
        ]

        for _, row in invalid_quantity_rows.iterrows():

            validation_errors.append(
                (
                    str(row["order_item_id"]),
                    "INVALID_QUANTITY",
                    (
                        f"Quantity must be greater "
                        f"than zero: {row['quantity']}"
                    )
                )
            )

    # -----------------------------------------------------
    # UNIT PRICE CHECK
    # -----------------------------------------------------

    if "unit_price" in order_items_df.columns:

        numeric_price = pd.to_numeric(
            order_items_df["unit_price"],
            errors="coerce"
        )

        invalid_price_rows = order_items_df[
            numeric_price.isna()
        ]

        for _, row in invalid_price_rows.iterrows():

            validation_errors.append(
                (
                    str(row["order_item_id"]),
                    "INVALID_UNIT_PRICE",
                    (
                        f"Invalid unit price: "
                        f"{row['unit_price']}"
                    )
                )
            )

        negative_price_rows = order_items_df[
            numeric_price < 0
        ]

        for _, row in negative_price_rows.iterrows():

            validation_errors.append(
                (
                    str(row["order_item_id"]),
                    "NEGATIVE_UNIT_PRICE",
                    (
                        f"Unit price cannot be "
                        f"negative: {row['unit_price']}"
                    )
                )
            )

    # -----------------------------------------------------
    # SAVE VALIDATION ERRORS
    # -----------------------------------------------------

    os.makedirs(
        ERROR_DIR,
        exist_ok=True
    )

    if validation_errors:

        error_rows = []

        for record_id, error_type, message in validation_errors:

            error_rows.append(
                {
                    "run_id": run_id,
                    "record_id": record_id,
                    "error_type": error_type,
                    "error_message": message
                }
            )

            insert_error_log(
                run_id=run_id,
                record_id=record_id,
                error_type=error_type,
                error_message=message
            )

        error_df = pd.DataFrame(
            error_rows
        )

        error_file = os.path.join(
            ERROR_DIR,
            "validation_errors.csv"
        )

        error_df.to_csv(
            error_file,
            index=False
        )

    # -----------------------------------------------------
    # COUNT ERRORS BY TYPE
    # -----------------------------------------------------

    customer_error_count = sum(
        1
        for x in validation_errors
        if (
            x[0] == "customers"
            or x[1] in [
                "NULL_CUSTOMER_ID",
                "DUPLICATE_CUSTOMER_ID"
            ]
        )
    )

    order_error_count = sum(
        1
        for x in validation_errors
        if (
            x[0] == "orders"
            or x[1] in [
                "NULL_ORDER_ID",
                "DUPLICATE_ORDER_ID",
                "INVALID_CUSTOMER_REFERENCE",
                "INVALID_ORDER_DATE",
                "INVALID_TOTAL_AMOUNT",
                "NEGATIVE_TOTAL_AMOUNT"
            ]
        )
    )

    order_item_error_count = sum(
        1
        for x in validation_errors
        if (
            x[0] == "order_items"
            or x[1] in [
                "NULL_ORDER_ITEM_ID",
                "DUPLICATE_ORDER_ITEM_ID",
                "INVALID_ORDER_REFERENCE",
                "INVALID_QUANTITY",
                "INVALID_UNIT_PRICE",
                "NEGATIVE_UNIT_PRICE"
            ]
        )
    )

    print(
        f"Customer validation errors: "
        f"{customer_error_count}"
    )

    print(
        f"Order validation errors: "
        f"{order_error_count}"
    )

    print(
        f"Order item validation errors: "
        f"{order_item_error_count}"
    )

    print(
        f"Total validation errors: "
        f"{len(validation_errors)}"
    )

    return validation_errors


# =========================================================
# STEP 3 — TRANSFORM
# =========================================================

def transform_data(
    customers_df,
    orders_df,
    order_items_df
):

    print("\nSTEP 3: TRANSFORMATION")
    print("--------------------------------------")

    os.makedirs(
        PROCESSED_DIR,
        exist_ok=True
    )

    # -----------------------------------------------------
    # CUSTOMERS
    # -----------------------------------------------------

    customers_df = customers_df.copy()

    customers_df["customer_id"] = (
        customers_df["customer_id"]
        .astype(str)
        .str.strip()
    )

    customers_df["customer_name"] = (
        customers_df["customer_name"]
        .astype(str)
        .str.strip()
    )

    customers_df["email"] = (
        customers_df["email"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    customers_df["city"] = (
        customers_df["city"]
        .astype(str)
        .str.strip()
    )

    # -----------------------------------------------------
    # ORDERS
    # -----------------------------------------------------

    orders_df = orders_df.copy()

    orders_df["order_id"] = pd.to_numeric(
        orders_df["order_id"],
        errors="coerce"
    ).astype("Int64")

    orders_df["customer_id"] = (
        orders_df["customer_id"]
        .astype(str)
        .str.strip()
    )

    orders_df["order_date"] = pd.to_datetime(
        orders_df["order_date"],
        errors="coerce"
    ).dt.date

    orders_df["order_status"] = (
        orders_df["order_status"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    orders_df["total_amount"] = pd.to_numeric(
        orders_df["total_amount"],
        errors="coerce"
    )

    # -----------------------------------------------------
    # ORDER ITEMS
    # -----------------------------------------------------

    order_items_df = order_items_df.copy()

    order_items_df["order_item_id"] = pd.to_numeric(
        order_items_df["order_item_id"],
        errors="coerce"
    ).astype("Int64")

    order_items_df["order_id"] = pd.to_numeric(
        order_items_df["order_id"],
        errors="coerce"
    ).astype("Int64")

    order_items_df["product_name"] = (
        order_items_df["product_name"]
        .astype(str)
        .str.strip()
    )

    order_items_df["quantity"] = pd.to_numeric(
        order_items_df["quantity"],
        errors="coerce"
    )

    order_items_df["unit_price"] = pd.to_numeric(
        order_items_df["unit_price"],
        errors="coerce"
    )

    order_items_df["line_total"] = (
        order_items_df["quantity"]
        * order_items_df["unit_price"]
    )

    # -----------------------------------------------------
    # SAVE TRANSFORMED FILES
    # -----------------------------------------------------

    customers_df.to_csv(
        os.path.join(
            PROCESSED_DIR,
            "customers_transformed.csv"
        ),
        index=False
    )

    orders_df.to_csv(
        os.path.join(
            PROCESSED_DIR,
            "orders_transformed.csv"
        ),
        index=False
    )

    order_items_df.to_csv(
        os.path.join(
            PROCESSED_DIR,
            "order_items_transformed.csv"
        ),
        index=False
    )

    print(
        "Transformation completed successfully."
    )

    return (
        customers_df,
        orders_df,
        order_items_df
    )


# =========================================================
# STEP 4 — INCREMENTAL LOAD
# =========================================================

def incremental_load(
    customers_df,
    orders_df,
    order_items_df
):

    print("\nSTEP 4: INCREMENTAL LOAD")
    print("--------------------------------------")

    # -----------------------------------------------------
    # GET LAST PROCESSED ORDER ID
    # -----------------------------------------------------

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
        ).fetchone()

    if result is None:

        last_processed_id = 0

    else:

        last_processed_id = (
            result[0]
            if result[0] is not None
            else 0
        )

    print(
        f"Last processed order ID: "
        f"{last_processed_id}"
    )

    # -----------------------------------------------------
    # FIND NEW ORDERS
    # -----------------------------------------------------

    new_orders = orders_df[
        orders_df["order_id"]
        > last_processed_id
    ].copy()

    print(
        f"New records found: "
        f"{len(new_orders)}"
    )

    # -----------------------------------------------------
    # NO NEW DATA
    # -----------------------------------------------------

    if new_orders.empty:

        update_query = text("""
            UPDATE etl_control
            SET
                last_run_time = :last_run_time,
                last_status = 'SUCCESS_NO_NEW_DATA'
            WHERE pipeline_name = :pipeline_name
        """)

        with engine.begin() as connection:

            connection.execute(
                update_query,
                {
                    "last_run_time": datetime.now(),
                    "pipeline_name": PIPELINE_NAME
                }
            )

        print("No new orders found.")

        return 0

    # -----------------------------------------------------
    # FIND RELATED CUSTOMERS
    # -----------------------------------------------------

    customer_ids = set(
        new_orders["customer_id"]
    )

    new_customers = customers_df[
        customers_df["customer_id"].isin(
            customer_ids
        )
    ].copy()

    # -----------------------------------------------------
    # FIND RELATED ORDER ITEMS
    # -----------------------------------------------------

    new_order_ids = set(
        new_orders["order_id"]
    )

    new_order_items = order_items_df[
        order_items_df["order_id"].isin(
            new_order_ids
        )
    ].copy()

    # -----------------------------------------------------
    # CUSTOMER INSERT
    # -----------------------------------------------------

    customer_insert = text("""
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

    # -----------------------------------------------------
    # ORDER INSERT
    # -----------------------------------------------------

    order_insert = text("""
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

    # -----------------------------------------------------
    # ORDER ITEM INSERT
    # -----------------------------------------------------

    item_insert = text("""
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
            product_name = VALUES(product_name),
            quantity = VALUES(quantity),
            unit_price = VALUES(unit_price)
    """)

    # -----------------------------------------------------
    # LOAD DATA
    # -----------------------------------------------------

    with engine.begin() as connection:

        for _, row in new_customers.iterrows():

            connection.execute(
                customer_insert,
                {
                    "customer_id": row["customer_id"],
                    "customer_name": row["customer_name"],
                    "email": row["email"],
                    "city": row["city"]
                }
            )

        for _, row in new_orders.iterrows():

            connection.execute(
                order_insert,
                {
                    "order_id": int(row["order_id"]),
                    "customer_id": row["customer_id"],
                    "order_date": row["order_date"],
                    "order_status": row["order_status"],
                    "total_amount": float(
                        row["total_amount"]
                    )
                }
            )

        for _, row in new_order_items.iterrows():

            connection.execute(
                item_insert,
                {
                    "order_item_id": int(
                        row["order_item_id"]
                    ),
                    "order_id": int(
                        row["order_id"]
                    ),
                    "product_name": row["product_name"],
                    "quantity": int(
                        row["quantity"]
                    ),
                    "unit_price": float(
                        row["unit_price"]
                    )
                }
            )

    # -----------------------------------------------------
    # UPDATE ETL CONTROL
    # -----------------------------------------------------

    latest_order_id = int(
        new_orders["order_id"].max()
    )

    update_control = text("""
        UPDATE etl_control
        SET
            last_processed_order_id = :last_processed_order_id,
            last_run_time = :last_run_time,
            last_status = 'SUCCESS'
        WHERE pipeline_name = :pipeline_name
    """)

    with engine.begin() as connection:

        connection.execute(
            update_control,
            {
                "last_processed_order_id": latest_order_id,
                "last_run_time": datetime.now(),
                "pipeline_name": PIPELINE_NAME
            }
        )

    print(
        f"Records loaded: "
        f"{len(new_orders)}"
    )

    print(
        f"New last processed order ID: "
        f"{latest_order_id}"
    )

    return len(new_orders)


# =========================================================
# MAIN PIPELINE
# =========================================================

def main():

    print("======================================")
    print("STARTING ETL PIPELINE")
    print("======================================")

    run_id = create_run_id()

    print(
        f"\nRun ID: {run_id}"
    )

    start_time = datetime.now()

    records_read = 0
    records_valid = 0
    records_rejected = 0
    records_loaded = 0

    try:

        # =================================================
        # STEP 1 — EXTRACT
        # =================================================

        (
            customers_df,
            orders_df,
            order_items_df
        ) = extract_data()

        records_read = (
            len(customers_df)
            + len(orders_df)
            + len(order_items_df)
        )

        # =================================================
        # STEP 2 — VALIDATION
        # =================================================

        validation_errors = validate_data(
            customers_df,
            orders_df,
            order_items_df,
            run_id
        )

        records_rejected = len(
            validation_errors
        )

        records_valid = (
            records_read
            - records_rejected
        )

        # =================================================
        # STOP IF VALIDATION ERRORS EXIST
        # =================================================

        if validation_errors:

            raise ValueError(
                f"Validation failed with "
                f"{len(validation_errors)} error(s). "
                f"See etl_error_log for details."
            )

        # =================================================
        # STEP 3 — TRANSFORM
        # =================================================

        (
            customers_df,
            orders_df,
            order_items_df
        ) = transform_data(
            customers_df,
            orders_df,
            order_items_df
        )

        # =================================================
        # STEP 4 — INCREMENTAL LOAD
        # =================================================

        records_loaded = incremental_load(
            customers_df,
            orders_df,
            order_items_df
        )

        # =================================================
        # SUCCESS AUDIT
        # =================================================

        end_time = datetime.now()

        insert_audit_log(
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            records_read=records_read,
            records_valid=records_valid,
            records_rejected=records_rejected,
            records_loaded=records_loaded,
            status="SUCCESS",
            error_message=None
        )

        print("\nSTEP 5: AUDIT LOGGING")
        print("--------------------------------------")

        print(
            "Audit log recorded successfully."
        )

        print("\n======================================")
        print(
            "ETL PIPELINE COMPLETED SUCCESSFULLY"
        )
        print("======================================")

    except Exception as error:

        end_time = datetime.now()

        error_message = str(error)

        print("\n======================================")
        print("ETL PIPELINE FAILED")
        print("======================================")

        print(
            "Error:",
            error_message
        )

        # -------------------------------------------------
        # PIPELINE ERROR LOG
        # -------------------------------------------------

        try:

            insert_error_log(
                run_id=run_id,
                record_id="PIPELINE",
                error_type="PIPELINE_ERROR",
                error_message=error_message
            )

            print(
                "Pipeline error written to "
                "etl_error_log."
            )

        except Exception as logging_error:

            print(
                "Could not write pipeline error:"
            )

            print(logging_error)

        # -------------------------------------------------
        # FAILED AUDIT LOG
        # -------------------------------------------------

        try:

            insert_audit_log(
                run_id=run_id,
                start_time=start_time,
                end_time=end_time,
                records_read=records_read,
                records_valid=records_valid,
                records_rejected=records_rejected,
                records_loaded=records_loaded,
                status="FAILED",
                error_message=error_message
            )

            print(
                "Failed run written to "
                "etl_audit_log."
            )

        except Exception as audit_error:

            print(
                "Could not write failed audit log:"
            )

            print(audit_error)

        print("\n======================================")
        print("Pipeline failed.")
        print("Check:")
        print("1. etl_error_log")
        print("2. etl_audit_log")
        print("======================================")


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()