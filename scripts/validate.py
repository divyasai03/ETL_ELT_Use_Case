from pathlib import Path
import pandas as pd


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
ERROR_DATA_DIR = PROJECT_ROOT / "data" / "error"

ERROR_DATA_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# Input files
# ---------------------------------------------------------

CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
ORDERS_FILE = RAW_DATA_DIR / "orders.csv"
ORDER_ITEMS_FILE = RAW_DATA_DIR / "order_items.csv"


# ---------------------------------------------------------
# Validation helper
# ---------------------------------------------------------

def check_required_columns(df, required_columns, file_name):
    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{file_name} is missing required columns: "
            f"{missing_columns}"
        )


# ---------------------------------------------------------
# Customer validation
# ---------------------------------------------------------

def validate_customers(customers_df):
    print("\nValidating customers...")

    required_columns = [
        "customer_id",
        "customer_name",
        "email",
        "city"
    ]

    check_required_columns(
        customers_df,
        required_columns,
        "customers.csv"
    )

    errors = []

    # Check missing customer IDs
    missing_customer_id = customers_df[
        customers_df["customer_id"].isna()
        | (customers_df["customer_id"].astype(str).str.strip() == "")
    ]

    for _, row in missing_customer_id.iterrows():
        errors.append({
            "record_id": "",
            "error_type": "MISSING_CUSTOMER_ID",
            "error_message": "Customer ID is missing"
        })

    # Check duplicate customer IDs
    duplicate_customers = customers_df[
        customers_df["customer_id"].duplicated(keep=False)
    ]

    for _, row in duplicate_customers.iterrows():
        errors.append({
            "record_id": str(row["customer_id"]),
            "error_type": "DUPLICATE_CUSTOMER_ID",
            "error_message": "Duplicate customer ID"
        })

    print(
        "Customer validation errors:",
        len(errors)
    )

    return errors


# ---------------------------------------------------------
# Order validation
# ---------------------------------------------------------

def validate_orders(orders_df, customers_df):
    print("\nValidating orders...")

    required_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "total_amount"
    ]

    check_required_columns(
        orders_df,
        required_columns,
        "orders.csv"
    )

    errors = []

    # Check missing order IDs
    missing_order_id = orders_df[
        orders_df["order_id"].isna()
    ]

    for _, row in missing_order_id.iterrows():
        errors.append({
            "record_id": "",
            "error_type": "MISSING_ORDER_ID",
            "error_message": "Order ID is missing"
        })

    # Check duplicate order IDs
    duplicate_orders = orders_df[
        orders_df["order_id"].duplicated(keep=False)
    ]

    for _, row in duplicate_orders.iterrows():
        errors.append({
            "record_id": str(row["order_id"]),
            "error_type": "DUPLICATE_ORDER_ID",
            "error_message": "Duplicate order ID"
        })

    # Check customer references
    valid_customer_ids = set(
        customers_df["customer_id"].dropna().astype(str)
    )

    for _, row in orders_df.iterrows():
        customer_id = str(row["customer_id"])

        if customer_id not in valid_customer_ids:
            errors.append({
                "record_id": str(row["order_id"]),
                "error_type": "INVALID_CUSTOMER_REFERENCE",
                "error_message": (
                    f"Customer ID {customer_id} "
                    "does not exist in customers.csv"
                )
            })

    # Validate order dates
    parsed_dates = pd.to_datetime(
        orders_df["order_date"],
        errors="coerce"
    )

    invalid_dates = orders_df[parsed_dates.isna()]

    for _, row in invalid_dates.iterrows():
        errors.append({
            "record_id": str(row["order_id"]),
            "error_type": "INVALID_ORDER_DATE",
            "error_message": "Invalid order date"
        })

    # Validate total amount
    invalid_amounts = orders_df[
        pd.to_numeric(
            orders_df["total_amount"],
            errors="coerce"
        ).isna()
    ]

    for _, row in invalid_amounts.iterrows():
        errors.append({
            "record_id": str(row["order_id"]),
            "error_type": "INVALID_TOTAL_AMOUNT",
            "error_message": "Total amount is not numeric"
        })

    negative_amounts = orders_df[
        pd.to_numeric(
            orders_df["total_amount"],
            errors="coerce"
        ) < 0
    ]

    for _, row in negative_amounts.iterrows():
        errors.append({
            "record_id": str(row["order_id"]),
            "error_type": "NEGATIVE_TOTAL_AMOUNT",
            "error_message": "Total amount cannot be negative"
        })

    print(
        "Order validation errors:",
        len(errors)
    )

    return errors


# ---------------------------------------------------------
# Order item validation
# ---------------------------------------------------------

def validate_order_items(order_items_df, orders_df):
    print("\nValidating order items...")

    required_columns = [
        "order_item_id",
        "order_id",
        "product_name",
        "quantity",
        "unit_price"
    ]

    check_required_columns(
        order_items_df,
        required_columns,
        "order_items.csv"
    )

    errors = []

    # Check missing order item IDs
    missing_item_id = order_items_df[
        order_items_df["order_item_id"].isna()
    ]

    for _, row in missing_item_id.iterrows():
        errors.append({
            "record_id": "",
            "error_type": "MISSING_ORDER_ITEM_ID",
            "error_message": "Order item ID is missing"
        })

    # Check duplicate order item IDs
    duplicate_items = order_items_df[
        order_items_df["order_item_id"].duplicated(keep=False)
    ]

    for _, row in duplicate_items.iterrows():
        errors.append({
            "record_id": str(row["order_item_id"]),
            "error_type": "DUPLICATE_ORDER_ITEM_ID",
            "error_message": "Duplicate order item ID"
        })

    # Check order references
    valid_order_ids = set(
        orders_df["order_id"].dropna()
    )

    for _, row in order_items_df.iterrows():
        order_id = row["order_id"]

        if order_id not in valid_order_ids:
            errors.append({
                "record_id": str(row["order_item_id"]),
                "error_type": "INVALID_ORDER_REFERENCE",
                "error_message": (
                    f"Order ID {order_id} "
                    "does not exist in orders.csv"
                )
            })

    # Validate quantity
    numeric_quantity = pd.to_numeric(
        order_items_df["quantity"],
        errors="coerce"
    )

    invalid_quantity = order_items_df[
        numeric_quantity.isna()
    ]

    for _, row in invalid_quantity.iterrows():
        errors.append({
            "record_id": str(row["order_item_id"]),
            "error_type": "INVALID_QUANTITY",
            "error_message": "Quantity must be numeric"
        })

    non_positive_quantity = order_items_df[
        numeric_quantity <= 0
    ]

    for _, row in non_positive_quantity.iterrows():
        errors.append({
            "record_id": str(row["order_item_id"]),
            "error_type": "INVALID_QUANTITY",
            "error_message": "Quantity must be greater than zero"
        })

    # Validate unit price
    numeric_price = pd.to_numeric(
        order_items_df["unit_price"],
        errors="coerce"
    )

    invalid_price = order_items_df[
        numeric_price.isna()
    ]

    for _, row in invalid_price.iterrows():
        errors.append({
            "record_id": str(row["order_item_id"]),
            "error_type": "INVALID_UNIT_PRICE",
            "error_message": "Unit price must be numeric"
        })

    negative_price = order_items_df[
        numeric_price < 0
    ]

    for _, row in negative_price.iterrows():
        errors.append({
            "record_id": str(row["order_item_id"]),
            "error_type": "NEGATIVE_UNIT_PRICE",
            "error_message": "Unit price cannot be negative"
        })

    print(
        "Order item validation errors:",
        len(errors)
    )

    return errors


# ---------------------------------------------------------
# Main validation process
# ---------------------------------------------------------

def validate_data():
    print("======================================")
    print("Starting data validation...")
    print("======================================")

    # Extract data
    customers_df = pd.read_csv(CUSTOMERS_FILE)
    orders_df = pd.read_csv(ORDERS_FILE)
    order_items_df = pd.read_csv(ORDER_ITEMS_FILE)

    # Validate each dataset
    customer_errors = validate_customers(
        customers_df
    )

    order_errors = validate_orders(
        orders_df,
        customers_df
    )

    order_item_errors = validate_order_items(
        order_items_df,
        orders_df
    )

    # Combine all errors
    all_errors = (
        customer_errors
        + order_errors
        + order_item_errors
    )

    print()
    print("======================================")

    if all_errors:
        print("Validation completed with errors.")
        print("Total validation errors:", len(all_errors))

        error_df = pd.DataFrame(all_errors)

        error_file = ERROR_DATA_DIR / "validation_errors.csv"

        error_df.to_csv(
            error_file,
            index=False
        )

        print("Error file created:")
        print(error_file)

    else:
        print("Validation completed successfully.")
        print("Total validation errors: 0")

    print("======================================")

    return all_errors


# ---------------------------------------------------------
# Program entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    validate_data()