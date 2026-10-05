from pathlib import Path
import pandas as pd


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

# Create processed directory if it does not exist
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# Input files
# ---------------------------------------------------------

CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
ORDERS_FILE = RAW_DATA_DIR / "orders.csv"
ORDER_ITEMS_FILE = RAW_DATA_DIR / "order_items.csv"


# ---------------------------------------------------------
# Output files
# ---------------------------------------------------------

CUSTOMERS_OUTPUT = PROCESSED_DATA_DIR / "customers_transformed.csv"
ORDERS_OUTPUT = PROCESSED_DATA_DIR / "orders_transformed.csv"
ORDER_ITEMS_OUTPUT = PROCESSED_DATA_DIR / "order_items_transformed.csv"


# ---------------------------------------------------------
# Transform customers
# ---------------------------------------------------------

def transform_customers(customers_df):
    print("\nTransforming customers...")

    # Remove leading/trailing spaces from text columns
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

    print(
        "Customers transformed:",
        len(customers_df)
    )

    return customers_df


# ---------------------------------------------------------
# Transform orders
# ---------------------------------------------------------

def transform_orders(orders_df):
    print("\nTransforming orders...")

    # Clean text columns
    orders_df["customer_id"] = (
        orders_df["customer_id"]
        .astype(str)
        .str.strip()
    )

    orders_df["order_status"] = (
        orders_df["order_status"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Convert order_id to integer
    orders_df["order_id"] = pd.to_numeric(
        orders_df["order_id"],
        errors="coerce"
    ).astype("Int64")

    # Convert order_date to date format
    orders_df["order_date"] = pd.to_datetime(
        orders_df["order_date"],
        errors="coerce"
    ).dt.date

    # Convert total_amount to numeric
    orders_df["total_amount"] = pd.to_numeric(
        orders_df["total_amount"],
        errors="coerce"
    )

    # Round amount to two decimal places
    orders_df["total_amount"] = (
        orders_df["total_amount"].round(2)
    )

    print(
        "Orders transformed:",
        len(orders_df)
    )

    return orders_df


# ---------------------------------------------------------
# Transform order items
# ---------------------------------------------------------

def transform_order_items(order_items_df):
    print("\nTransforming order items...")

    # Clean product name
    order_items_df["product_name"] = (
        order_items_df["product_name"]
        .astype(str)
        .str.strip()
    )

    # Convert IDs to integers
    order_items_df["order_item_id"] = pd.to_numeric(
        order_items_df["order_item_id"],
        errors="coerce"
    ).astype("Int64")

    order_items_df["order_id"] = pd.to_numeric(
        order_items_df["order_id"],
        errors="coerce"
    ).astype("Int64")

    # Convert quantity to integer
    order_items_df["quantity"] = pd.to_numeric(
        order_items_df["quantity"],
        errors="coerce"
    ).astype("Int64")

    # Convert unit price to numeric
    order_items_df["unit_price"] = pd.to_numeric(
        order_items_df["unit_price"],
        errors="coerce"
    )

    # Round unit price
    order_items_df["unit_price"] = (
        order_items_df["unit_price"].round(2)
    )

    # Calculate line total
    order_items_df["line_total"] = (
        order_items_df["quantity"]
        * order_items_df["unit_price"]
    )

    # Round calculated value
    order_items_df["line_total"] = (
        order_items_df["line_total"].round(2)
    )

    print(
        "Order items transformed:",
        len(order_items_df)
    )

    return order_items_df


# ---------------------------------------------------------
# Main transformation process
# ---------------------------------------------------------

def transform_data():

    print("======================================")
    print("Starting data transformation...")
    print("======================================")

    # -----------------------------------------------------
    # Read extracted CSV files
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

    print("\nInput data loaded successfully.")

    # -----------------------------------------------------
    # Apply transformations
    # -----------------------------------------------------

    customers_df = transform_customers(
        customers_df
    )

    orders_df = transform_orders(
        orders_df
    )

    order_items_df = transform_order_items(
        order_items_df
    )

    # -----------------------------------------------------
    # Save transformed data
    # -----------------------------------------------------

    customers_df.to_csv(
        CUSTOMERS_OUTPUT,
        index=False
    )

    orders_df.to_csv(
        ORDERS_OUTPUT,
        index=False
    )

    order_items_df.to_csv(
        ORDER_ITEMS_OUTPUT,
        index=False
    )

    # -----------------------------------------------------
    # Display results
    # -----------------------------------------------------

    print("\n======================================")
    print("Transformed data preview")
    print("======================================")

    print("\nCustomers:")
    print(customers_df.head())

    print("\nOrders:")
    print(orders_df.head())

    print("\nOrder Items:")
    print(order_items_df.head())

    print("\n======================================")
    print("Transformation completed successfully!")
    print("======================================")

    print("\nOutput files created:")

    print(CUSTOMERS_OUTPUT)
    print(ORDERS_OUTPUT)
    print(ORDER_ITEMS_OUTPUT)


# ---------------------------------------------------------
# Program entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    transform_data()