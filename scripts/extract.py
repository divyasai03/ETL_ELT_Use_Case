from pathlib import Path
import pandas as pd


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


# ---------------------------------------------------------
# CSV file paths
# ---------------------------------------------------------

CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
ORDERS_FILE = RAW_DATA_DIR / "orders.csv"
ORDER_ITEMS_FILE = RAW_DATA_DIR / "order_items.csv"


# ---------------------------------------------------------
# Extract function
# ---------------------------------------------------------

def extract_data():
    print("======================================")
    print("Starting data extraction...")
    print("======================================")

    # Check that the files exist
    for file_path in [
        CUSTOMERS_FILE,
        ORDERS_FILE,
        ORDER_ITEMS_FILE
    ]:
        if not file_path.exists():
            raise FileNotFoundError(
                f"Required file not found: {file_path}"
            )

    # Read CSV files
    customers_df = pd.read_csv(CUSTOMERS_FILE)
    orders_df = pd.read_csv(ORDERS_FILE)
    order_items_df = pd.read_csv(ORDER_ITEMS_FILE)

    # Display extraction results
    print()
    print("Customers extracted:", len(customers_df))
    print("Orders extracted:", len(orders_df))
    print("Order items extracted:", len(order_items_df))

    print()
    print("Customers columns:")
    print(list(customers_df.columns))

    print()
    print("Orders columns:")
    print(list(orders_df.columns))

    print()
    print("Order items columns:")
    print(list(order_items_df.columns))

    print()
    print("First 5 customers:")
    print(customers_df.head())

    print()
    print("First 5 orders:")
    print(orders_df.head())

    print()
    print("First 5 order items:")
    print(order_items_df.head())

    print()
    print("======================================")
    print("Data extraction completed successfully!")
    print("======================================")

    return customers_df, orders_df, order_items_df


# ---------------------------------------------------------
# Main program
# ---------------------------------------------------------

if __name__ == "__main__":
    extract_data()