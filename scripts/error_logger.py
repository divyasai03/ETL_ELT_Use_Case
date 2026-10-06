import uuid
from datetime import datetime

from sqlalchemy import text

from db_config import get_engine


# =========================================================
# MYSQL CONNECTION
# =========================================================


# =========================================================
# CREATE CONNECTION
# =========================================================

engine = get_engine()


# =========================================================
# PIPELINE NAME
# =========================================================

PIPELINE_NAME = "ecommerce_etl_pipeline"


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
# INSERT ERROR LOG
# =========================================================

def log_error(
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
# TEST ERROR LOGGING
# =========================================================

def main():

    print("======================================")
    print("Testing error logging...")
    print("======================================")

    run_id = create_run_id()

    # Simulate an invalid record
    record_id = "TEST-001"

    error_type = "DATA_VALIDATION_ERROR"

    error_message = (
        "Test error: customer ID does not exist"
    )

    log_error(
        run_id=run_id,
        record_id=record_id,
        error_type=error_type,
        error_message=error_message
    )

    print("\nError log inserted successfully.")

    print("Run ID:", run_id)
    print("Record ID:", record_id)
    print("Error type:", error_type)
    print("Error message:", error_message)

    print("\n======================================")
    print("Error logging test completed.")
    print("======================================")


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()

