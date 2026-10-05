import uuid
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# =========================================================
# MYSQL CONNECTION
# =========================================================

MYSQL_USER = "root"
MYSQL_PASSWORD = "mysql@123"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "airflow_etl_db"


# =========================================================
# CONNECTION
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
# PIPELINE DETAILS
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

    return f"{PIPELINE_NAME}_{timestamp}_{unique_id}"


# =========================================================
# INSERT AUDIT LOG
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
                "error_message": error_message,
            }
        )


# =========================================================
# TEST AUDIT LOGGING
# =========================================================

def main():

    print("======================================")
    print("Testing audit logging...")
    print("======================================")

    run_id = create_run_id()

    start_time = datetime.now()

    # Test values
    records_read = 10
    records_valid = 10
    records_rejected = 0
    records_loaded = 10
    status = "SUCCESS"
    error_message = None

    end_time = datetime.now()

    insert_audit_log(
        run_id=run_id,
        start_time=start_time,
        end_time=end_time,
        records_read=records_read,
        records_valid=records_valid,
        records_rejected=records_rejected,
        records_loaded=records_loaded,
        status=status,
        error_message=error_message
    )

    print("\nAudit log inserted successfully.")

    print("Run ID:", run_id)
    print("Records read:", records_read)
    print("Records valid:", records_valid)
    print("Records rejected:", records_rejected)
    print("Records loaded:", records_loaded)
    print("Status:", status)

    print("\n======================================")
    print("Audit logging test completed.")
    print("======================================")


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()