from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/mnt/c/Users/DivyaSaiAllampuri/Documents/ETL_ELT_Use_Case"


with DAG(
    dag_id="ecommerce_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mysql", "csv"],
) as dag:

    start = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'Starting ETL/ELT pipeline'",
    )

    run_pipeline = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=(
            f"cd '{PROJECT_DIR}' && "
            "export MYSQL_HOST=$(ip route | awk '/default/ {print $3; exit}') && "
            "python scripts/pipeline.py"
),
    )

    finish = BashOperator(
        task_id="finish_pipeline",
        bash_command="echo 'ETL/ELT pipeline completed successfully'",
    )

    start >> run_pipeline >> finish
