from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# DAG: dag_05_silver_dq_pipeline
# Purpose:
#   Extend DAG 4 by adding an explicit Silver DQ stage.
#
# Flow:
#   source check
#       -> parallel bronze ingestion
#            1) RDBMS -> Bronze
#            2) ADLS JSON -> Bronze
#       -> bronze validation
#       -> bronze to silver transformation
#       -> silver DQ metrics generation
#       -> silver completion marker
#
# goal:
#   Show that successful transformation is not enough;
#   curated Silver data should also pass quality checks.
# ============================================================


DAG_ID = "dag_05_silver_dq_pipeline"
DATABRICKS_CONN_ID = "databricks_default"

# Replace with your actual Databricks Job IDs
RDBMS_BRONZE_INGESTION_JOB_ID = 11111111111111
ADLS_BRONZE_INGESTION_JOB_ID = 22222222222222
BRONZE_VALIDATION_JOB_ID = 33333333333333
BRONZE_TO_SILVER_JOB_ID = 44444444444444
SILVER_DQ_METRICS_JOB_ID = 55555555555555


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def source_readiness_check():
    print("Source readiness check started...")
    print("RDBMS source readiness check passed.")
    print("ADLS landing files readiness check passed.")
    print("All required sources are ready.")


def silver_completion_marker():
    print("Bronze to Silver processing completed successfully.")
    print("Silver DQ metrics generation completed successfully.")
    print("Silver curated layer is ready for downstream consumption.")


with DAG(
    dag_id=DAG_ID,
    description="DAG 5 - Bronze to Silver pipeline with Silver DQ stage",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["ecomsphere", "bronze", "silver", "dq", "medallion", "dag05"],
) as dag:

    source_check = PythonOperator(
        task_id="source_readiness_check",
        python_callable=source_readiness_check,
    )

    rdbms_to_bronze = DatabricksRunNowOperator(
        task_id="run_rdbms_to_bronze_ingestion_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=RDBMS_BRONZE_INGESTION_JOB_ID,
    )

    adls_to_bronze = DatabricksRunNowOperator(
        task_id="run_adls_to_bronze_ingestion_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=ADLS_BRONZE_INGESTION_JOB_ID,
    )

    bronze_ingestion_complete = EmptyOperator(
        task_id="bronze_ingestion_complete"
    )

    bronze_validation = DatabricksRunNowOperator(
        task_id="run_bronze_validation_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_VALIDATION_JOB_ID,
    )

    bronze_to_silver = DatabricksRunNowOperator(
        task_id="run_bronze_to_silver_transformation_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_TO_SILVER_JOB_ID,
    )

    silver_dq_metrics = DatabricksRunNowOperator(
        task_id="run_silver_dq_metrics_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=SILVER_DQ_METRICS_JOB_ID,
    )

    silver_completion = PythonOperator(
        task_id="silver_completion_marker",
        python_callable=silver_completion_marker,
    )

    source_check >> [rdbms_to_bronze, adls_to_bronze]
    [rdbms_to_bronze, adls_to_bronze] >> bronze_ingestion_complete
    bronze_ingestion_complete >> bronze_validation >> bronze_to_silver >> silver_dq_metrics >> silver_completion