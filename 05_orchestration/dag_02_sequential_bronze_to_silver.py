from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# DAG: dag_02_sequential_bronze_to_silver
# Purpose:
#   Demonstrate sequential orchestration in Airflow using
#   multiple Databricks jobs for one simple source flow.
#
# Flow:
#   start
#     -> bronze ingestion
#     -> bronze validation
#     -> silver transformation
#     -> end
#
# Project scope:
#   product_catalog_feed
#     -> bronze_product_catalog_feed
#     -> silver_dim_product_master
#
# goal:
#   Show that Airflow can orchestrate a pipeline,
#   not just a single Databricks job.
# ============================================================


DAG_ID = "dag_02_sequential_bronze_to_silver"
DATABRICKS_CONN_ID = "databricks_default"

# Replace these with your actual Databricks Job IDs
BRONZE_INGESTION_JOB_ID = 11111111111111
BRONZE_VALIDATION_JOB_ID = 22222222222222
SILVER_TRANSFORMATION_JOB_ID = 33333333333333


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    description="DAG 2 - Sequential Bronze ingestion, validation, and Silver transformation",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="0 */6 * * *",   # every 6 hours
    catchup=False,
    tags=["ecomsphere", "bronze", "silver", "databricks", "dag02"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    bronze_ingestion = DatabricksRunNowOperator(
        task_id="run_bronze_ingestion_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_INGESTION_JOB_ID,
    )

    bronze_validation = DatabricksRunNowOperator(
        task_id="run_bronze_validation_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_VALIDATION_JOB_ID,
    )

    silver_transformation = DatabricksRunNowOperator(
        task_id="run_silver_transformation_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=SILVER_TRANSFORMATION_JOB_ID,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> bronze_ingestion >> bronze_validation >> silver_transformation >> end