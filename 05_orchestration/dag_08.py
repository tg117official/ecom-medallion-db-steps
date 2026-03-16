from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# DAG: dag_08_resilient_pipeline_with_failure_handling
# Purpose:
#   Extend DAG 7 by adding retries, trigger rules,
#   failure handling, and final status tasks.
#
# Flow:
#   source_checks
#       -> bronze_layer
#       -> silver_layer
#       -> gold_layer
#       -> success/failure finalization
#
# goal:
#   Show that production DAGs are built for resilience,
#   not just happy-path execution.
# ============================================================


DAG_ID = "dag_08_resilient_pipeline_with_failure_handling"
DATABRICKS_CONN_ID = "databricks_default"

# Replace with your actual Databricks Job IDs
RDBMS_BRONZE_INGESTION_JOB_ID = 11111111111111
ADLS_BRONZE_INGESTION_JOB_ID = 22222222222222
BRONZE_VALIDATION_JOB_ID = 33333333333333
BRONZE_TO_SILVER_JOB_ID = 44444444444444
SILVER_DQ_METRICS_JOB_ID = 55555555555555
SILVER_TO_GOLD_JOB_ID = 66666666666666


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def source_readiness_check():
    print("Source readiness check started...")
    print("RDBMS source readiness check passed.")
    print("ADLS landing files readiness check passed.")
    print("All required sources are ready.")


def pipeline_success_marker():
    print("Pipeline completed successfully.")
    print("Bronze, Silver, and Gold stages finished successfully.")
    print("Final pipeline status = SUCCESS")


def pipeline_failure_marker():
    print("One or more upstream tasks failed.")
    print("Pipeline finished with failure state.")
    print("Final pipeline status = FAILED")


def pipeline_finalizer():
    print("Finalizer task executed.")
    print("This task runs regardless of pipeline success or failure.")
    print("Used for cleanup, audit updates, or final notifications in real pipelines.")


with DAG(
    dag_id=DAG_ID,
    description="DAG 8 - Resilient TaskGroup pipeline with failure handling",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["ecomsphere", "bronze", "silver", "gold", "dq", "medallion", "dag08"],
) as dag:

    # ------------------------------------------------------------
    # Source Checks TaskGroup
    # ------------------------------------------------------------
    with TaskGroup(group_id="source_checks", tooltip="Source readiness checks") as source_checks:

        source_check = PythonOperator(
            task_id="source_readiness_check",
            python_callable=source_readiness_check,
        )

    # ------------------------------------------------------------
    # Bronze Layer TaskGroup
    # ------------------------------------------------------------
    with TaskGroup(group_id="bronze_layer", tooltip="Bronze ingestion and validation") as bronze_layer:

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

        [rdbms_to_bronze, adls_to_bronze] >> bronze_ingestion_complete >> bronze_validation

    # ------------------------------------------------------------
    # Silver Layer TaskGroup
    # ------------------------------------------------------------
    with TaskGroup(group_id="silver_layer", tooltip="Bronze to Silver transformation and DQ") as silver_layer:

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

        bronze_to_silver >> silver_dq_metrics

    # ------------------------------------------------------------
    # Gold Layer TaskGroup
    # ------------------------------------------------------------
    with TaskGroup(group_id="gold_layer", tooltip="Silver to Gold modeling") as gold_layer:

        silver_to_gold = DatabricksRunNowOperator(
            task_id="run_silver_to_gold_modeling_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=SILVER_TO_GOLD_JOB_ID,
        )

    # ------------------------------------------------------------
    # Final status tasks
    # ------------------------------------------------------------
    pipeline_success = PythonOperator(
        task_id="pipeline_success_marker",
        python_callable=pipeline_success_marker,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    pipeline_failure = PythonOperator(
        task_id="pipeline_failure_marker",
        python_callable=pipeline_failure_marker,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    pipeline_finalizer_task = PythonOperator(
        task_id="pipeline_finalizer",
        python_callable=pipeline_finalizer,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ------------------------------------------------------------
    # TaskGroup Dependencies
    # ------------------------------------------------------------
    source_checks >> bronze_layer >> silver_layer >> gold_layer

    gold_layer >> pipeline_success
    [source_checks, bronze_layer, silver_layer, gold_layer] >> pipeline_failure

    [pipeline_success, pipeline_failure] >> pipeline_finalizer_task