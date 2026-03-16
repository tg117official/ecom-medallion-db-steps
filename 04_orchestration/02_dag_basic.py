from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------
def source_readiness_check(**context):
    """
    Placeholder source readiness check.
    In production, this can verify:
    - RDBMS source availability
    - ADLS input path accessibility
    - expected file arrival freshness
    """
    print("Source readiness check passed.")


def bronze_reconciliation_check(**context):
    """
    Placeholder bronze reconciliation check.
    In production, this can validate:
    - row counts loaded into bronze
    - source vs bronze count tolerance
    - file ingestion freshness
    """
    print("Bronze reconciliation check passed.")


def gold_reconciliation_check(**context):
    """
    Placeholder gold reconciliation / post-load validation.
    In production, this can validate:
    - fact row counts
    - null foreign keys
    - amount reconciliation
    - summary table freshness
    """
    print("Gold reconciliation check passed.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="ecom_medallion_6hr_pipeline",
    default_args=default_args,
    description="6-hour E-commerce Medallion pipeline: Bronze -> Silver -> Gold",
    start_date=datetime(2026, 3, 1),
    schedule="0 */6 * * *",   # every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "medallion", "databricks", "delta", "airflow"],
) as dag:

    # --------------------------------------------------------------
    # Start / End
    # --------------------------------------------------------------
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # --------------------------------------------------------------
    # Pre-checks
    # --------------------------------------------------------------
    source_readiness = PythonOperator(
        task_id="source_readiness_check",
        python_callable=source_readiness_check,
    )

    # --------------------------------------------------------------
    # Bronze ingestion group
    # --------------------------------------------------------------
    with TaskGroup(group_id="bronze_layer_jobs") as bronze_layer_jobs:

        bronze_rdbms_ingestion = DatabricksRunNowOperator(
            task_id="bronze_rdbms_ingestion",
            databricks_conn_id="databricks_default",
            job_id=101,   # replace with your Databricks job ID
            notebook_params={
                "load_type": "incremental",
                "source_system": "rdbms",
                "target_layer": "bronze"
            },
        )

        bronze_file_ingestion = DatabricksRunNowOperator(
            task_id="bronze_file_ingestion",
            databricks_conn_id="databricks_default",
            job_id=102,   # replace with your Databricks Auto Loader job ID
            notebook_params={
                "load_type": "available_now",
                "source_system": "adls_files",
                "target_layer": "bronze"
            },
        )

    bronze_check = PythonOperator(
        task_id="bronze_reconciliation_check",
        python_callable=bronze_reconciliation_check,
    )

    # --------------------------------------------------------------
    # Silver jobs
    # --------------------------------------------------------------
    silver_transformation = DatabricksRunNowOperator(
        task_id="silver_transformation_job",
        databricks_conn_id="databricks_default",
        job_id=103,   # replace with your Databricks bronze->silver job ID
        notebook_params={
            "load_type": "incremental",
            "source_layer": "bronze",
            "target_layer": "silver"
        },
    )

    silver_dq_summary = DatabricksRunNowOperator(
        task_id="silver_dq_summary_job",
        databricks_conn_id="databricks_default",
        job_id=104,   # replace with your DQ metrics / audit job ID
        notebook_params={
            "layer": "silver",
            "publish_dq_results": "true"
        },
    )

    # --------------------------------------------------------------
    # Gold jobs
    # --------------------------------------------------------------
    gold_transformation = DatabricksRunNowOperator(
        task_id="gold_transformation_job",
        databricks_conn_id="databricks_default",
        job_id=105,   # replace with your Databricks silver->gold job ID
        notebook_params={
            "load_type": "incremental",
            "source_layer": "silver",
            "target_layer": "gold"
        },
    )

    gold_check = PythonOperator(
        task_id="gold_reconciliation_check",
        python_callable=gold_reconciliation_check,
    )

    # Optional final status marker / notification task
    pipeline_success = EmptyOperator(
        task_id="pipeline_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # --------------------------------------------------------------
    # Dependencies
    # --------------------------------------------------------------
    start >> source_readiness >> bronze_layer_jobs
    bronze_layer_jobs >> bronze_check
    bronze_check >> silver_transformation >> silver_dq_summary >> gold_transformation
    gold_transformation >> gold_check >> pipeline_success >> end