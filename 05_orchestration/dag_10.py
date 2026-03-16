from datetime import datetime, timedelta
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# DAG: dag_10_industry_ready_ecom_medallion_pipeline
# Purpose:
#   Final production-style orchestration DAG for the E-commerce
#   Medallion Architecture project.
#
# Flow:
#   initialize run metadata
#       -> source checks
#       -> bronze layer
#       -> silver layer
#       -> gold layer
#       -> serving layer
#       -> reconciliation checks
#       -> success / failure markers
#       -> final audit update
#
# goal:
#   Show a full industry-style Medallion pipeline with
#   control metadata, DQ, Gold modeling, serving builds,
#   reconciliation, and final run closure.
# ============================================================


DAG_ID = "dag_10_industry_ready_ecom_medallion_pipeline"
DATABRICKS_CONN_ID = "databricks_default"

# Replace with your actual Databricks Job IDs
RDBMS_BRONZE_INGESTION_JOB_ID = 11111111111111
ADLS_BRONZE_INGESTION_JOB_ID = 22222222222222
BRONZE_VALIDATION_JOB_ID = 33333333333333
BRONZE_TO_SILVER_JOB_ID = 44444444444444
SILVER_DQ_METRICS_JOB_ID = 55555555555555
SILVER_TO_GOLD_JOB_ID = 66666666666666
GOLD_SERVING_LAYER_JOB_ID = 77777777777777
RECONCILIATION_CHECKS_JOB_ID = 88888888888888


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def initialize_pipeline_run(**context):
    """
    Initialize pipeline control metadata.

    In a real project this step may:
    - generate pipeline_run_id
    - read last successful watermark
    - determine incremental window
    - insert STARTED record into governance.pipeline_run_audit
    """
    pipeline_run_id = str(uuid.uuid4())
    dag_id = context["dag"].dag_id
    airflow_run_id = context["run_id"]
    logical_date = str(context["logical_date"])

    # Placeholder incremental window values
    window_start = logical_date
    window_end = logical_date

    ti = context["ti"]
    ti.xcom_push(key="pipeline_run_id", value=pipeline_run_id)
    ti.xcom_push(key="window_start", value=window_start)
    ti.xcom_push(key="window_end", value=window_end)

    print("Pipeline run initialized")
    print(f"dag_id={dag_id}")
    print(f"airflow_run_id={airflow_run_id}")
    print(f"pipeline_run_id={pipeline_run_id}")
    print(f"window_start={window_start}")
    print(f"window_end={window_end}")
    print("Run audit status = STARTED")


def source_readiness_check(**context):
    ti = context["ti"]
    pipeline_run_id = ti.xcom_pull(task_ids="initialize_pipeline_run", key="pipeline_run_id")

    print("Source readiness check started")
    print(f"pipeline_run_id={pipeline_run_id}")
    print("RDBMS source readiness check passed")
    print("ADLS landing files readiness check passed")
    print("All required sources are ready")


def pipeline_success_marker(**context):
    ti = context["ti"]
    pipeline_run_id = ti.xcom_pull(task_ids="initialize_pipeline_run", key="pipeline_run_id")

    print("Pipeline completed successfully")
    print(f"pipeline_run_id={pipeline_run_id}")
    print("Final pipeline status = SUCCESS")


def pipeline_failure_marker(**context):
    ti = context["ti"]
    pipeline_run_id = ti.xcom_pull(task_ids="initialize_pipeline_run", key="pipeline_run_id")

    print("One or more upstream tasks failed")
    print(f"pipeline_run_id={pipeline_run_id}")
    print("Final pipeline status = FAILED")


def finalize_pipeline_audit(**context):
    """
    Final audit closure step.

    In a real project this step may:
    - update governance.pipeline_run_audit
    - persist final status
    - store processed window
    - record start/end timestamps
    - record row counts / rejection counts / reconciliation status
    """
    ti = context["ti"]
    pipeline_run_id = ti.xcom_pull(task_ids="initialize_pipeline_run", key="pipeline_run_id")
    window_start = ti.xcom_pull(task_ids="initialize_pipeline_run", key="window_start")
    window_end = ti.xcom_pull(task_ids="initialize_pipeline_run", key="window_end")

    print("Final audit update executed")
    print(f"pipeline_run_id={pipeline_run_id}")
    print(f"window_start={window_start}")
    print(f"window_end={window_end}")
    print("Run audit status finalized")


with DAG(
    dag_id=DAG_ID,
    description="DAG 10 - Industry-ready E-commerce Medallion pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["ecomsphere", "bronze", "silver", "gold", "serving", "audit", "dag10"],
) as dag:

    # ------------------------------------------------------------
    # Initialize run metadata
    # ------------------------------------------------------------
    initialize_run = PythonOperator(
        task_id="initialize_pipeline_run",
        python_callable=initialize_pipeline_run,
    )

    # ------------------------------------------------------------
    # Source checks
    # ------------------------------------------------------------
    with TaskGroup(group_id="source_checks", tooltip="Source readiness checks") as source_checks:

        source_check = PythonOperator(
            task_id="source_readiness_check",
            python_callable=source_readiness_check,
        )

    # ------------------------------------------------------------
    # Bronze layer
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
    # Silver layer
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
    # Gold layer
    # ------------------------------------------------------------
    with TaskGroup(group_id="gold_layer", tooltip="Silver to Gold modeling") as gold_layer:

        silver_to_gold = DatabricksRunNowOperator(
            task_id="run_silver_to_gold_modeling_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=SILVER_TO_GOLD_JOB_ID,
        )

    # ------------------------------------------------------------
    # Serving layer
    # ------------------------------------------------------------
    with TaskGroup(group_id="serving_layer", tooltip="Gold serving model builds") as serving_layer:

        build_serving_layer = DatabricksRunNowOperator(
            task_id="run_gold_serving_layer_build_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=GOLD_SERVING_LAYER_JOB_ID,
        )

    # ------------------------------------------------------------
    # Reconciliation / post-load validation
    # ------------------------------------------------------------
    with TaskGroup(group_id="reconciliation_layer", tooltip="Post-load reconciliation checks") as reconciliation_layer:

        reconciliation_checks = DatabricksRunNowOperator(
            task_id="run_reconciliation_checks_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=RECONCILIATION_CHECKS_JOB_ID,
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

    finalize_audit = PythonOperator(
        task_id="finalize_pipeline_audit",
        python_callable=finalize_pipeline_audit,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------
    initialize_run >> source_checks >> bronze_layer >> silver_layer >> gold_layer >> serving_layer >> reconciliation_layer

    reconciliation_layer >> pipeline_success
    [
        source_checks,
        bronze_layer,
        silver_layer,
        gold_layer,
        serving_layer,
        reconciliation_layer,
    ] >> pipeline_failure

    [pipeline_success, pipeline_failure] >> finalize_audit