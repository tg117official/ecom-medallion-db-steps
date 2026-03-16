from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# DAG: dag_03_parallel_bronze_ingestion
# Purpose:
#   Demonstrate parallel ingestion orchestration in Airflow.
#
# Flow:
#   source readiness check
#           |
#           +-------------------------------+
#           |                               |
#           v                               v
#   RDBMS -> bronze_orders        ADLS JSON -> bronze_order_event_log
#           |                               |
#           +---------------+---------------+
#                           |
#                           v
#                        join task
#                           |
#                           v
#                          end
#
# goal:
#   Show that independent source ingestion branches can run in parallel.
# ============================================================


DAG_ID = "dag_03_parallel_bronze_ingestion"
DATABRICKS_CONN_ID = "databricks_default"

# Replace these with your actual Databricks Job IDs
BRONZE_ORDERS_RDBMS_JOB_ID = 44444444444444
BRONZE_ORDER_EVENT_LOG_AUTOLOADER_JOB_ID = 55555555555555


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def source_readiness_check():
    """
    Simple placeholder readiness check for DAG 3.

    In a real project, this step could verify:
    - RDBMS source connectivity
    - ADLS landing path availability
    - expected file arrival
    - control table readiness

    For DAG 3, we keep it intentionally simple for teaching.
    """
    print("Source readiness check started...")
    print("RDBMS source assumed available.")
    print("ADLS JSON source assumed available.")
    print("Source readiness check passed.")


with DAG(
    dag_id=DAG_ID,
    description="DAG 3 - Parallel Bronze ingestion from RDBMS and ADLS JSON",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="0 */6 * * *",   # every 6 hours
    catchup=False,
    tags=["ecomsphere", "bronze", "parallel", "databricks", "dag03"],
) as dag:

    # ------------------------------------------------------------
    # Source readiness check
    # ------------------------------------------------------------
    source_check = PythonOperator(
        task_id="source_readiness_check",
        python_callable=source_readiness_check,
    )

    # ------------------------------------------------------------
    # Parallel branch 1:
    # RDBMS -> bronze_orders
    # ------------------------------------------------------------
    ingest_bronze_orders = DatabricksRunNowOperator(
        task_id="run_bronze_orders_rdbms_ingestion_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_ORDERS_RDBMS_JOB_ID,
    )

    # ------------------------------------------------------------
    # Parallel branch 2:
    # ADLS JSON / Auto Loader -> bronze_order_event_log
    # ------------------------------------------------------------
    ingest_bronze_order_event_log = DatabricksRunNowOperator(
        task_id="run_bronze_order_event_log_autoloader_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_ORDER_EVENT_LOG_AUTOLOADER_JOB_ID,
    )

    # ------------------------------------------------------------
    # Join task (fan-in point)
    # Waits for both parallel branches to complete successfully
    # ------------------------------------------------------------
    join_parallel_ingestion = EmptyOperator(
        task_id="join_parallel_ingestion"
    )

    # ------------------------------------------------------------
    # End task
    # ------------------------------------------------------------
    end = EmptyOperator(
        task_id="end"
    )

    # ------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------
    source_check >> [ingest_bronze_orders, ingest_bronze_order_event_log]
    [ingest_bronze_orders, ingest_bronze_order_event_log] >> join_parallel_ingestion >> end