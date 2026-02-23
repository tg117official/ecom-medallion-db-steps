# ==============================================================
# ALL TABLES CDC (Query-based, updated_at watermark) + JOB AUDIT
# Source: ecomsphere.src_oltp_sim.<table>
# Target: ecomsphere.bronze.<table>_cdc   (append-only CDC log)
# Meta:   ecomsphere.meta.cdc_watermark   (per table)
# Audit:  ecomsphere.meta.job_audit       (per run + step)
# ==============================================================

from datetime import datetime, timezone
import uuid
from pyspark.sql import functions as F

CATALOG = "ecomsphere"
SRC_SCHEMA = "src_oltp_sim"
BRONZE_SCHEMA = "bronze"
META_SCHEMA = "meta"

WATERMARK_TABLE = f"{CATALOG}.{META_SCHEMA}.cdc_watermark"
AUDIT_TABLE = f"{CATALOG}.{META_SCHEMA}.job_audit"

tables = [
    "brand",
    "category",
    "warehouse",
    "customer",
    "customer_address",
    "product",
    "product_variant",
    "inventory_level",
    "orders",
    "order_item",
    "payment",
    "shipment",
]

# --------------------------------------------------------------
# 1) Dynamic run_id for the whole job run (Airflow-friendly)
# --------------------------------------------------------------
run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
run_id = f"{run_ts}_{uuid.uuid4().hex[:6]}"
job_start = datetime.now(timezone.utc)

print("CDC RUN ID:", run_id)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{META_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# --------------------------------------------------------------
# 2) Ensure meta tables exist (safe)
# --------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
  source_schema STRING,
  source_table  STRING,
  watermark_col STRING,
  last_watermark_ts TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
  cdc_run_id STRING,
  pipeline_step STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  records_read BIGINT,
  records_written BIGINT,
  message STRING
) USING DELTA
""")

# --------------------------------------------------------------
# 3) Ensure watermark rows exist for all tables (no MERGE aliases)
# --------------------------------------------------------------
wm_seed_df = (
    spark.createDataFrame(
        [(SRC_SCHEMA, t, "updated_at") for t in tables],
        "source_schema STRING, source_table STRING, watermark_col STRING"
    )
    .withColumn("last_watermark_ts", F.to_timestamp(F.lit("1900-01-01 00:00:00")))
    .withColumn("updated_at", F.current_timestamp())
)

existing_wm = spark.table(WATERMARK_TABLE).select("source_schema", "source_table")
to_insert = wm_seed_df.join(existing_wm, ["source_schema", "source_table"], "left_anti")

if to_insert.take(1):
    (to_insert
     .select("source_schema", "source_table", "watermark_col", "last_watermark_ts", "updated_at")
     .write.format("delta").mode("append").saveAsTable(WATERMARK_TABLE))

# --------------------------------------------------------------
# 4) Helpers: audit insert/update
# --------------------------------------------------------------
def audit_started(step: str):
    spark.sql(f"""
    INSERT INTO {AUDIT_TABLE}
    VALUES (
      '{run_id}',
      '{step}',
      'STARTED',
      current_timestamp(),
      NULL,
      NULL,
      NULL,
      'Started'
    )
    """)

def audit_success(step: str, records_read: int, records_written: int, message: str):
    spark.sql(f"""
    UPDATE {AUDIT_TABLE}
    SET status='SUCCESS',
        ended_at=current_timestamp(),
        records_read={records_read},
        records_written={records_written},
        message="{message}"
    WHERE cdc_run_id='{run_id}' AND pipeline_step='{step}'
    """)

def audit_failed(step: str, err: str):
    # Escape double quotes in error to avoid breaking SQL string
    safe_err = err.replace('"', "'")
    spark.sql(f"""
    UPDATE {AUDIT_TABLE}
    SET status='FAILED',
        ended_at=current_timestamp(),
        message="Error: {safe_err}"
    WHERE cdc_run_id='{run_id}' AND pipeline_step='{step}'
    """)

# --------------------------------------------------------------
# 5) Run CDC for each table
# --------------------------------------------------------------
for t in tables:
    step = f"extract_{t}"
    audit_started(step)

    try:
        src_table = f"{CATALOG}.{SRC_SCHEMA}.{t}"
        bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{t}_cdc"

        # Read watermark for this table
        last_wm = (spark.table(WATERMARK_TABLE)
                   .filter((F.col("source_schema") == SRC_SCHEMA) & (F.col("source_table") == t))
                   .select("last_watermark_ts")
                   .collect()[0]["last_watermark_ts"])

        # Extract changes
        src_df = spark.table(src_table).filter(F.col("updated_at") > F.lit(last_wm))

        # If no changes, mark success and continue
        if src_df.rdd.isEmpty():
            audit_success(step, 0, 0, f"No changes since watermark {last_wm}")
            continue

        # Build CDC output
        cdc_df = (
            src_df
            .withColumn("cdc_run_id", F.lit(run_id))
            .withColumn("cdc_extracted_at", F.current_timestamp())
            .withColumn("cdc_op", F.when(F.col("is_deleted") == True, F.lit("SD")).otherwise(F.lit("IU")))
        )

        # Maintain column order: cdc cols + source cols
        ordered_cols = ["cdc_run_id", "cdc_extracted_at", "cdc_op"] + src_df.columns
        cdc_out = cdc_df.select(*ordered_cols).cache()

        records_written = cdc_out.count()
        records_read = records_written  # we read the same rows we wrote in query-based CDC

        # Append to bronze
        cdc_out.write.format("delta").mode("append").saveAsTable(bronze_table)

        # Move watermark to max(updated_at) from this batch
        new_wm = cdc_out.agg(F.max("updated_at").alias("max_updated_at")).collect()[0]["max_updated_at"]

        spark.sql(f"""
        UPDATE {WATERMARK_TABLE}
        SET last_watermark_ts = TIMESTAMP('{new_wm}'),
            updated_at = current_timestamp()
        WHERE source_schema='{SRC_SCHEMA}'
          AND source_table='{t}'
        """)

        audit_success(step, records_read, records_written,
                      f"Wrote {records_written} rows. Watermark {last_wm} -> {new_wm}")

    except Exception as e:
        audit_failed(step, str(e))
        raise

print("All-tables CDC completed. RUN ID:", run_id)

# --------------------------------------------------------------
# 6) Quick visibility (optional)
# --------------------------------------------------------------
display(
    spark.table(AUDIT_TABLE)
         .filter(F.col("cdc_run_id") == run_id)
         .orderBy("pipeline_step")
)

display(
    spark.table(WATERMARK_TABLE)
         .filter(F.col("source_schema") == SRC_SCHEMA)
         .orderBy("source_table")
)