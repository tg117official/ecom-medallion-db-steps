# ==============================================================
# SINGLE TABLE CDC (orders) + JOB AUDIT
# src_oltp_sim.orders  -->  bronze.orders_cdc
# ==============================================================

from datetime import datetime, timezone
import uuid
from pyspark.sql import functions as F

CATALOG = "ecomsphere"
SRC_SCHEMA = "src_oltp_sim"
BRONZE_SCHEMA = "bronze"
META_SCHEMA = "meta"
TABLE = "orders"

WATERMARK_TABLE = f"{CATALOG}.{META_SCHEMA}.cdc_watermark"
AUDIT_TABLE = f"{CATALOG}.{META_SCHEMA}.job_audit"
SRC_TABLE = f"{CATALOG}.{SRC_SCHEMA}.{TABLE}"
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE}_cdc"

# --------------------------------------------------------------
# 1) Dynamic Run ID
# --------------------------------------------------------------
run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
run_id = f"{run_ts}_{uuid.uuid4().hex[:6]}"
pipeline_step = f"extract_{TABLE}"
start_time = datetime.now(timezone.utc)

print("CDC RUN ID:", run_id)

# --------------------------------------------------------------
# 2) Insert STARTED Audit Record
# --------------------------------------------------------------
spark.sql(f"""
INSERT INTO {AUDIT_TABLE}
VALUES (
  '{run_id}',
  '{pipeline_step}',
  'STARTED',
  TIMESTAMP('{start_time}'),
  NULL,
  NULL,
  NULL,
  'CDC extraction started'
)
""")

try:
    # ----------------------------------------------------------
    # 3) Read Watermark
    # ----------------------------------------------------------
    last_wm = (spark.table(WATERMARK_TABLE)
               .filter((F.col("source_schema") == SRC_SCHEMA) &
                       (F.col("source_table") == TABLE))
               .select("last_watermark_ts")
               .collect()[0]["last_watermark_ts"])

    print("Current watermark:", last_wm)

    # ----------------------------------------------------------
    # 4) Extract Delta Changes
    # ----------------------------------------------------------
    src_df = spark.table(SRC_TABLE).filter(F.col("updated_at") > F.lit(last_wm))
    records_read = src_df.count()

    if records_read == 0:
        print("No changes detected")

        spark.sql(f"""
        UPDATE {AUDIT_TABLE}
        SET status='SUCCESS',
            ended_at=current_timestamp(),
            records_read=0,
            records_written=0,
            message='No changes detected'
        WHERE cdc_run_id='{run_id}' AND pipeline_step='{pipeline_step}'
        """)

    else:
        # ------------------------------------------------------
        # 5) Add CDC Columns
        # ------------------------------------------------------
        cdc_df = (src_df
                  .withColumn("cdc_run_id", F.lit(run_id))
                  .withColumn("cdc_extracted_at", F.current_timestamp())
                  .withColumn("cdc_op",
                              F.when(F.col("is_deleted")==True, F.lit("SD"))
                               .otherwise(F.lit("IU")))
                 )

        ordered_cols = ["cdc_run_id","cdc_extracted_at","cdc_op"] + src_df.columns
        cdc_out = cdc_df.select(*ordered_cols)

        # ------------------------------------------------------
        # 6) Write to Bronze
        # ------------------------------------------------------
        cdc_out.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
        records_written = cdc_out.count()

        print(f"Written {records_written} rows")

        # ------------------------------------------------------
        # 7) Move Watermark
        # ------------------------------------------------------
        new_wm = cdc_out.agg(F.max("updated_at")).collect()[0][0]

        spark.sql(f"""
        UPDATE {WATERMARK_TABLE}
        SET last_watermark_ts = TIMESTAMP('{new_wm}'),
            updated_at = current_timestamp()
        WHERE source_schema='{SRC_SCHEMA}'
          AND source_table='{TABLE}'
        """)

        # ------------------------------------------------------
        # 8) SUCCESS Audit Update
        # ------------------------------------------------------
        spark.sql(f"""
        UPDATE {AUDIT_TABLE}
        SET status='SUCCESS',
            ended_at=current_timestamp(),
            records_read={records_read},
            records_written={records_written},
            message='CDC extraction successful'
        WHERE cdc_run_id='{run_id}' AND pipeline_step='{pipeline_step}'
        """)

except Exception as e:

    # ----------------------------------------------------------
    # 9) FAILED Audit Update
    # ----------------------------------------------------------
    spark.sql(f"""
    UPDATE {AUDIT_TABLE}
    SET status='FAILED',
        ended_at=current_timestamp(),
        message='Error: {str(e)}'
    WHERE cdc_run_id='{run_id}' AND pipeline_step='{pipeline_step}'
    """)

    raise