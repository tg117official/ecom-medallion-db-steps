# ============================================================
# PySpark Shell Hands-on Lab (Structured Streaming - Files)
# Use in: pyspark (interactive shell) OR spark-shell with PySpark
#
# Goal: Practice in this exact concept order:
# 1) Read Modes (batch vs streaming)
# 2) Micro-batch execution (new files -> new batch)
# 3) Output modes (append / complete / update)
# 4) Checkpointing (restart behavior)
# 5) Trigger types (processingTime / once / availableNow)
# 6) Stateful vs Stateless (tie together)
#
# DATA:
# Use the 5 orders TSV files (orders_1.tsv ... orders_5.tsv)
# Manually drop them one-by-one into INPUT_DIR while queries run.
#
# NOTE:
# - Keep schema fixed and provide it (streaming cannot infer schema reliably).
# - In file streaming, Spark processes NEW files only (when checkpoint is used).
# - Make sure each TSV has header row, tab-separated.
# ============================================================

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, expr

# ----------------------------
# 0) Set Paths (EDIT THESE)
# ----------------------------
# Put your 5 TSV files into a "drop zone" folder gradually during the demo.
# Recommended folder structure (create manually in Windows Explorer):
#   C:\ss_demo\input\orders_tsv\    <-- drop files here one by one
#   C:\ss_demo\output\             <-- outputs go here
#   C:\ss_demo\checkpoints\        <-- checkpoints go here

BASE_DIR = r"01_data_ingestion\02_file_ingestion\data\streaming_data"  # <-- change if needed
INPUT_DIR = os.path.join(BASE_DIR, "input", "orders_tsv")
OUT_DIR = os.path.join(BASE_DIR, "output")
CKPT_DIR = os.path.join(BASE_DIR, "checkpoints")

print("INPUT_DIR =", INPUT_DIR)
print("OUT_DIR   =", OUT_DIR)
print("CKPT_DIR  =", CKPT_DIR)

# ----------------------------
# 1) Define Schema (Orders TSV)
# ----------------------------
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_ts", StringType(), True),       # parse to timestamp later
    StructField("order_status", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("city", StringType(), True),
])

# ============================================================
# EXERCISE 1: Read Modes (Batch vs Streaming)
# ============================================================

# 1A) Batch read: snapshot
# STEP:
# - Drop orders_1.tsv into INPUT_DIR before running this.
batch_df = (spark.read
            .schema(orders_schema)
            .option("header", True)
            .option("sep", "\t")
            .csv(INPUT_DIR))

batch_df.show(truncate=False)

# TEACHING POINT:
# - Now drop orders_2.tsv into INPUT_DIR and run batch_df.show() again.
# - You'll see new data only if you RE-RUN the batch read.
# - Batch read is a one-time snapshot.

# ============================================================
# EXERCISE 2: Micro-batch Execution Model (readStream + console)
# ============================================================

# 2A) Streaming read: continuous incremental processing
# IMPORTANT:
# - Provide schema explicitly.
orders_stream_raw = (spark.readStream
                     .schema(orders_schema)
                     .option("header", True)
                     .option("sep", "\t")
                     .csv(INPUT_DIR))

# Parse timestamps + add derived columns
orders_stream = (orders_stream_raw
                 .withColumn("order_ts", to_timestamp(col("order_ts"), "yyyy-MM-dd HH:mm:ss"))
                 .withColumn("order_amount", (col("quantity") * col("unit_price")).cast("double"))
                 .withColumn("ingest_ts", expr("current_timestamp()")))

# 2B) Start a console sink to SEE micro-batches
# STEP:
# - Start query, then drop orders_1.tsv, then orders_2.tsv, ... one by one.
q1 = (orders_stream.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", False)
      .trigger(processingTime="10 seconds")  # explains trigger later, but helps the demo
      .start())

# Let it run while you drop files.
# Stop it later using: q1.stop()

# ============================================================
# EXERCISE 3: Output Modes (Append vs Complete vs Update)
# We’ll use Aggregation to force statefulness.
# ============================================================

# 3A) Stateless query (Append works perfectly)
# Example: select/filter -> stateless
stateless_df = orders_stream.select("order_id", "city", "order_amount")

q2 = (stateless_df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", False)
      .trigger(processingTime="10 seconds")
      .start())

# Drop a file: you’ll see ONLY new rows printed (append).

# Stop: q2.stop()

# 3B) Stateful aggregation (groupBy) -> requires state
# Example: count orders per city
city_counts = orders_stream.groupBy("city").count()

# For aggregations:
# - append mode is NOT allowed unless you define watermark + window in many cases.
# - easiest demo: use COMPLETE mode to show full table each batch.
q3 = (city_counts.writeStream
      .format("console")
      .outputMode("complete")  # key point: complete prints entire result each batch
      .option("truncate", False)
      .trigger(processingTime="10 seconds")
      .start())

# Stop: q3.stop()

# 3C) Try UPDATE mode (shows only changed rows)
# NOTE: Some sinks support update better than console in certain versions.
q4 = (city_counts.writeStream
      .format("console")
      .outputMode("update")   # shows only updated counts each batch (if supported)
      .option("truncate", False)
      .trigger(processingTime="10 seconds")
      .start())

# Stop: q4.stop()

# ============================================================
# EXERCISE 4: Checkpointing (Restart Behavior)
# ============================================================

# Why checkpoint?
# - Without checkpoint, restart may reprocess files.
# - With checkpoint, Spark remembers progress (processed files + state).

ORDERS_CKPT_1 = os.path.join(CKPT_DIR, "orders_append_ckpt")
ORDERS_OUT_1  = os.path.join(OUT_DIR, "orders_append_parquet")

# 4A) Streaming write to Parquet WITH checkpoint
q5 = (orders_stream.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", ORDERS_OUT_1)
      .option("checkpointLocation", ORDERS_CKPT_1)
      .trigger(processingTime="10 seconds")
      .start())

# DEMO STEPS:
# 1) Let it process orders_1.tsv and orders_2.tsv.
# 2) Stop the query: q5.stop()
# 3) Start it again (run q5 definition again).
# 4) Drop orders_3.tsv -> it should process only the new file.
# 5) IMPORTANT: If you DELETE the checkpoint folder and restart,
#    Spark will treat old files as new and reprocess (classic demo).

# ============================================================
# EXERCISE 5: Trigger Types
# ============================================================

# 5A) processingTime trigger (already used above)
# - Runs every 10 seconds.

# 5B) once trigger: processes available data once and stops
# USE CASE DEMO:
# - Put 1 or 2 files in INPUT_DIR, then run this query.
ORDERS_CKPT_ONCE = os.path.join(CKPT_DIR, "orders_once_ckpt")
ORDERS_OUT_ONCE  = os.path.join(OUT_DIR, "orders_once_out")

q6 = (orders_stream.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", ORDERS_OUT_ONCE)
      .option("checkpointLocation", ORDERS_CKPT_ONCE)
      .trigger(once=True)
      .start())

# q6 will stop automatically after one micro-batch.

# 5C) availableNow trigger: process backlog and then stop (Spark 3.3+)
# If your local Spark supports it, this is very similar to Auto Loader's "catch-up" pattern.
ORDERS_CKPT_AVAIL = os.path.join(CKPT_DIR, "orders_availableNow_ckpt")
ORDERS_OUT_AVAIL  = os.path.join(OUT_DIR, "orders_availableNow_out")

q7 = (orders_stream.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", ORDERS_OUT_AVAIL)
      .option("checkpointLocation", ORDERS_CKPT_AVAIL)
      .trigger(availableNow=True)
      .start())

# If availableNow is not supported in your Spark version,
# it will throw an error. That itself is a good teaching moment:
# "Triggers depend on Spark version."

# ============================================================
# EXERCISE 6: Stateful vs Stateless (Tie Together)
# ============================================================

# 6A) Stateless: filter + select (append + no state store)
stateless_demo = orders_stream.filter(col("order_status") == "PLACED") \
                              .select("order_id", "order_status", "city", "order_amount")

q8 = (stateless_demo.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", False)
      .trigger(processingTime="10 seconds")
      .start())

# Stop: q8.stop()

# 6B) Stateful: aggregation (requires state store + checkpoint for recovery)
stateful_demo = orders_stream.groupBy("order_status").count()

ORDERS_CKPT_STATEFUL = os.path.join(CKPT_DIR, "orders_stateful_ckpt")
ORDERS_OUT_STATEFUL  = os.path.join(OUT_DIR, "orders_stateful_out")

q9 = (stateful_demo.writeStream
      .format("parquet")
      .outputMode("complete")  # complete is simplest to show full result
      .option("path", ORDERS_OUT_STATEFUL)
      .option("checkpointLocation", ORDERS_CKPT_STATEFUL)
      .trigger(processingTime="10 seconds")
      .start())

# KEY DEMO:
# - Stop q9, restart q9: counts continue correctly ONLY because of checkpoint.
# - Delete checkpoint and restart: counts rebuild from scratch and may reprocess files.

# ============================================================
# Helpful Commands (Run anytime in shell)
# ============================================================

# See all active queries:
# spark.streams.active

# Stop all:
# for qq in spark.streams.active:
#     qq.stop()

# TIP FOR CLASS:
# - Always stop earlier console queries before starting new ones,
#   otherwise students will see mixed outputs.
# ============================================================