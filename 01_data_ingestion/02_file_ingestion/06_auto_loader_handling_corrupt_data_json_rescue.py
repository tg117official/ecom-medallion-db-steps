from pyspark.sql.functions import current_timestamp, col

source_path = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/input/orders_json_rescue_demo/"
schema_location = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/_system/autoloader/schemas/orders_json_rescue_demo/"
checkpoint_location = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/_system/autoloader/checkpoints/orders_json_rescue_demo/"
target_table = "ecomsphere.bronze.bronze_orders_json_rescue"

base_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("city", StringType(), True),
    StructField("_rescued_data", StringType(), True)   # include rescued column in provided schema
])

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .schema(base_schema)
        .load(source_path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint_location)
      .trigger(availableNow=True)
      .toTable(target_table)
)


# Databricks documents that _rescued_data captures:
#
# fields missing from schema
# type mismatches
# case mismatches