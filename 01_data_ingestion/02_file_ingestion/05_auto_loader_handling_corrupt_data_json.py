from pyspark.sql.functions import current_timestamp, col

source_path = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/input/orders_json_rescue/"
schema_location = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/_system/autoloader/schemas/orders_json_rescue/"
checkpoint_location = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/_system/autoloader/checkpoints/orders_json_rescue/"
target_table = "ecomsphere.bronze.bronze_orders_json_rescue"

schema_hints = """
order_id STRING,
customer_id STRING,
product_id STRING,
order_ts STRING,
order_status STRING,
quantity INT,
unit_price DOUBLE,
payment_method STRING,
city STRING
"""

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaHints", schema_hints)
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df.writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_location)
      .outputMode("append")
      .trigger(processingTime="10 seconds")
      .toTable(target_table)
)