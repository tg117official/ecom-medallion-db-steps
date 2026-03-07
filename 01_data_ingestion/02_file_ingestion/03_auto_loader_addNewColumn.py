from pyspark.sql.functions import current_timestamp, col

source_path = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/input/orders_tsv/"
schema_location = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/_system/autoloader/schemas/orders_tsv/"
checkpoint_location = "abfss://data@tgdemodata.dfs.core.windows.net/streaming_data/_system/autoloader/checkpoints/orders_tsv/"
target_table = "ecomsphere.bronze.bronze_orders_tsv"

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("sep", "\t")
        .option("header", "true")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df.writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_location)
      .option("mergeSchema", "true")
      .outputMode("append")
      .trigger(availableNow=True)
      .toTable(target_table)
)