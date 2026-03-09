"""
Bronze Layer — UPI Batch Ingestion (PySpark)
Reads raw JSON files from /data/raw/upi and writes to Delta Lake bronze/upi_transactions
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, current_timestamp, lit, col, to_timestamp
)
from delta import configure_spark_with_delta_pip

BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/delta-lake/bronze")
RAW_PATH = os.getenv("UPI_OUTPUT_DIR", "/data/raw/upi")

builder = (SparkSession.builder
           .appName("Bronze-UPI-Ingest")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.databricks.delta.retentionDurationCheck.enabled", "false"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"[Bronze-UPI] Reading raw JSON from {RAW_PATH}")

df_raw = (spark.read
          .option("multiLine", "true")
          .json(f"{RAW_PATH}/*.json")
          .withColumn("_source_file", input_file_name())
          .withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn("_layer", lit("bronze"))
          .withColumn("_pipeline", lit("upi_batch")))

row_count = df_raw.count()
print(f"[Bronze-UPI] Loaded {row_count} records")

output = f"{BRONZE_PATH}/upi_transactions"
(df_raw.write
 .format("delta")
 .mode("append")
 .partitionBy("batch_date")
 .save(output))

print(f"[Bronze-UPI] Written to Delta: {output}")
spark.stop()