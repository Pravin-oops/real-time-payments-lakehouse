"""
Silver Layer — UPI Transformation (PySpark)
Reads bronze/upi_transactions → clean, standardise, enrich → silver/upi_transactions
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, to_timestamp, current_timestamp,
    lit, regexp_replace, round as spark_round, hash, abs as spark_abs
)
from delta.tables import DeltaTable

BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/delta-lake/bronze")
SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "/delta-lake/silver")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_SCORE_THRESHOLD", 0.75))
MIN_AMOUNT = float(os.getenv("MIN_TXN_AMOUNT", 1.0))

spark = (SparkSession.builder
         .appName("Silver-UPI-Transform")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.format("delta").load(f"{BRONZE_PATH}/upi_transactions")

silver_df = (bronze_df
    .filter(col("amount") >= MIN_AMOUNT)
    .filter(col("txn_id").isNotNull())
    .withColumn("txn_id", trim(col("txn_id")))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("sender_bank", upper(trim(col("sender_bank"))))
    .withColumn("receiver_bank", upper(trim(col("receiver_bank"))))
    .withColumn("amount_inr", spark_round(col("amount").cast("double"), 2))
    .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    .withColumn("is_failed", when(col("status") == "FAILED", True).otherwise(False))
    .withColumn("is_large_txn", when(col("amount_inr") > 10000, True).otherwise(False))
    .withColumn("txn_hash", spark_abs(hash(col("txn_id"))).cast("string"))
    .withColumn("_silver_timestamp", current_timestamp())
    .withColumn("_layer", lit("silver"))
    .drop("_source_file", "timestamp", "amount")
    .dropDuplicates(["txn_id"]))

silver_output = f"{SILVER_PATH}/upi_transactions"

if DeltaTable.isDeltaTable(spark, silver_output):
    dt = DeltaTable.forPath(spark, silver_output)
    (dt.alias("existing")
       .merge(silver_df.alias("updates"), "existing.txn_id = updates.txn_id")
       .whenNotMatchedInsertAll()
       .execute())
    print(f"[Silver-UPI] Upserted into {silver_output}")
else:
    (silver_df.write.format("delta").mode("overwrite")
     .partitionBy("batch_date").save(silver_output))
    print(f"[Silver-UPI] Created {silver_output}")

spark.stop()