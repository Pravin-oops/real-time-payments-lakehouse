"""
Silver Layer — Credit Card Transformation (PySpark)
Reads bronze/cc_swipes → clean, flag fraud, deduplicate → silver/cc_swipes
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp, lit, round as spark_round
)
from delta.tables import DeltaTable

BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/delta-lake/bronze")
SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "/delta-lake/silver")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_SCORE_THRESHOLD", 0.75))

spark = (SparkSession.builder
         .appName("Silver-CC-Transform")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.format("delta").load(f"{BRONZE_PATH}/cc_swipes")

silver_df = (bronze_df
    .filter(col("event_id").isNotNull())
    .filter(col("amount") > 0)
    .withColumn("card_type", upper(trim(col("card_type"))))
    .withColumn("pos_entry_mode", upper(trim(col("pos_entry_mode"))))
    .withColumn("amount_usd", spark_round(col("amount"), 2))
    .withColumn("is_fraud_flagged",
                when((col("fraud_score") >= FRAUD_THRESHOLD) | col("is_declined"), True)
                .otherwise(False))
    .withColumn("fraud_tier",
                when(col("fraud_score") >= 0.9, "HIGH")
                .when(col("fraud_score") >= FRAUD_THRESHOLD, "MEDIUM")
                .otherwise("LOW"))
    .withColumn("txn_status",
                when(col("is_declined"), "DECLINED").otherwise("APPROVED"))
    .withColumn("_silver_timestamp", current_timestamp())
    .withColumn("_layer", lit("silver"))
    .drop("amount", "is_declined", "partition", "offset", "kafka_timestamp")
    .dropDuplicates(["event_id"]))

silver_output = f"{SILVER_PATH}/cc_swipes"

if DeltaTable.isDeltaTable(spark, silver_output):
    dt = DeltaTable.forPath(spark, silver_output)
    (dt.alias("existing")
       .merge(silver_df.alias("new"), "existing.event_id = new.event_id")
       .whenNotMatchedInsertAll()
       .execute())
else:
    (silver_df.write.format("delta").mode("overwrite")
     .partitionBy("event_date").save(silver_output))

print(f"[Silver-CC] Written to {silver_output}")
spark.stop()