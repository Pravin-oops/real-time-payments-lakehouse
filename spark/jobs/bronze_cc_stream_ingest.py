"""
Bronze Layer — Credit Card Streaming Ingest (PySpark Structured Streaming)
Reads from Kafka topic credit-card-swipes and writes to Delta Lake bronze/cc_swipes
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
)

BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/delta-lake/bronze")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("CC_SWIPE_TOPIC", "credit-card-swipes")
TRIGGER = os.getenv("STREAMING_TRIGGER_INTERVAL", "30 seconds")
CHECKPOINT = f"{BRONZE_PATH}/checkpoints/cc_stream"

CC_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("card_token", StringType()),
    StructField("card_type", StringType()),
    StructField("last_four", StringType()),
    StructField("cardholder_name", StringType()),
    StructField("merchant_name", StringType()),
    StructField("merchant_id", StringType()),
    StructField("mcc", StringType()),
    StructField("mcc_description", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("terminal_id", StringType()),
    StructField("pos_entry_mode", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("zip_code", StringType()),
    StructField("approval_code", StringType()),
    StructField("response_code", StringType()),
    StructField("is_declined", BooleanType()),
    StructField("fraud_score", DoubleType()),
    StructField("network_latency_ms", IntegerType()),
])

spark = (SparkSession.builder
         .appName("Bronze-CC-Stream")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.streaming.schemaInference", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw_stream = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_SERVERS)
              .option("subscribe", TOPIC)
              .option("startingOffsets", "latest")
              .option("failOnDataLoss", "false")
              .load())

parsed = (raw_stream
          .select(from_json(col("value").cast("string"), CC_SCHEMA).alias("data"),
                  col("timestamp").alias("kafka_timestamp"),
                  col("partition"),
                  col("offset"))
          .select("data.*", "kafka_timestamp", "partition", "offset")
          .withColumn("event_time", to_timestamp(col("event_time")))
          .withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn("_layer", lit("bronze"))
          .withColumn("_pipeline", lit("cc_stream"))
          .withColumn("event_date", col("event_time").cast("date")))

query = (parsed.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", CHECKPOINT)
         .option("mergeSchema", "true")
         .partitionBy("event_date")
         .trigger(processingTime=TRIGGER)
         .start(f"{BRONZE_PATH}/cc_swipes"))

print(f"[Bronze-CC] Streaming query started. Checkpoint: {CHECKPOINT}")
query.awaitTermination()