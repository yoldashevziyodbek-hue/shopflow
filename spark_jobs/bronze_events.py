"""Bronze layer: Kafka shop.events -> partitioned Parquet (raw, no cleaning)."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, DoubleType)

KAFKA = "kafka:29092"
TOPIC = "shop.events"
OUT = "/data/bronze/events"
CHECKPOINT = "/data/checkpoints/bronze_events"

schema = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
    StructField("ingested_at", StringType()),
])

spark = SparkSession.builder.appName("shopflow-bronze-events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .load())

bronze = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"),
                  F.col("partition").alias("kafka_partition"),
                  F.col("offset").alias("kafka_offset"))
          .select("e.*", "kafka_partition", "kafka_offset")
          .withColumn("event_ts",
                      F.to_timestamp(F.regexp_replace("event_time", " UTC$", ""),
                                     "yyyy-MM-dd HH:mm:ss"))
          .withColumn("event_date", F.to_date("event_ts")))

query = (bronze.writeStream
         .format("parquet")
         .option("path", OUT)
         .option("checkpointLocation", CHECKPOINT)
         .partitionBy("event_date")
         .trigger(availableNow=True)
         .start())
query.awaitTermination()
print("Bronze write finished.")