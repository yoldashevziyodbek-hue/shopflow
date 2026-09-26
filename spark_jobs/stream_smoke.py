"""Smoke test: read shop.events from Kafka, parse JSON, show what Spark sees."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, DoubleType)

KAFKA = "kafka:29092"
TOPIC = "shop.events"

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

spark = (SparkSession.builder.appName("shopflow-stream-smoke").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .load())

events = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_ts",
                      F.to_timestamp(F.regexp_replace("event_time", " UTC$", ""),
                                     "yyyy-MM-dd HH:mm:ss")))

def show_batch(df, batch_id):
    print(f"=== batch {batch_id}: {df.count():,} rows ===")
    df.printSchema()
    df.show(5, truncate=False)
    df.groupBy("event_type").count().orderBy(F.desc("count")).show()
    print("null event_ts (parse failures):", df.filter("event_ts IS NULL").count())

query = (events.writeStream
         .foreachBatch(show_batch)
         .trigger(availableNow=True)
         .start())
query.awaitTermination()