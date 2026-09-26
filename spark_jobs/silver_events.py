"""Silver layer: clean & enrich bronze events -> partitioned Parquet."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

IN = "/data/bronze/events"
OUT = "/data/silver/events"

spark = SparkSession.builder.appName("shopflow-silver-events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet(IN)
before = bronze.count()

# 1. Drop invalid rows: missing keys or non-positive price
valid = bronze.filter(
    F.col("user_id").isNotNull()
    & F.col("product_id").isNotNull()
    & F.col("event_type").isin("view", "cart", "purchase")
    & (F.col("price") > 0)
)

# 2. Drop exact duplicates (same user, session, product, timestamp, type)
deduped = valid.dropDuplicates(
    ["user_id", "user_session", "product_id", "event_time", "event_type"]
)

# 3. Split category_code into levels (e.g. "electronics.smartphone" -> l1, l2)
parts = F.split(F.col("category_code"), r"\.")
silver = (deduped
          .withColumn("category_l1", parts.getItem(0))
          .withColumn("category_l2", parts.getItem(1))
          .withColumn("category_l3", parts.getItem(2))
          .drop("kafka_partition", "kafka_offset", "ingested_at"))

after = silver.count()
print(f"Bronze rows: {before:,}")
print(f"Silver rows: {after:,}  (dropped {before - after:,})")

silver.groupBy("event_type").count().orderBy(F.desc("count")).show()

(silver.write
       .mode("overwrite")
       .partitionBy("event_date")
       .parquet(OUT))

print("Silver write finished.")