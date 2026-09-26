"""Gold layer: daily business metrics + category funnel from silver events."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

IN = "/data/silver/events"
OUT_DAILY = "/data/gold/daily_metrics"
OUT_FUNNEL = "/data/gold/funnel_by_category"

spark = SparkSession.builder.appName("shopflow-gold-metrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(IN)

# --- 1. daily_metrics ---
daily = (silver.groupBy("event_date")
         .agg(
             F.countDistinct("user_id").alias("unique_users"),
             F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
             F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
             F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
             F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
         )
         .withColumn("avg_order_value",
                     F.round(F.col("revenue") / F.when(F.col("purchases") > 0, F.col("purchases")), 2))
         .withColumn("view_to_cart_rate",
                     F.round(100 * F.col("carts") / F.when(F.col("views") > 0, F.col("views")), 2))
         .withColumn("cart_to_purchase_rate",
                     F.round(100 * F.col("purchases") / F.when(F.col("carts") > 0, F.col("carts")), 2))
         .orderBy("event_date"))

print("=== daily_metrics ===")
daily.show(35, truncate=False)
daily.coalesce(1).write.mode("overwrite").parquet(OUT_DAILY)

# --- 2. funnel_by_category ---
funnel = (silver.filter(F.col("category_l1").isNotNull())
          .groupBy("category_l1")
          .agg(
              F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
              F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
              F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
              F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
          )
          .withColumn("view_to_cart_rate",
                      F.round(100 * F.col("carts") / F.when(F.col("views") > 0, F.col("views")), 2))
          .withColumn("cart_to_purchase_rate",
                      F.round(100 * F.col("purchases") / F.when(F.col("carts") > 0, F.col("carts")), 2))
          .orderBy(F.desc("revenue")))

print("=== funnel_by_category ===")
funnel.show(30, truncate=False)
funnel.coalesce(1).write.mode("overwrite").parquet(OUT_FUNNEL)

print("Gold write finished.")