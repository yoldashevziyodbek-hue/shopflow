"""Data quality checks for silver and gold layers. Exits non-zero on any failure."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "/data/silver/events"
GOLD_DAILY = "/data/gold/daily_metrics"
GOLD_FUNNEL = "/data/gold/funnel_by_category"

spark = SparkSession.builder.appName("shopflow-data-quality").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

failures = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name} {detail}")
    if not condition:
        failures.append(name)


# --- Silver checks ---
silver = spark.read.parquet(SILVER)
silver_count = silver.count()

check("silver: has rows", silver_count > 0, f"(count={silver_count:,})")

check("silver: no null user_id",
      silver.filter(F.col("user_id").isNull()).count() == 0)

check("silver: no null product_id",
      silver.filter(F.col("product_id").isNull()).count() == 0)

check("silver: no non-positive price",
      silver.filter(F.col("price") <= 0).count() == 0)

check("silver: event_type only view/cart/purchase",
      silver.filter(~F.col("event_type").isin("view", "cart", "purchase")).count() == 0)

dup_count = (silver.groupBy("user_id", "user_session", "product_id",
                             "event_time", "event_type")
             .count().filter("count > 1").count())
check("silver: no duplicate events", dup_count == 0, f"(dup groups={dup_count})")

check("silver: event_date within October 2019",
      silver.filter((F.col("event_date") < "2019-10-01")
                     | (F.col("event_date") > "2019-10-31")).count() == 0)

# --- Gold checks ---
daily = spark.read.parquet(GOLD_DAILY)
daily_days = daily.count()
check("gold daily_metrics: has 31 days", daily_days == 31, f"(days={daily_days})")

check("gold daily_metrics: no negative revenue",
      daily.filter(F.col("revenue") < 0).count() == 0)

check("gold daily_metrics: unique_users positive every day",
      daily.filter(F.col("unique_users") <= 0).count() == 0)

funnel = spark.read.parquet(GOLD_FUNNEL)
funnel_cats = funnel.count()
check("gold funnel_by_category: has categories", funnel_cats > 0, f"(categories={funnel_cats})")

check("gold funnel_by_category: no negative revenue",
      funnel.filter(F.col("revenue") < 0).count() == 0)

# --- Cross-layer check ---
gold_total_purchases = daily.agg(F.sum("purchases")).first()[0]
silver_purchases = silver.filter(F.col("event_type") == "purchase").count()
check("cross-layer: gold purchases match silver purchases",
      gold_total_purchases == silver_purchases,
      f"(gold={gold_total_purchases:,}, silver={silver_purchases:,})")

print()
if failures:
    print(f"RESULT: {len(failures)} check(s) FAILED: {failures}")
    spark.stop()
    raise SystemExit(1)
else:
    print("RESULT: all checks passed")
    spark.stop()