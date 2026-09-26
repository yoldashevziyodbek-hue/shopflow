from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("shopflow-check-bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/data/bronze/events")
total = df.count()
print(f"TOTAL ROWS: {total:,}")

df.groupBy("event_date").count().orderBy("event_date").show(35, truncate=False)