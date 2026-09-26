# ShopFlow — Streaming E-Commerce Data Pipeline

End-to-end data engineering portfolio project: simulates real-time e-commerce
clickstream events, ingests them through Kafka, processes them with Spark
(streaming + batch), and orchestrates the daily pipeline with Airflow.

Built on a real 42M-row e-commerce clickstream dataset
([REES46, via Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)),
sampled down to ~565K rows by user (not by row count), preserving full user
session history across the whole month.

## Architecture
producer.py (Python, confluent-kafka)
│ replays REES46 sample CSV, keyed by user_id, in event_time order
▼
Kafka topic: shop.events (3 partitions)
│
▼
Spark Structured Streaming ──▶ Bronze layer (Parquet, partitioned by event_date)
│ raw JSON parsed into typed columns, Kafka offsets kept for traceability
▼
Spark batch job ──▶ Silver layer (Parquet, partitioned by event_date)
│ drops invalid rows (bad price, missing keys) and exact duplicates
│ splits category_code into category_l1 / l2 / l3
▼
Spark batch job ──▶ Gold layer (Parquet)
│ daily_metrics: unique users, views/carts/purchases, revenue, funnel rates
│ funnel_by_category: same funnel broken out by top-level category
▼
Airflow DAG (shopflow_pipeline, @daily)
orchestrates bronze -> silver -> gold as a single idempotent chain


## Stack

| Layer          | Tool                                  |
|----------------|----------------------------------------|
| Ingestion      | Kafka 3.9 (KRaft mode), Python producer (confluent-kafka) |
| Processing     | Apache Spark 3.5.9 (Structured Streaming + batch) |
| Orchestration  | Apache Airflow 2.10 (LocalExecutor, Postgres metadata DB) |
| Storage        | Parquet on local volume (bronze/silver/gold) |
| Infra          | Docker Compose (Kafka, Spark master/worker, Airflow) |

## Results (October 2019 sample, ~565K events)

- **562,896** clean events after silver-layer validation (1,324 dropped: bad
  price or duplicate)
- **541,057** views / **11,991** carts / **9,848** purchases
- **electronics** drives ~85% of total revenue across 13 categories
- Daily active users stayed steady in the 2,200–3,150 range across the month

> Note: `cart_to_purchase_rate` exceeds 100% on several days/categories. This
> reflects a known property of the REES46 dataset: many `purchase` events
> occur without a matching `cart` event in the same session (direct
> "buy now" behavior), so purchases can outnumber carts.

## Running it

```bash
# 1. Start Kafka + Spark
docker compose up -d

# 2. Start Airflow (separate compose file, same network)
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build

# 3. Create the sample (from a raw REES46 CSV in data/raw/)
$env:SAMPLE_MOD = 75   # ~1/75th of users, preserving full sessions
python scripts/make_sample.py data/raw/2019-Oct.csv data/sample/events_sample.csv

# 4. Replay the sample into Kafka
python producer/producer.py --speed 0   # --speed 1440 for a slowed-down real-time demo

# 5. Trigger the pipeline
# Open http://localhost:8088 (admin/admin), unpause and trigger `shopflow_pipeline`
```

## Project layout

producer/ Kafka producer replaying the sample CSV
scripts/ make_sample.py — deterministic user-based sampling
spark_jobs/ bronze_events.py, silver_events.py, gold_metrics.py
airflow/dags/ shopflow_pipeline.py — daily DAG orchestrating the 3 Spark jobs
data/ raw/ (input CSV, gitignored) · sample/ · bronze/ · silver/ · gold/
docker-compose.yml Kafka + Spark
docker-compose.airflow.yml Airflow (Postgres, webserver, scheduler)
Dockerfile.airflow Airflow image with docker CLI (for spark-submit via docker exec)


## Status / next steps

- [x] Kafka ingestion + Spark Structured Streaming (bronze)
- [x] Batch cleaning (silver) and business aggregates (gold)
- [x] Airflow orchestration of the daily pipeline
- [ ] dbt + Snowflake semantic layer
- [ ] Automated data-quality tests
- [ ] CI/CD (GitHub Actions)