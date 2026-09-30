"""ShopFlow daily pipeline: bronze (Kafka->Parquet) -> silver (clean) -> gold (aggregates) -> quality checks."""
from datetime import datetime, timedelta, timezone

from airflow.operators.bash import BashOperator

from airflow import DAG

SPARK_SUBMIT = (
    "docker exec shopflow-spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.cores.max=2 --conf spark.executor.memory=1g "
)

default_args = {
    "owner": "ziyodbek",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="shopflow_pipeline",
    description="Bronze -> Silver -> Gold -> Quality checks for shop.events",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 26, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["shopflow"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_events",
        bash_command=(
            SPARK_SUBMIT
            + "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 "
            + "--conf spark.jars.ivy=/tmp/.ivy "
            + "/opt/spark-jobs/bronze_events.py"
        ),
    )

    silver = BashOperator(
        task_id="silver_events",
        bash_command=SPARK_SUBMIT + "/opt/spark-jobs/silver_events.py",
    )

    gold = BashOperator(
        task_id="gold_metrics",
        bash_command=SPARK_SUBMIT + "/opt/spark-jobs/gold_metrics.py",
    )

    quality = BashOperator(
        task_id="data_quality_tests",
        bash_command=SPARK_SUBMIT + "/opt/spark-jobs/data_quality_tests.py",
    )

    bronze >> silver >> gold >> quality