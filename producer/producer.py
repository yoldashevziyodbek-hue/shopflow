"""
Replay the REES46 sample CSV into Kafka as JSON events.

Events are sent in event_time order. --speed controls how fast simulated time passes
relative to wall-clock time (1440 = one simulated day per real minute; 0 = no throttling).
Messages are keyed by user_id, so all events of one user go to the same partition
and keep their order.

Usage (from the repo root):
    python producer/producer.py --speed 1440
    python producer/producer.py --max-rows 10000 --speed 0     # quick smoke test
"""
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

DEFAULT_SOURCE = "data/sample/events_sample.csv"
INT_FIELDS = ("product_id", "category_id", "user_id")
FLOAT_FIELDS = ("price",)
PROGRESS_EVERY = 50_000


def parse_event_time(value: str) -> datetime:
    """'2019-10-01 00:00:04 UTC' -> timezone-aware datetime."""
    naive = datetime.strptime(value.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")
    return naive.replace(tzinfo=timezone.utc)


def row_to_event(row: dict) -> dict:
    """Convert a CSV row into a typed event. Empty strings become null."""
    event = {}
    for key, value in row.items():
        if value == "":
            event[key] = None
        elif key in INT_FIELDS:
            event[key] = int(value)
        elif key in FLOAT_FIELDS:
            event[key] = float(value)
        else:
            event[key] = value
    event["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay sample events into Kafka")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_EVENTS", "shop.events"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:9092"))
    parser.add_argument("--speed", type=float, default=1440.0,
                        help="simulated seconds per real second (0 = as fast as possible)")
    parser.add_argument("--max-rows", type=int, default=0, help="stop after N rows (0 = all)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})
    stats = {"sent": 0, "delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            if stats["failed"] <= 5:
                print(f"Delivery failed: {err}", file=sys.stderr)
        else:
            stats["delivered"] += 1

    first_event_time = None
    wall_start = time.monotonic()

    try:
        with open(args.source, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                event_time = parse_event_time(row["event_time"])
                if first_event_time is None:
                    first_event_time = event_time

                if args.speed > 0:
                    target = (event_time - first_event_time).total_seconds() / args.speed
                    delay = target - (time.monotonic() - wall_start)
                    if delay > 0:
                        time.sleep(delay)

                payload = json.dumps(row_to_event(row)).encode("utf-8")
                key = row["user_id"].encode("utf-8")
                while True:
                    try:
                        producer.produce(args.topic, key=key, value=payload, on_delivery=on_delivery)
                        break
                    except BufferError:  # local queue full: let delivery callbacks drain it
                        producer.poll(1)
                producer.poll(0)

                stats["sent"] += 1
                if stats["sent"] % PROGRESS_EVERY == 0:
                    print(f"sent={stats['sent']:,} delivered={stats['delivered']:,} "
                          f"simulated_time={event_time.isoformat()}")
                if args.max_rows and stats["sent"] >= args.max_rows:
                    break
    except KeyboardInterrupt:
        print("Interrupted, flushing...")

    remaining = producer.flush(30)
    print(f"Done: sent={stats['sent']:,} delivered={stats['delivered']:,} "
          f"failed={stats['failed']:,} unflushed={remaining}")
    return 0 if stats["failed"] == 0 and remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

