"""
Deterministic user-based sample of the REES46 events CSV.

Keeps ALL events of users where user_id % SAMPLE_MOD == 0. This preserves complete
user sessions and spans the whole time range of the file (unlike taking the first N rows,
which would only cover a few hours).

Usage:
    python scripts/make_sample.py data/raw/2019-Oct.csv data/sample/events_sample.csv
    (PowerShell) $env:SAMPLE_MOD=70; python scripts/make_sample.py <src> <dst>
"""
import csv
import os
import sys

PROGRESS_EVERY = 5_000_000


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python scripts/make_sample.py <source.csv> <target.csv>")
        return 1

    src, dst = sys.argv[1], sys.argv[2]
    mod = int(os.getenv("SAMPLE_MOD", "70"))
    if mod < 1:
        print("SAMPLE_MOD must be >= 1")
        return 1

    total = kept = 0
    first_ts = last_ts = None

    with open(src, newline="", encoding="utf-8") as fin, \
         open(dst, "w", newline="", encoding="utf-8") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)

        header = next(reader)
        uid_idx = header.index("user_id")
        ts_idx = header.index("event_time")
        writer.writerow(header)

        for row in reader:
            total += 1
            if int(row[uid_idx]) % mod == 0:
                writer.writerow(row)
                kept += 1
                if first_ts is None:
                    first_ts = row[ts_idx]
                last_ts = row[ts_idx]
            if total % PROGRESS_EVERY == 0:
                print(f"  processed {total:,} rows, kept {kept:,}")

    print(f"SAMPLE_MOD={mod}")
    print(f"Rows read: {total:,}")
    print(f"Rows kept: {kept:,}")
    print(f"Time range: {first_ts} -> {last_ts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
