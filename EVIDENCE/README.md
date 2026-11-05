# Evidence — captured outputs from real pipeline runs

Two reference runs are captured here, so the pipeline's results can be verified without deploying it locally.

## [`full-run-2020-2025/`](full-run-2020-2025/) — the full dataset

Every published file back to 2020, for both Yellow and Green Taxi: 144 monthly parquet files, **229,094,032 rows** end-to-end through Kafka → Spark → Iceberg.

Captured 2026-05-08 with `data-range.env` set to `YEAR_START=2020 YEAR_END=2025 MONTH_END=12 TAXI_TYPES=yellow,green`.

| File | What it shows |
|---|---|
| [smoke-output.txt](full-run-2020-2025/smoke-output.txt) | `bash scripts/query.sh smoke` output: namespaces, tables, final row counts (yellow=223,412,046, green=5,681,986). |
| [spark-final-counts.txt](full-run-2020-2025/spark-final-counts.txt) | The `FINAL ROW COUNTS` log line printed by `streaming_job.py` after both streams drained. Independent confirmation that Iceberg row counts equal the totals Spark wrote. |
| [sample-message.txt](full-run-2020-2025/sample-message.txt) | One Kafka message read from `taxi.yellow`. Wire format: hour-truncated pickup-time as key, full trip record as JSON value. |
| [minio-listing.txt](full-run-2020-2025/minio-listing.txt) | MinIO bucket layout: 1,536 yellow + 134 green parquet data files across ~73 monthly partitions per table; ~2,750 metadata files (snapshots/manifests from 900+ micro-batch commits); 3.9 GiB total bucket footprint. |

**Headline numbers**

```
nessie.taxi.green    5,681,986 rows    (Green Taxi 2020-01 → 2025-12)
nessie.taxi.yellow 223,412,046 rows    (Yellow Taxi 2020-01 → 2025-12)
total              229,094,032 rows
warehouse footprint    3.9 GiB / 4,423 objects in MinIO

Producer wall time   ~103 min (initial run, 136/144 files) + 17 sec (targeted re-run for 8 files)
Spark wall time        25 min (~9.2 M rows/min throughput)
```

## [`3-month-demo/`](3-month-demo/) — fast-demo configuration

The same pipeline run with the **default `data-range.env`** of 2024 Q1 only — the small slice that finishes in ~5 minutes. Useful to show that the architecture scales smoothly between the 5-minute slice and the 2.5-hour full run, with no code changes — only a config edit.

| File | What it shows |
|---|---|
| [smoke-output.txt](3-month-demo/smoke-output.txt) | yellow=9,554,778 + green=167,585 = 9,722,363 rows. |
| [sample-message.txt](3-month-demo/sample-message.txt) | Same Kafka wire format as the full run. |
| [minio-listing.txt](3-month-demo/minio-listing.txt) | Smaller Iceberg layout (145 MiB / 30 objects) because of the 3-month range. |

## A note on partition outliers

Both listings show a few partitions with implausible dates (`tpep_pickup_datetime_month=2001-01`, `2002-12`, etc.). These are **NYC TLC source-data quality artifacts** — the published parquet files contain a small number of rows with mis-typed pickup timestamps, and Iceberg's monthly partitioning faithfully reflects them. Not a pipeline bug. A production lakehouse would add an upstream sanity filter (e.g., `WHERE pickup_datetime BETWEEN '2020-01-01' AND CURRENT_DATE`).
