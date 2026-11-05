#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-submit \
    --master "local[*]" \
    --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-2g}" \
    --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-2g}" \
    --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS:-8}" \
    /opt/spark/work-dir/streaming_job.py
