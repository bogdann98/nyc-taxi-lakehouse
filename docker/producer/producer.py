"""
NYC TLC parquet -> Kafka producer.

For each taxi type in TAXI_TYPES and each (year, month) in
[YEAR_START..YEAR_END][MONTH_START..MONTH_END], downloads the monthly parquet
from PARQUET_BASE_URL and streams its rows as JSON messages onto Kafka topic
`taxi.<type>`. Message key is the pickup datetime truncated to the hour, which
clusters related rows on the same partition for downstream Iceberg compaction.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

import pyarrow.parquet as pq
import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("producer")


# ── env / config ──────────────────────────────────────────────────────────────

def env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise SystemExit(f"missing required env var: {name}")
    return val.strip()


def parse_taxi_types(raw: str) -> list[str]:
    """Tolerate whitespace + casing — config file is hand-edited."""
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def month_range(y0: int, m0: int, y1: int, m1: int) -> Iterator[tuple[int, int]]:
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        yield y, m
        m += 1
        if m > 12:
            y, m = y + 1, 1


# ── parquet handling ──────────────────────────────────────────────────────────

# Schema quirk: Yellow uses `tpep_*`, Green uses `lpep_*`.
PICKUP_COL = {
    "yellow": "tpep_pickup_datetime",
    "green":  "lpep_pickup_datetime",
}


def download(url: str, dest: Path) -> bool:
    """
    Stream-download a parquet file with retries on transient errors.
    Returns False on 404 (file truly not published — skip).
    Retries on 403 / 5xx / network errors (CloudFront occasionally rate-limits).
    """
    import time
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        log.info("downloading %s (attempt %d/%d)", url, attempt, max_attempts)
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                if r.status_code == 404:
                    log.warning("not found (404): %s — skipping", url)
                    return False
                if r.status_code in (403, 429) or r.status_code >= 500:
                    # Transient — back off and retry
                    if attempt == max_attempts:
                        log.warning("giving up on %s after %d attempts (HTTP %d) — skipping",
                                    url, max_attempts, r.status_code)
                        return False
                    backoff = 2 ** attempt   # 2, 4, 8, 16 sec
                    log.warning("transient HTTP %d on %s — retrying in %ds", r.status_code, url, backoff)
                    time.sleep(backoff)
                    continue
                r.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            f.write(chunk)
            log.info("downloaded %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
            return True
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == max_attempts:
                log.warning("network error on %s after %d attempts: %s — skipping",
                            url, max_attempts, e)
                return False
            backoff = 2 ** attempt
            log.warning("network error %s on %s — retrying in %ds", type(e).__name__, url, backoff)
            time.sleep(backoff)
    return False


def hour_key(ts) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%dT%H")
    if isinstance(ts, date):
        return ts.strftime("%Y-%m-%dT00")
    return str(ts)[:13]


def json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


# ── streaming ─────────────────────────────────────────────────────────────────

def delivery_cb(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)


def stream_file(
    producer: Producer,
    parquet_path: Path,
    topic: str,
    taxi_type: str,
    batch_size: int,
) -> int:
    pickup = PICKUP_COL[taxi_type]
    pf = pq.ParquetFile(parquet_path)
    total = 0
    log_every = batch_size * 50  # ~ every 50k rows

    for batch in pf.iter_batches(batch_size=batch_size):
        rows = batch.to_pylist()
        for row in rows:
            key = hour_key(row.get(pickup))
            value = json.dumps(row, default=json_default)
            # Retry once if the local queue is full (back-pressure from broker).
            try:
                producer.produce(topic=topic, key=key, value=value, on_delivery=delivery_cb)
            except BufferError:
                producer.poll(1.0)
                producer.produce(topic=topic, key=key, value=value, on_delivery=delivery_cb)
        total += len(rows)
        producer.poll(0)
        if total % log_every == 0:
            log.info("[%s] produced %d rows", taxi_type, total)

    producer.flush()
    log.info("[%s] FILE DONE %s — %d rows", taxi_type, parquet_path.name, total)
    return total


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    bootstrap = env("KAFKA_BOOTSTRAP", "kafka.data.svc.cluster.local:9092")
    base_url   = env("PARQUET_BASE_URL")
    taxi_types = parse_taxi_types(env("TAXI_TYPES"))
    y0, m0 = int(env("YEAR_START")), int(env("MONTH_START"))
    y1, m1 = int(env("YEAR_END")),   int(env("MONTH_END"))
    batch_size = int(env("BATCH_SIZE", "1000"))

    unknown = [t for t in taxi_types if t not in PICKUP_COL]
    if unknown:
        raise SystemExit(f"unsupported taxi types: {unknown} (known: {list(PICKUP_COL)})")

    log.info(
        "bootstrap=%s  taxi_types=%s  range=%d-%02d..%d-%02d  batch=%d",
        bootstrap, taxi_types, y0, m0, y1, m1, batch_size,
    )

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 50,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 200_000,
        "queue.buffering.max.kbytes": 524_288,
    })

    grand_total = 0
    with tempfile.TemporaryDirectory() as tmp:
        for taxi_type in taxi_types:
            topic = f"taxi.{taxi_type}"
            for year, month in month_range(y0, m0, y1, m1):
                fname = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
                local = Path(tmp) / fname
                if not download(f"{base_url}/{fname}", local):
                    continue
                grand_total += stream_file(producer, local, topic, taxi_type, batch_size)
                local.unlink(missing_ok=True)

    producer.flush()
    log.info("ALL DONE — produced %d total rows", grand_total)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("producer crashed")
        sys.exit(1)
