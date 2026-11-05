"""
Spark Structured Streaming job: Kafka (taxi.yellow + taxi.green) -> Iceberg
tables in the Nessie catalog with files on MinIO.

Trigger is `availableNow` so the job processes everything currently in the
topics, then exits cleanly (matches the one-shot producer Job — gives the
reviewer a deterministic "data is ready" point for `make query`).
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("streaming_job")


# ── config from env ───────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.data.svc.cluster.local:9092")
NESSIE_URI      = os.environ.get("NESSIE_URI", "http://nessie.data.svc.cluster.local:19120/api/v2")
S3_ENDPOINT     = os.environ.get("S3_ENDPOINT", "http://minio.data.svc.cluster.local:9000")
S3_ACCESS_KEY   = os.environ["AWS_ACCESS_KEY_ID"]
S3_SECRET_KEY   = os.environ["AWS_SECRET_ACCESS_KEY"]
WAREHOUSE       = os.environ.get("WAREHOUSE", "s3a://lakehouse/warehouse")
CHECKPOINTS     = os.environ.get("CHECKPOINTS", "s3a://lakehouse/_checkpoints")


# ── per-taxi schemas (Yellow uses tpep_*, Green uses lpep_* + extra fields) ───

YELLOW_SCHEMA = StructType([
    StructField("VendorID",              IntegerType()),
    StructField("tpep_pickup_datetime",  TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count",       DoubleType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            DoubleType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("PULocationID",          IntegerType()),
    StructField("DOLocationID",          IntegerType()),
    StructField("payment_type",          LongType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",               DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("congestion_surcharge",  DoubleType()),
    StructField("Airport_fee",           DoubleType()),
])

GREEN_SCHEMA = StructType([
    StructField("VendorID",              IntegerType()),
    StructField("lpep_pickup_datetime",  TimestampType()),
    StructField("lpep_dropoff_datetime", TimestampType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("RatecodeID",            DoubleType()),
    StructField("PULocationID",          IntegerType()),
    StructField("DOLocationID",          IntegerType()),
    StructField("passenger_count",       DoubleType()),
    StructField("trip_distance",         DoubleType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",               DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("ehail_fee",             DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("payment_type",          LongType()),
    StructField("trip_type",             LongType()),
    StructField("congestion_surcharge",  DoubleType()),
])


# ── Spark wiring ──────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("nyc-taxi-streaming")
        # Iceberg + Nessie SQL extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # Nessie catalog
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref",          "main")
        .config("spark.sql.catalog.nessie.warehouse",    WAREHOUSE)
        # S3A → MinIO (HadoopFileIO is auto-selected for s3a:// warehouse URLs)
        .config("spark.hadoop.fs.s3a.endpoint",                 S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",               S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",               S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",        "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",   "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def create_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.taxi")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.taxi.yellow (
          VendorID int,
          tpep_pickup_datetime timestamp,
          tpep_dropoff_datetime timestamp,
          passenger_count double,
          trip_distance double,
          RatecodeID double,
          store_and_fwd_flag string,
          PULocationID int,
          DOLocationID int,
          payment_type bigint,
          fare_amount double,
          extra double,
          mta_tax double,
          tip_amount double,
          tolls_amount double,
          improvement_surcharge double,
          total_amount double,
          congestion_surcharge double,
          Airport_fee double
        )
        USING iceberg
        PARTITIONED BY (months(tpep_pickup_datetime))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.taxi.green (
          VendorID int,
          lpep_pickup_datetime timestamp,
          lpep_dropoff_datetime timestamp,
          store_and_fwd_flag string,
          RatecodeID double,
          PULocationID int,
          DOLocationID int,
          passenger_count double,
          trip_distance double,
          fare_amount double,
          extra double,
          mta_tax double,
          tip_amount double,
          tolls_amount double,
          ehail_fee double,
          improvement_surcharge double,
          total_amount double,
          payment_type bigint,
          trip_type bigint,
          congestion_surcharge double
        )
        USING iceberg
        PARTITIONED BY (months(lpep_pickup_datetime))
    """)


def stream_topic(spark, topic, schema, table, checkpoint):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        # Bound each micro-batch so a 100 GB Kafka backlog doesn't try to
        # materialize in memory at once. With availableNow, Spark loops
        # micro-batches until the topic is drained. 250K rows ≈ 125 MB of
        # JSON, comfortable in a 6 Gi pod with Iceberg's writer overhead.
        .option("maxOffsetsPerTrigger", "250000")
        .load()
    )
    parsed = (
        raw.select(from_json(col("value").cast("string"), schema).alias("d"))
           .select("d.*")
    )
    return (
        parsed.writeStream
        .format("iceberg")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table)
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("starting; KAFKA=%s NESSIE=%s S3=%s warehouse=%s",
             KAFKA_BOOTSTRAP, NESSIE_URI, S3_ENDPOINT, WAREHOUSE)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    create_tables(spark)

    log.info("starting yellow stream")
    y_q = stream_topic(spark, "taxi.yellow", YELLOW_SCHEMA,
                       "nessie.taxi.yellow", f"{CHECKPOINTS}/yellow")
    log.info("starting green stream")
    g_q = stream_topic(spark, "taxi.green",  GREEN_SCHEMA,
                       "nessie.taxi.green",  f"{CHECKPOINTS}/green")

    y_q.awaitTermination()
    log.info("yellow stream done")
    g_q.awaitTermination()
    log.info("green stream done")

    y = spark.sql("SELECT COUNT(*) FROM nessie.taxi.yellow").collect()[0][0]
    g = spark.sql("SELECT COUNT(*) FROM nessie.taxi.green").collect()[0][0]
    log.info("FINAL ROW COUNTS: yellow=%d  green=%d  total=%d", y, g, y + g)


if __name__ == "__main__":
    main()
