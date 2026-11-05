# NYC Taxi Lakehouse

A local data platform that streams **NYC TLC Yellow + Green Taxi** trip records through **Kafka → Spark Structured Streaming** into a lakehouse of **Apache Iceberg + Project Nessie + MinIO**, deployed on **Kubernetes (Minikube)** via **Helm**.

Single-command deploy: `make up`. Single-command query: `make query`. Single-command teardown: `make down`.

> Personal project, built to learn the stack end to end. Not production software.

---

## What it does

```
NYC TLC parquet URLs (Yellow + Green, configurable months/years)
         │
         ▼  HTTPS download (no data committed to repo)
  Producer Job (Python pod, pyarrow + confluent-kafka)
         │
         ▼  JSON messages, hour-truncated keys
  Kafka topics: taxi.yellow, taxi.green   (KRaft, 1 broker)
         │
         ▼  readStream
  Spark Structured Streaming Job (local[*], availableNow trigger)
         │
         ▼  writeStream — Iceberg sink
  Iceberg tables  ──── Nessie catalog (HTTP API, in-memory store)
         │
         ▼  s3a://lakehouse/warehouse
  MinIO  (S3-compatible object store)
         │
         ▼
  Spark SQL shell (`make query`)
```

All components run inside a single local Minikube cluster, in the `data` namespace. Each component is installed via Helm; the producer + Spark job are custom Docker images built and `minikube image load`-ed locally.

---

## Verified end-to-end

The pipeline has been validated against **both** the demo defaults and the literal full-brief range. All numbers below are captured in [`EVIDENCE/`](EVIDENCE/).

**Full 2020-2025 run** (literal brief — 144 files, both taxi types):

| | Yellow | Green | Total |
|---|---:|---:|---:|
| Rows produced to Kafka | 223,412,046 | 5,681,986 | **229,094,032** |
| Rows in Iceberg tables | 223,412,046 | 5,681,986 | **229,094,032** *(zero loss)* |
| Wall time | Producer ~105 min, Spark ~25 min, total ~2.2 hours | | |

**Default 3-month slice** (2024 Q1, fast-demo configuration):

| | Yellow | Green | Total |
|---|---:|---:|---:|
| Rows produced to Kafka | 9,554,778 | 167,585 | **9,722,363** |
| Rows in Iceberg tables | 9,554,778 | 167,585 | **9,722,363** *(zero loss)* |
| Wall time | Producer ~2.5 min, Spark ~70 sec, `make up` ~5 min | | |

Architecture and code are **identical** between the two — only `config/data-range.env` and a few PVC sizes differ.

---

## Prerequisites

| Tool | Tested version | Install |
|---|---|---|
| Docker Desktop | 29.x | https://docs.docker.com/desktop/ |
| Minikube | 1.38.x | `winget install Kubernetes.minikube` / brew |
| kubectl | 1.34.x | bundled with most Docker distros |
| Helm | 3.x or 4.x | `winget install Helm.Helm` / brew |
| GNU Make | 4.x | `winget install ezwinports.make` / built-in on macOS+Linux |
| Bash | any | Git Bash on Windows; native on macOS+Linux |
| Internet | required | for pulling images and the NYC parquet files |

### Resources
Docker Desktop must have at least **8 GB RAM** allocated; **12 GB recommended** to avoid Spark OOM under steady-state.

**On Windows with WSL2 backend** (Docker Desktop default), the slider is hidden and memory is controlled via `~/.wslconfig`. Create one if you don't have it:

```ini
# C:\Users\<you>\.wslconfig
[wsl2]
memory=12GB
processors=8
swap=4GB
```

Then `wsl --shutdown` and let Docker Desktop restart.

### Windows-specific notes
- **Run `make` from Git Bash** for cleanest output. PowerShell/cmd also work, but the `Makefile` invokes Git Bash explicitly via its absolute path (`C:/Program Files/Git/bin/bash.exe`) for shell-script targets, because the Windows-bundled `bash.exe` in `System32` is the WSL launcher (which fails when no real Linux distro is installed).
- The default `bash` in Git Bash auto-translates Linux-style argument paths (`/opt/...`) to Windows paths. Scripts that invoke `kubectl exec` set `MSYS_NO_PATHCONV=1` to defeat this.

---

## Quickstart

```bash
# 1. Bring everything up (cluster, infra, ingest, transform)
make up

# 2. Query the lakehouse
make query        # interactive spark-sql shell
make query smoke  # non-interactive: runs canonical queries and exits

# 3. Tear down
make down         # removes Helm releases + namespace, keeps Minikube cluster
make down-hard    # also deletes the Minikube cluster (frees most RAM/disk)
```

Other useful targets:

```bash
make help            # list all targets
make status          # pod / job / svc / pvc state
make images          # rebuild + reload producer + spark images only
make logs-producer   # tail producer log
make logs-spark      # tail Spark log
```

---

## Configuration

Data range is controlled by [`config/data-range.env`](config/data-range.env):

```ini
YEAR_START=2024
MONTH_START=1
YEAR_END=2024
MONTH_END=3
TAXI_TYPES=yellow, green
PARQUET_BASE_URL=https://d37ci6vzurychx.cloudfront.net/trip-data
```

The full range — every file back to 2020 — is roughly **140 monthly parquet files / 245 million rows / ~5 GB compressed / ~80–120 GB serialized as JSON on Kafka**, which is not realistic on a laptop in a single sitting. The default is therefore a **3-month slice** so the whole pipeline finishes in ~5 minutes; widen the range to verify behaviour at scale.

`make up` re-creates the `data-range` ConfigMap from this file every time, so changes take effect on the next run.

### Running the full 2020-2025 brief

The default infra is sized for the demo (4 GB Kafka PVC, 30 GB Minikube disk). To run the literal brief (~229M rows, ~2.2 hours), bump the sizes in three places **before** `make up`:

1. `config/data-range.env`:
   ```ini
   YEAR_START=2020
   MONTH_START=1
   YEAR_END=2025
   MONTH_END=12
   ```
2. `helm/values-kafka.yaml` → `controller.persistence.size: 150Gi` (handles ~110 GB JSON payload + headroom)
3. `helm/values-minio.yaml` → `persistence.size: 30Gi` (Iceberg parquet is compact: final ~4 GB)
4. `scripts/minikube-up.sh` → `MINIKUBE_DISK="200g"` default (host needs ~50 GB free for actual usage; vhdx is sparse)
5. `k8s/spark-streaming-job.yaml` → memory limit 6Gi; `docker/spark-job/streaming_job.py` → `maxOffsetsPerTrigger=250000` (smaller batches avoid GC thrash on the bigger workload)

Then `make down-hard && make up`. Captured outputs from this exact run are in [`EVIDENCE/full-run-2020-2025/`](EVIDENCE/full-run-2020-2025/).

---

## Querying the lakehouse

```bash
make query
```
drops you into a `spark-sql` shell pre-wired with the Nessie catalog and MinIO credentials.

Sample queries:

```sql
-- Discovery
SHOW NAMESPACES IN nessie;
SHOW TABLES    IN nessie.taxi;
DESCRIBE TABLE  nessie.taxi.yellow;

-- Counts
SELECT COUNT(*) FROM nessie.taxi.yellow;
SELECT COUNT(*) FROM nessie.taxi.green;

-- Yellow trips per month
SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       COUNT(*) AS trips
FROM nessie.taxi.yellow
GROUP BY 1
ORDER BY 1;

-- Average tip by payment type
SELECT payment_type,
       ROUND(AVG(tip_amount), 2) AS avg_tip,
       ROUND(AVG(total_amount), 2) AS avg_total
FROM nessie.taxi.yellow
GROUP BY 1
ORDER BY 1;

-- Iceberg time-travel: see the table's snapshot history
SELECT * FROM nessie.taxi.yellow.snapshots;
SELECT * FROM nessie.taxi.yellow.history;
```

### MinIO console (optional)

```bash
kubectl -n data port-forward svc/minio-console 9001:9001
# open http://localhost:9001  (admin / minioadmin123)
```

### Nessie API (optional)

```bash
kubectl -n data port-forward svc/nessie 19120:19120
curl http://localhost:19120/api/v2/trees       # list branches
```

---

## Repo layout

```
.
├── README.md
├── Makefile                       cross-platform; OS-aware bash invocation
├── config/data-range.env          which months / taxi types to ingest
├── helm/
│   ├── values-minio.yaml          standalone, 10 GB PV
│   ├── values-kafka.yaml          KRaft single combined node, PLAINTEXT
│   └── values-nessie.yaml         in-memory store, no auth
├── k8s/
│   ├── minio-bucket-job.yaml      one-shot: creates `lakehouse` bucket via mc
│   ├── producer-job.yaml          envFrom data-range ConfigMap
│   └── spark-streaming-job.yaml   pulls MinIO creds from the chart's secret
├── docker/
│   ├── producer/                  Python 3.12-slim + pyarrow + confluent-kafka
│   └── spark-job/                 apache/spark:3.5.3 + Iceberg/Nessie/AWS jars
└── scripts/
    ├── deploy.sh                  end-to-end orchestration (idempotent)
    ├── teardown.sh                soft (default) or hard (--hard) teardown
    ├── minikube-up.sh             cluster bring-up (called by deploy.sh)
    ├── load-images.sh             rebuild + load images only
    └── query.sh                   spark-sql shell (interactive or smoke mode)
```

---

## Design decisions and trade-offs

Each call-out is a deliberate choice; flagged here so the reasoning is visible.

| Decision | What I chose | What I considered | Why |
|---|---|---|---|
| Default data range | 2024 Q1 (3 months, ~9.7M rows) | Full 2020–present (~245M rows) | A default run should finish in a single sitting; the architecture is identical at any scale, only the data volume changes. Documented above how to widen. |
| Tables | Two: `nessie.taxi.yellow`, `nessie.taxi.green` | One unified table with a `taxi_type` column | Yellow and Green schemas differ (Green has `ehail_fee`, `trip_type`); merging would mean nulls in half the columns. |
| Spark deployment | `local[*]` mode in a single Job pod | `--master k8s://` with executor pods | The focus here is Iceberg/Nessie/MinIO mechanics, not Spark-on-K8s plumbing. `local[*]` keeps the failure surface small (one pod, one log stream) and avoids RBAC/executor-image-pull complexity. |
| Streaming trigger | `availableNow` (process all buffered messages, then exit) | `processingTime` continuous | The producer is one-shot — there's no "future data" arriving. `availableNow` gives a deterministic "data is ready" point. The streaming code paths are identical; only the trigger differs. |
| Catalog | Nessie with `IN_MEMORY` store | Nessie with RocksDB or JDBC | Simpler — no PVC plumbing for the catalog. Iceberg data files in MinIO survive any pod restart; only the catalog index would be lost. Documented as a known limitation. |
| Kafka | Bitnami chart, KRaft single combined node, PLAINTEXT, no SASL | Strimzi operator; multi-broker; SASL_PLAINTEXT | Bitnami is one chart with no CRDs — fastest path to a working cluster. Strimzi/operator-based would be production-grade but adds significant setup. |
| Bitnami image source | `bitnamilegacy/kafka` | `bitnami/kafka` | Bitnami removed free `bitnami/*` images from Docker Hub mid-2025. The free tier moved to `bitnamilegacy/*` for the transition period. |
| MinIO | Standalone, 1 replica, 10 GB PV | Distributed mode | Single-laptop deployment — distributed mode adds nothing here. |
| Iceberg partitioning | `PARTITIONED BY (months(<pickup_datetime>))` | Unpartitioned | Pickup-month is the natural slice for trip queries; gives Iceberg pruning for typical analytics. |
| FileIO | HadoopFileIO (S3A) — implicit from `s3a://` warehouse | Iceberg's native S3FileIO | Standard S3A configs every Spark practitioner knows; works with the same `hadoop-aws` jar already on the classpath. S3FileIO would be slightly faster but adds another bundle. |
| Helm version | Helm 4 (locally), but charts are Helm-3 compatible | Pin to Helm 3 | Helm 4 was already on PATH after `winget install`; all charts in use are Helm-3-compatible (no deprecated APIs). |
| Producer message format | JSON, key = pickup-hour ISO string | Avro + Schema Registry | JSON is debuggable in `kafka-console-consumer`; Avro would be more efficient but needs Schema Registry + a separate registry pod. |

---

## What I'd do with more time

In rough priority order:

1. **Schema Registry + Avro on Kafka** — type-safe contract between producer and consumer; smaller messages.
2. **Spark on Kubernetes (operator)** — driver + executors as separate pods, dynamic allocation, demonstrates real Spark-on-K8s deployment.
3. **Persistent Nessie backend** (Postgres or RocksDB on a PVC) so catalog state survives pod restarts.
4. **Real auth everywhere** — SASL_SCRAM on Kafka, HTTPS + tokens on Nessie, IAM-style policies on MinIO.
5. **Multi-broker Kafka with replication** — fault tolerance that mirrors a production setup.
6. **Iceberg compaction + expire-snapshots maintenance jobs** — keep small files and orphan snapshots from accumulating.
7. **Observability** — Prometheus + Grafana for Kafka/MinIO/Spark metrics, OpenTelemetry traces from producer through to Iceberg writer.
8. **CI integration test** — a GitHub Actions workflow that runs `make up && make query smoke && make down-hard` against a Kind cluster on every PR.
9. **Schema evolution test** — add a column to one of the topics, verify Iceberg evolves the table without downstream breakage.
10. **Backfill and replay tooling** — separate Job for re-ingesting a specific month or branch in Nessie.

---

## Version matrix (what was tested)

| Component | Version |
|---|---|
| Spark | 3.5.3 (Java 11, Scala 2.12) |
| Iceberg Spark Runtime | 1.6.1 |
| Nessie Spark Extensions | 0.99.0 |
| Hadoop AWS | 3.3.4 |
| AWS Java SDK Bundle | 1.12.262 |
| spark-sql-kafka | 3.5.3 |
| kafka-clients (transitive) | 3.4.1 |
| commons-pool2 | 2.11.1 |
| Bitnami Kafka chart | 32.4.3 (image `bitnamilegacy/kafka:4.0.0-debian-12-r10`) |
| MinIO chart | 5.4.0 |
| Nessie chart | 0.107.5 |
| Minikube | 1.38.1 (Kubernetes v1.35.1) |
| Docker Desktop | 29.4.1 |

---

## Troubleshooting

### "Bitnami `kafka:4.0.0-debian-12-r10` not found"
Bitnami removed free images from Docker Hub mid-2025; we override `image.repository: bitnamilegacy/kafka` in [`helm/values-kafka.yaml`](helm/values-kafka.yaml). If they remove the legacy registry too, switch to Strimzi or pin to an older chart version.

### Spark Job OOM-killed
Bump Docker Desktop / `.wslconfig` memory to at least 12 GB and recreate the cluster:
```bash
make down-hard
MINIKUBE_MEMORY_MB=10240 make up
```

### `make` fails with "command not found" on Windows
Either install via `winget install ezwinports.make`, or run scripts directly: `bash scripts/deploy.sh`.

### `kubectl exec` paths get mangled into `C:/Program Files/Git/...`
Git Bash auto-translates Linux-style args. Scripts already set `MSYS_NO_PATHCONV=1`. If running ad-hoc commands, prefix paths with `//` (e.g. `kubectl exec ... -- //opt/spark/bin/spark-sql`).

### `kafka-console-consumer` hangs
Single-node KRaft cluster takes a while for the `__consumer_offsets` topic to settle, which the consumer-group coordinator needs. Read by partition instead:
```bash
kubectl -n data exec kafka-controller-0 -c kafka -- \
  /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic taxi.yellow \
  --partition 0 --offset 0 --max-messages 1
```
Spark's Kafka source uses its own offset management and is unaffected.

### Spark sees more rows than expected
Re-running `make up` does not reset Kafka offsets or Iceberg checkpoints. The producer re-publishes all rows; Spark resumes from its checkpoint and writes the new messages, doubling the table. For a fresh state, run `make down && make up`.

---

## Teardown

```bash
make down        # remove Helm releases + namespace; cluster stays up
make down-hard   # the above + delete the Minikube cluster entirely
```

`make down-hard` is the cleanest "leave no trace" option — frees ~10 GB of disk and all the Docker memory.

---

## About this project

Built to work through a full streaming lakehouse end to end: Kafka → Spark Structured Streaming → Iceberg/Nessie/MinIO on Kubernetes via Helm, ingesting the public NYC TLC Yellow + Green Taxi parquet files.

The goal was to own every layer myself — container images, Helm values, Kubernetes manifests, cluster resource sizing and the Spark job — rather than work inside a pre-configured environment. The [troubleshooting](#troubleshooting) section is the list of things that actually broke along the way.

This is a learning project, not production software. See [What I'd do with more time](#what-id-do-with-more-time) for the known gaps.
