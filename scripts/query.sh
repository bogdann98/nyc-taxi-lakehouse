#!/usr/bin/env bash
# Opens a spark-sql shell pre-wired with the Nessie catalog + MinIO creds,
# so the lakehouse can be queried interactively.
#
# Modes:
#   bash query.sh                # interactive shell (drops into spark-sql)
#   bash query.sh smoke          # runs a few canonical queries, exits
#                                # (also auto-selected when stdin isn't a TTY)
set -euo pipefail

# Git Bash on Windows mangles Linux-style paths in arguments to native exes
# (turns "/opt/spark/bin/spark-sql" into "C:/Program Files/Git/opt/...").
# Disable the auto-translation for the kubectl invocations below.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

NS="${LAKEHOUSE_NAMESPACE:-data}"

MODE="${1:-auto}"
if [[ "$MODE" == "auto" ]]; then
  if [[ -t 0 ]]; then MODE=shell; else MODE=smoke; fi
fi

# Pull MinIO creds from the chart-managed secret — never hardcoded.
S3_USER=$(kubectl -n "$NS" get secret minio -o jsonpath='{.data.rootUser}'     | base64 -d)
S3_PASS=$(kubectl -n "$NS" get secret minio -o jsonpath='{.data.rootPassword}' | base64 -d)

# Spark configs shared by both modes.
SPARK_CONFS=(
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
  --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2
  --conf spark.sql.catalog.nessie.ref=main
  --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/warehouse
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
  --conf spark.hadoop.fs.s3a.path.style.access=true
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
  # Reads AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY from the pod env.
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider
  --conf spark.driver.memory=1g
  --conf spark.ui.showConsoleProgress=false
)

case "$MODE" in
  shell)
    cat <<'BANNER'
=========================================================================
  Spark SQL — Nessie / Iceberg / MinIO lakehouse
  Try:
    SHOW NAMESPACES IN nessie;
    SHOW TABLES IN nessie.taxi;
    SELECT COUNT(*) FROM nessie.taxi.yellow;
    SELECT date_trunc('month', tpep_pickup_datetime) AS m, COUNT(*) AS n
      FROM nessie.taxi.yellow GROUP BY 1 ORDER BY 1;
  Type 'exit;' or Ctrl-D to leave.
=========================================================================
BANNER
    exec kubectl -n "$NS" run spark-sql --rm -it --restart=Never \
      --image=lakehouse-spark:dev --image-pull-policy=IfNotPresent \
      --env="AWS_ACCESS_KEY_ID=$S3_USER" \
      --env="AWS_SECRET_ACCESS_KEY=$S3_PASS" \
      --command -- /opt/spark/bin/spark-sql "${SPARK_CONFS[@]}"
    ;;
  smoke)
    echo "==> Running smoke queries (non-interactive)"
    SQL="SHOW NAMESPACES IN nessie;
SHOW TABLES IN nessie.taxi;
SELECT 'yellow' AS taxi, COUNT(*) AS rows FROM nessie.taxi.yellow
UNION ALL
SELECT 'green',  COUNT(*)            FROM nessie.taxi.green
ORDER BY taxi;"
    exec kubectl -n "$NS" run spark-sql --rm -i --restart=Never \
      --image=lakehouse-spark:dev --image-pull-policy=IfNotPresent \
      --env="AWS_ACCESS_KEY_ID=$S3_USER" \
      --env="AWS_SECRET_ACCESS_KEY=$S3_PASS" \
      --command -- /opt/spark/bin/spark-sql "${SPARK_CONFS[@]}" -e "$SQL"
    ;;
  *)
    echo "Usage: $0 [shell|smoke]" >&2
    exit 1
    ;;
esac
