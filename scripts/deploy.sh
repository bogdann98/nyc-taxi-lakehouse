#!/usr/bin/env bash
# End-to-end deployment of the lakehouse stack.
# Idempotent: safe to re-run. Each step skips work if it's already done.
#
# Order:
#   1. Minikube cluster + `data` namespace
#   2. Helm repos + install MinIO, Kafka, Nessie
#   3. Wait for infra Ready
#   4. Create `lakehouse` bucket via Job
#   5. Build producer + spark-job images, load into Minikube
#   6. Create/refresh data-range ConfigMap
#   7. Run producer Job (Kafka ingest), wait for completion
#   8. Run Spark streaming Job (Iceberg write), wait for completion

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( dirname "$SCRIPT_DIR" )"
NS="${LAKEHOUSE_NAMESPACE:-data}"

step() { echo; echo "════════ $* ════════"; }

# ── 1. Cluster ────────────────────────────────────────────────────────────────
step "1/8  Minikube cluster"
bash "$SCRIPT_DIR/minikube-up.sh"

# ── 2. Helm repos + installs ──────────────────────────────────────────────────
step "2/8  Helm repos"
helm repo add bitnami https://charts.bitnami.com/bitnami     >/dev/null 2>&1 || true
helm repo add minio   https://charts.min.io/                 >/dev/null 2>&1 || true
helm repo add nessie  https://charts.projectnessie.org       >/dev/null 2>&1 || true
helm repo update >/dev/null

step "3/8  Helm install MinIO + Kafka + Nessie"
helm upgrade --install minio  minio/minio    -n "$NS" --version 5.4.0   -f "$ROOT_DIR/helm/values-minio.yaml"
helm upgrade --install kafka  bitnami/kafka  -n "$NS" --version 32.4.3  -f "$ROOT_DIR/helm/values-kafka.yaml"
helm upgrade --install nessie nessie/nessie  -n "$NS" --version 0.107.5 -f "$ROOT_DIR/helm/values-nessie.yaml"

# Bitnami Kafka uses OnDelete update strategy. If we just bumped the chart and
# the pod is stuck on the old image, kick it. Safe no-op if already correct.
if kubectl -n "$NS" get pod kafka-controller-0 >/dev/null 2>&1; then
  desired=$(kubectl -n "$NS" get statefulset kafka-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="kafka")].image}')
  current=$(kubectl -n "$NS" get pod      kafka-controller-0 -o jsonpath='{.spec.containers[?(@.name=="kafka")].image}')
  if [[ "$desired" != "$current" ]]; then
    echo "    kafka pod image stale; deleting to roll"
    kubectl -n "$NS" delete pod kafka-controller-0
  fi
fi

step "4/8  Wait for infra pods Ready (up to 5 min)"
kubectl -n "$NS" wait --for=condition=Ready pod \
  -l 'app.kubernetes.io/name in (minio,kafka,nessie)' \
  --timeout=300s

# ── 5. Bucket Job ─────────────────────────────────────────────────────────────
step "5/8  MinIO bucket"
kubectl -n "$NS" delete job minio-create-bucket --ignore-not-found
kubectl apply -f "$ROOT_DIR/k8s/minio-bucket-job.yaml"
kubectl -n "$NS" wait --for=condition=complete job/minio-create-bucket --timeout=120s

# ── 6. Build + load images ────────────────────────────────────────────────────
step "6/8  Build + load producer + spark-job images"
bash "$SCRIPT_DIR/load-images.sh"

# ── 7. ConfigMap from data-range.env ──────────────────────────────────────────
step "7/8  Refresh data-range ConfigMap from config/data-range.env"
kubectl -n "$NS" create configmap data-range \
  --from-env-file="$ROOT_DIR/config/data-range.env" \
  --dry-run=client -o yaml | kubectl apply -f -

# Wait for a Job's pod to be in a tail-able state (Running or already finished).
# Avoids the cosmetic "ContainerCreating" error from racing kubectl logs -f.
wait_pod_ready() {
  local label="$1" timeout=60 i=0
  while (( i < timeout )); do
    local phase
    phase=$(kubectl -n "$NS" get pods -l "$label" -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)
    [[ "$phase" =~ ^(Running|Succeeded|Failed)$ ]] && return 0
    sleep 1
    i=$((i + 1))   # NOT ((i++)) — that returns 0 on first iteration and trips `set -e`
  done
  return 0   # don't block deploy if pod start is slow; logs will reconnect
}

# ── 8. Producer Job (must finish before Spark since trigger=availableNow) ─────
step "8/8a  Producer Job — Kafka ingest"
kubectl -n "$NS" delete job producer --ignore-not-found
kubectl apply -f "$ROOT_DIR/k8s/producer-job.yaml"
wait_pod_ready "app=producer"
echo "       (tailing producer logs; this can take several minutes)"
kubectl -n "$NS" wait --for=condition=complete job/producer --timeout=1800s &
WAIT_PID=$!
kubectl -n "$NS" logs -l app=producer -f --tail=-1 || true
wait $WAIT_PID

# ── 8b. Spark streaming Job ───────────────────────────────────────────────────
step "8/8b  Spark streaming Job — Iceberg write"
kubectl -n "$NS" delete job spark-streaming --ignore-not-found
kubectl apply -f "$ROOT_DIR/k8s/spark-streaming-job.yaml"
wait_pod_ready "app=spark-streaming"
echo "       (tailing Spark logs; this typically takes 1-3 min)"
kubectl -n "$NS" wait --for=condition=complete job/spark-streaming --timeout=1200s &
WAIT_PID=$!
kubectl -n "$NS" logs -l app=spark-streaming -f --tail=-1 || true
wait $WAIT_PID

# ── Done ──────────────────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════════════════════════════════"
echo " ✅  Lakehouse is up. Query it with:    make query"
echo "    Iceberg files in MinIO:            mc ls local/lakehouse/warehouse/"
echo "    Open MinIO console (port-forward): kubectl -n $NS port-forward svc/minio-console 9001:9001"
echo "════════════════════════════════════════════════════════════════════════"
