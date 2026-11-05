#!/usr/bin/env bash
# Tears down the lakehouse stack. By default removes Helm releases + namespace
# but leaves the Minikube cluster running. Pass --hard to also delete the
# Minikube profile (frees the most disk/RAM).
set -euo pipefail

NS="${LAKEHOUSE_NAMESPACE:-data}"
PROFILE="${MINIKUBE_PROFILE:-lakehouse}"
HARD=0
for arg in "$@"; do
  [[ "$arg" == "--hard" ]] && HARD=1
done

step() { echo; echo "════════ $* ════════"; }

step "Delete K8s Jobs"
kubectl -n "$NS" delete job spark-streaming     --ignore-not-found
kubectl -n "$NS" delete job producer            --ignore-not-found
kubectl -n "$NS" delete job minio-create-bucket --ignore-not-found

step "Helm uninstall"
helm uninstall -n "$NS" nessie 2>/dev/null || true
helm uninstall -n "$NS" kafka  2>/dev/null || true
helm uninstall -n "$NS" minio  2>/dev/null || true

step "Delete PVCs (Helm doesn't clean these by default)"
kubectl -n "$NS" delete pvc --all --ignore-not-found

step "Delete namespace"
kubectl delete namespace "$NS" --ignore-not-found

if (( HARD )); then
  step "Delete Minikube profile (--hard)"
  minikube delete -p "$PROFILE"
else
  echo
  echo "Cluster '$PROFILE' left running. Use 'make down-hard' (or pass --hard) to delete it."
fi

echo
echo "✅  Teardown complete."
