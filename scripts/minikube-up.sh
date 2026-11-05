#!/usr/bin/env bash
# Brings up the Minikube cluster used by the lakehouse stack.
# Idempotent: safe to re-run; will fast-exit if the profile is already running.
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-lakehouse}"
CPUS="${MINIKUBE_CPUS:-4}"
MEMORY_MB="${MINIKUBE_MEMORY_MB:-9216}"
DISK="${MINIKUBE_DISK:-30g}"
DRIVER="${MINIKUBE_DRIVER:-docker}"
NAMESPACE="${LAKEHOUSE_NAMESPACE:-data}"

echo "==> Starting Minikube profile '${PROFILE}' (driver=${DRIVER}, cpus=${CPUS}, mem=${MEMORY_MB}MB, disk=${DISK})"

if minikube -p "${PROFILE}" status --format '{{.Host}}' 2>/dev/null | grep -qx Running; then
  echo "    cluster already running; skipping start"
else
  minikube start \
    --profile "${PROFILE}" \
    --driver "${DRIVER}" \
    --cpus "${CPUS}" \
    --memory "${MEMORY_MB}" \
    --disk-size "${DISK}"
fi

echo "==> Switching kubectl context to '${PROFILE}'"
kubectl config use-context "${PROFILE}"

echo "==> Ensuring namespace '${NAMESPACE}' exists"
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 \
  || kubectl create namespace "${NAMESPACE}"

echo "==> Cluster ready:"
kubectl get nodes
