#!/usr/bin/env bash
# Builds the producer + spark-job Docker images and loads them into Minikube.
# Useful on its own when iterating on the Python/Spark code without doing a
# full deploy.
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( dirname "$SCRIPT_DIR" )"
PROFILE="${MINIKUBE_PROFILE:-lakehouse}"

build_and_load() {
  local name="$1" context="$2"
  echo "==> Building $name"
  docker build -t "${name}:dev" "$context"
  echo "==> Loading $name into Minikube ($PROFILE)"
  minikube -p "$PROFILE" image load "${name}:dev"
}

build_and_load lakehouse-producer "$ROOT_DIR/docker/producer"
build_and_load lakehouse-spark    "$ROOT_DIR/docker/spark-job"

echo
echo "Images now in Minikube:"
minikube -p "$PROFILE" image ls | grep '^docker.io/library/lakehouse-' || true
