#!/usr/bin/env bash
# start-localstack.sh — starts a LocalStack container with DynamoDB enabled.
# Usage: ./hack/start-localstack.sh
# Requires: docker, LOCALSTACK_AUTH_TOKEN set in environment (for Pro features).
# The container is removed automatically when this script exits (Ctrl-C).

set -euo pipefail

if [[ -z "${LOCALSTACK_AUTH_TOKEN:-}" ]]; then
  echo "LOCALSTACK_AUTH_TOKEN is not set — DynamoDB works without it but some Pro features won't be available." >&2
fi

CONTAINER_NAME="localstack-kube-applier"
PORT="${LOCALSTACK_PORT:-4566}"

# Remove any stale container with the same name.
docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

echo "Starting LocalStack on port ${PORT} ..."
docker run --rm \
  --name "${CONTAINER_NAME}" \
  -p "${PORT}:4566" \
  -e "SERVICES=dynamodb" \
  -e "LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN:-}" \
  -e "DEBUG=0" \
  localstack/localstack

# 'docker run --rm' keeps the foreground; Ctrl-C stops and removes the container.
