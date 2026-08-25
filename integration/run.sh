#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)

cleanup() {
  if [[ "${KEEP_INTEGRATION:-0}" != "1" ]]; then
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
}

failure_logs() {
  status=$?
  if (( status != 0 )); then
    "${compose[@]}" ps || true
    "${compose[@]}" logs --no-color || true
  fi
  cleanup
  exit "${status}"
}
trap failure_logs EXIT

"${compose[@]}" down --volumes --remove-orphans
"${compose[@]}" up --build --wait --wait-timeout 180 redis minio-primary minio-low redis-updater tierer
"${compose[@]}" --profile tests run --build --rm --no-deps integration-tests
./integration/redis-restart.sh

trap - EXIT
cleanup
