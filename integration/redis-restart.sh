#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)
sentinel="cwm-minio-tierer:integration:aof-restart:$$"
value="persisted-$$"

"${compose[@]}" exec -T redis redis-cli SET "${sentinel}" "${value}" >/dev/null
"${compose[@]}" exec -T redis redis-cli WAITAOF 1 0 5000 >/dev/null
"${compose[@]}" restart -t 15 redis >/dev/null

for _ in $(seq 1 30); do
  if [[ "$("${compose[@]}" exec -T redis redis-cli --raw GET "${sentinel}" 2>/dev/null || true)" == "${value}" ]]; then
    "${compose[@]}" exec -T redis redis-cli DEL "${sentinel}" >/dev/null
    printf '%s\n' "Redis AOF restart persistence: passed"
    exit 0
  fi
  sleep 1
done

printf '%s\n' "Redis AOF restart persistence: sentinel was not recovered" >&2
exit 1
