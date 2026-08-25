#!/bin/sh
set -eu

: "${PRIMARY_ENDPOINT:?PRIMARY_ENDPOINT is required}"
: "${LOW_ENDPOINT:?LOW_ENDPOINT is required}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${ILM_BUCKET:?ILM_BUCKET is required}"
: "${LOW_BUCKET:?LOW_BUCKET is required}"
: "${TIER_NAME:?TIER_NAME is required}"

mc alias set primary "${PRIMARY_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc alias set low "${LOW_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb --ignore-existing "low/${LOW_BUCKET}"
mc mb --ignore-existing "primary/${ILM_BUCKET}"
mc version enable "primary/${ILM_BUCKET}"

if ! mc ilm tier add minio primary "${TIER_NAME}" \
  --endpoint "${LOW_ENDPOINT}" \
  --access-key "${MINIO_ROOT_USER}" \
  --secret-key "${MINIO_ROOT_PASSWORD}" \
  --bucket "${LOW_BUCKET}"; then
  # A duplicate is acceptable only when the named tier can be read back.
  mc ilm tier info primary "${TIER_NAME}"
fi

mc ilm tier info primary "${TIER_NAME}"
mc ilm tier check primary "${TIER_NAME}"
mc ilm rule remove --all --force "primary/${ILM_BUCKET}" >/dev/null 2>&1 || true
mc ilm rule add \
  --tags "cwm-tier=low" \
  --transition-days "0" \
  --transition-tier "${TIER_NAME}" \
  "primary/${ILM_BUCKET}"
mc ilm rule ls "primary/${ILM_BUCKET}"
