#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-hevlayer/layer-gateway:latest}"
PORT="${PORT:-8080}"
TURBOPUFFER_REGION="${TURBOPUFFER_REGION:-aws-us-east-1}"

if [[ -z "${TURBOPUFFER_API_KEY:-}" ]]; then
  echo "TURBOPUFFER_API_KEY is required." >&2
  exit 1
fi

exec docker run --rm -p "${PORT}:8080" \
  -e PORT=8080 \
  -e LAYER_SECRET_LOCAL_API_KEY="$TURBOPUFFER_API_KEY" \
  -e LAYER_STORE_JSON="{\"local\":{\"kind\":\"turbopuffer\",\"default\":true,\"endpoint\":{\"url\":\"https://api.turbopuffer.com\",\"region\":\"${TURBOPUFFER_REGION}\"},\"inboundAuth\":{\"mode\":\"deriveFromStore\"}}}" \
  "$IMAGE"
