#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Start public/ce/docker-compose.yml first. Override for an isolated compose project.
export PGVECTOR_GATEWAY_URL="${PGVECTOR_GATEWAY_URL:-http://127.0.0.1:8080}"
PYTHONPATH="$PWD/clients/python/src${PYTHONPATH:+:$PYTHONPATH}" python3 tools/sdk-harness/test/pgvector/client.py
(cd tools/sdk-harness && npx --no-install tsx test/pgvector/client.ts)
