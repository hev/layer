#!/usr/bin/env bash
# Start a customer-owned hevlayer gateway from the published Docker Hub image.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${HEVLAYER_IMAGE:-hevlayer/layer-gateway:latest}"
CONTAINER="${HEVLAYER_CONTAINER:-hevlayer-gateway}"
PORT="${HEVLAYER_PORT:-8080}"
PLATFORM="${HEVLAYER_PLATFORM:-}"
WAIT_SECONDS="${HEVLAYER_WAIT_SECONDS:-90}"
TPUF_API_KEY="${TURBOPUFFER_API_KEY:-${TPUF_API_KEY:-}}"
TARGET="docker"

usage() {
  cat <<'EOF'
Usage: ./scripts/quickstart.sh [options]

Start a local gateway using the published Docker Hub image. The command is
idempotent: rerunning it replaces the named container.

Options:
  --image IMAGE       Gateway image (default: hevlayer/layer-gateway:latest)
  --name NAME         Docker container name (default: hevlayer-gateway)
  --port PORT         Local gateway port (default: 8080)
  --platform PLATFORM Docker platform override, for example linux/amd64
  --kubernetes        Install into the current kubectl context with Helm
  --help              Show this help

Environment:
  TURBOPUFFER_API_KEY    Customer-owned Turbopuffer key. Prompted if unset.
  HEVLAYER_IMAGE         Same as --image.
  HEVLAYER_CONTAINER     Same as --name.
  HEVLAYER_PORT          Same as --port.
  HEVLAYER_PLATFORM      Same as --platform.
  HEVLAYER_WAIT_SECONDS  Readiness timeout in seconds (default: 90).
EOF
}

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

need_command() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required. Install it and rerun this command."
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

while (( $# > 0 )); do
  case "$1" in
    --image)
      (( $# >= 2 )) || die "--image requires a value"
      IMAGE="$2"
      shift 2
      ;;
    --name)
      (( $# >= 2 )) || die "--name requires a value"
      CONTAINER="$2"
      shift 2
      ;;
    --port)
      (( $# >= 2 )) || die "--port requires a value"
      PORT="$2"
      shift 2
      ;;
    --platform)
      (( $# >= 2 )) || die "--platform requires a value"
      PLATFORM="$2"
      shift 2
      ;;
    --kubernetes)
      TARGET="kubernetes"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1 (run with --help for usage)"
      ;;
  esac
done

is_positive_integer "$PORT" || die "port must be a positive integer (got: $PORT)"
(( PORT <= 65535 )) || die "port must be at most 65535 (got: $PORT)"
is_positive_integer "$WAIT_SECONDS" || die "HEVLAYER_WAIT_SECONDS must be a positive integer"

[[ -d "$ROOT/infra" ]] || die "quickstart must be run from a complete repository clone"

if [[ -z "$TPUF_API_KEY" ]]; then
  [[ -t 0 ]] || die "TURBOPUFFER_API_KEY is required when stdin is not interactive"
  printf 'Turbopuffer API key: '
  IFS= read -r -s TPUF_API_KEY
  printf '\n'
fi
[[ -n "$TPUF_API_KEY" ]] || die "Turbopuffer API key cannot be empty"

if [[ "$TARGET" == "kubernetes" ]]; then
  need_command kubectl
  need_command helm
  need_command curl
  kubectl cluster-info >/dev/null 2>&1 || die "the current kubectl context is not reachable"
  log "Installing the gateway into kubectl context $(kubectl config current-context)..."
  helm upgrade --install layer-ce "$ROOT/infra/helm/layer-ce" \
    --namespace layer --create-namespace \
    --set gateway.image="$IMAGE" \
    --set dashboard.enabled=false \
    --set-string vectorStore.credential.apiKey="$TPUF_API_KEY"
  kubectl rollout status deployment/layer-ce-gateway -n layer --timeout="${WAIT_SECONDS}s"

  port_forward_log="$(mktemp)"
  kubectl port-forward -n layer svc/layer-ce-gateway "${PORT}:8080" >"$port_forward_log" 2>&1 &
  port_forward_pid=$!
  # Invoked indirectly by the EXIT trap below.
  # shellcheck disable=SC2329
  cleanup_port_forward() {
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
    rm -f "$port_forward_log"
  }
  trap cleanup_port_forward EXIT

  base_url="http://127.0.0.1:${PORT}"
  deadline=$((SECONDS + WAIT_SECONDS))
  until curl -fsS "$base_url/health" >/dev/null 2>&1; do
    if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
      cat "$port_forward_log" >&2
      die "kubectl port-forward exited before the gateway responded"
    fi
    if (( SECONDS >= deadline )); then
      cat "$port_forward_log" >&2
      die "gateway did not respond through kubectl port-forward within ${WAIT_SECONDS}s"
    fi
    sleep 1
  done

  cat <<EOF

Gateway is running in Kubernetes.

Reach it locally:
  kubectl port-forward -n layer svc/layer-ce-gateway ${PORT}:8080

Then point your Turbopuffer-compatible client at http://127.0.0.1:${PORT} and
keep using your Turbopuffer API key as its bearer token.
EOF
  exit 0
fi

need_command docker
need_command curl

if ! docker info >/dev/null 2>&1; then
  die "Docker is installed but its daemon is not responding. Start Docker and rerun this command."
fi

log "Pulling published image $IMAGE..."
if [[ -n "$PLATFORM" ]]; then
  docker pull --platform "$PLATFORM" "$IMAGE"
else
  docker pull "$IMAGE"
fi

if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  log "Replacing existing container $CONTAINER..."
  docker rm -f "$CONTAINER" >/dev/null
fi

store_config='{"local":{"kind":"turbopuffer","default":true,"endpoint":{"url":"https://api.turbopuffer.com","region":"aws-us-east-1"},"inboundAuth":{"mode":"deriveFromStore"}}}'

docker_env=(
  -e PORT=8080
  -e LAYER_SECRET_LOCAL_API_KEY="$TPUF_API_KEY"
  -e LAYER_STORE_JSON="$store_config"
)

log "Starting $CONTAINER..."
if [[ -n "$PLATFORM" ]]; then
  docker run -d \
    --platform "$PLATFORM" \
    --name "$CONTAINER" \
    --restart unless-stopped \
    -p "127.0.0.1:${PORT}:8080" \
    "${docker_env[@]}" \
    "$IMAGE" >/dev/null
else
  docker run -d \
    --name "$CONTAINER" \
    --restart unless-stopped \
    -p "127.0.0.1:${PORT}:8080" \
    "${docker_env[@]}" \
    "$IMAGE" >/dev/null
fi

cleanup_failed_container() {
  docker logs "$CONTAINER" >&2 || true
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

base_url="http://127.0.0.1:${PORT}"
deadline=$((SECONDS + WAIT_SECONDS))
until curl -fsS "$base_url/health" >/dev/null 2>&1; do
  if ! docker container inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
    cleanup_failed_container
    die "gateway container exited before becoming healthy"
  fi
  if (( SECONDS >= deadline )); then
    cleanup_failed_container
    die "gateway did not respond at $base_url/health within ${WAIT_SECONDS}s"
  fi
  sleep 1
done

cat <<EOF

Gateway is running.
  URL:  $base_url

Point your Turbopuffer-compatible client at $base_url and keep using your
Turbopuffer API key as its bearer token.

Verify it:
  curl -H 'Authorization: Bearer <your-turbopuffer-key>' $base_url/v1/namespaces

Stop it:
  docker rm -f $CONTAINER
EOF
