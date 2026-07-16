#!/usr/bin/env bash
# Opinionated customer install: Terraform (VPC, EKS, IAM/IRSA, S3, ECR) then
# the Layer Helm release wired to those outputs. Companion to
# https://hevlayer.com/docs/install/ — this script is the fast path; the docs
# page is the reference for every value it sets and the
# bring-your-own-cluster alternative.
#
# Required:
#   TURBOPUFFER_API_KEY   upstream store credential (becomes the default
#                         VectorStore credential and, under deriveFromStore,
#                         the gateway bearer key)
# Optional:
#   AWS_REGION            default us-east-1
#   CLUSTER_NAME          default layer
#   NAMESPACE             default layer
#   HELM_RELEASE          default layer
#   LAYER_VERSION         image tag for hevlayer/* Docker Hub images (default latest)
#   LICENSE_TOKEN         signed hev layer license key (trial/commercial installs)
#   SYSTEM_NODE_INSTANCE_TYPE
#                         always-on system node instance type (default from
#                         Terraform: i4i.large)
#   DASHBOARD_USER        dashboard Basic Auth user (default admin)
#   DASHBOARD_PASSWORD    dashboard Basic Auth password (default generated)
#   SKIP_TERRAFORM=1      reuse existing Terraform outputs, run Helm only
set -euo pipefail

cd "$(dirname "$0")/.."

AWS_REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="${CLUSTER_NAME:-layer}"
NAMESPACE="${NAMESPACE:-layer}"
HELM_RELEASE="${HELM_RELEASE:-layer}"
HELM_CHART="infra/helm/layer"
TF_DIR="infra/terraform"
LAYER_VERSION="${LAYER_VERSION:-latest}"
SKIP_TERRAFORM="${SKIP_TERRAFORM:-0}"
DASHBOARD_USER="${DASHBOARD_USER:-admin}"
GENERATED_DASHBOARD_PASSWORD=0

log() { printf '\n==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

missing_install_assets=()
for path in "$HELM_CHART" "$TF_DIR" scripts/deploy-lb-controller.sh scripts/deploy-karpenter.sh; do
  [[ -e "$path" ]] || missing_install_assets+=("$path")
done
if (( ${#missing_install_assets[@]} > 0 )); then
  die "installation source is incomplete (missing: ${missing_install_assets[*]}). Use the complete Layer deployment source or onboarding artifact; see https://hevlayer.com/docs/install/"
fi

[[ "$SKIP_TERRAFORM" == "0" || "$SKIP_TERRAFORM" == "1" ]] || die "SKIP_TERRAFORM must be 0 or 1"
for bin in aws terraform helm kubectl jq openssl; do
  command -v "$bin" >/dev/null || die "missing prerequisite: $bin"
done
[[ -n "${TURBOPUFFER_API_KEY:-}" ]] || die "set TURBOPUFFER_API_KEY (see https://hevlayer.com/docs/install/)"
if [[ -z "${DASHBOARD_PASSWORD:-}" ]]; then
  DASHBOARD_PASSWORD="$(openssl rand -hex 24)"
  GENERATED_DASHBOARD_PASSWORD=1
fi
aws sts get-caller-identity >/dev/null || die "AWS credentials not configured"

if [[ "$SKIP_TERRAFORM" != "1" ]]; then
  log "Provisioning AWS resources (terraform apply in $TF_DIR)"
  tf_vars=(
    -var "region=$AWS_REGION"
    -var "cluster_name=$CLUSTER_NAME"
    -var "kubernetes_namespace=$NAMESPACE"
    -var 'bootstrap_cluster=true'
  )
  if [[ -n "${SYSTEM_NODE_INSTANCE_TYPE:-}" ]]; then
    tf_vars+=(-var "system_node_instance_type=$SYSTEM_NODE_INSTANCE_TYPE")
  fi
  terraform -chdir="$TF_DIR" init -input=false
  terraform -chdir="$TF_DIR" apply -auto-approve -input=false "${tf_vars[@]}"
fi

log "Reading Terraform outputs"
tf_out() { terraform -chdir="$TF_DIR" output -raw "$1"; }
S3_BUCKET="$(tf_out layer_s3_bucket_name)"
CLUSTER_NAME="$(tf_out cluster_name)"
NAMESPACE="$(tf_out layer_namespace)"
GATEWAY_ROLE_ARN="$(tf_out layer_gateway_role_arn)"
GATEWAY_SA="$(tf_out layer_service_account_name)"
DASHBOARD_ROLE_ARN="$(tf_out layer_dashboard_role_arn)"
DASHBOARD_SA="$(tf_out layer_dashboard_service_account_name)"
KARPENTER_INSTANCE_PROFILE="$(tf_out karpenter_node_instance_profile_name)"

log "Updating kubeconfig for cluster $CLUSTER_NAME"
aws eks update-kubeconfig --region "$AWS_REGION" --name "$CLUSTER_NAME"

log "Installing cluster networking and autoscaling"
AWS_REGION="$AWS_REGION" SKIP_TERRAFORM=1 ./scripts/deploy-lb-controller.sh
AWS_REGION="$AWS_REGION" SKIP_TERRAFORM=1 ./scripts/deploy-karpenter.sh

log "Installing the Layer Helm release"
VALUES_FILE="$(mktemp -t layer-values.XXXXXX.yaml)"
trap 'rm -f "$VALUES_FILE"' EXIT
jq -n \
  --arg api_key "$TURBOPUFFER_API_KEY" \
  --arg aws_region "$AWS_REGION" \
  --arg layer_version "$LAYER_VERSION" \
  --arg dashboard_user "$DASHBOARD_USER" \
  --arg dashboard_password "$DASHBOARD_PASSWORD" \
  --arg dashboard_sa "$DASHBOARD_SA" \
  --arg dashboard_role_arn "$DASHBOARD_ROLE_ARN" \
  --arg s3_bucket "$S3_BUCKET" \
  --arg gateway_sa "$GATEWAY_SA" \
  --arg gateway_role_arn "$GATEWAY_ROLE_ARN" \
  --arg cluster_name "$CLUSTER_NAME" \
  --arg karpenter_instance_profile "$KARPENTER_INSTANCE_PROFILE" \
  --arg license_token "${LICENSE_TOKEN:-}" \
  '{
    vectorStore: {
      credential: {apiKey: $api_key},
      endpoint: {url: "https://api.turbopuffer.com", region: ("aws-" + $aws_region)},
      inboundAuth: {mode: "deriveFromStore"}
    },
    gateway: {image: ("hevlayer/layer-gateway-pro:" + $layer_version)},
    operator: {enabled: true, image: ("hevlayer/layer-operator:" + $layer_version)},
    dashboard: {
      enabled: true,
      image: ("hevlayer/layer-dashboard:" + $layer_version),
      basicAuth: {user: $dashboard_user, password: $dashboard_password},
      serviceAccount: {name: $dashboard_sa, roleArn: $dashboard_role_arn}
    },
    s3: {bucket: $s3_bucket},
    serviceAccount: {name: $gateway_sa, roleArn: $gateway_role_arn},
    workerKarpenter: {
      enabled: true,
      clusterName: $cluster_name,
      instanceProfile: $karpenter_instance_profile
    }
  } + if $license_token == "" then {} else {license: {token: $license_token}} end' \
  > "$VALUES_FILE"

helm upgrade --install "$HELM_RELEASE" "$HELM_CHART" \
  --namespace "$NAMESPACE" --create-namespace \
  -f "$VALUES_FILE"

log "Waiting for the gateway to become ready"
kubectl -n "$NAMESPACE" rollout status statefulset \
  --selector "app.kubernetes.io/instance=$HELM_RELEASE,app.kubernetes.io/component=gateway" \
  --timeout=10m

log "Done. Next steps: https://hevlayer.com/docs/quickstart/"
if [[ "$GENERATED_DASHBOARD_PASSWORD" == "1" ]]; then
  log "Dashboard credentials: $DASHBOARD_USER / $DASHBOARD_PASSWORD"
fi
kubectl -n "$NAMESPACE" get pods
