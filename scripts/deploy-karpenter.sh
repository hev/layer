#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

TF_DIR="infra/terraform"
AWS_REGION="${AWS_REGION:-us-east-1}"
SKIP_TERRAFORM="${SKIP_TERRAFORM:-0}"
INSTALL_NVIDIA_PLUGIN="${INSTALL_NVIDIA_PLUGIN:-1}"

for arg in "$@"; do
  case "$arg" in
    --skip-terraform) SKIP_TERRAFORM=1 ;;
    --skip-nvidia-plugin) INSTALL_NVIDIA_PLUGIN=0 ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

if [[ "$SKIP_TERRAFORM" == "1" ]]; then
  log "Skipping terraform."
else
  log "Applying Terraform for Karpenter IAM and discovery tags..."
  terraform -chdir="$TF_DIR" init -input=false
  terraform -chdir="$TF_DIR" apply -auto-approve -input=false
fi

log "Reading Terraform outputs..."
CLUSTER_NAME=$(terraform -chdir="$TF_DIR" output -raw cluster_name 2>/dev/null || err "Missing terraform output: cluster_name")
CLUSTER_ENDPOINT=$(terraform -chdir="$TF_DIR" output -raw cluster_endpoint 2>/dev/null || err "Missing terraform output: cluster_endpoint")
KARPENTER_ROLE_ARN=$(terraform -chdir="$TF_DIR" output -raw karpenter_controller_role_arn 2>/dev/null || err "Missing terraform output: karpenter_controller_role_arn")
KARPENTER_VERSION=$(terraform -chdir="$TF_DIR" output -raw karpenter_chart_version 2>/dev/null || echo "1.12.1")

log "Updating kubeconfig..."
aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION"

log "Installing/upgrading Karpenter ${KARPENTER_VERSION}..."
helm registry logout public.ecr.aws >/dev/null 2>&1 || true
helm upgrade --install karpenter oci://public.ecr.aws/karpenter/karpenter \
  --version "$KARPENTER_VERSION" \
  --namespace kube-system \
  --create-namespace \
  --set "settings.clusterName=${CLUSTER_NAME}" \
  --set "settings.clusterEndpoint=${CLUSTER_ENDPOINT}" \
  --set "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=${KARPENTER_ROLE_ARN}" \
  --set replicas=1 \
  --set "nodeSelector.layer\\.hev\\.dev/node-role=system" \
  --set tolerations[0].key=layer.hev.dev/node-role \
  --set tolerations[0].operator=Equal \
  --set tolerations[0].value=system \
  --set tolerations[0].effect=NoSchedule \
  --wait

if [[ "$INSTALL_NVIDIA_PLUGIN" == "1" ]]; then
  log "Installing/upgrading NVIDIA device plugin for Karpenter GPU nodes..."
  helm repo add nvdp https://nvidia.github.io/k8s-device-plugin >/dev/null 2>&1 || true
  helm repo update nvdp
  helm upgrade --install nvidia-device-plugin nvdp/nvidia-device-plugin \
    --namespace nvidia-device-plugin \
    --create-namespace \
    --set "nodeSelector.layer\\.hev\\.dev/node-role=worker-gpu" \
    --set tolerations[0].key=layer.hev.dev/node-role \
    --set tolerations[0].operator=Equal \
    --set tolerations[0].value=worker-gpu \
    --set tolerations[0].effect=NoSchedule \
    --set tolerations[1].key=nvidia.com/gpu \
    --set tolerations[1].operator=Exists \
    --set tolerations[1].effect=NoSchedule \
    --wait
fi

log "Karpenter is installed. Watch with:"
log "  kubectl get pods -n kube-system -l app.kubernetes.io/name=karpenter"
log "  kubectl get nodepools,ec2nodeclasses,nodeclaims"
log "Layer worker and document-cache NodePools are rendered by the Layer chart."
