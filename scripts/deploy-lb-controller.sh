#!/usr/bin/env bash
# Install/upgrade the AWS Load Balancer Controller in kube-system.
#
# Prereqs (handled by Terraform apply):
#   - aws_iam_role.lb_controller with the upstream policy attached
#   - OIDC provider on the EKS cluster
#
# Usage:
#   ./scripts/deploy-lb-controller.sh
#
# Optional:
#   SKIP_TERRAFORM=1  Skip the terraform init/apply step
set -euo pipefail

cd "$(dirname "$0")/.."

TF_DIR="infra/terraform"
AWS_REGION="${AWS_REGION:-us-east-1}"
SKIP_TERRAFORM="${SKIP_TERRAFORM:-0}"

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

if [[ "$SKIP_TERRAFORM" == "1" ]]; then
  log "Skipping terraform."
else
  log "Applying Terraform (IRSA role + policy)..."
  terraform -chdir="$TF_DIR" init -input=false
  terraform -chdir="$TF_DIR" apply -auto-approve -input=false
fi

log "Reading Terraform outputs..."
CLUSTER_NAME=$(terraform -chdir="$TF_DIR" output -raw cluster_name 2>/dev/null) || err "Missing terraform output: cluster_name"
LB_ROLE_ARN=$(terraform -chdir="$TF_DIR" output -raw lb_controller_role_arn 2>/dev/null) || err "Missing terraform output: lb_controller_role_arn"
LB_CHART_VERSION=$(terraform -chdir="$TF_DIR" output -raw lb_controller_chart_version 2>/dev/null) || err "Missing terraform output: lb_controller_chart_version"
VPC_ID=$(terraform -chdir="$TF_DIR" output -raw vpc_id 2>/dev/null) || err "Missing terraform output: vpc_id"

log "Updating kubeconfig..."
aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION"

log "Adding eks helm repo..."
helm repo add eks https://aws.github.io/eks-charts >/dev/null 2>&1 || true
helm repo update eks

log "Installing/upgrading aws-load-balancer-controller chart ${LB_CHART_VERSION}..."
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
  --version "$LB_CHART_VERSION" \
  --namespace kube-system \
  --set "clusterName=${CLUSTER_NAME}" \
  --set "region=${AWS_REGION}" \
  --set "vpcId=${VPC_ID}" \
  --set "serviceAccount.create=true" \
  --set "serviceAccount.name=aws-load-balancer-controller" \
  --set "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=${LB_ROLE_ARN}" \
  --set "replicaCount=1" \
  --set "nodeSelector.layer\\.hev\\.dev/node-role=system" \
  --set "tolerations[0].key=layer.hev.dev/node-role" \
  --set "tolerations[0].operator=Equal" \
  --set "tolerations[0].value=system" \
  --set "tolerations[0].effect=NoSchedule" \
  --wait

log "AWS Load Balancer Controller installed. Verify with:"
log "  kubectl get pods -n kube-system -l app.kubernetes.io/name=aws-load-balancer-controller"
log "  kubectl get crds | grep elbv2"
