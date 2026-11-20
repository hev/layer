#!/usr/bin/env bash
# Installs cert-manager, the letsencrypt ClusterIssuer and Layer on the
# layer-azure AKS cluster. Secrets come from 1Password at run time and go
# straight into Kubernetes Secrets; nothing is written to disk or printed.
# Run under ~/.local/bin/with-op (or with an op session), after
# `terraform apply` and `az aks get-credentials ... --admin`.
#
#   with-op infra/terraform/azure/scripts/install-layer.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"
NS=${NS:-layer}
CERT_MANAGER_VERSION=${CERT_MANAGER_VERSION:-v1.21.3}
OP_VAULT=${OP_VAULT:-layer-factory}
# Re-mint the license when set to 1 (trial tokens expire; see the runbook).
MINT_LICENSE=${MINT_LICENSE:-auto}

helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version "$CERT_MANAGER_VERSION" -n cert-manager --create-namespace \
  --set crds.enabled=true --wait \
  --server-side=false # AKS's admissions enforcer co-owns the webhook namespaceSelector

# One always-on node: run the app-routing nginx at one replica (default 2).
kubectl patch nginxingresscontroller default --type merge \
  -p '{"spec":{"scaling":{"minReplicas":1,"maxReplicas":2}}}'

kubectl apply -f - <<'YAML'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    privateKeySecretRef:
      name: letsencrypt-account
    solvers:
      - http01:
          ingress:
            ingressClassName: webapprouting.kubernetes.azure.com
YAML

# The chart owns its Namespace; pre-create it (for the license Secret) with
# Helm's ownership metadata so the install adopts it.
kubectl apply -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $NS
  labels:
    app.kubernetes.io/managed-by: Helm
  annotations:
    meta.helm.sh/release-name: layer
    meta.helm.sh/release-namespace: $NS
YAML

# License: a trial token signed with the production key (kid hl-2026a), minted
# locally the way the license-mint Worker does (workers/license-mint). The key
# is the ed25519 key in "Layer License Signature SSH" (op returns PKCS#8 PEM;
# the Worker wants the base64 DER body). The LICENSE_SIGNING_PRIVATE_KEY_PKCS8_B64
# item only holds a reference to a vault this account cannot read.
if [ "$MINT_LICENSE" = 1 ] || ! kubectl -n "$NS" get secret hevlayer-license >/dev/null 2>&1; then
  token=$(LICENSE_SIGNING_PRIVATE_KEY_PKCS8_B64=$(op read "op://$OP_VAULT/Layer License Signature SSH/private key" | grep -v '^-----' | tr -d '\r\n') \
    node --input-type=module -e "
      import { mintTrialLicense } from '$repo/workers/license-mint/src/worker.js';
      const issued = await mintTrialLicense(
        { email: 'layer-azure@hevmind.com', domain: 'hevmind.com', org: 'layer-azure', source: 'layer-azure-cluster' },
        { LICENSE_SIGNING_KEY_ID: 'hl-2026a', LICENSE_SIGNING_PRIVATE_KEY_PKCS8_B64: process.env.LICENSE_SIGNING_PRIVATE_KEY_PKCS8_B64 });
      process.stdout.write(issued.token);")
  case "$token" in v4.public.*) ;; *) echo "license mint failed" >&2; exit 1 ;; esac
  printf %s "$token" | kubectl -n "$NS" create secret generic hevlayer-license --from-file=license=/dev/stdin \
    --dry-run=client -o yaml | kubectl apply -f -
  unset token
fi

values=$(mktemp)
trap 'rm -f "$values"' EXIT
"$here/helm-values.sh" > "$values"

# --set-file reads the secret from a process substitution, keeping it off argv.
helm upgrade --install layer "$repo/infra/helm/layer" -n "$NS" \
  -f "$repo/infra/helm/layer/values-azure.yaml" -f "$values" \
  --set-file vectorStore.credential.apiKey=<(op read "op://$OP_VAULT/layer-turbopuffer/credential" | tr -d '\r\n') \
  --set-file dashboard.basicAuth.password=<(op read "op://$OP_VAULT/layer-dashboard/password" | tr -d '\r\n') \
  --set license.existingSecret=hevlayer-license \
  --server-side=false \
  "$@"
# Client-side apply, as layer-prod's Helm 3 does: with server-side apply KEDA's
# scale subresource owns .spec.replicas on the document cache and the upgrade
# conflicts.
