#!/usr/bin/env bash
# Creates the Terraform state storage account inside the layer-azure resource
# group (the SP has no rights outside it) and grants the caller blob access.
# Idempotent. Needs `az login` as the layer-azure Terraform SP.
set -euo pipefail

RG=${RG:-layer-azure}
ACCOUNT=${ACCOUNT:-hevlayeraztfstate}
CONTAINER=${CONTAINER:-tfstate}

if ! az storage account show -g "$RG" -n "$ACCOUNT" -o none 2>/dev/null; then
  az storage account create -g "$RG" -n "$ACCOUNT" --sku Standard_LRS \
    --kind StorageV2 --min-tls-version TLS1_2 --allow-blob-public-access false \
    --allow-shared-key-access false -o none
fi
az storage account blob-service-properties update -g "$RG" -n "$ACCOUNT" \
  --enable-versioning true -o none

scope=$(az storage account show -g "$RG" -n "$ACCOUNT" --query id -o tsv)
# Object id of the signed-in principal, from the token (no Graph read needed).
assignee=$(az account get-access-token --query accessToken -o tsv | python3 -c '
import base64, json, sys
part = sys.stdin.read().split(".")[1]
print(json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))["oid"])')
if [ -z "$(az role assignment list --scope "$scope" --assignee "$assignee" \
  --role "Storage Blob Data Contributor" --query '[0].id' -o tsv)" ]; then
  az role assignment create --scope "$scope" --assignee-object-id "$assignee" \
    --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" -o none
  echo "waiting for the role assignment to propagate"
  sleep 60
fi

az storage container create --account-name "$ACCOUNT" -n "$CONTAINER" \
  --auth-mode login -o none
echo "state: $RG/$ACCOUNT/$CONTAINER"
