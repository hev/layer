# Workload Identity: one user-assigned identity per Layer component, federated
# to its Kubernetes service account through the cluster's OIDC issuer.
locals {
  identities = {
    gateway = {
      service_accounts = ["${var.layer_namespace}:layer"]
      blob_role        = "Storage Blob Data Contributor"
    }
    operator = {
      service_accounts = ["${var.layer_namespace}:layer-operator"]
      blob_role        = "Storage Blob Data Contributor"
    }
    dashboard = {
      service_accounts = ["${var.layer_namespace}:layer-dashboard"]
      blob_role        = "Storage Blob Data Reader"
    }
    # Operator-created pipeline and Function worker pods.
    worker = {
      service_accounts = [for ns in var.worker_namespaces : "${ns}:layer-worker"]
      blob_role        = "Storage Blob Data Contributor"
    }
  }

  federations = merge([
    for key, identity in local.identities : {
      for sa in identity.service_accounts : "${key}/${sa}" => {
        identity  = key
        namespace = split(":", sa)[0]
        name      = split(":", sa)[1]
      }
    }
  ]...)
}

resource "azurerm_user_assigned_identity" "layer" {
  for_each            = local.identities
  name                = "${var.name}-${each.key}"
  location            = local.location
  resource_group_name = local.rg
  tags                = var.tags
}

resource "azurerm_federated_identity_credential" "layer" {
  for_each                  = local.federations
  name                      = replace("${each.value.namespace}-${each.value.name}", ":", "-")
  user_assigned_identity_id = azurerm_user_assigned_identity.layer[each.value.identity].id
  audience                  = ["api://AzureADTokenExchange"]
  issuer                    = local.oidc_issuer_url
  subject                   = "system:serviceaccount:${each.value.namespace}:${each.value.name}"
}

resource "azurerm_role_assignment" "layer_blob" {
  for_each             = local.identities
  scope                = azurerm_storage_account.layer.id
  role_definition_name = each.value.blob_role
  principal_id         = azurerm_user_assigned_identity.layer[each.key].principal_id
  principal_type       = "ServicePrincipal"
}
