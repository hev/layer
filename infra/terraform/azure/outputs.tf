output "cluster_name" {
  value = azapi_resource.aks.name
}

output "resource_group_name" {
  value = local.rg
}

output "oidc_issuer_url" {
  value = local.oidc_issuer_url
}

output "node_resource_group" {
  value = azapi_resource.aks.output.properties.nodeResourceGroup
}

output "storage_account_name" {
  value = azurerm_storage_account.layer.name
}

output "object_store_url" {
  value = "az://${azurerm_storage_container.state.name}"
}

output "acr_login_server" {
  value = var.acr_enabled ? azurerm_container_registry.layer[0].login_server : null
}

output "tenant_id" {
  value = data.azurerm_client_config.current.tenant_id
}

# Workload Identity client ids for the Helm values
# (serviceAccount.clientId, dashboard.serviceAccount.clientId,
# operator.serviceAccount.clientId, workerIdentity.clientId).
output "workload_identity_client_ids" {
  value = { for key, identity in azurerm_user_assigned_identity.layer : key => identity.client_id }
}
