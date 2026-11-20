# Dev images (blob-backend gateway, operator) until a release carries them.
resource "azurerm_container_registry" "layer" {
  count               = var.acr_enabled ? 1 : 0
  name                = var.acr_name
  resource_group_name = local.rg
  location            = local.location
  sku                 = "Basic"
  admin_enabled       = false
  tags                = var.tags
}

resource "azurerm_role_assignment" "kubelet_acr_pull" {
  count                = var.acr_enabled ? 1 : 0
  scope                = azurerm_container_registry.layer[0].id
  role_definition_name = "AcrPull"
  principal_id         = local.kubelet_principal_id
  principal_type       = "ServicePrincipal"
}

# Lets the Terraform SP push dev images with `az acr login`.
resource "azurerm_role_assignment" "terraform_acr_push" {
  count                = var.acr_enabled ? 1 : 0
  scope                = azurerm_container_registry.layer[0].id
  role_definition_name = "AcrPush"
  principal_id         = data.azurerm_client_config.current.object_id
}
