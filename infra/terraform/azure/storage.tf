# Layer durable state (gateway snapshots, history, witness; pipeline worker
# snapshots) and backups. Shared keys are off: every reader and writer comes in
# through Workload Identity and Storage Blob Data Contributor.
resource "azurerm_storage_account" "layer" {
  name                            = var.storage_account_name
  resource_group_name             = local.rg
  location                        = local.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  default_to_oauth_authentication = true
  tags                            = var.tags
}

# The Terraform SP writes containers through Entra ID (storage_use_azuread).
resource "azurerm_role_assignment" "terraform_blob" {
  scope                = azurerm_storage_account.layer.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_container" "state" {
  name               = var.state_container_name
  storage_account_id = azurerm_storage_account.layer.id
  depends_on         = [azurerm_role_assignment.terraform_blob]
}

resource "azurerm_storage_container" "backups" {
  name               = var.backup_container_name
  storage_account_id = azurerm_storage_account.layer.id
  depends_on         = [azurerm_role_assignment.terraform_blob]
}
