# terraform init -backend-config=backend.example.hcl
# scripts/bootstrap-state.sh creates the account and container.
resource_group_name  = "layer-azure"
storage_account_name = "hevlayeraztfstate"
container_name       = "tfstate"
key                  = "layer-azure.tfstate"
use_azuread_auth     = true
