terraform {
  required_version = ">= 1.9.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.14"
    }
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.14"
    }
  }

  # State lives in a storage account inside the layer-azure resource group;
  # scripts/bootstrap-state.sh creates it. See backend.example.hcl.
  backend "azurerm" {}
}

# The service principal is scoped to the layer-azure resource group only, so
# the provider must not try to register resource providers at subscription
# scope.
provider "azurerm" {
  features {}
  resource_provider_registrations = "none"
  # The Layer storage account has shared keys disabled; data-plane calls
  # (containers) go through Entra ID.
  storage_use_azuread = true
}

provider "azapi" {
  skip_provider_registration = true
}
