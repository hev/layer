data "azurerm_resource_group" "layer" {
  name = var.resource_group_name
}

data "azurerm_client_config" "current" {}

locals {
  location = data.azurerm_resource_group.layer.location
  rg       = data.azurerm_resource_group.layer.name
}

resource "azurerm_virtual_network" "layer" {
  name                = "${var.name}-vnet"
  location            = local.location
  resource_group_name = local.rg
  address_space       = [var.vnet_cidr]
  tags                = var.tags
}

resource "azurerm_subnet" "nodes" {
  name                 = "nodes"
  resource_group_name  = local.rg
  virtual_network_name = azurerm_virtual_network.layer.name
  address_prefixes     = [var.nodes_subnet_cidr]
}
