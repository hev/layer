# Control-plane identity. Node Auto Provisioning with a custom VNet needs it to
# manage the nodes subnet.
resource "azurerm_user_assigned_identity" "aks" {
  name                = "${var.name}-aks"
  location            = local.location
  resource_group_name = local.rg
  tags                = var.tags
}

resource "azurerm_role_assignment" "aks_network" {
  scope                = azurerm_virtual_network.layer.id
  role_definition_name = "Network Contributor"
  principal_id         = azurerm_user_assigned_identity.aks.principal_id
  principal_type       = "ServicePrincipal"
}

# The AKS System pool runs cluster services on Standard_E4as_v7. The layersys
# User pool runs Layer on Standard_L8as_v3 and carries the Layer system role
# label and taint. Postgres and VictoriaMetrics retain managed-disk PVCs.
#
# The local-NVMe DaemonSet sets the layer.hev.dev/nvme=ready node label.
# Node initialization taints need the subscription preview feature
# Microsoft.ContainerService/NodeInitializationTaintsPreview, which this
# resource-group-scoped SP cannot register, and AKS's node webhook refuses to
# remove a taint configured on a pool. When enabling it, drop the system label
# from the System pool so Layer pods land on the L8 node.
#
# Worker pools are not agent pools: Node Auto Provisioning (Karpenter for
# Azure) runs them from the chart's AKSNodeClass/NodePool pairs (worker-cpu on
# Standard_D4as_v7, worker-gpu on Standard_NC4as_T4_v3, document-cache on
# Standard_L8as_v3), all scaling to zero. NAP's own default NodePools are
# disabled so nothing is provisioned outside those pools.
resource "azapi_resource" "aks" {
  type      = "Microsoft.ContainerService/managedClusters@${var.aks_api_version}"
  name      = var.name
  parent_id = data.azurerm_resource_group.layer.id
  location  = local.location
  tags      = var.tags

  schema_validation_enabled = false

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aks.id]
  }

  body = {
    sku = {
      name = "Base"
      tier = "Free"
    }
    properties = {
      kubernetesVersion       = var.kubernetes_version
      dnsPrefix               = var.name
      enableRBAC              = true
      nodeResourceGroup       = "MC_${local.rg}_${var.name}_${local.location}"
      oidcIssuerProfile       = { enabled = true }
      securityProfile         = { workloadIdentity = { enabled = true } }
      ingressProfile          = { webAppRouting = { enabled = true } }
      nodeProvisioningProfile = { mode = "Auto", defaultNodePools = "None" }
      # KEDA scales operator-created workers and the document cache.
      workloadAutoScalerProfile = { keda = { enabled = true } }
      autoUpgradeProfile = {
        upgradeChannel       = "patch"
        nodeOSUpgradeChannel = "NodeImage"
      }
      networkProfile = {
        networkPlugin     = "azure"
        networkPluginMode = "overlay"
        networkDataplane  = "cilium"
        networkPolicy     = "cilium"
        podCidr           = var.pod_cidr
        serviceCidr       = var.service_cidr
        dnsServiceIP      = var.dns_service_ip
        loadBalancerSku   = "standard"
        outboundType      = "loadBalancer"
      }
      agentPoolProfiles = [
        {
          name         = "system"
          mode         = "System"
          type         = "VirtualMachineScaleSets"
          count        = 1
          vmSize       = var.system_vm_size
          osType       = "Linux"
          osSKU        = "Ubuntu"
          osDiskSizeGB = 64
          osDiskType   = "Managed"
          maxPods      = 110
          vnetSubnetID = azurerm_subnet.nodes.id
          nodeLabels   = var.layer_nvme_pool_enabled ? {} : { "layer.hev.dev/node-role" = "system" }
        }
      ]
    }
  }

  response_export_values = [
    "properties.oidcIssuerProfile.issuerURL",
    "properties.identityProfile.kubeletidentity.objectId",
    "properties.identityProfile.kubeletidentity.clientId",
    "properties.nodeResourceGroup",
    "properties.fqdn",
    "properties.currentKubernetesVersion",
  ]

  timeouts {
    create = "60m"
    update = "60m"
    delete = "60m"
  }

  # A GET returns every agent pool, including the optional layersys (its own
  # resource below), so the pool list always diffs. The System pool is set at
  # creation; change it with az aks nodepool (add the new pool, then delete
  # the old) or by recreating the cluster. See docs/runbooks/layer-azure.md.
  lifecycle {
    ignore_changes = [body.properties.agentPoolProfiles]
  }

  depends_on = [azurerm_role_assignment.aks_network]
}

resource "azapi_resource" "layer_system_pool" {
  count     = var.layer_nvme_pool_enabled ? 1 : 0
  type      = "Microsoft.ContainerService/managedClusters/agentPools@${var.aks_api_version}"
  name      = "layersys"
  parent_id = azapi_resource.aks.id

  schema_validation_enabled = false

  body = {
    properties = {
      mode              = "User"
      type              = "VirtualMachineScaleSets"
      count             = 1
      vmSize            = var.nvme_pool_vm_size
      availabilityZones = length(var.nvme_pool_zones) > 0 ? var.nvme_pool_zones : null
      osType            = "Linux"
      osSKU             = "Ubuntu"
      osDiskSizeGB      = 128
      osDiskType        = "Managed"
      maxPods           = 110
      vnetSubnetID      = azurerm_subnet.nodes.id
      nodeLabels        = { "layer.hev.dev/node-role" = "system" }
      nodeTaints        = ["layer.hev.dev/node-role=system:NoSchedule"]
    }
  }

  timeouts {
    create = "45m"
    update = "45m"
    delete = "45m"
  }
}

locals {
  oidc_issuer_url      = azapi_resource.aks.output.properties.oidcIssuerProfile.issuerURL
  kubelet_principal_id = azapi_resource.aks.output.properties.identityProfile.kubeletidentity.objectId
}
