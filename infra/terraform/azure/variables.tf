variable "resource_group_name" {
  description = "Existing resource group that holds every layer-azure resource. The Terraform service principal is scoped to it."
  type        = string
  default     = "layer-azure"
}

variable "name" {
  description = "Base name for the cluster and its resources."
  type        = string
  default     = "layer-azure"
}

variable "kubernetes_version" {
  description = "AKS Kubernetes minor version (GA). Patch upgrades follow the patch auto-upgrade channel."
  type        = string
  default     = "1.37"
}

variable "aks_api_version" {
  description = "Microsoft.ContainerService API version (GA) for the managed cluster."
  type        = string
  default     = "2026-09-01"
}

variable "system_vm_size" {
  description = "VM size of the AKS System pool. It carries the Layer system role only when layersys is disabled."
  type        = string
  default     = "Standard_E4as_v7"
}

variable "layer_nvme_pool_enabled" {
  description = "Add the layersys pool for Luxir and Aerospike: 1x Standard_L8as_v3 with local NVMe."
  type        = bool
  default     = true
}

variable "nvme_pool_vm_size" {
  description = "SKU of the optional layersys NVMe pool."
  type        = string
  default     = "Standard_L8as_v3"
}

variable "nvme_pool_zones" {
  description = "Zones for the layersys pool. Standard_L8as_v3 is NotAvailableForSubscription in every eastus2 zone for this subscription, so it is regional (no zones)."
  type        = list(string)
  default     = []
}

variable "vnet_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "nodes_subnet_cidr" {
  type    = string
  default = "10.42.0.0/20"
}

variable "pod_cidr" {
  type    = string
  default = "10.244.0.0/16"
}

variable "service_cidr" {
  type    = string
  default = "10.43.0.0/16"
}

variable "dns_service_ip" {
  type    = string
  default = "10.43.0.10"
}

variable "storage_account_name" {
  description = "Storage account for Layer durable state (az://layer). Globally unique."
  type        = string
  default     = "hevlayeraz"
}

variable "state_container_name" {
  description = "Blob container for gateway snapshots, history and witness."
  type        = string
  default     = "layer"
}

variable "backup_container_name" {
  description = "Blob container for vmbackup and artifacts."
  type        = string
  default     = "layer-backups"
}

variable "acr_enabled" {
  description = "Create the dev ACR. Needs Microsoft.ContainerRegistry registered on the subscription (a subscription-scope action the Terraform SP cannot perform)."
  type        = bool
  default     = true
}

variable "acr_name" {
  type    = string
  default = "hevlayeraz"
}

variable "layer_namespace" {
  description = "Kubernetes namespace of the Layer Helm release."
  type        = string
  default     = "layer"
}

variable "worker_namespaces" {
  description = "Namespaces whose operator-created worker pods (service account layer-worker) are federated to the worker identity. Azure allows 20 federated credentials per identity."
  type        = list(string)
  default     = ["layer"]
}

variable "tags" {
  type = map(string)
  default = {
    Project     = "hevlayer"
    Environment = "layer-azure"
    ManagedBy   = "terraform"
  }
}
