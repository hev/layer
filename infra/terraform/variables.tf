variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "layer-prod"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes minor version"
  type        = string
  default     = "1.35"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "wal_bucket_name" {
  description = "Optional explicit name for the legacy mesh WAL bucket. Defaults to the environment-derived name."
  type        = string
  default     = ""
}

variable "layer_wal_bucket_name" {
  description = "Optional explicit name for the Layer durable history/snapshot bucket. Defaults to the environment-derived name."
  type        = string
  default     = ""
}

variable "bootstrap_cluster" {
  description = "Provision the optional EKS/VPC/node bootstrap stack. Set false when binding Layer to an existing cluster."
  type        = bool
  default     = false
}

variable "existing_cluster_oidc_issuer_url" {
  description = "OIDC issuer URL for an existing EKS cluster. Optional when Terraform can describe cluster_name."
  type        = string
  default     = ""
}

variable "existing_cluster_oidc_provider_arn" {
  description = "IAM OIDC provider ARN for an existing EKS cluster. Optional when it follows the standard account-local EKS provider ARN."
  type        = string
  default     = ""
}

variable "node_instance_type" {
  description = "Default EC2 instance type for non-specialized node groups"
  type        = string
  default     = "m6a.large"
}

variable "system_node_instance_type" {
  description = "Optional EC2 instance type override for the always-on Layer system node group"
  type        = string
  default     = "i4i.large"
}

variable "system_node_root_volume_size" {
  description = "Size in GiB of the system node root/image volume"
  type        = number
  default     = 40
}

variable "system_node_data_volume_size" {
  description = "Size in GiB of the gp3 data volume mounted at /mnt/nvme on system nodes. Set to 0 to use local instance store when present, falling back to the root volume."
  type        = number
  default     = 0
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "worker_subnet_type" {
  description = "Subnet class used by managed and Karpenter worker nodes. Public is the 0.1 fresh-cluster default so workers use the internet gateway instead of a standing NAT gateway."
  type        = string
  default     = "public"

  validation {
    condition     = contains(["public", "private"], var.worker_subnet_type)
    error_message = "worker_subnet_type must be either public or private."
  }
}

variable "enable_nat_gateway" {
  description = "Provision a managed NAT Gateway for private-subnet worker egress to third-party services."
  type        = bool
  default     = false
}

variable "enable_s3_gateway_endpoint" {
  description = "Provision a free S3 gateway VPC endpoint and route S3 traffic locally through the VPC route tables."
  type        = bool
  default     = true
}

variable "enable_karpenter_bootstrap" {
  description = "When bootstrap_cluster is true, provision IAM prerequisites for Karpenter."
  type        = bool
  default     = true
}

variable "enable_lb_controller_irsa" {
  description = "Provision the AWS Load Balancer Controller IRSA role. Existing clusters that already own this role can disable it."
  type        = bool
  default     = true
}

variable "enable_hev_shop_efs_bootstrap" {
  description = "When bootstrap_cluster is true, provision the legacy hev-shop EFS filesystem and EFS CSI add-on."
  type        = bool
  default     = true
}

variable "kubernetes_namespace" {
  description = "Kubernetes namespace used by the Helm release"
  type        = string
  default     = "layer"
}

variable "mesh_service_account_name" {
  description = "Kubernetes service account name used by mesh pods"
  type        = string
  default     = "mesh"
}

variable "ecr_repository_name" {
  description = "ECR repository name for the mesh runtime image"
  type        = string
  default     = "mesh"
}

variable "layer_service_account_name" {
  description = "Kubernetes service account name used by layer gateway pods"
  type        = string
  default     = "layer"
}

variable "layer_dashboard_service_account_name" {
  description = "Kubernetes service account name used by the layer dashboard pod"
  type        = string
  default     = "layer-dashboard"
}

variable "karpenter_chart_version" {
  description = "Karpenter Helm chart version to install"
  type        = string
  default     = "1.12.1"
}

variable "lb_controller_chart_version" {
  description = "aws-load-balancer-controller Helm chart version (ships LBC v2.13.4)"
  type        = string
  default     = "1.13.4"
}

variable "public_dns_enabled" {
  description = "Toggle Route53 ALIAS records for public hostnames. Requires manage_public_dns=true. Set to true only after the shared ALB (IngressGroup hev-public) is provisioned and smoke-tested."
  type        = bool
  default     = false
}

variable "manage_public_dns" {
  description = "Provision hevlayer-managed Route53 hosted zones and ACM certificates. Disabled by default; most installs bring existing DNS/TLS."
  type        = bool
  default     = false
}

variable "efs_csi_addon_version" {
  description = "AWS EFS CSI driver EKS add-on version"
  type        = string
  default     = "v3.1.0-eksbuild.1"
}
