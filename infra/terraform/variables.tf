variable "cluster_name" {
  description = "Name for the CE EKS cluster and its discovery tags."
  type        = string
  default     = "hevlayer-ce"
}

variable "aws_region" {
  description = "AWS region in which to create the cluster."
  type        = string
  default     = "us-east-1"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.32"
}

variable "vpc_cidr" {
  description = "CIDR for the new CE VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "document_cache" {
  description = "Use an i4i NVMe system/cache node. Disable for the smaller no-document-cache footprint."
  type        = bool
  default     = true
}

variable "cache_instance_type" {
  description = "Always-on instance when document_cache is enabled."
  type        = string
  default     = "i4i.large"
}

variable "small_instance_type" {
  description = "Always-on instance when document_cache is disabled."
  type        = string
  default     = "m7i.large"
}

variable "gateway_image" {
  description = "Published CE gateway image. Override with an immutable Docker Hub tag for production."
  type        = string
  default     = "hevlayer/layer-gateway:latest"

  validation {
    condition     = !startswith(var.gateway_image, "ghcr.io/hev/")
    error_message = "hev-built images are published to ECR or Docker Hub, never ghcr.io/hev/*."
  }
}

variable "turbopuffer_api_key" {
  description = "Customer-owned Turbopuffer key used for upstream and deriveFromStore auth. Stored as sensitive Terraform state; use an encrypted remote backend."
  type        = string
  sensitive   = true
}

variable "turbopuffer_region" {
  description = "Turbopuffer region for the default VectorStore."
  type        = string
  default     = "aws-us-east-1"
}

variable "gateway_replicas" {
  description = "Number of always-on CE gateway replicas."
  type        = number
  default     = 1
}

variable "tags" {
  description = "Additional AWS resource tags."
  type        = map(string)
  default     = {}
}
