module "hevlayer_ce" {
  source = "../.."

  cluster_name        = "hevlayer-ce-small"
  turbopuffer_api_key = var.turbopuffer_api_key
  document_cache      = false
  small_instance_type = "m7i.large"
}

variable "turbopuffer_api_key" {
  type      = string
  sensitive = true
}
