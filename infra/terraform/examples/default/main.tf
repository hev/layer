module "hevlayer_ce" {
  source = "../.."

  cluster_name        = "hevlayer-ce"
  turbopuffer_api_key = var.turbopuffer_api_key
  document_cache      = true
  cache_instance_type = "i4i.large"
}

variable "turbopuffer_api_key" {
  type      = string
  sensitive = true
}
