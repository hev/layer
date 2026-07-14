resource "helm_release" "gateway" {
  name             = "hevlayer-ce"
  namespace        = "hevlayer"
  create_namespace = true
  chart            = "${path.module}/../helm/layer-ce"
  wait             = true

  values = [yamlencode({
    gateway = {
      image    = var.gateway_image
      replicas = var.gateway_replicas
    }
    vectorStore = {
      endpoint = {
        region = var.turbopuffer_region
      }
    }
    dashboard = {
      enabled = false
    }
  })]

  set_sensitive {
    name  = "vectorStore.credential.apiKey"
    value = var.turbopuffer_api_key
  }

  depends_on = [module.eks]
}
