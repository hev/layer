resource "helm_release" "karpenter" {
  namespace        = "kube-system"
  create_namespace = false
  name             = "karpenter"
  repository       = "oci://public.ecr.aws/karpenter"
  chart            = "karpenter"
  version          = "1.5.0"
  wait             = true

  values = [yamlencode({
    settings = {
      clusterName       = module.eks.cluster_name
      interruptionQueue = module.karpenter.queue_name
    }
    serviceAccount = { name = "karpenter" }
  })]
}

resource "helm_release" "karpenter_pools" {
  namespace = "kube-system"
  name      = "hevlayer-ce-pools"
  chart     = "${path.module}/../helm/karpenter-pools"
  wait      = true

  values = [yamlencode({
    clusterName = var.cluster_name
    nodeRole    = module.karpenter.node_iam_role_name
    tags        = local.tags
  })]

  depends_on = [helm_release.karpenter]
}
