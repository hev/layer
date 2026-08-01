# The demo install uses a gp3 PVC for the document cache. Fresh EKS clusters
# do not include the EBS CSI driver, so bootstrap it with least-privilege IRSA
# instead of granting volume permissions to the system-node role.
resource "aws_iam_role" "ebs_csi_controller" {
  count = var.bootstrap_cluster ? 1 : 0

  name = "${var.cluster_name}-ebs-csi-controller"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = local.oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.oidc_provider}:aud" = "sts.amazonaws.com"
          "${local.oidc_provider}:sub" = "system:serviceaccount:kube-system:ebs-csi-controller-sa"
        }
      }
    }]
  })

  tags = {
    Name = "${var.cluster_name}-ebs-csi-controller"
  }
}

resource "aws_iam_role_policy_attachment" "ebs_csi_controller" {
  count = var.bootstrap_cluster ? 1 : 0

  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
  role       = aws_iam_role.ebs_csi_controller[0].name
}

resource "aws_eks_addon" "ebs_csi" {
  count = var.bootstrap_cluster ? 1 : 0

  cluster_name                = aws_eks_cluster.main[0].name
  addon_name                  = "aws-ebs-csi-driver"
  service_account_role_arn    = aws_iam_role.ebs_csi_controller[0].arn
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  configuration_values = jsonencode({
    controller = {
      replicaCount = 1
      nodeSelector = {
        "layer.hev.dev/node-role" = "system"
      }
      tolerations = [
        {
          key      = "layer.hev.dev/node-role"
          operator = "Equal"
          value    = "system"
          effect   = "NoSchedule"
        }
      ]
    }
  })

  depends_on = [
    aws_eks_node_group.system,
    aws_iam_role_policy_attachment.ebs_csi_controller,
  ]
}
