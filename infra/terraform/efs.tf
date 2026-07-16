resource "aws_security_group" "hev_shop_efs" {
  count = var.bootstrap_cluster && var.enable_hev_shop_efs_bootstrap ? 1 : 0

  name_prefix = "${var.cluster_name}-hev-shop-efs-"
  description = "Allow EKS worker nodes to mount hev-shop shared EFS"
  vpc_id      = aws_vpc.main[0].id

  ingress {
    description     = "NFS from EKS worker nodes"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_eks_cluster.main[0].vpc_config[0].cluster_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.cluster_name}-hev-shop-efs"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_efs_file_system" "hev_shop" {
  count = var.bootstrap_cluster && var.enable_hev_shop_efs_bootstrap ? 1 : 0

  creation_token = "${var.cluster_name}-hev-shop"
  encrypted      = true

  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"

  lifecycle_policy {
    transition_to_ia = "AFTER_7_DAYS"
  }

  tags = {
    Name = "${var.cluster_name}-hev-shop"
  }
}

resource "aws_efs_mount_target" "hev_shop_private" {
  count = var.bootstrap_cluster && var.enable_hev_shop_efs_bootstrap ? 1 : 0

  file_system_id  = aws_efs_file_system.hev_shop[0].id
  subnet_id       = aws_subnet.private[0].id
  security_groups = [aws_security_group.hev_shop_efs[0].id]
}

resource "aws_efs_mount_target" "hev_shop_private_b" {
  count = var.bootstrap_cluster && var.enable_hev_shop_efs_bootstrap ? 1 : 0

  file_system_id  = aws_efs_file_system.hev_shop[0].id
  subnet_id       = aws_subnet.private_b[0].id
  security_groups = [aws_security_group.hev_shop_efs[0].id]
}

resource "aws_iam_role" "efs_csi_controller" {
  count = var.bootstrap_cluster && var.enable_hev_shop_efs_bootstrap ? 1 : 0

  name = "${var.cluster_name}-efs-csi-controller"

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
          "${local.oidc_provider}:sub" = "system:serviceaccount:kube-system:efs-csi-controller-sa"
        }
      }
    }]
  })

  tags = {
    Name = "${var.cluster_name}-efs-csi-controller"
  }
}

resource "aws_iam_role_policy_attachment" "efs_csi_controller" {
  count = var.bootstrap_cluster && var.enable_hev_shop_efs_bootstrap ? 1 : 0

  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEFSCSIDriverPolicy"
  role       = aws_iam_role.efs_csi_controller[0].name
}

resource "aws_eks_addon" "efs_csi" {
  count = var.bootstrap_cluster && var.enable_hev_shop_efs_bootstrap ? 1 : 0

  cluster_name                = aws_eks_cluster.main[0].name
  addon_name                  = "aws-efs-csi-driver"
  addon_version               = var.efs_csi_addon_version
  service_account_role_arn    = aws_iam_role.efs_csi_controller[0].arn
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
    aws_iam_role_policy_attachment.efs_csi_controller,
  ]
}
