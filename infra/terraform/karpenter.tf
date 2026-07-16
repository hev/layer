data "aws_partition" "current" {}

resource "aws_iam_instance_profile" "karpenter_nodes" {
  count = var.bootstrap_cluster && var.enable_karpenter_bootstrap ? 1 : 0

  name = "${var.cluster_name}-karpenter-node"
  role = aws_iam_role.eks_nodes[0].name

  tags = {
    Name                     = "${var.cluster_name}-karpenter-node"
    "karpenter.sh/discovery" = var.cluster_name
  }
}

resource "aws_iam_service_linked_role" "ec2_spot" {
  count = var.bootstrap_cluster && var.enable_karpenter_bootstrap ? 1 : 0

  aws_service_name = "spot.amazonaws.com"
}

resource "aws_iam_role_policy_attachment" "eks_ssm_managed_instance_core" {
  count = var.bootstrap_cluster && var.enable_karpenter_bootstrap ? 1 : 0

  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
  role       = aws_iam_role.eks_nodes[0].name
}

resource "aws_iam_role" "karpenter_controller" {
  count = var.bootstrap_cluster && var.enable_karpenter_bootstrap ? 1 : 0

  name = "${var.cluster_name}-karpenter-controller"

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
          "${local.oidc_provider}:sub" = "system:serviceaccount:kube-system:karpenter"
        }
      }
    }]
  })

  tags = {
    Name                     = "${var.cluster_name}-karpenter-controller"
    "karpenter.sh/discovery" = var.cluster_name
  }
}

resource "aws_iam_policy" "karpenter_controller" {
  count = var.bootstrap_cluster && var.enable_karpenter_bootstrap ? 1 : 0

  name        = "${var.cluster_name}-karpenter-controller"
  description = "Permissions for Karpenter to provision EKS worker nodes"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowRegionalEC2ReadActions"
        Effect = "Allow"
        Action = [
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeCapacityReservations",
          "ec2:DescribeImages",
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceStatus",
          "ec2:DescribeInstanceTypeOfferings",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplates",
          "ec2:DescribePlacementGroups",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSpotPriceHistory",
          "ec2:DescribeSubnets",
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "aws:RequestedRegion" = var.region
          }
        }
      },
      {
        Sid    = "AllowPricingReadActions"
        Effect = "Allow"
        Action = [
          "pricing:GetProducts",
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowSSMReadActions"
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
        ]
        Resource = "arn:${data.aws_partition.current.partition}:ssm:${var.region}::parameter/aws/service/*"
      },
      {
        Sid    = "AllowAPIServerEndpointDiscovery"
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster",
        ]
        Resource = aws_eks_cluster.main[0].arn
      },
      {
        Sid    = "AllowEC2NodeLifecycle"
        Effect = "Allow"
        Action = [
          "ec2:CreateFleet",
          "ec2:CreateLaunchTemplate",
          "ec2:CreateTags",
          "ec2:DeleteLaunchTemplate",
          "ec2:RunInstances",
          "ec2:TerminateInstances",
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowPassingExistingNodeRole"
        Effect = "Allow"
        Action = [
          "iam:PassRole",
        ]
        Resource = aws_iam_role.eks_nodes[0].arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "ec2.amazonaws.com"
          }
        }
      },
      {
        Sid    = "AllowInstanceProfileReadActions"
        Effect = "Allow"
        Action = [
          "iam:GetInstanceProfile",
          "iam:ListInstanceProfiles",
        ]
        Resource = "*"
      },
    ]
  })

  tags = {
    Name                     = "${var.cluster_name}-karpenter-controller"
    "karpenter.sh/discovery" = var.cluster_name
  }
}

resource "aws_iam_role_policy_attachment" "karpenter_controller" {
  count = var.bootstrap_cluster && var.enable_karpenter_bootstrap ? 1 : 0

  policy_arn = aws_iam_policy.karpenter_controller[0].arn
  role       = aws_iam_role.karpenter_controller[0].name
}
