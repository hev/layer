data "aws_caller_identity" "current" {}

data "aws_eks_cluster" "existing" {
  count = var.bootstrap_cluster || var.existing_cluster_oidc_issuer_url != "" ? 0 : 1

  name = var.cluster_name
}

locals {
  managed_cluster_oidc_issuer_url = try(aws_eks_cluster.main[0].identity[0].oidc[0].issuer, "")
  existing_cluster_oidc_issuer_url = try(coalesce(
    var.existing_cluster_oidc_issuer_url,
    try(data.aws_eks_cluster.existing[0].identity[0].oidc[0].issuer, "")
  ), "")
  cluster_oidc_issuer_url    = var.bootstrap_cluster ? local.managed_cluster_oidc_issuer_url : local.existing_cluster_oidc_issuer_url
  oidc_provider              = replace(local.cluster_oidc_issuer_url, "https://", "")
  existing_oidc_provider_arn = local.oidc_provider == "" ? "" : "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${local.oidc_provider}"
  oidc_provider_arn          = var.bootstrap_cluster ? try(aws_iam_openid_connect_provider.eks[0].arn, "") : try(coalesce(var.existing_cluster_oidc_provider_arn, local.existing_oidc_provider_arn), "")

  cluster_endpoint              = var.bootstrap_cluster ? try(aws_eks_cluster.main[0].endpoint, null) : try(data.aws_eks_cluster.existing[0].endpoint, null)
  cluster_certificate_authority = var.bootstrap_cluster ? try(aws_eks_cluster.main[0].certificate_authority[0].data, null) : try(data.aws_eks_cluster.existing[0].certificate_authority[0].data, null)
  cluster_arn                   = var.bootstrap_cluster ? try(aws_eks_cluster.main[0].arn, null) : try(data.aws_eks_cluster.existing[0].arn, null)
  cluster_security_group_id     = var.bootstrap_cluster ? try(aws_eks_cluster.main[0].vpc_config[0].cluster_security_group_id, "") : try(data.aws_eks_cluster.existing[0].vpc_config[0].cluster_security_group_id, "")
  cluster_vpc_id                = var.bootstrap_cluster ? try(aws_vpc.main[0].id, "") : try(data.aws_eks_cluster.existing[0].vpc_config[0].vpc_id, "")
}

resource "terraform_data" "existing_cluster_irsa_context" {
  count = var.bootstrap_cluster ? 0 : 1

  input = local.cluster_oidc_issuer_url

  lifecycle {
    precondition {
      condition     = local.oidc_provider != "" && local.oidc_provider_arn != ""
      error_message = "existing-cluster installs require an EKS OIDC issuer and IAM provider ARN; set existing_cluster_oidc_issuer_url to skip DescribeCluster when cluster_name cannot be described."
    }
  }
}

# IAM role for mesh service account (IRSA)
resource "aws_iam_role" "mesh_sa" {
  name = "${var.cluster_name}-mesh-sa-role"

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
        }
        StringLike = {
          "${local.oidc_provider}:sub" = "system:serviceaccount:*:${var.mesh_service_account_name}"
        }
      }
    }]
  })

  tags = {
    Name = "${var.cluster_name}-mesh-sa-role"
  }
}

resource "aws_iam_policy" "mesh_s3" {
  name        = "${var.cluster_name}-mesh-s3-access"
  description = "S3 access for mesh WAL bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:CreateBucket",
          "s3:HeadBucket",
        ]
        Resource = [
          aws_s3_bucket.wal.arn,
          "${aws_s3_bucket.wal.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mesh_s3" {
  policy_arn = aws_iam_policy.mesh_s3.arn
  role       = aws_iam_role.mesh_sa.name
}
