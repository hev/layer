# ---------- Layer: S3 bucket ----------

locals {
  layer_wal_bucket_name = var.layer_wal_bucket_name != "" ? var.layer_wal_bucket_name : lower("hevlayer-turbopuffer-${var.environment}-${data.aws_caller_identity.current.account_id}-${var.region}")
}

resource "aws_s3_bucket" "layer_wal" {
  bucket = local.layer_wal_bucket_name

  tags = {
    Name = local.layer_wal_bucket_name
  }
}

resource "aws_s3_bucket_versioning" "layer_wal" {
  bucket = aws_s3_bucket.layer_wal.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer_wal" {
  bucket = aws_s3_bucket.layer_wal.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "layer_wal" {
  bucket = aws_s3_bucket.layer_wal.id

  rule {
    id     = "expire-layer-history"
    status = "Enabled"

    filter {
      prefix = "history/"
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  rule {
    id     = "expire-layer-snapshots"
    status = "Enabled"

    filter {
      prefix = "snapshots/"
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

}

resource "aws_s3_bucket_public_access_block" "layer_wal" {
  bucket = aws_s3_bucket.layer_wal.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------- Layer: ECR repository ----------

resource "aws_ecr_repository" "layer_gateway" {
  name                 = "layer-gateway"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name = "layer-gateway"
  }
}

resource "aws_ecr_lifecycle_policy" "layer_gateway" {
  repository = aws_ecr_repository.layer_gateway.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

resource "aws_ecr_repository" "layer_dashboard" {
  name                 = "layer-dashboard"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name = "layer-dashboard"
  }
}

resource "aws_ecr_lifecycle_policy" "layer_dashboard" {
  repository = aws_ecr_repository.layer_dashboard.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

resource "aws_ecr_repository" "layer_operator" {
  name                 = "layer-operator"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name = "layer-operator"
  }
}

resource "aws_ecr_lifecycle_policy" "layer_operator" {
  repository = aws_ecr_repository.layer_operator.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# ---------- Layer: IRSA roles ----------

resource "aws_iam_role" "layer_sa" {
  name = "${var.cluster_name}-layer-sa-role"

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
          "${local.oidc_provider}:sub" = "system:serviceaccount:${var.kubernetes_namespace}:${var.layer_service_account_name}"
        }
      }
    }]
  })

  tags = {
    Name = "${var.cluster_name}-layer-sa-role"
  }
}

resource "aws_iam_policy" "layer_s3" {
  name        = "${var.cluster_name}-layer-s3-access"
  description = "S3 access for layer WAL bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:CreateBucket",
          "s3:HeadBucket",
        ]
        Resource = [
          aws_s3_bucket.layer_wal.arn,
          "${aws_s3_bucket.layer_wal.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "layer_s3" {
  policy_arn = aws_iam_policy.layer_s3.arn
  role       = aws_iam_role.layer_sa.name
}

resource "aws_iam_policy" "layer_cost_read" {
  name        = "${var.cluster_name}-layer-cost-read"
  description = "Read AWS Cost Explorer invoice data for Layer gateway cost API"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CostExplorerInvoiceSpend"
        Effect = "Allow"
        Action = [
          "ce:GetCostAndUsage",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "layer_cost_read" {
  policy_arn = aws_iam_policy.layer_cost_read.arn
  role       = aws_iam_role.layer_sa.name
}

resource "aws_iam_role" "layer_dashboard_sa" {
  name = "${var.cluster_name}-layer-dashboard-sa-role"

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
          "${local.oidc_provider}:sub" = "system:serviceaccount:${var.kubernetes_namespace}:${var.layer_dashboard_service_account_name}"
        }
      }
    }]
  })

  tags = {
    Name = "${var.cluster_name}-layer-dashboard-sa-role"
  }
}
