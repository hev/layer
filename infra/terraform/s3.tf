locals {
  wal_bucket_name = var.wal_bucket_name != "" ? var.wal_bucket_name : lower("hevmesh-wal-${var.environment}-${data.aws_caller_identity.current.account_id}-${var.region}")
}

resource "aws_s3_bucket" "wal" {
  bucket = local.wal_bucket_name

  tags = {
    Name = local.wal_bucket_name
  }
}

resource "aws_s3_bucket_versioning" "wal" {
  bucket = aws_s3_bucket.wal.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "wal" {
  bucket = aws_s3_bucket.wal.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "wal" {
  bucket = aws_s3_bucket.wal.id

  rule {
    id     = "expire-benchmark-data"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }
}

resource "aws_s3_bucket_public_access_block" "wal" {
  bucket = aws_s3_bucket.wal.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
