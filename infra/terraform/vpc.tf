data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  az                    = data.aws_availability_zones.available.names[0]
  az2                   = data.aws_availability_zones.available.names[1]
  worker_subnet_ids     = var.bootstrap_cluster ? (var.worker_subnet_type == "public" ? [aws_subnet.public[0].id, aws_subnet.public_b[0].id] : [aws_subnet.private[0].id]) : []
  worker_discovery_tags = { "karpenter.sh/discovery" = var.cluster_name }
}

resource "aws_vpc" "main" {
  count = var.bootstrap_cluster ? 1 : 0

  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name                                        = "${var.cluster_name}-vpc"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

# Single AZ — no cross-AZ noise in benchmarks

resource "aws_subnet" "public" {
  count = var.bootstrap_cluster ? 1 : 0

  vpc_id                  = aws_vpc.main[0].id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 1)
  availability_zone       = local.az
  map_public_ip_on_launch = true

  tags = merge({
    Name                                        = "${var.cluster_name}-public"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                    = "1"
  }, var.worker_subnet_type == "public" ? local.worker_discovery_tags : {})
}

resource "aws_subnet" "private" {
  count = var.bootstrap_cluster ? 1 : 0

  vpc_id            = aws_vpc.main[0].id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 10)
  availability_zone = local.az

  tags = merge({
    Name                                        = "${var.cluster_name}-private"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"           = "1"
  }, var.worker_subnet_type == "private" ? local.worker_discovery_tags : {})
}

resource "aws_internet_gateway" "main" {
  count = var.bootstrap_cluster ? 1 : 0

  vpc_id = aws_vpc.main[0].id

  tags = {
    Name = "${var.cluster_name}-igw"
  }
}

resource "aws_eip" "nat" {
  count = var.bootstrap_cluster && var.enable_nat_gateway ? 1 : 0

  domain = "vpc"

  tags = {
    Name = "${var.cluster_name}-nat-eip"
  }

  depends_on = [aws_internet_gateway.main]
}

resource "aws_nat_gateway" "main" {
  count = var.bootstrap_cluster && var.enable_nat_gateway ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "${var.cluster_name}-nat"
  }

  depends_on = [aws_internet_gateway.main]
}

resource "aws_route_table" "public" {
  count = var.bootstrap_cluster ? 1 : 0

  vpc_id = aws_vpc.main[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main[0].id
  }

  tags = {
    Name = "${var.cluster_name}-public-rt"
  }
}

resource "aws_route_table" "private" {
  count = var.bootstrap_cluster ? 1 : 0

  vpc_id = aws_vpc.main[0].id

  dynamic "route" {
    for_each = var.enable_nat_gateway ? [1] : []

    content {
      cidr_block     = "0.0.0.0/0"
      nat_gateway_id = aws_nat_gateway.main[0].id
    }
  }

  tags = {
    Name = "${var.cluster_name}-private-rt"
  }
}

resource "aws_subnet" "public_b" {
  count = var.bootstrap_cluster ? 1 : 0

  vpc_id                  = aws_vpc.main[0].id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 2)
  availability_zone       = local.az2
  map_public_ip_on_launch = true

  tags = merge({
    Name                                        = "${var.cluster_name}-public-b"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                    = "1"
  }, var.worker_subnet_type == "public" ? local.worker_discovery_tags : {})
}

resource "aws_subnet" "private_b" {
  count = var.bootstrap_cluster ? 1 : 0

  vpc_id            = aws_vpc.main[0].id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 11)
  availability_zone = local.az2

  tags = merge({
    Name                                        = "${var.cluster_name}-private-b"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"           = "1"
  }, var.worker_subnet_type == "private" ? local.worker_discovery_tags : {})
}

resource "aws_route_table_association" "public" {
  count = var.bootstrap_cluster ? 1 : 0

  subnet_id      = aws_subnet.public[0].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_route_table_association" "public_b" {
  count = var.bootstrap_cluster ? 1 : 0

  subnet_id      = aws_subnet.public_b[0].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_route_table_association" "private" {
  count = var.bootstrap_cluster ? 1 : 0

  subnet_id      = aws_subnet.private[0].id
  route_table_id = aws_route_table.private[0].id
}

resource "aws_route_table_association" "private_b" {
  count = var.bootstrap_cluster ? 1 : 0

  subnet_id      = aws_subnet.private_b[0].id
  route_table_id = aws_route_table.private[0].id
}
