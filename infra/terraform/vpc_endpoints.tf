resource "aws_vpc_endpoint" "s3" {
  count = var.bootstrap_cluster && var.enable_s3_gateway_endpoint ? 1 : 0

  vpc_id            = aws_vpc.main[0].id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = [
    aws_route_table.public[0].id,
    aws_route_table.private[0].id,
  ]

  tags = {
    Name = "${var.cluster_name}-s3-gateway"
  }
}
