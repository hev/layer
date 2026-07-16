################################################################################
# Route53 hosted zones
#
# These zones were created automatically when the domains were registered via
# Route53 Domains (see scripts/route53-register-domains.sh). They are imported
# into Terraform state with:
#
#   terraform import aws_route53_zone.hev_shop Z0862227M3QN6HT72EAD
#   terraform import aws_route53_zone.hevlayer Z0918742O0BVF8S5G2WE
#   terraform import aws_route53_zone.hevmesh  Z0918354DCPHHK5P5S57
#   terraform import aws_route53_zone.hevkit   Z08404601W9XS1PB7DV2N
#
# DNS records for individual hostnames live in route53_records.tf so that
# zone management and record management stay readable as the set grows.
################################################################################

resource "aws_route53_zone" "hev_shop" {
  count = var.manage_public_dns ? 1 : 0

  name = "hev-shop.com"

  tags = {
    Name = "hev-shop.com"
  }
}

resource "aws_route53_zone" "hevlayer" {
  count = var.manage_public_dns ? 1 : 0

  name = "hevlayer.com"

  tags = {
    Name = "hevlayer.com"
  }
}

resource "aws_route53_zone" "hevmesh" {
  count = var.manage_public_dns ? 1 : 0

  name = "hevmesh.com"

  tags = {
    Name = "hevmesh.com"
  }
}

resource "aws_route53_zone" "hevkit" {
  count = var.manage_public_dns ? 1 : 0

  name = "hevkit.com"

  tags = {
    Name = "hevkit.com"
  }
}
