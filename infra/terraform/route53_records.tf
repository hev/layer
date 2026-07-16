################################################################################
# Public DNS records pointing at the shared ALB.
#
# Gated by var.public_dns_enabled so we don't try to look up the ALB before
# the Ingresses have been applied. Sequence:
#
#   1. Apply terraform with manage_public_dns = true and public_dns_enabled = false.
#      This creates zones, certs, and IAM without ALIAS records.
#   2. Install LB controller; apply hev-shop Ingresses; upgrade layer chart with
#      ingress.enabled=true. Wait for the shared ALB to surface (`kubectl get
#      ingress -A` shows ADDRESS populated).
#   3. Smoke-test against the ALB DNS name directly (Host header).
#   4. Set public_dns_enabled = true and apply again.
#
# The ALB is discovered by the tag LBC automatically applies to IngressGroup
# load balancers: `ingress.k8s.aws/stack = <group.name>`.
################################################################################

data "aws_lb" "shared" {
  count = var.manage_public_dns && var.public_dns_enabled ? 1 : 0

  tags = {
    "ingress.k8s.aws/stack" = "hev-public"
    "elbv2.k8s.aws/cluster" = var.cluster_name
  }
}

locals {
  alb_dns_name = var.manage_public_dns && var.public_dns_enabled ? data.aws_lb.shared[0].dns_name : ""
  alb_zone_id  = var.manage_public_dns && var.public_dns_enabled ? data.aws_lb.shared[0].zone_id : ""
}

resource "aws_route53_record" "hev_shop_apex" {
  count   = var.manage_public_dns && var.public_dns_enabled ? 1 : 0
  zone_id = aws_route53_zone.hev_shop[0].zone_id
  name    = "hev-shop.com"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "hev_shop_www" {
  count   = var.manage_public_dns && var.public_dns_enabled ? 1 : 0
  zone_id = aws_route53_zone.hev_shop[0].zone_id
  name    = "www.hev-shop.com"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "hev_shop_api" {
  count   = var.manage_public_dns && var.public_dns_enabled ? 1 : 0
  zone_id = aws_route53_zone.hev_shop[0].zone_id
  name    = "api.hev-shop.com"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "hevlayer_us_east_1" {
  count   = var.manage_public_dns && var.public_dns_enabled ? 1 : 0
  zone_id = aws_route53_zone.hevlayer[0].zone_id
  name    = "aws-us-east-1.hevlayer.com"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "hevlayer_dashboard" {
  count   = var.manage_public_dns && var.public_dns_enabled ? 1 : 0
  zone_id = aws_route53_zone.hevlayer[0].zone_id
  name    = "dashboard.hevlayer.com"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}
