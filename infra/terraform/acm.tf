################################################################################
# ACM certificates (DNS-validated)
#
# Issued in us-east-1 alongside the ALB. Each cert covers the apex plus a
# wildcard SAN so subdomains can be added without re-issuing.
#
# The for_each on the validation records uses static keys (the domain names)
# rather than the cert's domain_validation_options. This lets Terraform plan
# from a clean slate without -target hacks: the keys are known at plan time
# even though the record name/value are only known after the cert is created.
################################################################################

locals {
  hev_shop_san = toset(["hev-shop.com", "*.hev-shop.com"])
  hevlayer_san = toset(["hevlayer.com", "*.hevlayer.com"])

  hev_shop_validation = {
    for dvo in try(aws_acm_certificate.hev_shop[0].domain_validation_options, []) : dvo.domain_name => dvo
  }
  hevlayer_validation = {
    for dvo in try(aws_acm_certificate.hevlayer[0].domain_validation_options, []) : dvo.domain_name => dvo
  }
}

resource "aws_acm_certificate" "hev_shop" {
  count = var.manage_public_dns ? 1 : 0

  domain_name               = "hev-shop.com"
  subject_alternative_names = ["*.hev-shop.com"]
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name = "hev-shop.com"
  }
}

resource "aws_route53_record" "hev_shop_cert_validation" {
  for_each = var.manage_public_dns ? local.hev_shop_san : toset([])

  zone_id         = aws_route53_zone.hev_shop[0].zone_id
  name            = local.hev_shop_validation[each.value].resource_record_name
  type            = local.hev_shop_validation[each.value].resource_record_type
  ttl             = 60
  records         = [local.hev_shop_validation[each.value].resource_record_value]
  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "hev_shop" {
  count = var.manage_public_dns ? 1 : 0

  certificate_arn         = aws_acm_certificate.hev_shop[0].arn
  validation_record_fqdns = [for r in aws_route53_record.hev_shop_cert_validation : r.fqdn]
}

resource "aws_acm_certificate" "hevlayer" {
  count = var.manage_public_dns ? 1 : 0

  domain_name               = "hevlayer.com"
  subject_alternative_names = ["*.hevlayer.com"]
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name = "hevlayer.com"
  }
}

resource "aws_route53_record" "hevlayer_cert_validation" {
  for_each = var.manage_public_dns ? local.hevlayer_san : toset([])

  zone_id         = aws_route53_zone.hevlayer[0].zone_id
  name            = local.hevlayer_validation[each.value].resource_record_name
  type            = local.hevlayer_validation[each.value].resource_record_type
  ttl             = 60
  records         = [local.hevlayer_validation[each.value].resource_record_value]
  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "hevlayer" {
  count = var.manage_public_dns ? 1 : 0

  certificate_arn         = aws_acm_certificate.hevlayer[0].arn
  validation_record_fqdns = [for r in aws_route53_record.hevlayer_cert_validation : r.fqdn]
}
