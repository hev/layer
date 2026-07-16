output "cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = local.cluster_endpoint
}

output "cluster_name" {
  description = "EKS cluster name"
  value       = var.cluster_name
}

output "cluster_certificate_authority" {
  description = "EKS cluster CA certificate (base64)"
  value       = local.cluster_certificate_authority
  sensitive   = true
}

output "kubeconfig_command" {
  description = "Command to update kubeconfig"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${var.cluster_name}"
}

output "s3_bucket_name" {
  description = "WAL S3 bucket name"
  value       = aws_s3_bucket.wal.id
}

output "mesh_role_arn" {
  description = "IRSA role ARN for mesh service account"
  value       = aws_iam_role.mesh_sa.arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for mesh images"
  value       = aws_ecr_repository.mesh.repository_url
}

output "ecr_amazon_reviews_url" {
  description = "ECR repository URL for amazon-reviews images"
  value       = aws_ecr_repository.amazon_reviews.repository_url
}

output "ecr_hev_shop_indexer_url" {
  description = "ECR repository URL for hev-shop-indexer images"
  value       = aws_ecr_repository.hev_shop_indexer.repository_url
}

output "ecr_hev_shop_search_url" {
  description = "ECR repository URL for hev-shop-search images"
  value       = aws_ecr_repository.hev_shop_search.repository_url
}

output "ecr_hev_shop_web_url" {
  description = "ECR repository URL for hev-shop-web images"
  value       = aws_ecr_repository.hev_shop_web.repository_url
}

output "layer_role_arn" {
  description = "IRSA role ARN for layer gateway service account"
  value       = aws_iam_role.layer_sa.arn
}

output "layer_gateway_role_arn" {
  description = "IRSA role ARN for layer gateway service account"
  value       = aws_iam_role.layer_sa.arn
}

output "layer_dashboard_role_arn" {
  description = "IRSA role ARN for layer dashboard service account"
  value       = aws_iam_role.layer_dashboard_sa.arn
}

output "layer_service_account_name" {
  description = "Kubernetes service account name used by layer gateway pods"
  value       = var.layer_service_account_name
}

output "layer_namespace" {
  description = "Kubernetes namespace expected by the Layer IRSA trust policies"
  value       = var.kubernetes_namespace
}

output "layer_dashboard_service_account_name" {
  description = "Kubernetes service account name used by the layer dashboard pod"
  value       = var.layer_dashboard_service_account_name
}

output "layer_s3_bucket_name" {
  description = "Layer WAL S3 bucket name"
  value       = aws_s3_bucket.layer_wal.id
}

output "ecr_layer_gateway_url" {
  description = "ECR repository URL for layer-gateway images"
  value       = aws_ecr_repository.layer_gateway.repository_url
}

output "ecr_layer_dashboard_url" {
  description = "ECR repository URL for layer-dashboard images"
  value       = aws_ecr_repository.layer_dashboard.repository_url
}

output "ecr_layer_operator_url" {
  description = "ECR repository URL for layer-operator images"
  value       = aws_ecr_repository.layer_operator.repository_url
}

output "karpenter_controller_role_arn" {
  description = "IRSA role ARN for the Karpenter controller service account"
  value       = try(aws_iam_role.karpenter_controller[0].arn, null)
}

output "karpenter_node_instance_profile_name" {
  description = "Instance profile name used by Karpenter-created worker nodes"
  value       = try(aws_iam_instance_profile.karpenter_nodes[0].name, null)
}

output "karpenter_chart_version" {
  description = "Karpenter Helm chart version"
  value       = var.karpenter_chart_version
}

output "hev_shop_efs_file_system_id" {
  description = "EFS filesystem ID for hev-shop shared data"
  value       = try(aws_efs_file_system.hev_shop[0].id, null)
}

output "efs_csi_controller_role_arn" {
  description = "IRSA role ARN for the EFS CSI controller"
  value       = try(aws_iam_role.efs_csi_controller[0].arn, null)
}

output "route53_zone_ids" {
  description = "Route53 hosted zone IDs keyed by domain"
  value = var.manage_public_dns ? tomap({
    "hev-shop.com" = aws_route53_zone.hev_shop[0].zone_id
    "hevlayer.com" = aws_route53_zone.hevlayer[0].zone_id
    "hevmesh.com"  = aws_route53_zone.hevmesh[0].zone_id
    "hevkit.com"   = aws_route53_zone.hevkit[0].zone_id
  }) : tomap({})
}

output "acm_cert_arns" {
  description = "Issued ACM certificate ARNs keyed by domain"
  value = var.manage_public_dns ? tomap({
    "hev-shop.com" = aws_acm_certificate_validation.hev_shop[0].certificate_arn
    "hevlayer.com" = aws_acm_certificate_validation.hevlayer[0].certificate_arn
  }) : tomap({})
}

output "lb_controller_role_arn" {
  description = "IRSA role ARN for the AWS Load Balancer Controller"
  value       = try(aws_iam_role.lb_controller[0].arn, null)
}

output "lb_controller_chart_version" {
  description = "aws-load-balancer-controller Helm chart version"
  value       = var.lb_controller_chart_version
}

output "vpc_id" {
  description = "VPC ID hosting the EKS cluster when known"
  value       = local.cluster_vpc_id
}
