output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "configure_kubectl" {
  description = "Command to configure kubectl for this cluster."
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${module.eks.cluster_name}"
}

output "gateway_service" {
  description = "Command that prints the gateway service endpoint."
  value       = "kubectl -n hevlayer get service hevlayer-ce"
}

output "system_instance_type" {
  description = "Selected always-on system/cache instance type."
  value       = local.system_instance_type
}
