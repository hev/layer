moved {
  from = aws_eip.nat
  to   = aws_eip.nat[0]
}

moved {
  from = aws_nat_gateway.main
  to   = aws_nat_gateway.main[0]
}

moved {
  from = aws_vpc.main
  to   = aws_vpc.main[0]
}

moved {
  from = aws_subnet.public
  to   = aws_subnet.public[0]
}

moved {
  from = aws_subnet.private
  to   = aws_subnet.private[0]
}

moved {
  from = aws_subnet.public_b
  to   = aws_subnet.public_b[0]
}

moved {
  from = aws_subnet.private_b
  to   = aws_subnet.private_b[0]
}

moved {
  from = aws_internet_gateway.main
  to   = aws_internet_gateway.main[0]
}

moved {
  from = aws_route_table.public
  to   = aws_route_table.public[0]
}

moved {
  from = aws_route_table.private
  to   = aws_route_table.private[0]
}

moved {
  from = aws_route_table_association.public
  to   = aws_route_table_association.public[0]
}

moved {
  from = aws_route_table_association.public_b
  to   = aws_route_table_association.public_b[0]
}

moved {
  from = aws_route_table_association.private
  to   = aws_route_table_association.private[0]
}

moved {
  from = aws_route_table_association.private_b
  to   = aws_route_table_association.private_b[0]
}

moved {
  from = aws_iam_role.eks_cluster
  to   = aws_iam_role.eks_cluster[0]
}

moved {
  from = aws_iam_role_policy_attachment.eks_cluster_policy
  to   = aws_iam_role_policy_attachment.eks_cluster_policy[0]
}

moved {
  from = aws_iam_role_policy_attachment.eks_vpc_resource_controller
  to   = aws_iam_role_policy_attachment.eks_vpc_resource_controller[0]
}

moved {
  from = aws_security_group.eks_cluster
  to   = aws_security_group.eks_cluster[0]
}

moved {
  from = aws_cloudwatch_log_group.eks
  to   = aws_cloudwatch_log_group.eks[0]
}

moved {
  from = aws_eks_cluster.main
  to   = aws_eks_cluster.main[0]
}

moved {
  from = aws_ec2_tag.eks_cluster_security_group_discovery
  to   = aws_ec2_tag.eks_cluster_security_group_discovery[0]
}

moved {
  from = aws_iam_openid_connect_provider.eks
  to   = aws_iam_openid_connect_provider.eks[0]
}

moved {
  from = aws_iam_role.eks_nodes
  to   = aws_iam_role.eks_nodes[0]
}

moved {
  from = aws_iam_role_policy_attachment.eks_worker_node
  to   = aws_iam_role_policy_attachment.eks_worker_node[0]
}

moved {
  from = aws_iam_role_policy_attachment.eks_cni
  to   = aws_iam_role_policy_attachment.eks_cni[0]
}

moved {
  from = aws_iam_role_policy_attachment.ecr_read
  to   = aws_iam_role_policy_attachment.ecr_read[0]
}

moved {
  from = aws_iam_instance_profile.karpenter_nodes
  to   = aws_iam_instance_profile.karpenter_nodes[0]
}

moved {
  from = aws_iam_service_linked_role.ec2_spot
  to   = aws_iam_service_linked_role.ec2_spot[0]
}

moved {
  from = aws_iam_role_policy_attachment.eks_ssm_managed_instance_core
  to   = aws_iam_role_policy_attachment.eks_ssm_managed_instance_core[0]
}

moved {
  from = aws_iam_role.karpenter_controller
  to   = aws_iam_role.karpenter_controller[0]
}

moved {
  from = aws_iam_policy.karpenter_controller
  to   = aws_iam_policy.karpenter_controller[0]
}

moved {
  from = aws_iam_role_policy_attachment.karpenter_controller
  to   = aws_iam_role_policy_attachment.karpenter_controller[0]
}

moved {
  from = aws_iam_role.lb_controller
  to   = aws_iam_role.lb_controller[0]
}

moved {
  from = aws_iam_policy.lb_controller
  to   = aws_iam_policy.lb_controller[0]
}

moved {
  from = aws_iam_role_policy_attachment.lb_controller
  to   = aws_iam_role_policy_attachment.lb_controller[0]
}

moved {
  from = aws_security_group.hev_shop_efs
  to   = aws_security_group.hev_shop_efs[0]
}

moved {
  from = aws_efs_file_system.hev_shop
  to   = aws_efs_file_system.hev_shop[0]
}

moved {
  from = aws_efs_mount_target.hev_shop_private
  to   = aws_efs_mount_target.hev_shop_private[0]
}

moved {
  from = aws_efs_mount_target.hev_shop_private_b
  to   = aws_efs_mount_target.hev_shop_private_b[0]
}

moved {
  from = aws_iam_role.efs_csi_controller
  to   = aws_iam_role.efs_csi_controller[0]
}

moved {
  from = aws_iam_role_policy_attachment.efs_csi_controller
  to   = aws_iam_role_policy_attachment.efs_csi_controller[0]
}

moved {
  from = aws_eks_addon.efs_csi
  to   = aws_eks_addon.efs_csi[0]
}

moved {
  from = aws_route53_zone.hev_shop
  to   = aws_route53_zone.hev_shop[0]
}

moved {
  from = aws_route53_zone.hevlayer
  to   = aws_route53_zone.hevlayer[0]
}

moved {
  from = aws_route53_zone.hevmesh
  to   = aws_route53_zone.hevmesh[0]
}

moved {
  from = aws_route53_zone.hevkit
  to   = aws_route53_zone.hevkit[0]
}

moved {
  from = aws_acm_certificate.hev_shop
  to   = aws_acm_certificate.hev_shop[0]
}

moved {
  from = aws_acm_certificate.hevlayer
  to   = aws_acm_certificate.hevlayer[0]
}

moved {
  from = aws_acm_certificate_validation.hev_shop
  to   = aws_acm_certificate_validation.hev_shop[0]
}

moved {
  from = aws_acm_certificate_validation.hevlayer
  to   = aws_acm_certificate_validation.hevlayer[0]
}
