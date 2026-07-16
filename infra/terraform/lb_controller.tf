################################################################################
# AWS Load Balancer Controller IRSA role
#
# Allows the kube-system/aws-load-balancer-controller service account to manage
# ALBs/NLBs, target groups, listeners, security groups, and tag resources.
#
# The attached policy is the upstream policy shipped by the LBC project at
# the pinned version. Update lb_controller_iam_policy.json from
# https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v<version>/docs/install/iam_policy.json
# when bumping the Helm chart.
################################################################################

resource "aws_iam_role" "lb_controller" {
  count = var.enable_lb_controller_irsa ? 1 : 0

  name = "${var.cluster_name}-lb-controller"

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
          "${local.oidc_provider}:sub" = "system:serviceaccount:kube-system:aws-load-balancer-controller"
        }
      }
    }]
  })

  tags = {
    Name = "${var.cluster_name}-lb-controller"
  }
}

resource "aws_iam_policy" "lb_controller" {
  count = var.enable_lb_controller_irsa ? 1 : 0

  name        = "${var.cluster_name}-lb-controller"
  description = "AWS Load Balancer Controller policy (upstream v2.13.4)"
  policy      = file("${path.module}/lb_controller_iam_policy.json")
}

resource "aws_iam_role_policy_attachment" "lb_controller" {
  count = var.enable_lb_controller_irsa ? 1 : 0

  policy_arn = aws_iam_policy.lb_controller[0].arn
  role       = aws_iam_role.lb_controller[0].name
}
