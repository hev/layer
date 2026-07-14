# Minimal AWS module

This module creates a fresh VPC, EKS cluster, one always-on system node,
Karpenter CPU and GPU pools that start at zero, and the CE gateway Helm release.
It deliberately excludes Layer's hosted-account, DNS, ECR, dashboard, operator,
RBAC, and internal deployment machinery.

The default uses `i4i.large`: its local NVMe is the useful baseline when you
plan to add a document cache on the system/cache node. The CE module does not
install Layer's managed document-cache control plane. If you do not plan to run
a document cache, use the cheaper footprint:

```hcl
module "hevlayer_ce" {
  source = "github.com/hev/layer//infra/terraform"

  turbopuffer_api_key = var.turbopuffer_api_key
  document_cache      = false
  small_instance_type = "m7i.large"
}
```

For the default footprint, omit `document_cache` or set it to `true`. Apply with
AWS credentials for the customer's account:

```sh
terraform init
terraform apply -var 'turbopuffer_api_key=tpuf_...'
```

Terraform secrets are still recorded in state even when marked sensitive. Use
an encrypted, access-controlled remote backend and do not commit `.tfvars` or
state files.

The CPU and GPU NodePools contain no standing nodes: Karpenter creates them only
for matching pending pods and consolidates them after demand disappears. The
single managed system node keeps Kubernetes, Karpenter, and the gateway alive.
