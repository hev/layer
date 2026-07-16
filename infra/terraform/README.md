# layer Terraform

Terraform uses a partial S3 backend so each AWS account can supply its own state
bucket and lock table at init time. Do not commit account-specific backend
files.

## Fresh account backend

Create the backend resources in the target account:

```sh
make bootstrap
```

Then create `backend.hcl` from `backend.example.hcl` and set the account's
bucket name:

```hcl
bucket         = "hevlayer-123456789012-terraform-state"
key            = "layer/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "terraform-lock"
encrypt        = true
```

Initialize Terraform with the account backend:

```sh
terraform init -backend-config=backend.hcl
# or
make init TF_BACKEND_CONFIG=backend.hcl
```

## Cluster storage

The i4i system node's instance-store devices are combined as RAID0 by nodeadm
and mounted at `/mnt/k8s-disks/0`. The Layer Helm chart installs the default
`local-path` StorageClass and dynamically creates volumes beneath that mount.
`WaitForFirstConsumer` and the StorageClass topology constrain consumers to the
system node that owns the bytes.

This storage is fast but ephemeral: an i4i node replacement, Karpenter recycle,
or underlying instance failure permanently destroys its local volumes. Use it
only for caches, scratch data, externally backed-up metrics, and rebuildable
demo state. Keep durable/system-of-record data in S3 or another durable store.
The legacy `gp2` class is not supported on `layer-prod` because the cluster does
not install EBS CSI; `hev-shop-efs` is only for workloads that require shared
RWX storage.
