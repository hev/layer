resource "aws_iam_role" "eks_nodes" {
  count = var.bootstrap_cluster ? 1 : 0

  name = "${var.cluster_name}-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role_policy_attachment" "eks_worker_node" {
  count = var.bootstrap_cluster ? 1 : 0

  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_nodes[0].name

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role_policy_attachment" "eks_cni" {
  count = var.bootstrap_cluster ? 1 : 0

  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_nodes[0].name

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role_policy_attachment" "ecr_read" {
  count = var.bootstrap_cluster ? 1 : 0

  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_nodes[0].name

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_launch_template" "system_nodes" {
  count = var.bootstrap_cluster ? 1 : 0

  name_prefix = "${var.cluster_name}-system-"

  dynamic "block_device_mappings" {
    for_each = var.system_node_data_volume_size > 0 ? [var.system_node_data_volume_size] : []

    content {
      device_name = "/dev/xvdb"

      ebs {
        delete_on_termination = true
        encrypted             = true
        iops                  = 3000
        throughput            = 125
        volume_size           = block_device_mappings.value
        volume_type           = "gp3"
      }
    }
  }

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      delete_on_termination = true
      encrypted             = true
      iops                  = 3000
      throughput            = 125
      volume_size           = var.system_node_root_volume_size
      volume_type           = "gp3"
    }
  }

  user_data = base64encode(<<-USERDATA
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==BOUNDARY=="

%{if var.system_node_data_volume_size > 0~}
--==BOUNDARY==
Content-Type: text/x-shellscript; charset="us-ascii"

#!/bin/bash
set -euo pipefail

# EBS data-volume fallback (non-i4i instances): format and mount the gp3
# data volume for hostPath-backed state. Container image and ephemeral
# storage stay on the root volume in this mode.
DATA_MOUNT="/mnt/k8s-disks/0"
LEGACY_MOUNT="/mnt/nvme"
DATA_DEV="/dev/nvme1n1"

while [ ! -b "$DATA_DEV" ]; do sleep 1; done
if ! blkid "$DATA_DEV" >/dev/null 2>&1; then
  mkfs.xfs -f "$DATA_DEV"
fi
mkdir -p "$DATA_MOUNT"
mountpoint -q "$DATA_MOUNT" || mount "$DATA_DEV" "$DATA_MOUNT"
chmod 777 "$DATA_MOUNT"
uuid="$(blkid -s UUID -o value "$DATA_DEV" || true)"
if [ -n "$uuid" ] && ! grep -q "UUID=$uuid" /etc/fstab; then
  echo "UUID=$uuid $DATA_MOUNT xfs defaults,noatime,nofail 0 2" >> /etc/fstab
fi
ln -sfn "$DATA_MOUNT" "$LEGACY_MOUNT"

%{else~}
--==BOUNDARY==
Content-Type: application/node.eks.aws

---
apiVersion: node.eks.aws/v1alpha1
kind: NodeConfig
spec:
  instance:
    localStorage:
      # RAID0 the i4i instance store and place containerd + the kubelet on
      # it, so image churn and ephemeral storage land on the local NVMe
      # instead of the small EBS root volume (the DiskPressure trigger in
      # GH #92). nodeadm mounts the array at /mnt/k8s-disks/0.
      strategy: RAID0

--==BOUNDARY==
Content-Type: text/x-shellscript; charset="us-ascii"

#!/bin/bash
set -euo pipefail

# nodeadm mounts the RAID0 instance store at /mnt/k8s-disks/0. Keep the
# /mnt/nvme compatibility symlink that the Postgres and VictoriaMetrics
# hostPaths reference; the Aerospike hostPath already lives directly under
# /mnt/k8s-disks/0. mkdir is a no-op if nodeadm has already mounted it.
mkdir -p /mnt/k8s-disks/0
ln -sfn /mnt/k8s-disks/0 /mnt/nvme
%{endif~}
--==BOUNDARY==--
USERDATA
  )

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "${var.cluster_name}-system-node"
      Project = "hevlayer"
    }
  }

  tag_specifications {
    resource_type = "volume"
    tags = {
      Project = "hevlayer"
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_autoscaling_group_tag" "system_nodes_project" {
  count = var.bootstrap_cluster ? 1 : 0

  autoscaling_group_name = aws_eks_node_group.system[0].resources[0].autoscaling_groups[0].name

  tag {
    key                 = "Project"
    value               = "hevlayer"
    propagate_at_launch = true
  }
}

resource "aws_eks_node_group" "system" {
  count = var.bootstrap_cluster ? 1 : 0

  cluster_name           = aws_eks_cluster.main[0].name
  node_group_name_prefix = "system-"
  node_role_arn          = aws_iam_role.eks_nodes[0].arn
  subnet_ids             = local.worker_subnet_ids
  instance_types         = [coalesce(var.system_node_instance_type, var.node_instance_type)]

  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 1
  }

  launch_template {
    id      = aws_launch_template.system_nodes[0].id
    version = aws_launch_template.system_nodes[0].latest_version
  }

  labels = {
    "layer.hev.dev/node-role" = "system"
  }

  taint {
    key    = "layer.hev.dev/node-role"
    value  = "system"
    effect = "NO_SCHEDULE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node,
    aws_iam_role_policy_attachment.eks_cni,
    aws_iam_role_policy_attachment.ecr_read,
  ]

  tags = {
    Name = "${var.cluster_name}-system"
  }

  lifecycle {
    create_before_destroy = true
  }
}
