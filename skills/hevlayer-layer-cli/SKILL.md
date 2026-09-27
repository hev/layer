---
name: hevlayer-layer-cli
description: >-
  Operate hev layer with the layer CLI. Use when the user asks an agent to
  inspect Layer environments, query docs through layer ask, list or get
  indexes, pipelines, warehouses, or UDFs, open the operations TUI, delete
  indexes, install Layer on AWS, or run Function manifests.
---

# hev layer CLI

Use `layer` to operate hevlayer from the terminal. In a Layer checkout, prefer
a repo-local binary:

    go build -o layer ./apps/layer-cli
    ./layer --help

Use `./layer ...` for a repo-local binary and `layer ...` for one on `PATH`.
Prefer `-o json` for agent parsing and do not print API keys.

For docs questions, start with `layer ask` against the committed digest:

    layer ask grep "<query>"
    layer ask cat "<section-id>"
    layer ask tree
    layer ask glossary get "<term>"

For read-only operational inspection, prefer:

    layer -o json env ls
    layer -o json env show [NAME]
    layer -o json index list
    layer -o json index get NAME
    layer -o json pipeline list
    layer -o json pipeline get ID
    layer -o json udf list
    layer -o json udf get UDF_ID

`layer install` provisions a full AWS environment from a checkout — VPC, EKS,
IAM/IRSA, S3, ECR via Terraform, then the Helm release. `layer install status`
and `layer install uninstall` round out the lifecycle. It needs cloud
credentials; confirm the account and profile before running it.

Only `layer run` needs Kubernetes access by default. It applies a Function CR,
registers the UDF spec with the gateway, triggers discovery, and optionally
watches until the queue drains.

Confirm the target environment, gateway URL, kube context, and Kubernetes
namespace before mutating state. Mutating commands include `layer env add`,
`layer env use`, `layer env rm`, `layer index delete`, `layer install`,
`layer run`, and `layer run --rm`.

Resolve configuration in this order: explicit flags, `LAYER_*` or `HEVLAYER_*`
environment variables, `--env` or `LAYER_ENV`, the active
`~/.hevlayer/config.toml` environment, then the built-in base URL.
