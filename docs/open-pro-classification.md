# Open / Pro Classification

The open gateway is a single-tenant retrieval/proxy data plane. The search is
open; the intelligence and multi-tenant control plane are pro.

## Open

- Turbopuffer-compatible query, fetch, metadata, namespace list, namespace write,
  delete, patch, and scan surfaces.
- Hybrid text, automatic query routing, fuzzy surfacing, RRF fusion, explicit
  federated query, multi-query, read-only VectorStore listing, metrics catalog,
  and metrics proxy.
- Zero-config standalone store resolution, with overrides via `LAYER_STORE_FILE` or `LAYER_STORE_JSON`.
- Single pass-through inbound auth via `inboundAuth.mode=deriveFromStore`, where
  the upstream store API key is also the gateway bearer token.

## Pro

- Pipelines, UDFs, agentic/LLM routes, RBAC and minted keys, key management,
  Warehouses, managed document/blob cache, history, checkpoints, restore,
  activity/clickstream analytics, cost/finops, Kubernetes CRD reconciliation,
  operator, dashboard, and hosted multi-tenancy.

Open builds omit pro code rather than shipping it disabled. Pro composition is
provided by the private `layer-gateway-pro` binary in the upstream monorepo.
