# Layer gateway

> **Generated mirror — do not send PRs here.** Published automatically from
> the private `hev/layer` monorepo. Edits here are overwritten on the
> next release. File bugs and requests as
> [issues](https://github.com/hev/layer/issues); fixes land upstream and
> flow back on the next mirror release.

A transparent, turbopuffer-shaped proxy that makes your existing store better
without changing client code.

```text
╔════════════╗      ╔════════════╗          ╔═══ turbopuffer ════════════════════════╗
║ layer      ║░     ║ layer      ║░         ║                                        ║░
║ clients    ║◀────▶║ gateway    ║◀──API───▶║  ┏━━━━━━━━━┓  ┏━━━━━━━━━┓              ║░
║            ║░     ║            ║░         ║  ┃ ANN     ┃  ┃ BM25    ┃              ║░
╚════════════╝░     ╚════════════╝░         ║  ┗━━━━━━━━━┛  ┗━━━━━━━━━┛              ║░
 ░░░░░░░░░░░░░░      ░░░░░░░░░░░░░░         ║                                        ║░
                                            ╚════════════════════════════════════════╝░
                                             ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

Your application keeps speaking the turbopuffer wire it already speaks. The
gateway sits in the middle and adds the following retrieval features:

## Features

Each feature is documented at [hevlayer.com/docs](https://hevlayer.com/docs/):

- **[Hybrid search with RRF fusion](https://hevlayer.com/docs/api/query#hybrid-text-fusion).**
  Vector and full-text in one request, fused with reciprocal-rank fusion — not
  two round-trips you stitch together yourself.
- **[Automatic query routing](https://hevlayer.com/docs/api/query#query-routing).**
  The `Auto` router reads the *shape* of a query and picks vector, lexical, or
  hybrid per request. You send text; it decides how to search.
- **[Fuzzy / typo-tolerant surfacing](https://hevlayer.com/docs/api/query#surfacing-fallback).**
  Misspellings and near-misses surface instead of returning nothing, with a
  badge that tells you *why* a result was surfaced.
- **[Materialized facet values](https://hevlayer.com/docs/api/scans#values-mode).**
  Real facet rails computed from the corpus, not tallies of whatever rows a
  page happened to return.
- **[Scatter-gather scans](https://hevlayer.com/docs/api/scans).** Read the
  whole namespace efficiently, in parallel — ids, counts, and values modes.
- **[Federated queries](https://hevlayer.com/docs/api/federated-query).** Fan
  one query across many namespaces by naming them — cross-namespace search
  without a bespoke aggregator.
- **[Built-in metrics](https://hevlayer.com/docs/dashboard).** A
  Prometheus-compatible `/metrics` endpoint and a PromQL proxy at
  `/v2/metrics`, with a named metrics catalog — query latency, routing, and
  errors are observable out of the box.

## Quick Start

Run the community gateway in front of your existing turbopuffer account in
three steps. No license key or signup needed — your turbopuffer API key is the
gateway bearer token, and your data stays where it is.

```sh
export TURBOPUFFER_API_KEY="tpuf_..."
export LAYER_NAMESPACE="products"
export LAYER_GATEWAY_URL="http://localhost:8080"
```

**1. Start the gateway**

```sh
docker run --rm -p 8080:8080 \
  -e PORT=8080 \
  -e LAYER_SECRET_LOCAL_API_KEY="$TURBOPUFFER_API_KEY" \
  -e LAYER_STORE_JSON='apiVersion: hevlayer.com/v1alpha1
kind: VectorStore
metadata:
  name: local
spec:
  kind: turbopuffer
  default: true
  endpoint:
    url: https://api.turbopuffer.com
    region: aws-us-east-1
  credential:
    secretRef:
      name: local
      key: api-key
  inboundAuth:
    mode: deriveFromStore' \
  hevlayer/layer-gateway
```

**2. Initialize the namespace**

```sh
curl -X POST "$LAYER_GATEWAY_URL/v2/namespaces/$LAYER_NAMESPACE/init" \
  -H "Authorization: Bearer $TURBOPUFFER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"schema_version": 1, "shard_count": 8}'
```

**3. Run a query**

```sh
curl -X POST "$LAYER_GATEWAY_URL/v2/namespaces/$LAYER_NAMESPACE/query" \
  -H "Authorization: Bearer $TURBOPUFFER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "rank_by": ["title", "BM25", "wireless earbuds"],
    "top_k": 10
  }'
```

The same turbopuffer-compatible routes your application already calls now go
through the gateway.

Telemetry can be disabled with `LAYER_TELEMETRY=off` or `DO_NOT_TRACK=1`.
See `docs/telemetry.md`.

## Install

For a real cluster, one script brings up an opinionated install — EKS, IAM,
S3, and the Helm release — from a Layer source checkout or onboarding artifact
and a single upstream store credential:

```sh
export LAYER_SRC="/path/to/layer"
export TURBOPUFFER_API_KEY="tpuf_..."
curl -fsSL https://hevlayer.com/install.sh | bash
```

The script runs Terraform (VPC, EKS, IAM/IRSA, S3, ECR) and then installs the
Helm chart wired to those outputs. Details, the values it accepts, and the
bring-your-own-cluster path are documented at
[hevlayer.com/docs/install](https://hevlayer.com/docs/install/).

## Layout

- `apps/layer-gateway` — the open standalone gateway binary and library.
- `crates/vectorstore-core` — open vector-store clients, routing, and wire types.
- `crates/metrics-catalog` — public metric catalog metadata.

## License

Layer gateway is source-available under the Business Source License 1.1 and
converts to Apache-2.0 on the change date in `LICENSE`.

The license governs copyright. It does not grant trademark rights in the Layer
or hevlayer names; see `TRADEMARKS.md`.
