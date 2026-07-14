# Layer gateway

> **Generated mirror — do not send PRs here.** Published automatically from
> the private `hev/layer` monorepo. Edits here are overwritten on the
> next release. File bugs and requests as
> [issues](https://github.com/hev/layer/issues); fixes land upstream and
> flow back on the next mirror release.

A transparent, turbopuffer-shaped proxy that makes your existing store better
without changing client code. It is source-available under BSL 1.1 and free
to self-host at any scale.

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

- **[Automatic query routing](https://hevlayer.com/docs/api/query#query-routing).**
  The `Auto` router reads the *shape* of a query and picks vector, lexical, or
  [hybrid with RRF fusion](https://hevlayer.com/docs/api/query#hybrid-text-fusion)
  per request — vector and full-text in one request, fused with
  reciprocal-rank fusion. You send text; it decides how to search.
- **[Fuzzy / typo-tolerant surfacing](https://hevlayer.com/docs/api/query#surfacing-fallback).**
  Misspellings and near-misses surface instead of returning nothing, with a
  badge that tells you *why* a result was surfaced.
- **Search by id, within a radius, or at a point in time.**
  [Query by id](https://hevlayer.com/docs/api/query#query-by-id) ranks by
  stored document vectors ("more like these"),
  [radius scans](https://hevlayer.com/docs/api/scans#radius-count) select
  everything within a distance ball, and
  [temporal `as_of` / `between` windows](https://hevlayer.com/docs/api/query#stable-reads)
  pin any query to a moment in time — composing with filters and every
  routing strategy.
- **[Batch queries](https://hevlayer.com/docs/api/query#batch-query).** Run
  up to 16 ranked legs — each independently vector or full-text, with its own
  filters and `top_k` — in a single round trip.
- **[Scatter-gather scans](https://hevlayer.com/docs/api/scans).** Read the
  whole namespace efficiently, in parallel — row selection by filter,
  full-text, hybrid text, or radius.
- **[Match counts](https://hevlayer.com/docs/api/scans#count-mode).** Count
  everything a selector matches across the namespace — a
  [filter](https://hevlayer.com/docs/api/scans#count-mode), a
  [full-text](https://hevlayer.com/docs/api/scans#full-text-count) or
  [hybrid text](https://hevlayer.com/docs/api/scans#hybrid-text-count) query,
  or a [distance ball](https://hevlayer.com/docs/api/scans#radius-count).
- **[Facet values](https://hevlayer.com/docs/api/scans#values-mode).**
  Distinct values of an attribute with per-value counts, scanned live against
  the corpus — real facet rails, not tallies of whatever rows a page happened
  to return.
- **[Federated queries](https://hevlayer.com/docs/api/federated-query).** Fan
  one query across many namespaces by naming them — cross-namespace search
  without a bespoke aggregator.
- **[Built-in metrics](https://hevlayer.com/docs/dashboard).** A
  Prometheus-compatible `/metrics` endpoint and a PromQL proxy at
  `/v2/metrics`, with a named metrics catalog — query latency, routing, and
  errors are observable out of the box.
- **[A CLI with an ops TUI](https://hevlayer.com/docs/cli).** `layer init`,
  namespace listing and deletion, snapshot jobs and snapshot policies, and a
  read-only terminal dashboard — every command speaks the gateway API with
  just your API key, no cluster access needed.

## Pro features

Everything above runs free, with no signup. A hev layer license key unlocks
the pro tier on the same gateway —
[start a trial](https://hevlayer.com/#start-trial) and a signed key arrives
by email:

- **[Materialized facets and stable queries](https://hevlayer.com/docs/api/scans#precomputed-serving).**
  The same facet rails and reads, served from snapshot-precomputed
  aggregates — materialized ahead of time instead of scanned live.
- **[Autoscaling transform runtime and pipelines](https://hevlayer.com/docs/api/pipelines).**
  Two-stage indexing — extract and chunk on CPU, embed on GPU — with UDF
  workers that scale with load.
- **[Search history and clickstream](https://hevlayer.com/docs/api/search-history).**
  Per-namespace query and click history, persisted and replayable.
- **[Agentic search](https://hevlayer.com/docs/api/agents).** A configured
  reasoning loop over your indices: plan, fan out for recall, score for
  relevance, and return the standard row shape.

See [licensing](https://hevlayer.com/docs/licensing) for how keys are issued,
installed, and verified (offline — the gateway never phones home).

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

## Docker Compose

The repo ships a `docker-compose.yml` for the same gateway with a restart
policy and health check. Copy `.env.example` to `.env`, fill in your
turbopuffer API key, and:

```sh
docker compose up
```

## Layout

- `apps/layer-gateway` — the open standalone gateway binary and library.
- `crates/vectorstore-core` — open vector-store clients, routing, and wire types.
- `crates/metrics-catalog` — public metric catalog metadata.

## Beyond the gateway

Everything above ships in this repo. The commercial edition adds the pieces
that run *behind* the gateway in your cluster: the Kubernetes **operator**, the
**function runtime** (embedding, classification, tagging, and attribute
migration as one Kubernetes-native `Function` primitive, with leasing, retries,
writeback, and scale-to-zero owned for you), and the **dashboard**. It is
licensed per operator deployment. The full docs live at
[hevlayer.com/docs](https://hevlayer.com/docs), and you can start a trial at
[hevlayer.com](https://hevlayer.com/#start-trial).

## License

Layer gateway is source-available under the Business Source License 1.1 and
converts to Apache-2.0 on the change date in `LICENSE`.

The license governs copyright. It does not grant trademark rights in the Layer
or hevlayer names; see `TRADEMARKS.md`.
