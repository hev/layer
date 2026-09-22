# Layer gateway

> Generated from `hev/layer-pro`; [report issues](https://github.com/hev/layer/issues). Edits land upstream.

Layer Community Edition is a local retrieval gateway over Postgres.
You need Docker with Compose, Git, and curl; no account, API key, license,
compiler, or client build.

## Start

```sh
git clone --branch v0.6 https://github.com/hev/layer.git
cd layer
export TURBOPUFFER_API_KEY=""
export GATEWAY_IMAGE=hevlayer/layer-gateway:edge
docker compose up -d --wait
export LAYER_GATEWAY_URL="http://localhost:${GATEWAY_PORT:-8080}"
export LAYER_NAMESPACE="${LAYER_NAMESPACE:-products}"
curl --fail "$LAYER_GATEWAY_URL/health"
```


The `v0.6` branch and `edge` image track development. Data stays in a local
Docker volume. See the [CE quickstart](https://hevlayer.com/docs/ce/quickstart)
for SDK examples, fixed release coordinates, and other stores.

## Write

The first write creates the namespace.

```sh
curl --fail-with-body "$LAYER_GATEWAY_URL/v2/namespaces/$LAYER_NAMESPACE" \
  -H 'Content-Type: application/json' \
  -d '{
    "distance_metric": "cosine_distance",
    "schema": {"title": {"type": "string", "full_text_search": true}},
    "upsert_rows": [
      {"id": "earbuds", "title": "wireless earbuds", "vector": [1, 0, 0]},
      {"id": "speaker", "title": "portable speaker", "vector": [0, 1, 0]}
    ]
  }'
```


## Query

```sh
curl --fail-with-body "$LAYER_GATEWAY_URL/v2/namespaces/$LAYER_NAMESPACE/query" \
  -H 'Content-Type: application/json' \
  -d '{"rank_by": ["vector", "ANN", [1, 0, 0]], "top_k": 1, "include_attributes": true}'
```


The first row has `id: "earbuds"` and `$dist: 0`.

## Stop

```sh
docker compose down
```

[CE docs](https://hevlayer.com/docs/ce) ·
[Telemetry](docs/telemetry.md) ·
[Business Source License 1.1](LICENSE) · [Trademarks](TRADEMARKS.md)
