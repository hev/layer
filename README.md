# Layer gateway

> Generated from `hev/layer-pro`; [report issues](https://github.com/hev/layer/issues). Edits land upstream.

Layer Community Edition runs the retrieval gateway on your laptop with a local
Postgres database or your existing Turbopuffer account. You'll need Docker with
Compose, Git, and `curl`. The local database needs no account, API key, or
license key.

## Start

```sh
git clone https://github.com/hev/layer.git
cd layer
docker compose up -d --wait
curl --fail http://localhost:8080/health
```

The gateway listens on `localhost:8080`. Compose includes a Postgres database
with pgvector and `pg_search`, which the gateway uses unless
`TURBOPUFFER_API_KEY` is set in your shell. Setting the key selects Turbopuffer
instead, and each request then needs
`-H "Authorization: Bearer $TURBOPUFFER_API_KEY"`. The Compose file runs the
latest release; set `GATEWAY_IMAGE=hevlayer/layer-gateway:edge` only to opt
into the development build. See the
[CE quickstart](https://hevlayer.com/docs/ce/quickstart) for both backends.

## Write

The first write creates the namespace.

```sh
curl --fail-with-body http://localhost:8080/v2/namespaces/products \
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
curl --fail-with-body http://localhost:8080/v2/namespaces/products/query \
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
