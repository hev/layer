# Layer gateway

> Generated from `hev/layer-pro`; [report issues](https://github.com/hev/layer/issues). Edits land upstream.

Layer Community Edition runs the retrieval gateway on your laptop in front of
your turbopuffer account. You'll need a turbopuffer API key, Docker with
Compose, Git, and `curl`. No Layer account or license key is required.

## Start

Replace `tpuf_...` with your turbopuffer API key. The gateway reads it at
startup and exits with a one-line error if it is missing.

```sh
git clone https://github.com/hev/layer.git
cd layer
export TURBOPUFFER_API_KEY="tpuf_..."
docker compose up -d --wait
curl --fail http://localhost:8080/health
```

The gateway listens on `localhost:8080` and stores your data in your
turbopuffer account. Clients authenticate with the same key, sent as
`Authorization: Bearer $TURBOPUFFER_API_KEY`. Compose also starts `embed`, a
CPU embedding service with no route out of your machine. The Compose file runs
the latest release; set `GATEWAY_IMAGE=hevlayer/layer-gateway:edge` only to
opt into the development build. See the
[CE quickstart](https://hevlayer.com/docs/ce/quickstart).

## Write

The first write creates the namespace.

```sh
curl --fail-with-body http://localhost:8080/v2/namespaces/products \
  -H "Authorization: Bearer $TURBOPUFFER_API_KEY" \
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
  -H "Authorization: Bearer $TURBOPUFFER_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"rank_by": ["vector", "ANN", [1, 0, 0]], "top_k": 1, "include_attributes": true}'
```

The first row has `id: "earbuds"` and `$dist: 0`.

## Stop

```sh
docker compose down
```

The `products` namespace stays in your turbopuffer account until you delete it.

[CE docs](https://hevlayer.com/docs/ce) ·
[Telemetry](docs/telemetry.md) ·
[Business Source License 1.1](LICENSE) · [Trademarks](TRADEMARKS.md)
