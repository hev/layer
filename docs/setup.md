# Install and configure Community Edition

Community Edition is the Layer gateway on your turbopuffer account. For local
development, follow the [CE quickstart](https://hevlayer.com/docs/ce/quickstart):
export `TURBOPUFFER_API_KEY`, then `docker compose up` starts the gateway and
the embedding service. Without the key the gateway exits at startup with a
one-line error telling you to export it.

## Local development and CI

The gateway sends writes and queries to your turbopuffer account. Clients use
`http://localhost:8080` as their base URL and send the same key as
`Authorization: Bearer $TURBOPUFFER_API_KEY`. `TPUF_URL` sets the turbopuffer
endpoint (default `https://api.turbopuffer.com`).

For GitHub Actions, store the key as a repository secret and start the CE
image with it. Give each run its own namespace and delete it afterwards, so
concurrent jobs do not share data:

```yaml
name: CE quickstart
on: [push, pull_request, workflow_dispatch]
jobs:
  query:
    runs-on: ubuntu-latest
    env:
      TURBOPUFFER_API_KEY: ${{ secrets.TURBOPUFFER_API_KEY }}
      NS: ci-products-${{ github.run_id }}-${{ github.run_attempt }}
    steps:
      - name: Start gateway
        run: |
          docker run -d --name gateway -p 8080:8080 \
            -e TURBOPUFFER_API_KEY -e LAYER_TELEMETRY=off \
            -e AWS_EC2_METADATA_DISABLED=true \
            hevlayer/layer-gateway:0.8.0
          for _ in $(seq 1 60); do
            if curl --fail --silent http://localhost:8080/health; then exit 0; fi
            sleep 2
          done
          docker logs gateway
          exit 1
      - name: Write and query
        run: |
          curl --fail-with-body "http://localhost:8080/v2/namespaces/$NS" \
            -H "Authorization: Bearer $TURBOPUFFER_API_KEY" \
            -H 'Content-Type: application/json' \
            -d '{"distance_metric":"cosine_distance","upsert_rows":[{"id":"earbuds","vector":[1,0,0]}]}'
          curl --fail-with-body "http://localhost:8080/v2/namespaces/$NS/query" \
            -H "Authorization: Bearer $TURBOPUFFER_API_KEY" \
            -H 'Content-Type: application/json' \
            -d '{"rank_by":["vector","ANN",[1,0,0]],"top_k":1}' | \
            python3 -c 'import json,sys; assert json.load(sys.stdin)["rows"][0]["id"] == "earbuds"'
      - name: Delete namespace and stop gateway
        if: always()
        run: |
          curl --silent -X DELETE "http://localhost:8080/v2/namespaces/$NS" \
            -H "Authorization: Bearer $TURBOPUFFER_API_KEY" || true
          docker rm -f gateway
```

## Embedding service

The Compose bundle also starts `embed`, the CPU embedding service
(`hevlayer/layer-embed`). Its image bakes `sentence-transformers/all-MiniLM-L6-v2`
and `BAAI/bge-small-en-v1.5` (both 384 dimensions), so it needs no account,
key, GPU or download after the image pull. It joins only the internal `embed`
Compose network: the gateway reaches it as `LAYER_EMBED_URL=http://embed:8081`,
nothing on the host can, and the container has no route out of the machine.
Both models are verified, loaded and warmed before the service reports healthy;
the gateway waits for that. Override the image with `EMBED_IMAGE`, as with
`GATEWAY_IMAGE`.

To add or override text models, mount a bundle directory (a `manifest.json` in
the image's format plus the files it lists) read-only and point the service at
it from a Compose override file:

```yaml
services:
  embed:
    environment:
      LAYER_EMBED_MODELS_DIR: /models
    volumes:
      - ./my-models:/models:ro
```

Changing the bundle needs a restart and a full re-index of the namespaces that
used the replaced model. See the [embedding docs](https://hevlayer.com/docs/ce/api/embed#bringing-your-own-weights).

## Standalone config

The standalone gateway runs with Docker, Compose, or a bare binary.
Configuration uses the `VectorStore` resource shape without requiring a
Kubernetes control plane.

Standalone and compose runs do not need Kubernetes to resolve `VectorStore`s.
Set `LAYER_STORE_FILE` to a YAML or JSON file containing a resource-shaped connection:

```yaml
apiVersion: hevlayer.com/v1alpha1
kind: VectorStore
metadata:
  name: turbopuffer-default
spec:
  kind: turbopuffer
  default: true
  endpoint:
    url: https://api.turbopuffer.com
    region: aws-us-east-1
  credential:
    secretRef:
      name: layer
      key: turbopuffer-api-key
  inboundAuth:
    mode: deriveFromStore
```

For this example, set `LAYER_SECRET_LAYER_TURBOPUFFER_API_KEY=tpuf_...`
before starting the gateway. The env var name is `LAYER_SECRET_` plus the
Secret name and key uppercased, with punctuation replaced by underscores.

For more than one store, use a Kubernetes-style list:

```yaml
apiVersion: v1
kind: List
items:
  - apiVersion: hevlayer.com/v1alpha1
    kind: VectorStore
    metadata:
      name: turbopuffer-default
    spec:
      kind: turbopuffer
      default: true
      endpoint:
        url: https://api.turbopuffer.com
        region: aws-us-east-1
      credential:
        secretRef:
          name: layer
          key: turbopuffer-api-key
      inboundAuth:
        mode: deriveFromStore
```

The standalone file uses the fields shown above. Kubernetes installs read
`credential.secretRef` from the cluster; standalone runs resolve the same
`secretRef` from env. Inline `LAYER_STORE_JSON` accepts the same resource shape
for short-lived local runs. If neither variable is set, the gateway builds a
default turbopuffer store from `TURBOPUFFER_API_KEY` and `TPUF_URL`, and exits
at startup if the key is unset.

## Connection

| Field | Purpose |
| --- | --- |
| `kind` | The backend engine: `turbopuffer`. |
| `default` | Selects the default store for requests. A single store is treated as the default. |
| `endpoint.url` | Upstream API base URL. |
| `endpoint.region` | Operator-visible region label for this store. |
| `turbopuffer.orgId` | Optional turbopuffer organization id for dashboard deep links and support orientation. It is not used for auth or routing. |
| `credential.secretRef` | An environment-backed Secret reference for the upstream credential. |

## Inbound auth

`inboundAuth.mode` controls what bearer token the gateway accepts:

| Mode | Behavior |
| --- | --- |
| `deriveFromStore` | Default. The gateway accepts the default store's credential as the inbound bearer. This is the single-tenant BYOC shape. |
| `keys` | The gateway accepts the listed independent key Secrets and enforces their `read`, `write`, and `admin` scopes. |
| `open` | No inbound auth. Use only for explicitly open environments. |

Under `deriveFromStore`, clients set `Authorization: Bearer <store key>`
when calling the gateway. Standalone runs resolve the configured Secret references from environment
variables using the naming rule above.

Under `keys`, each key refers to an environment-backed Secret:

```yaml
spec:
  inboundAuth:
    mode: keys
    keys:
      - name: shop-rw
        scopes: [read, write]
        secretRef:
          name: layer
          key: layer-inbound-shop-rw-api-key
```

`read` covers GET/HEAD routes and read-shaped POST routes such as query,
batch fetch, scans, and metrics proxy queries. `write` covers namespace
writes. `admin` also satisfies `read` and `write`.
