# Install and configure Community Edition

For local development, follow the [CE quickstart](https://hevlayer.com/docs/ce/quickstart):
`docker compose up` starts the gateway and Postgres without an account or key.

## Local development and CI

The CE Compose bundle includes Postgres with pgvector and `pg_search`. Leave
`TURBOPUFFER_API_KEY` unset to keep writes and queries local; set it to use
an existing Turbopuffer account. Python and TypeScript clients use
`http://localhost:8080` as their base URL, as shown in the quickstart.

For GitHub Actions, a Postgres service container provides the database.
Start the CE image after the service is healthy, then run the same HTTP or
SDK requests. This example uses an ephemeral database port so concurrent
jobs do not contend for port 5432:

```yaml
name: CE quickstart
on: [push, pull_request, workflow_dispatch]
jobs:
  query:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: paradedb/paradedb:0.18.0@sha256:fe7c2638af33e6c3b3821bc8c7fab18629e5f43eb6ea3ad86a2966aca855dbb2
        env:
          POSTGRES_USER: layer
          POSTGRES_PASSWORD: local-layer
          POSTGRES_DB: layer
        ports:
          - 5432/tcp
        options: >-
          --health-cmd "pg_isready -U layer -d layer"
          --health-interval 2s --health-timeout 3s --health-retries 30
    steps:
      - name: Start gateway
        env:
          PG_PORT: ${{ job.services.postgres.ports['5432'] }}
        run: |
          docker run -d --name gateway --network host \
            -e "PGVECTOR_URL=postgresql://layer:local-layer@127.0.0.1:$PG_PORT/layer" \
            -e TURBOPUFFER_API_KEY= -e LAYER_TELEMETRY=off \
            -e AWS_EC2_METADATA_DISABLED=true \
            hevlayer/layer-gateway:0.6.0
          for _ in $(seq 1 60); do
            if curl --fail --silent http://localhost:8080/health; then exit 0; fi
            sleep 2
          done
          docker logs gateway
          exit 1
      - name: Write and query
        run: |
          curl --fail-with-body http://localhost:8080/v2/namespaces/ci-products \
            -H 'Content-Type: application/json' \
            -d '{"distance_metric":"cosine_distance","upsert_rows":[{"id":"earbuds","vector":[1,0,0]}]}'
          curl --fail-with-body http://localhost:8080/v2/namespaces/ci-products/query \
            -H 'Content-Type: application/json' \
            -d '{"rank_by":["vector","ANN",[1,0,0]],"top_k":1}' | \
            python3 -c 'import json,sys; assert json.load(sys.stdin)["rows"][0]["id"] == "earbuds"'
      - name: Stop gateway
        if: always()
        run: docker rm -f gateway
```

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
for short-lived local runs. If neither variable is set, the gateway uses a
default store selected by its deployment configuration.



The CE Compose bundle selects Postgres when `TURBOPUFFER_API_KEY` is unset or
blank; see the [quickstart](https://hevlayer.com/docs/ce/quickstart) for the local stack and
Turbopuffer option.



## Connection



| Field | Purpose |
| --- | --- |
| `kind` | The backend engine. `turbopuffer` or `pgvector` (see [Postgres](#postgres-pgvector)). |
| `default` | Selects the default store for requests. A single store is treated as the default. |
| `endpoint.url` | Upstream API base URL, or the PostgreSQL connection URI for `pgvector`. |
| `endpoint.region` | Operator-visible region label for this store. |
| `turbopuffer.orgId` | Optional turbopuffer organization id for dashboard deep links and support orientation. It is not used for auth or routing. |
| `credential.secretRef` | An environment-backed Secret reference for the upstream credential. |







## Postgres (`pgvector`)

`spec.kind: pgvector` connects the standalone gateway to Postgres with pgvector
and ParadeDB `pg_search`. Use the [CE quickstart](https://hevlayer.com/docs/ce/quickstart) for a bundled
local database, or point `LAYER_STORE_FILE` at this resource-shaped config:

```yaml
apiVersion: hevlayer.com/v1alpha1
kind: VectorStore
metadata:
  name: postgres-default
spec:
  kind: pgvector
  default: true
  endpoint:
    url: postgresql://layer:local-layer@postgres:5432/layer
  inboundAuth:
    mode: open
```

This example uses the Compose database hostname and local credentials. `open`
is for local development; use [independent inbound keys](#inbound-auth) for
an authenticated gateway. A default Postgres store requires `keys` or `open`;
`deriveFromStore` is rejected.

The Kubernetes operator CRD does not accept `pgvector`. Load this shape through
`LAYER_STORE_FILE` or `LAYER_STORE_JSON` in a standalone gateway, rather than
applying it with `kubectl`.

### Connection and readiness

| Field | Postgres configuration |
| --- | --- |
| `spec.kind` | `pgvector`. |
| `spec.endpoint.url` | PostgreSQL connection URI, including database and database authentication. Store a credential-bearing config securely; `credential.secretRef` does not supply the SQL password. |
| `spec.endpoint.region` | Optional operator-visible label; does not select the database. |
| `spec.default` | Selects this store for namespaces without an explicit store reference. |
| `spec.inboundAuth` | Gateway client authentication, independent of SQL authentication. |

The target database must have `vector` **0.8.0** and `pg_search` **0.18.0**
installed. The gateway checks both extension versions at startup and fails
initialization if either does not match. The database role must be able to
create the `layer_pgvector` schema and its tables and indexes, and read, write,
alter, and drop the namespace tables it owns. Use a dedicated backend database;
this database holds namespace documents and is separate from Layer's
pipeline/embed indexing-state database.

### Namespace-to-table mapping

Layer creates one owned table per logical namespace on its first write, plus
`layer_pgvector.namespaces` to record the mapping and schema. Physical table
names are opaque hashes of the store scope and logical namespace, not SQL
identifiers supplied by a caller. The scope combines
`LAYER_VECTORSTORE_NAMESPACE` and the resolved `VectorStore` name; keep both
stable when restarting against existing data.

Each table stores the document body as JSONB and declared attributes in typed
columns. API calls continue to use logical namespace names. Namespace deletion
removes the owned table, its indexes, and its registry entry. Layer manages
these tables; this connection does not map arbitrary existing SQL tables into
namespaces.

### Index choices

Declare attributes in the [write schema](https://hevlayer.com/docs/ce/api/write). Layer creates indexes
from that schema:

| Declaration | Physical index |
| --- | --- |
| Vector attribute such as `"vector": "[3]f32"` | pgvector HNSW with `m=16`, `ef_construction=64`. `cosine_distance` uses cosine operators; `euclidean_squared` uses L2 operators. |
| String attribute with `full_text_search: true` | `pg_search` BM25 index using the default tokenizer. |
| Filterable numeric or boolean attribute | B-tree index. String scalar filters scan; text ranking uses the BM25 index. |

`distance_metric` defaults to `cosine_distance` and is fixed for a namespace.
HNSW is the adapter's vector index choice; there is no `VectorStore` field for
IVFFlat or HNSW tuning. Text ranking uses the shared
[BM25-class scoring caveat](https://hevlayer.com/docs/ce/stores#fts-ranking).
Consult the generated [Postgres capability matrix](https://hevlayer.com/docs/ce/stores)
for wire-feature support and gaps, including combinations that require checking
the linked API reference.



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
