# Install and configure Community Edition

> Existing PostgreSQL 17 volumes require the backup and restore migration
> below before using the PostgreSQL 18 bundle.


For local development, follow the [CE quickstart](https://hevlayer.com/docs/ce/quickstart):
`docker compose up` starts the gateway and Postgres without an account or key.

## Local development and CI

The CE Compose bundle includes Postgres with pgvector and `pg_search`. Leave
`TURBOPUFFER_API_KEY` unset to keep writes and queries local; set it to use
an existing Turbopuffer account. Python and TypeScript clients use
`http://localhost:8080` as their base URL, as shown in the quickstart.

For GitHub Actions, a Postgres service container provides the database.
Start the CE image after the service is healthy, then run HTTP or SDK
requests. This example uses an ephemeral database port so concurrent
jobs do not contend for port 5432:

```yaml
name: CE quickstart
on: [push, pull_request, workflow_dispatch]
jobs:
  query:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: paradedb/paradedb:0.26.0@sha256:52fc9c95fdfd462201168d1d82334ed61f85cbd921957cab083ec39800a217ac
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
            hevlayer/layer-gateway:0.7.3
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

The target database must have `vector` **0.8.6** and `pg_search` **0.26.0**
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
| String attributes with `full_text_search: true` | One `pg_search` BM25 index over every such attribute, default tokenizer. Declaring another full-text attribute later rebuilds the index inside that write. |
| Filterable numeric or boolean attribute | B-tree index, `NULLS FIRST`, read forward for ascending and backward for descending `rank_by` order. String scalar filters scan, and ordering by a string attribute sorts the filtered rows; text ranking uses the BM25 index. |
| Every namespace, for `id` | An expression index on the id sort key: unsigned-integer ids first, numerically, then string ids in byte order. Ordered scans, filter-only queries, and `["id", "Gt", last]` paging read it. |

String attributes order and compare in byte order (`COLLATE "C"`) in both
`rank_by` ordering and `Gt`, `Gte`, `Lt`, and `Lte` filters, independent of
the database's default collation. Namespaces created before 0.7 keep their
`ASC NULLS LAST` scalar indexes and have no id sort-key index; their queries
return the same rows but sort instead of reading an index.

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

### Upgrade an existing CE Postgres volume

Back up before changing the database image. The old ParadeDB 0.18 bundle uses
PostgreSQL 17; the 0.26 bundle uses PostgreSQL 18 and stores PGDATA under
`/var/lib/postgresql/18/docker`. PostgreSQL cannot open a 17 data directory
with an 18 server. This bundle mounts a **new** `postgres18` volume at
`/var/lib/postgresql`, preserving the old `postgres` volume. Starting the new
bundle without restoring it gives an empty database.

Use a maintenance window: stop gateway writes, keep the old database running,
and make a logical archive with the old image's `pg_dump`. These commands are
for the default CE Compose project and dedicated Layer database; use the same
project name and credentials as the existing installation. Back up any other
schemas separately. Preserve the old Compose file and gateway/database image
digests for rollback. Do not run `docker compose down -v`.

```sh
# Run BEFORE replacing the old Compose file.
docker compose stop gateway
mkdir -m 700 -p backups
# Archive includes registry, documents, schemas and all index definitions.
docker compose exec -T postgres pg_dump -U layer -d layer \
  --format=custom --schema=layer_pgvector > backups/layer-pg17.dump
# Save roles if you customized the database role. Protect this credential file.
docker compose exec -T postgres pg_dumpall -U layer --globals-only \
  > backups/pg17-globals.sql
# Check the archive is readable; rehearse restoration on an isolated copy too.
docker compose exec -T postgres pg_restore --list < backups/layer-pg17.dump
# Preserve the old volume. Then install the new Compose file.
docker compose down
```

Start only the new database, verify its extensions, and restore the archive.
For the default bundle, `vector` and `pg_search` are already installed by the
image. For an external database, install the exact versions required above
before restoring. Restore with the dedicated owning role; recreate customized
roles deliberately from the globals backup rather than blindly applying it.

```sh
docker compose up -d --wait postgres
docker compose exec -T postgres psql -X -U layer -d layer -v ON_ERROR_STOP=1 \
  -c "SELECT version(); SELECT extname,extversion FROM pg_extension WHERE extname IN ('vector','pg_search');"
docker compose exec -T postgres pg_restore -U layer -d layer \
  --exit-on-error --no-owner --no-acl < backups/layer-pg17.dump
docker compose exec -T postgres psql -X -U layer -d layer -v ON_ERROR_STOP=1 \
  -c 'ANALYZE; SELECT count(*) FROM layer_pgvector.namespaces;'
docker compose up -d --wait gateway
curl --fail http://localhost:8080/health
```

`pg_restore` creates new BM25 and HNSW indexes from their definitions: it does
not reuse the old index files. Check namespace/document counts against the old
database, then run representative BM25 and vector queries through the gateway
before reopening writes. Keep `LAYER_VECTORSTORE_NAMESPACE` and the
VectorStore name unchanged so the namespace scope still maps to the same tables.
A successful startup alone does not prove that the data was restored.

If upgrading extensions **within the same PostgreSQL major version**, follow
ParadeDB's extension upgrade instructions, back up first, and rebuild the BM25
indexes after the extension update. Existing indexes need a rebuild to gain
the new postings/fieldnorm layout. The following uses catalog names and safe
identifier quoting; it touches only Layer's BM25 indexes. Run it while the
gateway is stopped, allow enough disk for index rebuilds, and plan for blocking
locks. Do not assume `REINDEX CONCURRENTLY` is supported.

```sh
docker compose exec -T postgres psql -X -U layer -d layer -v ON_ERROR_STOP=1 <<'SQL'
SELECT format('REINDEX INDEX %I.%I;', n.nspname, c.relname)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_am am ON am.oid = c.relam
WHERE n.nspname = 'layer_pgvector' AND c.relkind = 'i' AND am.amname = 'bm25'
\gexec
ANALYZE;
SQL
```

For rollback, stop the gateway and new database, restore the saved old Compose
file and old gateway image, and attach the untouched PostgreSQL 17 volume.
Never attach the new PostgreSQL 18 volume to the old image. Writes accepted
after cutover will not be present in the old volume; keep writes paused until
validation finishes, or reconcile them before rollback. Do not remove either
volume until backup restoration and application queries have been verified.
