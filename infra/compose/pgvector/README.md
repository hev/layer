# Postgres acceptance bundle

The acceptance commands below pull the gateway and pinned ParadeDB image.
Docker with Compose is the only host prerequisite; no Rust, Python, or Node
toolchain is required. The clients image builds only the Python and TypeScript
test harness. Namespace deletion works without object storage. The optional
`purge-storage.yaml` override is for configured-S3 recovery tests.

From the layer-pro repository root:

```sh
export COMPOSE_FILE="$PWD/public/ce/docker-compose.yml"
export ACCEPTANCE_CONTEXT="$PWD"
unset TURBOPUFFER_API_KEY
export PGVECTOR_TEST_PREFIX=scratch-lyr-35
docker compose up --wait
curl --fail http://localhost:8080/health
docker compose --profile acceptance run --build --rm clients
docker compose down -v --remove-orphans
```

From the public `hev/layer` checkout, omit `ACCEPTANCE_CONTEXT` and export
`COMPOSE_FILE=docker-compose.yml`
before running the commands. Its root contains the Compose definition and
acceptance inputs. Ensure `.env` also leaves
`TURBOPUFFER_API_KEY` empty. An absent or blank key selects local pgvector;
a nonblank key selects Turbopuffer. The acceptance suite requires pgvector.

The single service definition lives in `public/ce/docker-compose.yml` and is
copied unchanged to the public repository. `GATEWAY_IMAGE` overrides the
published pre-release `edge` tag, for example with a released version or a
locally staged PR image. `GATEWAY_PORT` changes the default host port 8080.
The database has no published host port. `down -v` deletes local database data.

Both suites run 76 cases using the committed generated clients, delete their
scratch namespaces in a `finally` block, and verify cleanup. CI uses a Docker
artifact from the existing gateway mirror job for PRs, and pulls the published
image for main and manual runs. Compose never builds the gateway.

ParadeDB is pinned to the `0.18.0` multi-architecture digest in the shared
Compose file. The gateway requires `vector 0.8.0` and `pg_search 0.18.0`.
The unmodified server runs separately from the BSL gateway; its AGPL license
and source are at [ParadeDB v0.18.0](https://github.com/paradedb/paradedb/tree/v0.18.0).

## Phase-one behavior

Full row/column upserts, deletes by ID, schema updates, namespace listing and
deletion, document fetch, scalar filters, dense ANN and single-field BM25 are
implemented. Incompatible schema or vector dimensions are validation errors.
Unknown write/query options and deferred primitives return 422
`UnsupportedByStore`, naming the feature; mixed rejected writes have no effects.

Dense indexes use HNSW (`m=16`, `ef_construction=64`), with `ef_search=100`,
strict iterative scans and a 20,000-tuple scan bound. Cosine distances remain
cosine distances; L2 is squared only in the returned score. HNSW remains
approximate. BM25 uses the explicit `default` tokenizer and an internal integer key
field. Scores are materialized before scalar filtering because the pinned
extension can otherwise choose a scalar index scan without BM25 scores. This
preserves filtered results but can materialize many matching documents.

Gateway fusion uses the existing `Auto` expression with a numeric vector,
`route: fused` and explicit `fuzziness: 0`. This phase uses the BM25 anchor plus
the dense leg; the response echoes the effective legs. Nonzero/automatic fuzzy
search and hybrid cursors are unsupported. Raw multi-query remains 422.
There are no synthetic Turbopuffer backlog, watermark or billing measurements.
The required write-response billing object is empty.

This bundle is the standalone phase-one gate. Kubernetes operator discovery,
production enablement and the SciFact ranking gate
are separate work. Do not point this backend at the indexing-queue database.
