# Shared work identity

The opt-in Layer Pro API uses PlanetScale Postgres for hosted identity. Kit,
agents and workers use the same authenticated HTTP API with hosted or local
Layer. The test stack installs an isolated schema and seeds workspace grants.
Deployment and workspace provisioning are separate from this slice.

The namespace is `work-<workspace UUID>`. Clients need a minted key with an
explicit grant for that namespace on its routed Layer store. In Pro, a Layer
administrator provisions the `work-identity` claim on that store entitlement
alongside the exact namespace and required read or write scope. Delegated
vector-store minters cannot issue claims. In CE, the local identity grant file
supplies the store, exact namespace and scopes. Both adapters check grants
before database access. Use bearer credentials in the Authorization header.

POST to `/v2/namespaces/{namespace}/work-identity` to write and
`/v2/namespaces/{namespace}/work-identity/query` to read. Bodies are JSON
objects tagged by `op`. Send canonical opaque UUID references to sanitized
request, scope and evidence objects held by the caller. The identity database
stores those references, independent of the objects' storage locations.

- `create` takes `idempotency_key`, optional `parent_id`, `request_ref`
  and `scope_ref`. Reusing the key with the same fields returns the original
  ID and current canonical address. Different fields produce a conflict.
- `move` takes `id`, `expected_revision`, optional `parent_id` and a
  positive integer `segment`. Keeping the parent renames the ask. Leaving
  the parent absent moves the ask to the root. Descendants move atomically.
- `revise` takes `id`, `expected_revision` and `scope_ref`.
- `link` takes `id`, `expected_revision`, `kind` (`attempt`, `pr`,
  `execution` or `artifact`) and `reference`.
- `accept` takes `id`, `expected_revision`, `scope_revision` and
  `evidence_ref`. An artifact link on that ask must carry the evidence.
- `resolve` takes `address`. `get` takes `id`. Both return the ask,
  including its ID, canonical address, provenance, current parent, revisions,
  links and acceptance.
- `list` and `history` take optional `after` and `limit`. Pages use
  `items` and `next_after`. Limits default to 25 and range from 1 to 100.
  List cursors are allocation ordinals and history cursors are change ordinals.

A root can execute directly and has its own evidence and acceptance. Child
acceptance can satisfy a dependency declared by the caller. Completion still
requires explicit acceptance on the ask's current scope revision. A scope
revision clears the previous acceptance while preserving its history.

Each used address remains reserved for the original ask. Old addresses
resolve directly to the ask ID and return its current address. The slice has
a history budget of 8,192 changes per workspace, including one change per
affected ask in a subtree move. This also bounds permanent alias reservations
to 8,192 per workspace. A mutation that exceeds the budget returns
`409 capacity` and rolls back in full. Reads and idempotent create replays
remain available. Automatic allocation considers segments 1 through 8,193
at the selected prefix. Any future history retention must preserve the address
ledger and must publish a cursor expiry policy before deleting history.

A Layer or database outage yields an explicit retryable failure. A timed-out
response can leave the commit outcome unknown. Retry create with the same key
and payload. For other writes, read back current state before attempting the
write again. Kit's ordinary dispatch remains available through its existing
execution path. Identity operations must surface their failure to callers.

### Wire details

UUID fields are JSON strings. Request bodies accept up to 4 KiB. Successful
operations return HTTP 200. An ask is the following JSON object:

| Field | Type |
| --- | --- |
| `id`, `workspace_id`, `request_ref`, `scope_ref` | UUID |
| `address`, `origin_address` | string |
| `segments` | array of positive integers |
| `parent_id`, `origin_parent_id` | UUID or null |
| `revision`, `scope_revision` | positive integer |
| `links` | array of objects with `kind` and UUID `reference` |
| `acceptance` | null or an object with `scope_revision` and UUID `evidence_ref` |

History items have `cursor` (integer), `event` (string), and `ask` (the
snapshot at that change). Pages expose `has_more` and the last returned
ordinal in `next_after`. Retain the last non-null cursor between polls.

Errors contain `error` (string) and `retryable` (boolean).
HTTP 400 means invalid input, 401 means missing or invalid authentication, 404
means absent or inaccessible workspace or ask, 409 means conflict or fixture
capacity, and 503 means temporary unavailability. A request has a five-second
database deadline. The gateway makes one transaction attempt per request.
The fixture bounds concurrent pool connections, live asks, depth, and links.

The local fixture uses `LAYER_WORK_IDENTITY_DATABASE_URL` on the Layer
process only. Use a verified TLS Postgres connection for hosted Layer. The
experimental slice contract stays here while the test stack is opt-in.

### Isolated client verification

Run `scripts/test-work-identity-postgres` for the gateway's transaction suite.
To connect a client, run `scripts/test-work-identity-postgres -- /path/to/client-check`
on Oven. The script starts a fresh loopback-only Postgres container with
workspace label `hev`. Containerized CI uses the Docker host’s private bridge
to reach Postgres. The test starts Layer on a random loopback HTTP port
and invokes the executable with `WORK_IDENTITY_FIXTURE_LAYER_URL`,
`WORK_IDENTITY_FIXTURE_NAMESPACE`, and `WORK_IDENTITY_FIXTURE_BEARER`.
Read the synthetic bearer from the environment and keep it out of output.
The executable receives no Postgres URL. The runner bounds the executable
to two minutes and cleans up Layer and Postgres when it exits.

### Standalone Layer configuration

Build the CE gateway with `cargo build --locked -p hevlayer-gateway --features work-identity`.
Set `LAYER_WORK_IDENTITY_DATABASE_URL` on the gateway process to opt in. Use a
dedicated local database and initialize it with
`apps/layer-gateway/tests/fixtures/work-identity.sql`. Add workspace rows with
the desired UUID and address label as part of isolated fixture setup.

CE seeds local bearer grants from the file named by
`LAYER_WORK_IDENTITY_KEYS_FILE`. Keep the file private to the gateway process.
The file accepts up to 128 grants and 64 KiB. Each JSON array entry has a locally minted `hvl_` bearer in `token`, the
Layer store name in `store`, exact workspace namespace names in `namespaces`,
and `read` and/or `write` in `scopes`. Generate each bearer from fresh random
bytes, for example with `printf hvl_; openssl rand -hex 24`. Start the gateway
with `LAYER_BIND_HOST=127.0.0.1` for loopback-only use. Changes to grants take
effect on restart. Hosted Layer verifies managed minted keys. Both accept the
same `Authorization: Bearer <token>` header and require an explicit grant for
the routed namespace and requested scope.

The CE transaction suite starts the standalone gateway on a random loopback
port and restarts it against the same Postgres container to verify durability.
Its retrieval routing metadata points to a loopback placeholder. All identity
data is persisted in the isolated fixture database.
