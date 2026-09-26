# Layer CPU embedding sidecar

Standalone implementation of [RFC 0120](../../docs/rfcs/0120-cpu-embedding-sidecar.md)
(private protocol v1), based on reviewed commit
`36de7479562b758baca257bcbebe46a23a3c5bce`. This nested Cargo workspace is
independently runnable. Packaging is wired: the CE Compose bundle
(`public/ce/docker-compose.yml`) starts it as `embed` on an internal network,
the public mirror ships this directory whole and builds the image from it
(`scripts/mirror-gateway.sh --embed-image`), the continuous mirror publishes
`hevlayer/layer-embed:edge` and the release train publishes `:X.Y.Z`, and the
Helm chart runs it as a gateway sidecar (`gateway.embed`, with
`gateway.localModels` as the mount extension). The gateway's HTTP embedding
provider and capability integration are separate work; until they land the
gateway ignores `LAYER_EMBED_URL`.

The production binary performs real Candle 0.11 CPU BERT inference. It loads
verified local safetensors, warms every effective model, then reports ready.
There is no runtime download, account, provider key, GPU, Python, remote code,
pickle loader or fallback embedder. The image recipe bakes both stock bundles.

| Stock ID | Dimensions | Pooling | Token ceiling | Query prefix |
|---|---:|---|---:|---|
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | masked mean | 256 | empty |
| `BAAI/bge-small-en-v1.5` | 384 | CLS | 512 | `Represent this sentence for searching relevant passages: ` |

Document prefixes are empty. Outputs are finite, L2-normalized f32 vectors.
**Every model or preprocessing change requires a full re-index**, including
CE-to-CE and CE-to-hosted changes. Equal dimensions do not make vector spaces
compatible. There is no third stock model or hosted dimension-parity target.

## Build and native validation

Keep the factory's inherited shared `CARGO_TARGET_DIR` when present.

```sh
cargo fmt --manifest-path apps/layer-embed/Cargo.toml --check
cargo test --manifest-path apps/layer-embed/Cargo.toml
cargo clippy --manifest-path apps/layer-embed/Cargo.toml --all-targets -- -D warnings
```

Ordinary tests use a tiny randomly initialized BERT fixture, real Candle forward
passes and controlled tensors. They do not download models or require a cluster.
The two stock tests are explicitly ignored unless invoked with prepared artifacts.

Preparation is a **build/development operation**, never a startup step in the
stock image. Choose an external directory, outside the Git worktree:

```sh
python3 apps/layer-embed/scripts/prepare-artifacts.py /absolute/external/models
LAYER_EMBED_BAKED_DIR=/absolute/external/models \
  cargo run --manifest-path apps/layer-embed/Cargo.toml --locked
```

`artifacts.lock.json` pins upstream commits, file sizes, SHA-256s and notices.
The preparation script downloads only pinned URLs and fails on integrity mismatch.
Artifacts are excluded from Git and the Docker build context. `manifest.json` is
canonical JSON; the runtime computes RFC 8785 hashes for model records and the
effective manifest. The exact locked revisions are:

- MiniLM: `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`
- BGE: `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`

To independently compare both purposes and models against Transformers:

```sh
uv run --python 3.12 --with torch==2.8.0 --with transformers==4.56.2 \
  apps/layer-embed/scripts/reference.py /absolute/external/models /absolute/external/reference.json
RAYON_NUM_THREADS=1 CANDLE_NUM_THREADS=1 TOKENIZERS_PARALLELISM=false \
  LAYER_EMBED_TEST_MODELS=/absolute/external/models \
  LAYER_EMBED_TEST_REFERENCE=/absolute/external/reference.json \
  cargo test --manifest-path apps/layer-embed/Cargo.toml --test stock -- --ignored --nocapture
```

Reference generation uses local artifacts, eager CPU attention and no remote code.
Tests enforce cosine >= 0.9999, component error <= 1e-4, unit norm within 1e-4,
384 dimensions, positional order including duplicates across microbatches, and
single-input versus padded-batch parity. Mount acceptance checks add/override
and rejection of an old fingerprint. Reference vectors and weights stay external.

## Private HTTP and process configuration

The default bind is `0.0.0.0:8081`; keep the service on an isolated container
network, or set `LAYER_EMBED_BIND=127.0.0.1:8081` for native use. This is a private,
unauthenticated protocol, not a public gateway API. The gateway will use
`LAYER_EMBED_URL` in the later integration; the sidecar does not consume it.

- `GET /health/live` reports HTTP-loop liveness.
- `GET /health/ready` reports loading/shutdown or the ready manifest fingerprint.
- `GET /v1/models` returns sorted model discovery, fingerprints and fixed limits.
- `POST /v1/embeddings` accepts the RFC's required fields: `model`,
  `artifact_sha256`, `dimensions`, `purpose`, `modality`, `inputs`, `timeout_ms`.

The gateway assigns `document` or `query` purpose and applies the discovered
prefix exactly once. The sidecar validates the required prefix and nonblank
user content; it never prepends again. Clients use the discovery fingerprint
and receive one vector per input in the original order. No custom instruction,
revision, unknown field, duplicate key or image modality is accepted by CE.
Tokenizer truncation is disabled. Errors use the RFC's JSON envelope and codes.

Limits: 1 MiB body, 32 inputs, 64 KiB per input, 4096 total unpadded tokens,
120000 ms maximum deadline. The deadline includes body receipt, validation,
queueing and inference. At most two batches execute (one on a single-core host),
with eight additional admission slots. Admission includes bodies being read,
so slow body readers consume bounded capacity. FIFO semaphore waiters are removed
on timeout or connection close. HTTP/1 read EOF cancels outstanding work; callers
must keep the connection open for the response. Microbatches contain at most four
inputs. Candle and tokenizer parallelism are restricted to one thread per active
batch; kernels run off the async executor. Expired/disconnected running work keeps
its permits until the kernel exits and discards its output. SIGTERM disables
readiness and admission, cancels queued work and drains within a 30-second budget.

`LAYER_EMBED_BAKED_DIR` defaults to `/opt/layer-embed/models`. Missing, corrupt or
incompatible artifacts fail startup nonzero. Both stock models are required;
there is no reduced-menu readiness. Required artifact paths are relative to their
bundle root; traversal and escaping symlinks are rejected. Weights are read into
owned memory after verification, avoiding mutable mmap-backed inference.

## Optional offline mounts

`LAYER_EMBED_MODELS_DIR=/models` loads an additional read-only bundle with the
same version-1 manifest format as the baked `manifest.json`. Mounted records
explicitly override matching IDs and can add namespaced local BERT IDs. Duplicate
IDs within a bundle fail. Required `config.json` and `tokenizer.json` sit beside
`model.safetensors`; every consumed artifact is listed with its size and hash.
Dimensions must equal BERT hidden size; token limits must fit its positional
embeddings and the 4096-token batch ceiling. Only f32 BERT, CLS/masked-mean,
L2 normalization and preprocessing version 1 are supported. All effective entries
must verify, load and warm up before readiness. Changing a bundle requires restart
and a full re-index; clients must preserve the old profile pin to detect changes.

This extension consumes an already-built local bundle. Arbitrary remote HF
checkpoint provisioning, user revision selection, new architectures, GPU pools,
autoscaling and pipeline-stage embedding belong to Pro. No mount is needed by the
stock image. The Helm chart's `gateway.localModels` values mount a bundle
read-only into the sidecar and set this variable; the Compose bundle takes the
same mount through an override file.

## Image and integration gates

`Dockerfile` uses digest-pinned multi-stage bases, the locked Rust dependencies,
and the verified artifact recipe. Build context is `apps/layer-embed`. The final
nonroot image contains the binary and model/license files, with no Python or
package installer. No image is built or published by native tests. CI's
`gateway-mirror` job builds the image from the staged public tree with Depot
(build-only, then `--load` for the Compose gate); `pgvector / phase-one` runs
`scripts/test-ce-embed.py`, the RFC 0120 cold Compose gate with every network
internal (ready within budget, private network only, no outbound connect,
discovery and inference for both models, gateway reachability, restart). The
release train publishes `hevlayer/layer-embed:X.Y.Z` for linux/amd64 and
linux/arm64 next to the gateway image, never to ghcr.io.

The later gateway stage must implement the shared HTTP provider (also used by
LYR-113), gateway-owned purpose/cache identity, immutable profile pins, default-off
`local-embedding`, RFC 0118 pgvector correctness and LYR-116 shared local chunking.
The sidecar stays one input to one vector. Existing `embed`/`Embed` spelling,
`prefer: local` and the `lattice` alias remain gateway concerns. Correct the public
model/menu, sidecar-weight, mount and Postgres/chunk documentation only when that
integration works. RFC 0110's goal is zero-setup clone-and-go, not a mandatory
in-process implementation.

Model redistribution terms and upstream attribution are in `licenses/` and the
bundles' model cards. The application remains under the repository's existing
license; this package does not authorize CE publication or change that license.
