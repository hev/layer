---
name: hevlayer-search-app
description: >-
  Build a working search application on hev layer, locally, with no account
  and no API key. Use when the user wants search over a corpus — a Hugging
  Face dataset, their own documents, a product catalog — and needs the schema
  designed, the source connected, the text chunked and embedded, the queries
  written, and a UI generated. Covers the local Docker Compose stack (gateway,
  Postgres, and the CPU embedding service), the Warehouse and Pipeline configuration shapes, chunking
  strategies, filter and full-text schema design, the turbopuffer-shaped query
  wire, and generating the front end from the namespace schema.
---

# Build a search app on hev layer

You are building a search application. hev layer is the gateway it talks to:
one HTTP API, turbopuffer-shaped, in front of whichever store holds the data.
Locally that store is Postgres, running in the same Compose file, with no
account and no key.

**The most important rule in this skill:** the gateway owns retrieval. Ranking,
fusion, tokenization, typo tolerance, facet counting, and query routing happen
*inside* it. Your application posts a query and renders the response. If you
find yourself writing reciprocal-rank fusion, a tokenizer, a BM25
implementation, or a client-side rescoring pass, stop — you have either picked
the wrong request shape or hit a real gap. Check
[`store-capabilities.md`](store-capabilities.md) before you write a line of
retrieval code.

## Order of work

1. Bring the stack up.
2. Model the corpus as a schema — this decides what the app can do later.
3. Declare the source and the chunking.
4. Index.
5. Query.
6. Generate the interface.

Do them in that order. The schema constrains every stage after it, and
changing it once rows are written means a reindex.

---

## 1. Bring the stack up

```sh
git clone https://github.com/hev/layer.git
cd layer
export TURBOPUFFER_API_KEY=""
docker compose up -d --wait
curl --fail http://localhost:8080/health
```

The gateway is on `localhost:8080`. Compose also starts Postgres with
`pgvector` and `pg_search`, and `embed`, the bundled CPU embedding service.
The Compose file runs the latest release. An unset, empty, or whitespace-only
`TURBOPUFFER_API_KEY` selects local Postgres; a nonblank key selects
turbopuffer instead and is sent as `Authorization: Bearer <key>`. Nothing
leaves the machine in the local configuration.

No `Authorization` header is needed against the local stack. Every `curl` in
this skill omits it; add it when the user has pointed the gateway at their own
turbopuffer account.

---

## 2. Model the corpus

The schema is the design document for the whole app. Each attribute's
declaration decides whether it can be searched, filtered, or only returned.

```json
{
  "distance_metric": "cosine_distance",
  "schema": {
    "title":    {"type": "string", "full_text_search": true},
    "body":     {"type": "string", "full_text_search": true, "fuzzy": true},
    "category": {"type": "string", "filterable": true},
    "year":     {"type": "uint", "filterable": true},
    "source_url": {"type": "string"}
  }
}
```

Decide each field deliberately:

| Want | Declare | Unlocks |
| --- | --- | --- |
| Rank text by relevance | `"full_text_search": true` | `BM25` rank expressions on that field |
| Tolerate typos in that text | `"fuzzy": true` as well | Fuzzy `HybridText` legs — turbopuffer only |
| Narrow results | `"filterable": true` | `filters` predicates, and facet rails over the field |
| Only display it | neither | returned via `include_attributes`, nothing else |
| Semantic similarity | a `vector` on the row | `ANN` rank expressions |

Rules worth knowing before you commit:

- **Typo tolerance is a turbopuffer feature.** On turbopuffer, `HybridText`
  runs a BM25 leg plus one fuzzy leg per token and fuses them, so the field
  needs both `full_text_search` and `fuzzy`. The local Postgres store has no
  fuzzy index: it serves `HybridText` with `fuzziness: 0` only, which ranks by
  BM25 without typo tolerance.
- **The id type is fixed by the first write** — string or integer. Later
  writes with the other type are rejected.
- **`_hevlayer_*` attribute names are reserved.** The gateway stamps its own;
  a write that sets one is rejected.
- **Nested JSON objects are not an attribute type.** Flatten, or carry the
  value as a serialized string that you do not filter on.
- **Filterable does not mean every operator.** Locally, equality, comparison,
  `In`, `Not`, `NotIn`, and boolean composition work; array-containment,
  regex, and the advanced operators do not. The matrix is authoritative.

Ask the user what the app needs to *do* — "filter by year and category, search
the body with typo tolerance, show the title and a link" — and derive the
schema from the answer rather than from the source columns.

---

## 3. Declare the source and the chunking

Layer's Kubernetes deployment models ingestion as two objects: a **Warehouse**
(where rows come from) and a **Pipeline** (what happens to them on the way in).
Locally you run the same configuration without the operator, as a single
non-scaling worker. Write the config in the real shapes even locally — that is
what makes the local app lift to a cluster unchanged.

### The Warehouse

For a Hugging Face dataset:

```yaml
kind: huggingface
huggingface:
  endpoint: https://huggingface.co
  # tokenSecretRef is optional; public datasets need no credential.
```

A public dataset is credentialless. A gated or private one needs a Hugging
Face read token — locally, an `HF_TOKEN` environment variable.

### The source block

```yaml
sourceRef:
  kind: huggingface
  warehouseRef: huggingface-hub
  repo: rajpurkar/squad          # Hub repo id
  config: plain_text             # dataset config; omit for the default
  split: train                   # train | validation | test | …
  revision: ~                    # omit to pin the current parquet-ref commit
  mapping:
    id: id                       # column → document id
    text: context                # the column indexed and embedded
    attributes: [title, question]  # columns carried as attributes
```

`mapping.text` is required — it is the column that gets indexed and embedded.
`mapping.id` is optional; without it, ids are synthesized as
`{config}/{split}#{offset}`, which is stable within a revision but not across
one, so name a natural key for anything long-lived. `mapping.attributes`
omitted or `[]` carries every remaining scalar column; binary columns (image,
audio) are skipped.

Pin `revision` for anything you will re-run. Omitting it resolves to the
dataset's current parquet-ref commit, which is fine for a one-shot local index
and a drift hazard for anything repeated.

### Chunking

A dataset's text column is usually a whole document. The `chunk` block declares
how it is split, and it belongs on the source:

```yaml
  chunk:
    strategy: recursive     # none | fixed | recursive | sentence | markdown
    unit: tokens            # tokens | characters
    size: 512
    overlap: 64
    tokenizer: cl100k_base  # when unit: tokens
```

| Strategy | Use it when |
| --- | --- |
| `none` | Rows are already short — one document per row. The default. |
| `fixed` | Uniform blocks matter more than boundaries. |
| `recursive` | General prose. Walks a paragraph → line → sentence → word ladder, keeping each piece under `size`. The right default for documents. |
| `sentence` | Retrieval targets should be individual sentences. |
| `markdown` | Structured docs — splits on headings, so sections stay whole. |

Each source row becomes one document, and `text` splits into chunks. **The
chunk is what gets indexed and returned**, so a hit is a passage, not a book.
Each chunk lands as a row with id `{documentId}#{i}`, carrying the document's
attributes plus `_hevlayer_parent_id` and `_hevlayer_chunk_index` — use the
parent id to group or deduplicate hits from the same document in the UI.

Sizing, in practice: `size: 512` tokens with `overlap: 64` is a reasonable
default for prose and most embedding models. Smaller chunks sharpen retrieval
and lose context; larger ones do the reverse. Overlap of 10–15% of `size`
keeps a sentence spanning a boundary from being lost by both neighbors. Pin
`tokenizer` so boundaries stay reproducible across reindexes.

---

## 4. Index

Locally, one process does what the cluster's Pipeline worker does at scale:
read the source, apply the chunk block, embed, write. No queue, no
autoscaling, one replica.

Keep the worker's inputs in the same environment contract the cluster injects,
so the local run and the deployed one differ only in who sets them:

| Variable | Value |
| --- | --- |
| `HEVLAYER_BASE_URL` | `http://localhost:8080` |
| `HEVLAYER_TARGET_NAMESPACE` | The namespace being written |
| `HEVLAYER_SOURCE_REF` | The source block above, as JSON |
| `HEVLAYER_WAREHOUSE` | The warehouse connection, as JSON, no credential |

Two things differ locally and you must account for both:

- **Embedding happens in your indexer.** The local Postgres store does not
  accept an `embed` block in the schema (a write that declares one returns
  `422 UnsupportedByStore`), so vectors arrive on the row. Use a CPU embedding model — `fastembed` with
  `BAAI/bge-small-en-v1.5` (384 dimensions) is a good default and needs no GPU
  and no API key. Whatever you pick, embed the *query* with the same model at
  search time or the vectors are meaningless.
- **Rows are written straight to the namespace.** Post to
  `POST /v2/namespaces/{ns}` in batches. The first write creates the namespace,
  so send the schema with it.

```sh
curl --fail-with-body http://localhost:8080/v2/namespaces/squad \
  -H 'Content-Type: application/json' \
  -d '{
    "distance_metric": "cosine_distance",
    "schema": {
      "text":  {"type": "string", "full_text_search": true},
      "title": {"type": "string", "filterable": true}
    },
    "upsert_rows": [
      {"id": "s1#0", "text": "…", "title": "Amazon rainforest", "vector": [0.01, …]}
    ]
  }'
```

Batch a few hundred rows per request. Rows in one request are applied whole —
a body mixing a supported shape with an unsupported one is rejected entirely,
never partially applied.

Watch the index catch up:

```sh
curl http://localhost:8080/v2/namespaces/squad/metadata
```

The `layer` block reports `indexed` and `index_lag_rows`. Rows can be present
and counted before their vectors are indexed, so wait for `indexed: true`
before judging result quality — early emptiness is usually an unfinished
index, not a bad schema.

---

## 5. Query

One route: `POST /v2/namespaces/{ns}/query`. What kind of search you get is
decided by the `rank_by` expression, not by a different endpoint.

```sh
# Lexical — BM25 over an indexed text field
-d '{"rank_by": ["text", "BM25", "wireless earbuds"], "top_k": 10, "include_attributes": true}'

# Semantic — ANN over the row vectors
-d '{"rank_by": ["vector", "ANN", [0.01, 0.02, …]], "top_k": 10}'

# Hybrid text, locally — Postgres serves HybridText with fuzziness 0 only
-d '{"rank_by": ["text", "HybridText", "rainforest climate", {"fuzziness": 0}], "top_k": 10}'

# Typo-tolerant, on turbopuffer — BM25 plus per-token fuzzy legs, RRF-fused
-d '{"rank_by": ["text", "HybridText", "conection timout"], "top_k": 10}'
```

Add predicates and projection with `filters` and `include_attributes`:

```json
{
  "rank_by": ["text", "HybridText", "rainforest climate", {"fuzziness": 0}],
  "top_k": 20,
  "filters": ["title", "Eq", "Amazon rainforest"],
  "include_attributes": ["title", "text", "source_url"]
}
```

`rank_by` is mutually exclusive with the top-level `vector` and
`nearest_to_id` shapes. Results come back under `rows`, each with a `$dist`.

**`HybridText` is where most of the value is, and it is free.** The gateway
tokenizes the input with turbopuffer's own tokenizer — the same code that
segmented the text at index time — expands it into one BM25 leg plus a fuzzy
leg per token, and fuses them with RRF. One expression in, typo-tolerant
ranked results out. Defaults: `fuzziness: "auto"`, `rank_constant: 60`,
`per_leg_limit: clamp(5 × top_k, 50, 200)`. Pass a fourth tuple element to
tune them. Locally the default `"auto"` returns `422 UnsupportedByStore`, so
pass `{"fuzziness": 0}`; the fuzzy legs need turbopuffer. Set
`include_leg_breakdown: true` to get per-row `$fused.legs`
attribution — which is what you render when the UI shows *why* a row matched.

The response carries a `hybrid` block echoing the expansion that actually ran.
Surface it. The gateway's decision is a feature of the app, not an internal
detail: a badge saying a result was surfaced by fuzzy match rather than exact
is more convincing than the result itself.

### Before reaching for anything else

Read [`store-capabilities.md`](store-capabilities.md). A `no` in the local
column means the gateway returns:

```json
{"error": "UnsupportedByStore", "store": "pgvector", "route": "…", "message": "…"}
```

That is a declared gap, not a bug and not something to route around. **Do not
implement the missing feature in the application tier.** Either use a shape
the store serves, or tell the user the namespace needs a backend that serves
it. Silently reimplementing facets, fusion, or filtering in the app is the
failure mode this whole boundary exists to prevent.

Locally that notably rules out: fuzzy `HybridText` (`fuzziness` other than
`0`), array-containment and regex filters, facets, `searchAfter` cursors,
multi-vector and sparse ranking, `aggregate_by`, schema `embed`, and automatic
query routing. Plan the app's features against
the matrix, not against the docs index.

### The wire itself

Layer speaks turbopuffer's API. For request and response shapes beyond what is
here, read turbopuffer's own machine-readable docs rather than guessing:

- <https://turbopuffer.com/llms.txt> — the index
- <https://turbopuffer.com/llms-full.txt> — the full corpus

For what Layer adds on top — routing, `HybridText`, stable reads, the `layer`
metadata block — read <https://hevlayer.com/docs/api/query>, or use the
`hevlayer-docs` skill to query the docs from the command line.

---

## 6. Generate the interface

Do not hand-roll a results page. `hev/layer-ui` turns a namespace schema into
a configured search interface, and the schema you designed in stage 2 is its
input.

```sh
git clone https://github.com/hev/layer-ui.git
cd layer-ui
npm run dev            # Node 22+, no install step
```

Open `http://127.0.0.1:4387/apps.html` and:

1. Paste the namespace schema, or the whole `/metadata` response — both are
   accepted.
2. Generate the interface. Filter controls follow the field types and index
   settings you declared: scalar string, numeric, date, and boolean filters
   appear for filterable fields; a schema with no full-text index generates a
   browse-only interface. Controls that the schema cannot support stay visible
   with the reason shown — which is a useful review of stage 2.
3. Choose the namespace, search field, result layout, request limit, displayed
   fields, and the title/image/source mappings.
4. Download `search-app.json` — the presentation definition, with no query
   drafts, sample rows, or credentials in it.

Then serve it:

```sh
npm run build:runtime
puff serve -n squad --app ./search-app.json --ui-dir ./dist-runtime
```

Run that from the layer-ui checkout with `puff` installed and pointed at your
gateway. The Apps page itself sends no requests and needs no key — it is a
configurator working from the schema alone, so it is safe to iterate on the
interface before any rows exist.

If the generated interface is close but not right, edit `search-app.json` and
re-serve rather than rebuilding the page by hand. It is a spec, not a scaffold.

---

## 7. Graduating to a cluster

The local stack is the same gateway, the same wire, and the same
Warehouse/Pipeline configuration the Kubernetes deployment uses. Moving up
means handing the config to the operator instead of running the worker
yourself:

- The warehouse block becomes a `Warehouse` resource; the source and chunk
  blocks become a `Pipeline`.
- The single local worker becomes `spec.scaling` — `mode: fixed` pins one
  replica, `mode: autoscale` scales it on queue depth, and
  `replicas.min: 0` with a `spec.schedule` cron runs it nightly.
- Chunking and embedding split across two stages: extract and chunk on CPU,
  embed on GPU workers.
- Ingest writes stage into the pipeline queue rather than going straight to the
  namespace.

Reference: <https://hevlayer.com/docs/kubernetes/warehouse-crd> and
<https://hevlayer.com/docs/kubernetes/pipeline-crd>.

---

## Checklist before calling it done

- [ ] Every retrieval decision is made by the gateway. No fusion, tokenization,
      scoring, or filtering logic in the application.
- [ ] Every feature the app uses is `yes` for the target store in
      `store-capabilities.md`.
- [ ] The same embedding model is used at index time and query time.
- [ ] `/metadata` reports `indexed: true` before result quality is judged.
- [ ] Chunk hits are grouped or deduplicated by `_hevlayer_parent_id` in the UI.
- [ ] The gateway's decision is visible in the UI — the `hybrid` echo, a
      surfacing badge, or leg attribution.
- [ ] No API key is required to run the app locally.
