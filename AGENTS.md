# Agent guide

This repository ships agent skills. If your harness reads a skill directory,
install them and stop reading here:

```sh
scripts/install-skills.sh          # → ${CODEX_HOME:-~/.codex}/skills
```

Claude Code needs nothing — `.claude/skills/` already links to `skills/`.

| Skill | Use it for |
| --- | --- |
| `skills/hevlayer-search-app` | Building a search application on Layer. Start here. |
| `skills/hevlayer-docs` | Answering Layer questions from the docs. |
| `skills/hevlayer-layer-cli` | Driving the `layer` CLI. |

If your harness has no skill directory, paste the body of the relevant
`SKILL.md` below this line. The files are written to work either way.

## Orientation

Layer is a turbopuffer-shaped gateway: one HTTP API in front of whichever store
holds the data. Locally that store is Postgres, in the same Compose file, with
no account and no API key.

```sh
export TURBOPUFFER_API_KEY=""
docker compose up -d --wait
curl --fail http://localhost:8080/health
```

An empty `TURBOPUFFER_API_KEY` selects local Postgres; a nonblank one selects
turbopuffer and doubles as the gateway bearer token.

## The one rule

**The gateway owns retrieval.** Ranking, fusion, tokenization, typo tolerance,
facet counting, and query routing happen inside it. An application posts a
query and renders the response. Writing reciprocal-rank fusion, a tokenizer, a
BM25 implementation, or a client-side rescoring pass means the request shape is
wrong or the feature is genuinely unsupported — not that it should be
reimplemented here.

Before using a wire feature, check
`skills/hevlayer-search-app/store-capabilities.md`. A `no` there means the
gateway returns `422 UnsupportedByStore`, which is a declared gap, not a bug.

## Layout

- `apps/layer-gateway` — the standalone gateway binary and library.
- `apps/layer-cli` — the `layer` CLI, including the `install` lifecycle.
- `crates/vectorstore-core` — vector-store clients, routing, and wire types.
- `crates/metrics-catalog` — public metric catalog metadata.
- `infra/terraform`, `infra/helm/layer` — the AWS footprint and Helm chart.
- `skills/` — the agent skills described above.

## Reference

- Layer docs: <https://hevlayer.com/docs>, plain text at
  <https://hevlayer.com/docs/ce/llms.txt> and
  <https://hevlayer.com/docs/ce/llms-full.txt>
- The turbopuffer wire itself: <https://turbopuffer.com/llms.txt>
