---
name: hevlayer-docs
description: >-
  Query the hev layer docs from the command line. Use when the user asks about
  Layer — the turbopuffer gateway, stable reads, the stable watermark, the
  document cache, warm jobs, scans (filter, full-text, and radius), snapshots,
  pipelines, UDFs, the Index/InfraRules/Pipeline/Function CRDs, compute pools,
  install via Terraform or Helm, failure modes, or the dashboard.
---

# hev layer docs

Answer Layer questions from the docs, not from memory. Every verb is a keyless
read — nothing to sign up for, nothing to configure beyond the endpoint.

    ask --endpoint https://hevlayer.com/api/ask/ce search "<question>"
    ask --endpoint https://hevlayer.com/api/ask/ce section get "<id>"
    ask --endpoint https://hevlayer.com/api/ask/ce overview
    ask --endpoint https://hevlayer.com/api/ask/ce glossary get "<term>"

Start with `search`; fetch sections for detail; use `overview` when you need
the full map. The `ce` endpoint serves the Community Edition view of the
docs; use `https://hevlayer.com/api/ask/pro` for the full product. Section ids
look like `api/query#stable-reads`. Cite sections in
your answer as https://hevlayer.com plus the returned `url` field.

| Verb | Returns |
| --- | --- |
| `overview` | Orientation context plus the full section map with stable ids |
| `search "<query>"` | Ranked sections with snippets and deep links |
| `section get "<id>"` | One section: summary, exact identifiers, source URL |
| `glossary get "<term>"` | A product term resolved through its aliases |

Search runs over a committed, reviewable digest of the docs — the same corpus,
heading by heading, that renders on the site. Every anchor is verified against
the rendered pages in CI, so a cited deep link always resolves.

If `ask` is missing, install it:
`go install github.com/hev/ask/cmd/ask@latest`

The docs are also available as plain text at
https://hevlayer.com/docs/ce/llms.txt (index) and
https://hevlayer.com/docs/ce/llms-full.txt (full corpus). Prefer the CLI
when you can run commands — it ranks, resolves aliases, and costs a fraction
of the tokens.
