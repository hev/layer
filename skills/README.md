# Agent skills

Three skills ship with this repository. They are plain `SKILL.md` files with
YAML frontmatter — no harness-specific format, no runtime, nothing to install
beyond putting the directory where your agent looks for skills.

| Skill | Use it for |
| --- | --- |
| [`hevlayer-search-app`](hevlayer-search-app/SKILL.md) | Building a search application: schema design, connecting a source, chunking, indexing, querying, generating the UI. Start here. |
| [`hevlayer-docs`](hevlayer-docs/SKILL.md) | Answering Layer questions from the docs instead of from memory. |
| [`hevlayer-layer-cli`](hevlayer-layer-cli/SKILL.md) | Driving the `layer` CLI — environments, indexes, pipelines, install. |

`hevlayer-search-app` also carries
[`store-capabilities.md`](hevlayer-search-app/store-capabilities.md), a
generated table of which wire features each backend serves. It is the answer to
"can I do X?" — an agent should read it rather than guess and hit a `422`.

## Install

**Claude Code** picks these up from a clone with no setup: `.claude/skills/`
in this repository links to the directories here.

**Any other harness** — Codex, or anything that reads a skill directory:

```sh
scripts/install-skills.sh
```

It copies the three skills into `$AGENT_SKILL_HOME`, defaulting to
`${CODEX_HOME:-~/.codex}/skills`. Point it wherever your harness looks:

```sh
AGENT_SKILL_HOME=~/.config/my-agent/skills scripts/install-skills.sh
```

**No skill directory at all?** Paste the body of a `SKILL.md` into `AGENTS.md`,
`CLAUDE.md`, or your harness's equivalent. The files are written to work either
way; nothing in them depends on being loaded as a skill.

## Keeping them current

`store-capabilities.md` is generated. Do not edit it by hand — it is
regenerated from the gateway's own capability declarations whenever a backend
changes.
