#!/usr/bin/env bash
#
# install-skills.sh — copy this repository's agent skills into a harness
# skill directory.
#
# Usage:
#   scripts/install-skills.sh                 # → ${CODEX_HOME:-~/.codex}/skills
#   AGENT_SKILL_HOME=~/.claude/skills scripts/install-skills.sh
#   scripts/install-skills.sh --list          # print what would be installed
#
# Claude Code needs none of this: .claude/skills/ in this repo already links
# to skills/, so a clone is enough.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$REPO_ROOT/skills"
DEST="${AGENT_SKILL_HOME:-${CODEX_HOME:-$HOME/.codex}/skills}"

skills=()
for dir in "$SRC"/*/; do
  [[ -f "$dir/SKILL.md" ]] || continue
  skills+=("$(basename "$dir")")
done

if [[ ${#skills[@]} -eq 0 ]]; then
  echo "no skills found in $SRC" >&2
  exit 1
fi

if [[ "${1:-}" == "--list" ]]; then
  printf '%s\n' "${skills[@]}"
  exit 0
fi

mkdir -p "$DEST"
for skill in "${skills[@]}"; do
  rm -rf "${DEST:?}/$skill"
  cp -R "$SRC/$skill" "$DEST/$skill"
  echo "installed $skill -> $DEST/$skill"
done

echo
echo "Set AGENT_SKILL_HOME to install elsewhere. Restart your agent to pick them up."
