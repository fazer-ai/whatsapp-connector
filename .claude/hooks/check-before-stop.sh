#!/bin/bash
# Runs `make check-offline` before letting the agent stop, but only if relevant files
# changed. Returns ok:false with a reason if checks fail, so the agent continues to fix
# issues.
#
# The offline half on purpose: `make check` needs a PostgreSQL and a Redis of its own, and
# a stop hook that fails because nothing is listening on this machine would block every
# agent here for a reason that has nothing to do with what it just wrote. What this hook
# owes is fast feedback on uncommitted work; the dialect passes are CI's job and the
# round's, and `make check` is what says so.

# Read stdin (hook input JSON) — check if stop hook is already active to avoid infinite loops
input=$(cat)
stop_hook_active=$(echo "$input" | jq -r '.stop_hook_active // false')

if [ "$stop_hook_active" = "true" ]; then
  echo '{"ok": true}'
  exit 0
fi

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo '{"ok": false, "reason": "Unable to resolve git repository root before running make check."}'
  exit 0
}
cd "$repo_root" || {
  echo '{"ok": false, "reason": "Unable to access git repository root before running make check."}'
  exit 0
}

# Only run if there are uncommitted changes to files that make check cares about
changed_files=$(git diff --name-only HEAD 2>/dev/null; git diff --name-only --cached 2>/dev/null; git ls-files --others --exclude-standard 2>/dev/null)
# The Makefile and the workflows are in the filter because `internal/toolchain` reads
# them: without them, a change to exactly the files that fence validates ends the turn
# having run nothing, and the fence is skipped for its own subject. The fence asserts
# this list covers what it reads, so the two cannot drift apart in silence.
relevant=$(echo "$changed_files" | grep -E '(\.go$|^go\.(mod|sum)$|^contract/|^Makefile$|^\.github/workflows/.*\.ya?ml$)' | head -1)

if [ -z "$relevant" ]; then
  echo '{"ok": true}'
  exit 0
fi

if make check-offline 2>&1; then
  echo '{"ok": true}'
else
  echo '{"ok": false, "reason": "make check-offline failed (lint, go mod tidy, or the SQLite test pass). Fix it above before stopping. Note this is the offline half: the PostgreSQL and Redis passes need servers and run in CI, or here via make check."}'
fi
