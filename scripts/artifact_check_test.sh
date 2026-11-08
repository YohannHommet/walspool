#!/usr/bin/env bash
# Self-test for scripts/artifact-check.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK="$ROOT/scripts/artifact-check.sh"
mkdir -p "$ROOT/tmp"
TMP="$(mktemp -d "$ROOT/tmp/artifact-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

expect_fail() {
  local label="$1"
  shift
  set +e
  "$@" >"$TMP/out" 2>&1
  local code=$?
  set -e
  if [[ "$code" -ne 1 ]]; then
    echo "FAIL: $label expected exit 1, got $code" >&2
    cat "$TMP/out" >&2
    exit 1
  fi
}

# Missing file.
expect_fail "missing artifact" "$CHECK" "$TMP/missing.md"

# Empty file.
: >"$TMP/empty.md"
expect_fail "empty artifact" "$CHECK" "$TMP/empty.md"

cat >"$TMP/good.md" <<'EOF'
# Decision
Use artifact-first delegation.

## Files
- AGENTS.md
- docs/harness/DELEGATION_CONTRACT.md

## Risks
- checkpoint overhead
EOF

if ! "$CHECK" "$TMP/good.md" --max-age 60 --require '# Decision' --require '## Risks' >"$TMP/out" 2>&1; then
  echo "FAIL: valid artifact was rejected" >&2
  cat "$TMP/out" >&2
  exit 1
fi
grep -q 'PASS: artifact' "$TMP/out" || { echo "FAIL: missing PASS output" >&2; cat "$TMP/out" >&2; exit 1; }

expect_fail "missing required heading" "$CHECK" "$TMP/good.md" --require '## Tests'

cp "$TMP/good.md" "$TMP/stale.md"
touch -t 202001010000 "$TMP/stale.md"
expect_fail "stale artifact" "$CHECK" "$TMP/stale.md" --max-age 60

set +e
"$CHECK" "$TMP/good.md" --max-age nope >"$TMP/out" 2>&1
usage=$?
set -e
if [[ "$usage" -ne 2 ]]; then
  echo "FAIL: invalid max-age expected exit 2, got $usage" >&2
  cat "$TMP/out" >&2
  exit 1
fi

echo "PASS: artifact-check self-test"
