#!/usr/bin/env bash
# Self-test for scripts/artifact-check.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK="$ROOT/scripts/artifact-check.sh"
mkdir -p "$ROOT/tmp"
TMP="$(mktemp -d "$ROOT/tmp/artifact-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

expect_code() {
  local expected="$1"
  local label="$2"
  shift 2
  set +e
  "$@" >"$TMP/out" 2>&1
  local code=$?
  set -e
  if [[ "$code" -ne "$expected" ]]; then
    echo "FAIL: $label expected exit $expected, got $code" >&2
    cat "$TMP/out" >&2
    exit 1
  fi
}

expect_fail() {
  expect_code 1 "$@"
}

expect_usage() {
  expect_code 2 "$@"
}

# 1. Missing file
expect_fail "missing artifact" "$CHECK" "$TMP/missing.md"

# 2. Empty file
: >"$TMP/empty.md"
expect_fail "empty artifact" "$CHECK" "$TMP/empty.md"

# 3. Directory target
expect_fail "directory target" "$CHECK" "$TMP"

# 4. Valid file
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

# 5. Options placed BEFORE and AFTER path
if ! "$CHECK" --max-age 60 "$TMP/good.md" --require '# Decision' >"$TMP/out" 2>&1; then
  echo "FAIL: options before and after path were rejected" >&2
  cat "$TMP/out" >&2
  exit 1
fi

# 6. Relative path support
(
  cd "$TMP"
  if ! "$CHECK" good.md --max-age 60 >out 2>&1; then
    echo "FAIL: relative path was rejected" >&2
    cat out >&2
    exit 1
  fi
)

# 7. Symlink rejection
ln -s "$TMP/good.md" "$TMP/symlink.md"
expect_fail "symlink artifact" "$CHECK" "$TMP/symlink.md"

# 8. Unreadable file (permissions denied)
cp "$TMP/good.md" "$TMP/unreadable.md"
chmod 000 "$TMP/unreadable.md"
expect_fail "unreadable artifact" "$CHECK" "$TMP/unreadable.md"
chmod 644 "$TMP/unreadable.md"

# 9. Future modification time rejection (anti-gaming)
cp "$TMP/good.md" "$TMP/future.md"
touch -t 203801010000 "$TMP/future.md"
expect_fail "future artifact" "$CHECK" "$TMP/future.md"

# 10. Path '-' (stdin) rejection
expect_fail "stdin path" "$CHECK" "-"

# 11. Size limit exceeded
expect_fail "size limit exceeded" "$CHECK" "$TMP/good.md" --max-bytes 5

# 12. Missing required heading
expect_fail "missing required heading" "$CHECK" "$TMP/good.md" --require '## Tests'

# 13. Stale artifact
cp "$TMP/good.md" "$TMP/stale.md"
touch -t 202001010000 "$TMP/stale.md"
expect_fail "stale artifact" "$CHECK" "$TMP/stale.md" --max-age 60

# 14. Target starting with hyphen via --
cp "$TMP/good.md" "$TMP/-hyphen-target.md"
if ! "$CHECK" --max-age 60 -- "$TMP/-hyphen-target.md" >"$TMP/out" 2>&1; then
  echo "FAIL: target starting with hyphen using -- was rejected" >&2
  cat "$TMP/out" >&2
  exit 1
fi

# 15. Usage errors (exit code 2)
expect_usage "invalid max-age" "$CHECK" "$TMP/good.md" --max-age nope
expect_usage "missing max-age arg" "$CHECK" "$TMP/good.md" --max-age
expect_usage "invalid max-bytes" "$CHECK" "$TMP/good.md" --max-bytes nope
expect_usage "missing max-bytes arg" "$CHECK" "$TMP/good.md" --max-bytes
expect_usage "missing require arg" "$CHECK" "$TMP/good.md" --require
expect_usage "empty require arg" "$CHECK" "$TMP/good.md" --require ""
expect_usage "whitespace require arg" "$CHECK" "$TMP/good.md" --require "   "
expect_usage "no arguments" "$CHECK"
expect_usage "unknown option" "$CHECK" "$TMP/good.md" --unknown-flag
expect_usage "multiple targets" "$CHECK" "$TMP/good.md" "$TMP/stale.md"
(
  cd "$TMP"
  expect_usage "hyphen target without --" "$CHECK" -hyphen-target.md
)

echo "PASS: artifact-check self-test"
