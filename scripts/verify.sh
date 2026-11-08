#!/usr/bin/env bash
# Fast verification sensor for walspool (warm cache < 5s)
set -eo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Locate the Go toolchain without assuming a maintainer-specific home path.
if ! command -v go >/dev/null 2>&1; then
  for candidate in "$HOME/go/bin" /usr/local/go/bin /opt/homebrew/bin; do
    if [ -x "$candidate/go" ]; then
      PATH="$candidate:$PATH"
      break
    fi
  done
fi
if ! command -v go >/dev/null 2>&1; then
  echo "walspool: go executable not found; add Go to PATH" >&2
  exit 127
fi

export CGO_ENABLED=0

# Keep the sensor self-contained and avoid a root-owned shared Go cache.
export GOCACHE="${GOCACHE:-$PROJECT_ROOT/tmp/gocache}"
export TMPDIR="${TMPDIR:-$PROJECT_ROOT/tmp}"
mkdir -p "$GOCACHE" "$TMPDIR"

echo "==> [walspool] Running Go static analyzer (vet)..."
go vet ./...

if [[ "$1" == "--bench" ]]; then
  echo "==> [walspool] Running quick benchmark sanity check..."
  go test -bench=BenchmarkHub_Ingest -run=^$ -benchtime=500ms ./...
else
  echo "==> [walspool] Running unit tests..."
  go test ./... "$@"
fi

echo "==> [walspool] Running feedback-loop self-test..."
"$PROJECT_ROOT/scripts/feedback_loop_test.sh"

echo "==> [walspool] All verification checks passed cleanly."
