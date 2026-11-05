#!/usr/bin/env bash
# Fast verification sensor for walspool (< 2.5s)
set -eo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Ensure clean Go toolchain environment without GOPATH/GOROOT collisions
export GOROOT="/home/yohann/go"
export PATH="$GOROOT/bin:$PATH"
export GOPATH="/home/yohann/go/pkg"
export CGO_ENABLED=0

echo "==> [walspool] Running Go static analyzer (vet)..."
go vet ./...

if [[ "$1" == "--bench" ]]; then
  echo "==> [walspool] Running quick benchmark sanity check..."
  go test -bench=BenchmarkHub_Ingest -run=^$ -benchtime=500ms ./...
else
  echo "==> [walspool] Running unit tests..."
  go test ./... "$@"
fi

echo "==> [walspool] All verification checks passed cleanly."
