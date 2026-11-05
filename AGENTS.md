# walspool: Agent Guidelines

Sub-microsecond, embeddable Write-Ahead Log (WAL) buffer and real-time observability hub in pure Go.

## Architecture & Boundaries

- **Core Engine (`engine.go`, `hub.go`, `ports.go`, `record.go`, `storage_*.go`)**:
  - **Contract-First**: Any new capability or storage driver MUST be specified via interfaces in [`ports.go`](ports.go) before implementation.
  - **Zero External Dependencies**: Core engine relies strictly on the Go standard library (`os`, `sync`, `bufio`, `hash/crc32`, `net/http`, `log/slog`). External third-party packages are strictly forbidden in core.
- **Daemon & Integrations**:
  - `cmd/walspool`: Standalone daemon binary.
  - `cmd/sidecar`: OTLP telemetry streaming agent.

## Verification Sensor

Always execute the project verification sensor before declaring any task complete:

```bash
./scripts/verify.sh
```

For performance-sensitive changes to ingestion or storage paths, verify latency and allocations:
```bash
./scripts/verify.sh --bench
```
*(Ingestion target: sub-microsecond latency, <= 2 allocs/op)*

## Standards & Mechanical Sympathy

- **Pure Go**: Strictly `CGO_ENABLED=0`. Zero CGO or native C library requirements.
- **Allocation Avoidance**: Ingestion and append paths must minimize or eliminate heap allocations (use `sync.Pool`, pre-allocated ring buffers, and defensive slicing).
- **Concurrency & Races**: Use lock-free atomics or fine-grained channel synchronization. Never introduce unbounded goroutine spawning or lock contention on hot paths.
- **Retry & Backoff Loops**: Timer resets and decorrelated backoff calculations must produce zero heap allocations. Use reusable `*time.Timer` with explicit draining and `math/rand/v2` without global mutex locks.
- **Cache-Line Alignment & False Sharing**: In highly concurrent structures, hot atomic counters, cursors, and synchronization primitives must be isolated on dedicated 64-byte cache line boundaries (e.g. `_ [56]byte` padding). Group cold fields at the tail. Always enforce layout via static tests in `*_internal_test.go` using `unsafe.Offsetof` checking `offset % 64 == 0` and pairwise distance `>= 64` bytes.
- **Ring Buffers & Secondary Indexing**: Ring buffer eviction must run in strict O(1) time with 0 slice reallocations. Secondary indexes over fixed-capacity ring buffers must use preallocated intrusive integral cursor links (e.g. `slotLinks` with `indexSpan`) instead of dynamic slices. This eliminates `runtime.growslice`, avoids `copy()` translation overhead, and keeps hash map entries as direct value buckets without heap escape.
- **Commit Discipline (DCO)**: All git commits must be signed off to certify the Developer Certificate of Origin: `git commit -s -m "..."`.
