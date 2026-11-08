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

## Feedback Auto-Improve Loop

When the sensor fails, do not re-prompt blindly or rerun unchanged code. Run:

```bash
./scripts/feedback-loop.sh
```

The wrapper runs the sensor, writes a bounded `.walspool/feedback/latest.md`,
and counts identical failure signatures.

- Attempts 1-2: fix only the first failure, then rerun the loop.
- Attempt 3 with the same signature: stop changing production code and patch the
  harness first:
  - Guidance gap: update this `AGENTS.md` or the relevant leaf rule.
  - Sensor gap: add a regression test or a `scripts/verify.sh` check.
  - Execution gap: add a guard/hook or a forbidden-move check.
- Never weaken an assertion, skip a test, or rerun unchanged code after a failure.
- Run `./scripts/feedback_loop_test.sh` when changing the loop itself.

Loop state is local and ignored under `.walspool/feedback/`; it is not committed.

## Delegation Contract

Delegated work is checkpointed, not open-ended. Record an artifact path and a hard
stop before spawning a teammate; a non-empty skeleton is due at T+5, the Lead
nudges at T+5 and interrupts/reassigns at T+7, and no silent thinking may exceed
3 minutes. Validate with `./scripts/artifact-check.sh <path> --max-age 300 --require '<heading>'`. Role deadlines and the watchdog algorithm are in
[docs/harness/DELEGATION_CONTRACT.md](docs/harness/DELEGATION_CONTRACT.md).

## Standards & Mechanical Sympathy

- **Pure Go**: Strictly `CGO_ENABLED=0`. Zero CGO or native C library requirements.
- **Allocation Avoidance**: Ingestion and append paths must minimize or eliminate heap allocations (use `sync.Pool`, pre-allocated ring buffers, and defensive slicing).
- **Concurrency & Races**: Use lock-free atomics or fine-grained channel synchronization. Never introduce unbounded goroutine spawning or lock contention on hot paths.
- **Retry & Backoff Loops**: Timer resets and decorrelated backoff calculations must produce zero heap allocations. Use reusable `*time.Timer` with explicit draining and `math/rand/v2` without global mutex locks.
- **Cache-Line Alignment & False Sharing**: In highly concurrent structures, hot atomic counters, cursors, and synchronization primitives must be isolated on dedicated 64-byte cache line boundaries (e.g. `_ [56]byte` padding). Group cold fields at the tail. Always enforce layout via static tests in `*_internal_test.go` using `unsafe.Offsetof` checking `offset % 64 == 0` and pairwise distance `>= 64` bytes.
- **Ring Buffers & Secondary Indexing**: Ring buffer eviction must run in strict O(1) time with 0 slice reallocations. Secondary indexes over fixed-capacity ring buffers must use preallocated intrusive integral cursor links (e.g. `slotLinks` with `indexSpan`) instead of dynamic slices. This eliminates `runtime.growslice`, avoids `copy()` translation overhead, and keeps hash map entries as direct value buckets without heap escape.
- **Batch Ingestion Atomicity (All-or-Nothing)**: Endpoints processing batched records (such as HTTP `POST /v1/enqueue/batch`) must enforce two-phase atomic validation in memory prior to spooling. If any single item fails schema validation, topic resolution, or byte bounds (1 MB max payload, 10 MB total batch), the entire batch must be rejected immediately (HTTP 400 or 413) without any partial WAL append or LogHub index pollution. If capacity or disk saturation occurs during spooling (`walspool.ErrSpoolFull`), fail immediately with HTTP 503 Service Unavailable and a `Retry-After: 1` header reporting exact enqueued progress.
- **Streaming NDJSON Ingestion (Zero-Alloc & Anti-OOM)**: Streaming NDJSON endpoints (e.g. `POST /v1/enqueue` with `application/x-ndjson` or `application/jsonl`) must read records line-by-line using pooled `bufio.Reader` (64 KiB fast-path) without reading the entire HTTP body into memory. Each individual line must be capped strictly at 1 MiB (`maxNDJSONLineBytes`) to prevent OOM DoS attacks (returning HTTP 413 Request Entity Too Large if exceeded). Ingestion must immediately persist records to WAL and dispatch to LogHub in real-time, supporting continuous streaming clients while handling errors gracefully (returning HTTP 202 with `partial_success` in resilient mode, or HTTP 400 on the first invalid line in strict mode `?strict=true`).
- **Commit Discipline (DCO)**: All git commits must be signed off to certify the Developer Certificate of Origin: `git commit -s -m "..."`.
