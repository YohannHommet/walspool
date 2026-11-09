# Walspool v1.2.0 — Release Notes

**Tag:** `v1.2.0`  
**Release date:** 2026-11-09  
**License:** FSL-1.1-MIT

## Highlights

v1.2.0 is the community-hardening release. It closes the remaining P0 safety gaps, all P1 ingest/reliability gaps, and the high-value architecture and harness findings from the five-role audit.

### Storage safety

- **WAL layout mismatch refusal.** The file engine now classifies the directory before creating or truncating anything. A segmented directory opened with default options, a checkpoint-only directory, a malformed segment name, a symlinked WAL, or an ambiguous `active.wal` + `segment-000...` layout returns `ErrStorageFormatMismatch` (wrapping `ErrPreconditionViolated`) with zero mutation.
- **Atomic capacity admission.** `Config.MaxCapacity` is now enforced by a CAS reservation in `Engine`, seeded from the recovered uncommitted backlog and released only after a successful commit. It works for every `StorageEngine`, including third-party adapters.
- **Bounded sidecar spool** (from the earlier bounded-spool work): 64 MiB segments, pruning, and a serialized-byte quota (`-max-spool-bytes`, default 1 GiB).

### Ingest reliability

- Shared `ValidateRecord` / `MaxTopicBytes` contract; memory and file storage now reject the same topic identically.
- NDJSON resilient mode returns `202 partial_success` with a `first_error` line instead of aborting after a durable prefix. Strict mode still fails fast with progress.
- Every ingest 503 now carries `Retry-After: 1` and exact enqueued/line progress.
- OTLP is two-phase: all records are normalized and validated before any spool write; oversized bodies return 413; capacity failures return 503 with progress.
- Single-enqueue bodies over the bound return 413 instead of being silently truncated.

### Observability

- Engine `Stats()` exposes accepted enqueues, append errors, spool-full rejections, committed records, delivery attempts, retries, sink errors, permanent rejections, and corrupt skips.
- New `/metrics` series are additive: `walspool_engine_*`, `walspool_uncommitted_bytes`.
- `StorageStats` / `StorageByteStats` formalize disk and byte reporting; the anonymous `DiskBytes` assertion is gone.
- `DiskBytes` is cached for 5 seconds to prevent per-scrape directory stat storms.

### Architecture and harness

- Sidecar `main.go` reduced from 1,948 to 207 lines, split into `server.go`, `ingest.go`, `batch.go`, `logs.go`, `metrics.go`, `httpsink.go`, and `config.go`.
- `Hub.Ingest` split into normalization, O(1) eviction/insert, subscriber snapshot, and out-of-lock notification steps.
- JSON metadata enrichment moved into the core as `EnrichPayloadMetadata`; the LogHub schema is no longer a sidecar-wire detail.
- File recovery truncation branches collapsed into one helper with a reused header buffer and no per-frame `Seek`.
- `quality_checker.py` now fails closed when `git diff` fails instead of reporting a false PASS.
- Removed hardcoded maintainer Go paths from squad/stress tooling; CI invokes `./scripts/verify.sh`; golangci-lint pinned.

## Upgrade notes

- **No on-disk format change** for valid directories.
- Incompatible WAL directories now fail loudly instead of silently recovering zero records. Open the directory with the options that wrote it, or migrate explicitly.
- Resilient NDJSON clients should treat `202 partial_success` as a successful partial ingestion and inspect `first_error`/`errors` before retrying.
- 503 bodies gain additive `enqueued`, `total`/`line`, and `Retry-After` fields.
- New metrics are additive; existing metric names are unchanged.
- Library defaults (`MaxSegmentSize=0`, `MaxUncommittedBytes=0`) remain unchanged for ADR-001 compatibility; the sidecar opts into bounded mode.

## Verification

```
go test -count=1 ./...   PASS
go vet ./...             PASS
gofmt -l -s .            clean
./scripts/verify.sh      PASS (including harness self-tests)
quality_checker.py main  PASS
```

The GitHub Actions `CI` job still fails before runner assignment (`runner_name` empty, zero steps) on this repository. That is an Actions runner-provisioning issue, not a code failure; the complete local release gate is green.

## Documentation

- [README](../../README.md)
- [Architecture](../architecture/ARCHITECTURE.md)
- [ADRs](../architecture/adr/)
- [2026-11-09 codebase snapshot](../audit/CODEBASE_SNAPSHOT_2026-11-09.md)
