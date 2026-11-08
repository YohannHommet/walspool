# Walspool Bug Report

**Commit under test:** `d9a4eeb71250f2b4e868ac6ef85c5f2c9a0e5f89`
**Verification date:** 2026-11-08
**Environment:** Linux x86_64, Go 1.24.3, `CGO_ENABLED=0`
**Method:** one independent verifier per hypothesis, each in a detached git worktree.
A finding is admitted as a bug only when the verifier produced a deterministic test
that fails on the commit under test, and the failure is backed by source evidence.
**Status:** all eight confirmed defects are fixed in `fix/verified-bugs`, and the
reproduction tests are permanent regression tests.

## Result

| Category | Count |
|---|---|
| Confirmed defects | 8 |
| Hypotheses rejected after contract/source review | 2 |
| Confirmed operational gaps (not engine contract defects) | 1 |
| Documentation corrections | 1 |

### Confirmed defects

| ID | Severity | Title |
|---|---|---|
| BUG-01 | High | Acknowledged appends can still be buffered in userspace memory |
| BUG-02 | High | A failed checkpoint after permanent rejection can spin the dispatcher |
| BUG-03 | High | Records over 10 MB are accepted and then truncated on restart |
| BUG-04 | Medium | `Config.MaxCapacity` is declared, defaulted, and ignored |
| BUG-05 | Medium | `Commit` accepts offsets beyond the log and advances the checkpoint past the tail |
| BUG-06 | Medium | `walspool monitor` parses metrics the sidecar never exports |
| BUG-07 | Medium | CLI `--format raw` bypasses terminal sanitization |
| BUG-08 | Medium | Batch metadata enrichment can turn a client error into HTTP 500 |

## Regression tests

The eight reproduction tests are now permanent tests in the package directories:

| ID | Test | Package |
|---|---|---|
| BUG-01 | [`bug_durability_ack_test.go`](bug_durability_ack_test.go) | `.` |
| BUG-02 | [`bug_commit_error_loop_test.go`](bug_commit_error_loop_test.go) | `.` |
| BUG-03 | [`bug_oversized_record_test.go`](bug_oversized_record_test.go) | `.` |
| BUG-04 | [`bug_maxcapacity_ignored_test.go`](bug_maxcapacity_ignored_test.go) | `.` |
| BUG-05 | [`bug_commit_overadvance_test.go`](bug_commit_overadvance_test.go) | `.` |
| BUG-06 | [`bug_monitor_metrics_test.go`](cmd/sidecar/bug_monitor_metrics_test.go) | `./cmd/sidecar` |
| BUG-07 | [`bug_cli_raw_sanitize_test.go`](cmd/walspool/bug_cli_raw_sanitize_test.go) | `./cmd/walspool` |
| BUG-08 | [`bug_batch_enrich_500_test.go`](cmd/sidecar/bug_batch_enrich_500_test.go) | `./cmd/sidecar` |

Run `go test ./...`; these tests fail at `d9a4eeb` and pass with the fixes.

---

## BUG-01 — Acknowledged appends can still be buffered in userspace memory

**Severity:** High · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`bug_durability_ack_test.go`](bug_durability_ack_test.go), `TestBugDurabilityAck`
**Status:** Fixed

### Source evidence

`Append` serializes the record into `f.writer`, a `bufio.Writer`, and returns the new
offset without flushing it ([storage_file.go:542-605](storage_file.go#L542-L605), write
at line 571, return at line 604). The write+`fsync` path inside `Append` runs only for
`SyncEveryRecord` ([storage_file.go:576-585](storage_file.go#L576-L585)). The default
policy is `SyncInterval` at 50 ms ([storage_file.go:55](storage_file.go#L55)); that
policy only starts a background ticker ([storage_file.go:148-151](storage_file.go#L148-L151)),
and the ticker calls `Flush` ([storage_file.go:157-201](storage_file.go#L157-L201)).
`Engine.Enqueue` calls `storage.Append` and returns immediately
([engine.go:152-168](engine.go#L152-L168)), so the HTTP `202` path can acknowledge a
record that has not even reached the kernel.

The documented contract is stronger: `Enqueue` accepts into "durable storage"
([ports.go:11](ports.go#L11)), `Append` "atomically commits" ([ports.go:42](ports.go#L42)),
and the README says records are acknowledged with `202` "only after persisting to the
local WAL" ([README.md:489](README.md#L489)) and claims "100% NVMe durable"
([README.md:57](README.md#L57)).

### Reproduction and observed result

The test uses `SyncInterval: time.Hour` to keep the periodic flusher outside the
measurement window, appends one record, and checks `active.wal` immediately. It ignores
the `fsync` question and tests the weaker requirement that a successful append has at
least left process memory:

```text
--- FAIL: TestBugDurabilityAck (0.01s)
    bug_durability_ack_test.go:63: durability contract violated: Append returned success
    (record acknowledged) but active.wal is empty (size=0 bytes); after explicit Flush
    size=60 bytes (flushErr=<nil>).
FAIL
FAIL	github.com/YohannHommet/walspool	0.011s
```

The test fails on `d9a4eeb`; the same branch is exercised by the default 50 ms interval,
where the exposure window is merely shorter.

### Impact

A SIGKILL, panic, OOM kill, or non-graceful container stop loses acknowledged records
that are still in the `bufio.Writer`. Under the default interval that is up to 50 ms of
ingestion; with a larger configured interval it can be arbitrarily long. This
contradicts the advertised at-least-once and crash-durability guarantees and is
especially dangerous for the audit and billing workloads the README targets.

### Fix implemented

Flush `f.writer` to the OS file handle before `Append` returns under `SyncInterval`
(retain `fsync` batching in the periodic flusher), and route flush errors through the
existing `rollbackTo` path. Alternatively, flush in `Enqueue`/the HTTP handler before
sending `202`, and document the remaining power-loss window. `SyncEveryRecord` already
provides full `fsync`-per-ack durability. The implemented change adds one `write(2)` per
acknowledged append; `fsync` remains batched under `SyncInterval`.

---

## BUG-02 — A failed checkpoint after permanent rejection can spin the dispatcher

**Severity:** High · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`bug_commit_error_loop_test.go`](bug_commit_error_loop_test.go), `TestBugCommitErrorLoop`
**Status:** Fixed

### Source evidence

On `ErrPermanentRejection`, the engine commits past the batch but discards the commit
error and reports success:

```go
lastOffset := batch[len(batch)-1].Offset
_ = e.storage.Commit(lastOffset)
return true, nil
```

See [engine.go:313-319](engine.go#L313-L319). `runDispatcher` treats `(true, nil)` as
"records were drained" and immediately continues without sleeping
([engine.go:263-266](engine.go#L263-L266)). Backoff is only applied when the drain
returns a non-nil error ([engine.go:232-249](engine.go#L232-L249)). The two other
commit-error paths in the same function correctly return the error
([engine.go:292-295](engine.go#L292-L295), [engine.go:325-327](engine.go#L325-L327)).

### Reproduction and observed result

The test supplies a storage whose `ReadBatch` returns one record and whose `Commit`
returns `ErrStorageUnavailable`, plus a sink that returns `ErrPermanentRejection`. It
calls the internal `drainPendingBatches` seam directly:

```text
--- FAIL: TestBugCommitErrorLoop (0.00s)
    bug_commit_error_loop_test.go:68: BUG: drainPendingBatches returned nil error after
    storage.Commit failed with walspool: storage engine failure (drained=true); the
    commit failure is silently swallowed, so the dispatcher will immediately re-read and
    re-deliver the same permanently-rejected batch in a hot loop
FAIL
FAIL	github.com/YohannHommet/walspool	0.002s
```

### Impact

Under a compound fault — a permanent sink rejection plus a checkpoint failure such as a
full disk or I/O error — the dispatcher re-reads, re-delivers, and re-commits the same
batch at full speed, with no sleep and no backoff. It also re-delivers a payload the
contract says must not be retried. The source path never selects `stopCh` while
`drained` is true, so `Engine.Close` can remain blocked in `doneWg.Wait`
([engine.go:214](engine.go#L214)). The hot-loop and shutdown consequences are
source-derived; the swallowed error is directly test-confirmed.

### Fix implemented

Propagate the commit error so the dispatcher takes its backoff path:

```go
if errors.Is(deliverErr, ErrPermanentRejection) {
    lastOffset := batch[len(batch)-1].Offset
    if err := e.storage.Commit(lastOffset); err != nil {
        return false, err
    }
    return true, nil
}
```

---

## BUG-03 — Records over 10 MB are accepted and then truncated on restart

**Severity:** High · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`bug_oversized_record_test.go`](bug_oversized_record_test.go), `TestBugOversizedRecord`
**Status:** Fixed

### Source evidence

Recovery refuses any frame larger than `maxRecoverableRecordSize` (10 MB) and truncates
the WAL at that point ([storage_file.go:19](storage_file.go#L19),
[storage_file.go:383-391](storage_file.go#L383-L391)). The constant is documented as an
anti-OOM guard against corrupt headers, not as a write-side limit. `Append` has no
equivalent cap ([storage_file.go:542-599](storage_file.go#L542-L599)). The engine's
payload limit is caller-configurable ([engine.go:20](engine.go#L20)), so an embedder can
raise it above 10 MB and legally enqueue a record that storage accepts and recovery later
discards.

### Reproduction and observed result

The test appends a record with a payload of `10*1024*1024 + 1` bytes, closes the engine
gracefully, and reopens it:

```text
--- FAIL: TestBugOversizedRecord (0.08s)
    bug_oversized_record_test.go:68: Append accepted a 10485761-byte record (limit is
    10485760) but after close/reopen UncommittedCount() = 0, want 1: the acknowledged
    record was silently discarded on recovery
FAIL
FAIL	github.com/YohannHommet/walspool	0.084s
```

### Impact

Silent, unrecoverable loss of an acknowledged record. Recovery truncates the frame at
offset 0 and reports zero uncommitted records; any records appended after it are lost as
well, and no error or log line is emitted. Direct `StorageEngine.Append` callers and
`Engine.Enqueue` callers with a raised `MaxPayloadSize` are both exposed.

### Fix implemented

Apply the cap symmetrically on the write path before touching the WAL: reject
`headerSize + len(topic) + len(payload) > maxRecoverableRecordSize` with
`ErrPreconditionViolated` (or a dedicated error). If >10 MB records are intended to be
supported, make the recovery limit configurable and consistent with the writer so
recovery never truncates a frame the writer accepted.

---

## BUG-04 — `Config.MaxCapacity` is declared, defaulted, and ignored

**Severity:** Medium · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`bug_maxcapacity_ignored_test.go`](bug_maxcapacity_ignored_test.go), `TestBugMaxcapacityIgnored`
**Status:** Fixed

### Source evidence

`MaxCapacity` is declared as the backpressure quota and defaulted to 10000
([engine.go:21](engine.go#L21), [engine.go:32](engine.go#L32)). A repository-wide search
finds no read of `e.cfg.MaxCapacity` or `cfg.MaxCapacity` anywhere. `New` normalizes
`BatchSize`, `FlushInterval`, and `MaxPayloadSize` but has no `MaxCapacity` branch
([engine.go:81-89](engine.go#L81-L89)). Quota enforcement lives only in the storage
constructor argument, e.g. `MemoryStorageEngine` ([storage_memory.go:36-39](storage_memory.go#L36-L39)).

### Reproduction and observed result

The test uses a storage quota of 100, sets `cfg.MaxCapacity = 1`, and uses a sink that
always returns `ErrSinkUnavailable` so the first record cannot commit. The second
`Enqueue` should return `ErrSpoolFull`:

```text
--- FAIL: TestBugMaxcapacityIgnored (0.00s)
    bug_maxcapacity_ignored_test.go:43: second Enqueue: got <nil>, want ErrSpoolFull
    (Config.MaxCapacity=1 is ignored)
FAIL
FAIL	github.com/YohannHommet/walspool	0.003s
```

### Impact

A documented, defaulted public knob is a no-op. Operators tuning backpressure through
`Config.MaxCapacity` get no effect; the effective quota is the storage constructor value.
`ErrSpoolFull` and HTTP 503 therefore occur later than configured. Storage still bounds
growth, so this is not data loss or corruption, hence Medium.

### Fix implemented

`New` now lowers the built-in storage adapter's quota through the internal
`limitCapacity` method, so `Config.MaxCapacity` is enforced atomically inside `Append`
for both `MemoryStorageEngine` and `FileStorageEngine`. It never raises an explicitly
configured storage quota. For custom `StorageEngine` implementations without that
capability, `Enqueue` keeps a check-then-act fallback using `UncommittedCount`. The
sidecar now sets `spoolCfg.MaxCapacity = cfg.MaxRecords`, so its two quota knobs agree.

---

## BUG-05 — `Commit` accepts offsets beyond the log and advances the checkpoint past the tail

**Severity:** Medium · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`bug_commit_overadvance_test.go`](bug_commit_overadvance_test.go), `TestBugCommitOveradvance`
**Status:** Fixed

### Source evidence

`Commit` only compares the requested checkpoint with the previous checkpoint; it never
checks the log tail, then durably writes `checkpoint.meta` and sets
`f.checkpoint = offset+1` ([storage_file.go:681-691](storage_file.go#L681-L691)).
`UncommittedCount` clamps the resulting negative count to zero
([storage_file.go:750-763](storage_file.go#L750-L763)). `Append` uses the same count for
its capacity guard, so backpressure stops firing ([storage_file.go:550-553](storage_file.go#L550-L553)).
`ReadBatch` starts at the checkpoint and returns nothing when it exceeds the index
([storage_file.go:617-623](storage_file.go#L617-L623)). Recovery already treats a
checkpoint ahead of the WAL as invalid and clamps it
([storage_file.go:229-231](storage_file.go#L229-L231),
[storage_file.go:326-328](storage_file.go#L326-L328)), which confirms the invariant.
`MemoryStorageEngine.Commit` has the same unchecked advance
([storage_memory.go:69-83](storage_memory.go#L69-L83)).

### Reproduction and observed result

With three records at offsets 0–2, the test calls `Commit(99)` and checks the pending
count:

```text
--- FAIL: TestBugCommitOveradvance (0.04s)
    bug_commit_overadvance_test.go:51: Commit(99) accepted a non-existent offset and
    advanced the checkpoint past the log: Commit returned nil and UncommittedCount()=0;
    want a validation error or a clamped checkpoint with 3 pending records
FAIL
FAIL	github.com/YohannHommet/walspool	0.047s
```

### Impact

Records appended after an over-advanced commit receive offsets below the checkpoint and
are never returned by `ReadBatch` for the life of the process; delivery silently skips
them until a restart clamps the checkpoint. Because the uncommitted count goes negative,
`Append`'s `maxCapacity` guard no longer fires, defeating spool-full backpressure. The
persisted `checkpoint.meta` is inconsistent with the WAL and forces the recovery
warning/clamp path. This is reachable through the public `StorageEngine` port by a
caller that supplies a wrong offset.

### Fix implemented

Validate the offset against the log tail before any side effect:

```go
totalRecords := f.baseOffset + Offset(len(f.records))
if offset >= totalRecords {
    return fmt.Errorf("%w: commit offset %d is beyond the log (%d records)",
        ErrPreconditionViolated, offset, totalRecords)
}
```

Alternatively clamp `newCheckpoint` to `total`; stale commits at or below the current
checkpoint must remain idempotent no-ops. Apply the same validation to
`MemoryStorageEngine.Commit`.

---

## BUG-06 — `walspool monitor` parses metrics the sidecar never exports

**Severity:** Medium · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`cmd/sidecar/bug_monitor_metrics_test.go`](cmd/sidecar/bug_monitor_metrics_test.go), `TestBugMonitorMetrics`
**Status:** Fixed

### Source evidence

`monitor.go` parses `walspool_storage_disk_bytes`, `walspool_sink_flushes_total`,
`walspool_sink_errors_total`, and `walspool_sink_retries_total`
([cmd/walspool/monitor.go:52-71](cmd/walspool/monitor.go#L52-L71)) and renders them as
"Size on disk", "Sink Flushes", "Errors", and "Retries"
([cmd/walspool/monitor.go:293](cmd/walspool/monitor.go#L293),
[cmd/walspool/monitor.go:300](cmd/walspool/monitor.go#L300)).

The sidecar's `/metrics` handler serves `SidecarMetrics.FormatPrometheus`
([cmd/sidecar/main.go:533-542](cmd/sidecar/main.go#L533-L542)). That exposition emits
only ingested/delivered counters, active SSE, uncommitted records, dropped events, and
ring capacity/count ([cmd/sidecar/main.go:182-258](cmd/sidecar/main.go#L182-L258)).
`SidecarMetrics` tracks only ingested/delivered maps
([cmd/sidecar/main.go:131-158](cmd/sidecar/main.go#L131-L158)); no sink flush/error/retry
counters or disk-size gauge exist. `monitor_test.go` only exercises the parser against
synthetic text, so the producer/consumer mismatch is untested.

### Reproduction and observed result

The test calls the real producer and checks for the four names:

```text
--- FAIL: TestBugMonitorMetrics (0.00s)
    bug_monitor_metrics_test.go:21: sidecar /metrics output is missing
    "walspool_storage_disk_bytes", which cmd/walspool/monitor.go parses and renders
    bug_monitor_metrics_test.go:21: sidecar /metrics output is missing
    "walspool_sink_flushes_total", which cmd/walspool/monitor.go parses and renders
    bug_monitor_metrics_test.go:21: sidecar /metrics output is missing
    "walspool_sink_errors_total", which cmd/walspool/monitor.go parses and renders
    bug_monitor_metrics_test.go:21: sidecar /metrics output is missing
    "walspool_sink_retries_total", which cmd/walspool/monitor.go parses and renders
FAIL
FAIL	github.com/YohannHommet/walspool/cmd/sidecar	0.005s
```

### Impact

Operators see silently incorrect zeros for disk WAL size and sink health (flushes,
errors, retries). Disk growth and outbound sink failures are invisible in the TUI,
defeating its observability purpose. No data corruption or crash, hence Medium.

### Fix implemented

`SidecarMetrics` now tracks sink flush, error, and retry-triggering counters, and
`FormatPrometheus` emits all four metrics the monitor parses. `FileStorageEngine.DiskBytes`
supplies the disk-size gauge through an optional interface, and `HTTPSink` increments the
sink counters on delivery attempts, failures, and retry-eligible failures. The monitor's
existing parser now receives real values instead of permanent zeros.

---

## BUG-07 — CLI `--format raw` bypasses terminal sanitization

**Severity:** Medium · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`cmd/walspool/bug_cli_raw_sanitize_test.go`](cmd/walspool/bug_cli_raw_sanitize_test.go), `TestBugCliRawSanitize`
**Status:** Fixed

### Source evidence

The `raw` formatter writes the payload verbatim:

```go
case "raw":
    _, err := fmt.Fprintf(w, "%s\n", string(ev.Payload))
    return err
```

See [cmd/walspool/stream.go:237-239](cmd/walspool/stream.go#L237-L239). The `pretty`
formatter does sanitize the payload ([cmd/walspool/stream.go:275](cmd/walspool/stream.go#L275)).
`SanitizeTerminal` is the intended neutralizer for ANSI/C0/C1/BiDi sequences
([cmd/walspool/security.go:24](cmd/walspool/security.go#L24)). ADR-002 requires filtering
before display
([ADR-002-standalone-cli-tui.md:66-73](docs/architecture/adr/ADR-002-standalone-cli-tui.md#L66-L73)),
and the CLI workspace guideline (untracked in this repository snapshot) mandates
sanitization of every payload rendered to stdout/stderr.

### Reproduction and observed result

The test passes an event whose payload is `\x1b[31mRED\x1b[0m` through the raw formatter:

```text
--- FAIL: TestBugCliRawSanitize (0.00s)
    bug_cli_raw_sanitize_test.go:29: raw output contains a raw ESC (0x1b) byte:
    "\x1b[31mRED\x1b[0m\n"; untrusted payload must be sanitized via SanitizeTerminal
    before writing to stdout
FAIL
FAIL	github.com/YohannHommet/walspool/cmd/walspool	0.026s
```

### Impact

Attacker-controlled log payloads can inject ANSI/OSC/C1 terminal control sequences when
an operator runs `walspool stream --format raw`. Effects include cursor movement, screen
clearing or rewriting, title changes, spoofed log lines, and potentially OSC 52 clipboard
hijacking. This is a CWE-117 / terminal escape injection issue and contradicts the
sanitized `pretty` path. Rated Medium because `raw` is an explicit opt-in format and is
often piped, although piped output can still reach a terminal.

### Fix implemented

Sanitize the payload before writing in the raw branch:

```go
case "raw":
    _, err := fmt.Fprintf(w, "%s\n", SanitizeTerminal(string(ev.Payload), false))
    return err
```

Use `singleLine=false` to preserve payload newlines, or `true` if raw output must remain
one line per event. Add the malicious-payload case to `stream_test.go`.

---

## BUG-08 — Batch metadata enrichment can turn a client error into HTTP 500

**Severity:** Medium · **Confidence:** High · **Verdict:** Confirmed

**Test:** [`cmd/sidecar/bug_batch_enrich_500_test.go`](cmd/sidecar/bug_batch_enrich_500_test.go), `TestBugBatchEnrich500`
**Status:** Fixed

### Source evidence

Phase 1 validates only the raw `len(item.Payload)` against `MaxPayloadSize` and returns
`400 invalid_batch_item` before enrichment
([cmd/sidecar/main.go:1028-1036](cmd/sidecar/main.go#L1028-L1036)). Phase 1 then calls
`enrichPayloadMetadata`, which re-marshals a JSON object and appends missing metadata
keys, growing the payload ([cmd/sidecar/main.go:1039](cmd/sidecar/main.go#L1039)). The
engine re-checks the enriched payload and returns `ErrPreconditionViolated`
([engine.go:132-133](engine.go#L132-L133)). Phase 2 only special-cases `ErrSpoolFull`;
every other enqueue error becomes `500 internal_error`
([cmd/sidecar/main.go:1053-1077](cmd/sidecar/main.go#L1053-L1077), line 1070). The
single-item and NDJSON handlers already map `ErrPreconditionViolated` to HTTP 400
([cmd/sidecar/main.go:648-652](cmd/sidecar/main.go#L648-L652),
[cmd/sidecar/main.go:861-865](cmd/sidecar/main.go#L861-L865)), confirming this is a
client fault.

### Reproduction and observed result

The test builds a JSON object 10 bytes below the 1 MiB limit, sends it through
`POST /v1/enqueue/batch` with `Service: "svc"`, and asserts HTTP 400. Enrichment adds
`,"service":"svc"` (16 bytes), producing a 1,048,582-byte payload:

```text
--- FAIL: TestBugBatchEnrich500 (0.03s)
    bug_batch_enrich_500_test.go:63: expected 400 Bad Request for enriched-oversized
    batch item, got 500: {"error":"internal_error","message":"walspool: precondition
    violated: payload size 1048582 exceeds limit of 1048576","enqueued":0,"total":1}
FAIL
FAIL	github.com/YohannHommet/walspool/cmd/sidecar	0.037s
```

### Impact

Clients receive a 500 for a request they can fix and may retry indefinitely. If an
earlier batch item is valid, it is enqueued before the enriched-oversized item fails in
Phase 2, producing partial WAL ingestion alongside the 500. The 1 MiB per-record limit
is effectively enforced only after enrichment, so the endpoint's client-error surface is
inconsistent.

### Fix implemented

Phase 1 now re-checks the enriched payload and returns HTTP 400 `invalid_batch_item`
with the failing index before any Phase 2 spooling, preserving all-or-nothing
validation. Phase 2 also maps `ErrPreconditionViolated` from `Enqueue` to HTTP 400 as a
defensive fallback.

---

## Rejected after verification

These hypotheses were investigated with the same worktree/test process but did **not**
survive source and contract review. They are recorded so the initial analysis is not
mistaken for confirmed defects.

### R-01 — Single-file WAL growth is documented legacy behavior

The hypothesis was that `FileStorageEngine` never reclaims committed records in
single-file mode. The observation is real: after committing 200 records,
`len(engine.records)` is 200 and `active.wal` retains all bytes. However, this is the
documented v1.x contract, not a leak:

- `MaxSegmentSize <= 0` explicitly disables rotation and retains the historical
  single-file mode ([storage_file.go:46-48](storage_file.go#L46-L48),
  [storage_file.go:52-57](storage_file.go#L52-L57)).
- `Commit` only prunes when `MaxSegmentSize > 0`
  ([storage_file.go:743-745](storage_file.go#L743-L745)).
- ADR-001 preserves the single-file mode for backward compatibility and explicitly
  rejects in-place single-file compaction for v1.1
  ([ADR-001:25](docs/architecture/adr/ADR-001-multi-segment-rotation.md#L25),
  [ADR-001:84-85](docs/architecture/adr/ADR-001-multi-segment-rotation.md#L84-L85),
  [ADR-001:99](docs/architecture/adr/ADR-001-multi-segment-rotation.md#L99)).
- The opt-in segmented mode does reclaim: the verifier's test showed 40 records in
  256-byte segments collapsing to 5 retained index entries and one active segment.

**Operational gap (OPS-01, High):** the shipped sidecar uses
`NewFileStorageEngine` with the legacy default and exposes no `MaxSegmentSize` option
([cmd/sidecar/main.go:1510](cmd/sidecar/main.go#L1510)), so its WAL and index are not
auto-pruned. This is a configuration/product gap, not an engine contract defect.
Recommended follow-up: expose `-max-segment-size`/`WALSPOOL_MAX_SEGMENT_SIZE` and choose
a bounded production default, while preserving the library's backward-compatible default
of zero. The single-file reclamation roadmap item is issue #20
([README.md:564](README.md#L564)).

### R-02 — Lazy CRC validation is the documented storage contract

The hypothesis was that recovery should reject a mid-WAL CRC mismatch at open. The
verifier found that indexing a well-framed but CRC-damaged frame is deliberate:
`ReadBatch` returns `*CorruptRecordError` for exactly that offset and the dispatcher
skips it ([ports.go:43-48](ports.go#L43-L48), [record.go:79-84](record.go#L79-L84),
[engine.go:287-297](engine.go#L287-L297)). Three existing regression tests pin this
behavior: `TestDiskWAL_CorruptRecordSkipped_DeliveryResumes`,
`TestFileStorage_ReadBatch_IsolatesCorruptRecord`, and `TestFileStorage_CRC32Rejection`.
Truncating at the first CRC mismatch would delete intact records behind it and remap
offsets against `checkpoint.meta`.

**Documentation correction (DOC-01, Low, fixed):** the architecture document and README
claimed CRC32 detection during startup recovery
([ARCHITECTURE.md:142](docs/architecture/ARCHITECTURE.md#L142),
[README.md:492](README.md#L492)). They now state that startup recovery validates frame
framing and truncates torn tails, while CRC32 is verified by `ReadBatch` and reported as
a per-offset `*CorruptRecordError`. No production-code change was required.

---

## Scope notes

- Baseline `go vet ./...` and `go test ./...` passed at `d9a4eeb`; the eight regression
  tests fail there and pass on this branch.
- Verification on this branch: `go vet ./...` and `go test ./...` pass. A local
  `-race` run could not execute because the environment lacks libc headers; CI runs
  `go test -race` on Linux and macOS.
- The fixes are intentionally minimal: per-append kernel writes trade some batching
  throughput for crash-safe acknowledgements; the `fsync` policy remains configurable.
- Initial-analysis items that are documentation/product drift rather than executable
  defects — OpenAPI route coverage, benchmark-number discrepancies, hardcoded paths in
  `scripts/verify.sh`, and release archive contents — are not included here because they
  were not verified with a failing test. They remain worth a separate documentation/CI
  cleanup pass.
