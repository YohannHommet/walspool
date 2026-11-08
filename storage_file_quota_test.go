package walspool

import (
	"bufio"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// quotaRecord builds a deterministic record and returns the serialized WAL frame
// whose byte length is the unit the MaxUncommittedBytes quota counts.
func quotaRecord(t *testing.T, id uint64, topic string, payloadSize int) (Record, []byte) {
	t.Helper()
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + (int(id)+i)%26)
	}
	rec := Record{ID: id, Timestamp: time.Unix(0, int64(id)), Topic: topic, Payload: payload}
	data, err := rec.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal record %d failed: %v", id, err)
	}
	return rec, data
}

// newQuotaEngine opens a deterministic engine: SyncBatchCommit has no background
// flusher, so byte accounting is asserted against exact values with no sleeps.
func newQuotaEngine(t *testing.T, dir string, maxCapacity int, maxUncommittedBytes int64) *FileStorageEngine {
	t.Helper()
	opts := DefaultFileStorageOptions()
	opts.SyncPolicy = SyncBatchCommit
	opts.MaxUncommittedBytes = maxUncommittedBytes
	eng, err := NewFileStorageEngineWithOptions(dir, maxCapacity, opts)
	if err != nil {
		t.Fatalf("init engine failed: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

// Step 1: an append whose serialized frame would exceed MaxUncommittedBytes must be
// rejected with ErrSpoolFull before any write, and the accepted prefix must remain intact.
func TestFileStorage_MaxUncommittedBytesRejectsAppend(t *testing.T) {
	dir := t.TempDir()
	recA, dataA := quotaRecord(t, 1, "quota.topic", 16)
	recB, dataB := quotaRecord(t, 2, "quota.topic", 64)
	if len(dataB) <= len(dataA) {
		t.Fatalf("test premise broken: frame B (%d) must exceed frame A (%d)", len(dataB), len(dataA))
	}

	eng := newQuotaEngine(t, dir, 100, int64(len(dataA)))

	off, err := eng.Append(recA)
	if err != nil {
		t.Fatalf("append A under exact quota failed: %v", err)
	}
	if off != 0 {
		t.Fatalf("append A returned offset %d, want 0", off)
	}

	if _, err := eng.Append(recB); !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("append B over quota: got %v, want ErrSpoolFull", err)
	}

	if got := eng.UncommittedBytes(); got != int64(len(dataA)) {
		t.Fatalf("UncommittedBytes after rejected append = %d, want %d", got, len(dataA))
	}
	count, err := eng.UncommittedCount()
	if err != nil {
		t.Fatalf("UncommittedCount failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("UncommittedCount after rejected append = %d, want 1", count)
	}
}

// Step 2: every accepted append must add exactly its serialized frame length, keeping
// the counter identical to the cumulative sum of indexed frame lengths.
func TestFileStorage_MaxUncommittedBytesAccounting(t *testing.T) {
	dir := t.TempDir()
	eng := newQuotaEngine(t, dir, 100, 0)

	var want int64
	for i := 0; i < 3; i++ {
		rec, data := quotaRecord(t, uint64(i+1), "accounting", 8+i*8)
		want += int64(len(data))
		off, err := eng.Append(rec)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if off != Offset(i) {
			t.Fatalf("append %d returned offset %d, want %d", i, off, i)
		}
		if got := eng.UncommittedBytes(); got != want {
			t.Fatalf("after append %d: UncommittedBytes = %d, want %d", i, got, want)
		}
	}
}

// Step 7: zero disables the byte quota (legacy library behavior), including the value
// produced by DefaultFileStorageOptions.
func TestFileStorage_MaxUncommittedBytesZeroIsUnlimited(t *testing.T) {
	if got := DefaultFileStorageOptions().MaxUncommittedBytes; got != 0 {
		t.Fatalf("DefaultFileStorageOptions().MaxUncommittedBytes = %d, want 0 (legacy unlimited)", got)
	}

	dir := t.TempDir()
	eng := newQuotaEngine(t, dir, 1000, 0)

	var want int64
	for i := 0; i < 50; i++ {
		rec, data := quotaRecord(t, uint64(i+1), "unlimited", 1024)
		want += int64(len(data))
		if _, err := eng.Append(rec); err != nil {
			t.Fatalf("append %d rejected under zero (unlimited) quota: %v", i, err)
		}
	}
	if got := eng.UncommittedBytes(); got != want {
		t.Fatalf("UncommittedBytes under zero quota = %d, want %d", got, want)
	}
}

// Step 8: the quota unit is the serialized frame (header + topic + payload), never the
// payload length. A payload-only implementation would accept at quota-1 and report 10.
func TestFileStorage_MaxUncommittedBytesIncludesFrameOverhead(t *testing.T) {
	const topic = "orders"
	rec, data := quotaRecord(t, 1, topic, 10)
	wantFrame := headerSize + len(topic) + 10
	if len(data) != wantFrame {
		t.Fatalf("test premise broken: frame is %d bytes, want %d", len(data), wantFrame)
	}

	rejectDir := t.TempDir()
	rejector := newQuotaEngine(t, rejectDir, 100, int64(wantFrame-1))
	if _, err := rejector.Append(rec); !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("quota %d (frame is %d): got %v, want ErrSpoolFull", wantFrame-1, wantFrame, err)
	}
	if got := rejector.UncommittedBytes(); got != 0 {
		t.Fatalf("UncommittedBytes after rejected append = %d, want 0", got)
	}

	acceptDir := t.TempDir()
	acceptor := newQuotaEngine(t, acceptDir, 100, int64(wantFrame))
	if _, err := acceptor.Append(rec); err != nil {
		t.Fatalf("quota %d (frame is %d) must accept the frame: %v", wantFrame, wantFrame, err)
	}
	if got := acceptor.UncommittedBytes(); got != int64(wantFrame) {
		t.Fatalf("UncommittedBytes = %d, want frame length %d", got, wantFrame)
	}
	if got := acceptor.UncommittedBytes(); got == 10 {
		t.Fatalf("UncommittedBytes reported payload bytes (10) instead of frame bytes (%d)", wantFrame)
	}
}

// Step 3: committing a prefix frees exactly the committed frame bytes, and the freed
// room immediately admits a new append.
func TestFileStorage_MaxUncommittedBytesFreesOnCommit(t *testing.T) {
	dir := t.TempDir()
	recA, dataA := quotaRecord(t, 1, "commit", 8)
	recB, dataB := quotaRecord(t, 2, "commit", 16)
	recC, dataC := quotaRecord(t, 3, "commit", 24)
	recD, dataD := quotaRecord(t, 4, "commit", 32)
	quota := int64(len(dataA) + len(dataB) + len(dataC) + len(dataD))
	eng := newQuotaEngine(t, dir, 100, quota)

	for i, rec := range []Record{recA, recB, recC} {
		if _, err := eng.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	if err := eng.Commit(1); err != nil {
		t.Fatalf("Commit(1) failed: %v", err)
	}
	if got, want := eng.UncommittedBytes(), int64(len(dataC)); got != want {
		t.Fatalf("UncommittedBytes after Commit(1) = %d, want %d", got, want)
	}
	if count, err := eng.UncommittedCount(); err != nil || count != 1 {
		t.Fatalf("UncommittedCount after Commit(1) = %d (err %v), want 1", count, err)
	}

	if _, err := eng.Append(recD); err != nil {
		t.Fatalf("append D after freed quota failed: %v", err)
	}
	if got, want := eng.UncommittedBytes(), int64(len(dataC)+len(dataD)); got != want {
		t.Fatalf("UncommittedBytes after append D = %d, want %d", got, want)
	}

	if err := eng.Commit(3); err != nil {
		t.Fatalf("Commit(3) failed: %v", err)
	}
	if got := eng.UncommittedBytes(); got != 0 {
		t.Fatalf("UncommittedBytes after full commit = %d, want 0", got)
	}
}

// Step 4: an interior commit frees exactly the committed prefix, not zero and not every
// frame, and repeating the same commit stays a no-op.
func TestFileStorage_MaxUncommittedBytesPartialCommit(t *testing.T) {
	dir := t.TempDir()
	recA, dataA := quotaRecord(t, 1, "partial", 8)
	recB, dataB := quotaRecord(t, 2, "partial", 16)
	recC, dataC := quotaRecord(t, 3, "partial", 24)
	recD, dataD := quotaRecord(t, 4, "partial", 32)
	quota := int64(len(dataA) + len(dataB) + len(dataC) + len(dataD))
	eng := newQuotaEngine(t, dir, 100, quota)

	for i, rec := range []Record{recA, recB, recC, recD} {
		if _, err := eng.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	if err := eng.Commit(1); err != nil {
		t.Fatalf("Commit(1) failed: %v", err)
	}
	want := int64(len(dataC) + len(dataD))
	if got := eng.UncommittedBytes(); got != want {
		t.Fatalf("interior Commit(1): UncommittedBytes = %d, want %d (lenC+lenD)", got, want)
	}
	if count, err := eng.UncommittedCount(); err != nil || count != 2 {
		t.Fatalf("UncommittedCount = %d (err %v), want 2", count, err)
	}

	if err := eng.Commit(1); err != nil {
		t.Fatalf("repeated Commit(1) failed: %v", err)
	}
	if got := eng.UncommittedBytes(); got != want {
		t.Fatalf("repeated Commit(1): UncommittedBytes = %d, want unchanged %d", got, want)
	}
}

// Step 5: stale or repeated commits must not subtract twice and the counter must never
// underflow; an out-of-range commit is rejected without touching the counter.
func TestFileStorage_MaxUncommittedBytesCommitStaleNoop(t *testing.T) {
	dir := t.TempDir()
	recA, _ := quotaRecord(t, 1, "stale", 8)
	recB, _ := quotaRecord(t, 2, "stale", 16)
	eng := newQuotaEngine(t, dir, 100, 0)

	for i, rec := range []Record{recA, recB} {
		if _, err := eng.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	if err := eng.Commit(1); err != nil {
		t.Fatalf("Commit(1) failed: %v", err)
	}
	if got := eng.UncommittedBytes(); got != 0 {
		t.Fatalf("UncommittedBytes after Commit(1) = %d, want 0", got)
	}

	if err := eng.Commit(0); err != nil {
		t.Fatalf("stale Commit(0) must be a no-op, got error: %v", err)
	}
	if err := eng.Commit(1); err != nil {
		t.Fatalf("stale Commit(1) must be a no-op, got error: %v", err)
	}
	if got := eng.UncommittedBytes(); got != 0 {
		t.Fatalf("UncommittedBytes after stale commits = %d, want 0", got)
	}

	if err := eng.Commit(2); !errors.Is(err, ErrPreconditionViolated) {
		t.Fatalf("Commit(2) past the log: got %v, want ErrPreconditionViolated", err)
	}
	if got := eng.UncommittedBytes(); got != 0 {
		t.Fatalf("UncommittedBytes after rejected commit = %d, want 0", got)
	}
	if got := eng.UncommittedBytes(); got < 0 {
		t.Fatalf("UncommittedBytes underflowed to %d", got)
	}
}

// Step 6: recovery must reconstruct the counter from the recovered index, starting at the
// clamped checkpoint. Torn tail bytes are truncated before indexing, so they never count.
func TestFileStorage_MaxUncommittedBytesRecovery(t *testing.T) {
	recoverOpts := func(maxUncommittedBytes int64) FileStorageOptions {
		opts := DefaultFileStorageOptions()
		opts.SyncPolicy = SyncBatchCommit
		opts.MaxUncommittedBytes = maxUncommittedBytes
		return opts
	}

	t.Run("CommittedAndUncommitted", func(t *testing.T) {
		dir := t.TempDir()
		recA, dataA := quotaRecord(t, 1, "recover", 8)
		recB, dataB := quotaRecord(t, 2, "recover", 16)
		recC, dataC := quotaRecord(t, 3, "recover", 24)
		recD, dataD := quotaRecord(t, 4, "recover", 64)
		recE, dataE := quotaRecord(t, 5, "recover", 8)
		// The quota must admit the initial uncommitted prefix and D after reopen, yet
		// reject E: max(A+B+C, C+D) <= quota < C+D+E.
		quota := int64(len(dataC) + len(dataD) + len(dataE) - 1)
		if initial := int64(len(dataA) + len(dataB) + len(dataC)); initial > quota {
			t.Fatalf("test premise broken: initial prefix %d exceeds quota %d", initial, quota)
		}
		opts := recoverOpts(quota)

		eng, err := NewFileStorageEngineWithOptions(dir, 100, opts)
		if err != nil {
			t.Fatalf("init failed: %v", err)
		}
		for i, rec := range []Record{recA, recB, recC} {
			if _, err := eng.Append(rec); err != nil {
				t.Fatalf("append %d failed: %v", i, err)
			}
		}
		if err := eng.Commit(1); err != nil {
			t.Fatalf("Commit(1) failed: %v", err)
		}
		if err := eng.Close(); err != nil {
			t.Fatalf("close failed: %v", err)
		}

		reopened, err := NewFileStorageEngineWithOptions(dir, 100, opts)
		if err != nil {
			t.Fatalf("reopen failed: %v", err)
		}
		defer reopened.Close()

		if got := reopened.UncommittedBytes(); got != int64(len(dataC)) {
			t.Fatalf("UncommittedBytes after reopen = %d, want %d (lenC)", got, len(dataC))
		}
		if count, err := reopened.UncommittedCount(); err != nil || count != 1 {
			t.Fatalf("UncommittedCount after reopen = %d (err %v), want 1", count, err)
		}

		if _, err := reopened.Append(recD); err != nil {
			t.Fatalf("append D within recovered quota failed: %v", err)
		}
		if got, want := reopened.UncommittedBytes(), int64(len(dataC)+len(dataD)); got != want {
			t.Fatalf("UncommittedBytes after append D = %d, want %d", got, want)
		}
		if _, err := reopened.Append(recE); !errors.Is(err, ErrSpoolFull) {
			t.Fatalf("append E over recovered quota: got %v, want ErrSpoolFull", err)
		}
	})

	t.Run("TornTailNotCounted", func(t *testing.T) {
		dir := t.TempDir()
		recA, dataA := quotaRecord(t, 1, "torn", 8)
		recB, dataB := quotaRecord(t, 2, "torn", 16)
		opts := recoverOpts(0)

		eng, err := NewFileStorageEngineWithOptions(dir, 100, opts)
		if err != nil {
			t.Fatalf("init failed: %v", err)
		}
		for i, rec := range []Record{recA, recB} {
			if _, err := eng.Append(rec); err != nil {
				t.Fatalf("append %d failed: %v", i, err)
			}
		}
		if err := eng.Close(); err != nil {
			t.Fatalf("close failed: %v", err)
		}

		walPath := filepath.Join(dir, "active.wal")
		raw, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			t.Fatalf("open wal for torn-tail injection failed: %v", err)
		}
		if _, err := raw.Write([]byte{0xde, 0xad, 0xbe}); err != nil {
			_ = raw.Close()
			t.Fatalf("write torn tail failed: %v", err)
		}
		if err := raw.Close(); err != nil {
			t.Fatalf("close wal after torn-tail injection failed: %v", err)
		}

		reopened, err := NewFileStorageEngineWithOptions(dir, 100, opts)
		if err != nil {
			t.Fatalf("reopen failed: %v", err)
		}
		defer reopened.Close()

		want := int64(len(dataA) + len(dataB))
		if got := reopened.UncommittedBytes(); got != want {
			t.Fatalf("UncommittedBytes after torn-tail reopen = %d, want %d (garbage must not count)", got, want)
		}
		if count, err := reopened.UncommittedCount(); err != nil || count != 2 {
			t.Fatalf("UncommittedCount after torn-tail reopen = %d (err %v), want 2", count, err)
		}
	})
}

// Step 9: rollbackTo must only subtract dropped uncommitted frames. A failed write after an
// accepted prefix keeps the prefix bytes; a direct rollback drops exactly the truncated tail
// and never goes negative.
func TestFileStorage_MaxUncommittedBytesRollbackOnWriteFailure(t *testing.T) {
	t.Run("WriteFailureKeepsAcceptedCounter", func(t *testing.T) {
		dir := t.TempDir()
		recA, dataA := quotaRecord(t, 1, "rollback", 16)
		recB, dataB := quotaRecord(t, 2, "rollback", 32)
		sizeA, sizeB := len(dataA), len(dataB)

		opts := FileStorageOptions{BufferSize: sizeA + sizeB + 8, SyncPolicy: SyncBatchCommit}
		eng, err := NewFileStorageEngineWithOptions(dir, 100, opts)
		if err != nil {
			t.Fatalf("init failed: %v", err)
		}
		defer eng.Close()

		// Accept A fully, then let B's flush fail with ENOSPC.
		faulty := &faultyWriter{target: eng.walFile, budget: sizeA + sizeB/2}
		eng.writer = bufio.NewWriterSize(faulty, sizeA+sizeB+8)

		if _, err := eng.Append(recA); err != nil {
			t.Fatalf("append A failed: %v", err)
		}
		if _, err := eng.Append(recB); !errors.Is(err, ErrStorageUnavailable) {
			t.Fatalf("append B: got %v, want ErrStorageUnavailable", err)
		}

		if got := eng.UncommittedBytes(); got != int64(sizeA) {
			t.Fatalf("UncommittedBytes after failed append = %d, want %d (only A accepted)", got, sizeA)
		}
		if len(eng.records) != 1 {
			t.Fatalf("expected 1 indexed record after rollback, got %d", len(eng.records))
		}
	})

	t.Run("RollbackDropsOnlyUncommittedFrames", func(t *testing.T) {
		dir := t.TempDir()
		recA, dataA := quotaRecord(t, 1, "rollback", 8)
		recB, dataB := quotaRecord(t, 2, "rollback", 16)
		recC, dataC := quotaRecord(t, 3, "rollback", 24)
		eng := newQuotaEngine(t, dir, 100, 0)

		for i, rec := range []Record{recA, recB, recC} {
			if _, err := eng.Append(rec); err != nil {
				t.Fatalf("append %d failed: %v", i, err)
			}
		}
		wantAB := int64(len(dataA) + len(dataB))

		// Drop C: truncate at C's start.
		eng.mu.Lock()
		eng.rollbackTo(eng.records[2].Pos)
		eng.mu.Unlock()
		if got := eng.UncommittedBytes(); got != wantAB {
			t.Fatalf("UncommittedBytes after dropping C = %d, want %d (lenA+lenB)", got, wantAB)
		}

		// Drop A and B too: every uncommitted frame is freed, never negative.
		eng.mu.Lock()
		eng.rollbackTo(eng.records[0].Pos)
		eng.mu.Unlock()
		if got := eng.UncommittedBytes(); got != 0 {
			t.Fatalf("UncommittedBytes after full rollback = %d, want 0", got)
		}
		if got := eng.UncommittedBytes(); got < 0 {
			t.Fatalf("UncommittedBytes underflowed to %d", got)
		}
		_ = dataC
	})
}

// B1 (review): the deployed sidecar path uses segment rotation plus recoverSegmented.
// This locks exact byte accounting across rotation, interior commit, prune/baseOffset
// movement, close/reopen, and quota saturation after the reopen.
func TestFileStorage_MaxUncommittedBytesSegmentedLifecycle(t *testing.T) {
	dir := t.TempDir()
	const segmentSize = 512
	const frameSize = 166 // 29-byte header + 9-byte topic "segmented" + 128-byte payload
	quota := int64(5 * frameSize)

	opts := DefaultFileStorageOptions()
	opts.SyncPolicy = SyncBatchCommit
	opts.MaxSegmentSize = segmentSize
	opts.MaxUncommittedBytes = quota

	eng, err := NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	// (a) Append enough frames to force a rotation and assert the exact cumulative sum.
	frames := make([][]byte, 0, 5)
	var want int64
	for i := 0; i < 5; i++ {
		rec, data := quotaRecord(t, uint64(i+1), "segmented", 128)
		if len(data) != frameSize {
			t.Fatalf("test premise broken: frame %d is %d bytes, want %d", i, len(data), frameSize)
		}
		frames = append(frames, data)
		want += int64(len(data))
		off, err := eng.Append(rec)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if off != Offset(i) {
			t.Fatalf("append %d returned offset %d, want %d", i, off, i)
		}
		if got := eng.UncommittedBytes(); got != want {
			t.Fatalf("after append %d: UncommittedBytes = %d, want %d", i, got, want)
		}
	}
	segs, err := filepath.Glob(filepath.Join(dir, "segment-*.wal"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	if len(segs) < 2 {
		t.Fatalf("expected at least one rotation, got segment files %v", segs)
	}

	// (b) Commit an interior offset covering the whole first segment: exact freed prefix,
	// then the committed segment must be pruned and baseOffset advanced.
	freed := int64(len(frames[0]) + len(frames[1]) + len(frames[2]))
	if err := eng.Commit(2); err != nil {
		t.Fatalf("Commit(2) failed: %v", err)
	}
	want -= freed
	if got := eng.UncommittedBytes(); got != want {
		t.Fatalf("after interior Commit(2): UncommittedBytes = %d, want %d", got, want)
	}
	if eng.baseOffset != 3 {
		t.Fatalf("after prune: baseOffset = %d, want 3", eng.baseOffset)
	}
	if len(eng.records) != 2 {
		t.Fatalf("after prune: %d indexed records, want 2", len(eng.records))
	}
	for _, entry := range eng.records {
		if entry.Offset < 3 {
			t.Fatalf("pruned record still indexed at offset %d", entry.Offset)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "segment-000000000000.wal")); !os.IsNotExist(err) {
		t.Fatalf("fully committed first segment must be pruned, stat error = %v", err)
	}

	// (c) Close and reopen with the same options: recoverSegmented must rebuild the counter.
	if err := eng.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	reopened, err := NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("segmented reopen failed: %v", err)
	}
	defer reopened.Close()

	if got := reopened.UncommittedBytes(); got != want {
		t.Fatalf("after segmented reopen: UncommittedBytes = %d, want %d", got, want)
	}
	if count, err := reopened.UncommittedCount(); err != nil || count != 2 {
		t.Fatalf("after segmented reopen: UncommittedCount = %d (err %v), want 2", count, err)
	}

	// (d) Append after the reopen until saturation, then assert the byte guard, not the
	// count guard, is what rejects; an interior commit must free room again.
	for i := 0; i < 3; i++ {
		rec, data := quotaRecord(t, uint64(10+i), "segmented", 128)
		want += int64(len(data))
		if _, err := reopened.Append(rec); err != nil {
			t.Fatalf("append %d after reopen failed: %v", i, err)
		}
		if got := reopened.UncommittedBytes(); got != want {
			t.Fatalf("after reopen append %d: UncommittedBytes = %d, want %d", i, got, want)
		}
	}
	if want != quota {
		t.Fatalf("test premise broken: accounted %d bytes, want saturation at quota %d", want, quota)
	}
	saturated := want
	saturatedRec, _ := quotaRecord(t, 99, "segmented", 128)
	if _, err := reopened.Append(saturatedRec); !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("append at saturation: got %v, want ErrSpoolFull", err)
	}
	if got := reopened.UncommittedBytes(); got != saturated {
		t.Fatalf("UncommittedBytes after rejected append = %d, want %d", got, saturated)
	}
	if count, err := reopened.UncommittedCount(); err != nil || count >= 1000 {
		t.Fatalf("UncommittedCount = %d (err %v): the byte guard, not the count guard, must trigger", count, err)
	}

	freed = int64(len(frames[3]) + len(frames[4]))
	if err := reopened.Commit(4); err != nil {
		t.Fatalf("Commit(4) after reopen failed: %v", err)
	}
	want -= freed
	if got := reopened.UncommittedBytes(); got != want {
		t.Fatalf("after Commit(4): UncommittedBytes = %d, want %d", got, want)
	}
	rec, data := quotaRecord(t, 20, "segmented", 128)
	want += int64(len(data))
	if _, err := reopened.Append(rec); err != nil {
		t.Fatalf("append after freed quota failed: %v", err)
	}
	if got := reopened.UncommittedBytes(); got != want {
		t.Fatalf("after final append: UncommittedBytes = %d, want %d", got, want)
	}
}

// LOW (review): the quota guard must not wrap near MaxInt64. The subtraction form keeps
// the admission decision exact for any positive quota the sidecar accepts.
func TestFileStorage_MaxUncommittedBytesNearMaxInt64(t *testing.T) {
	dir := t.TempDir()
	eng := newQuotaEngine(t, dir, 100, math.MaxInt64)
	rec, data := quotaRecord(t, 1, "overflow", 16)
	frame := int64(len(data))
	if frame < 2 {
		t.Fatalf("test premise broken: frame is %d bytes", frame)
	}

	// Put the counter so that counter+frame overflows int64: the addition-form guard would
	// wrap negative and wrongly accept the frame.
	seed := int64(math.MaxInt64) - frame/2
	eng.mu.Lock()
	eng.uncommittedBytes = seed
	eng.mu.Unlock()

	if _, err := eng.Append(rec); !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("near-MaxInt64 quota: got %v, want ErrSpoolFull (addition-form guard wraps)", err)
	}
	if got := eng.UncommittedBytes(); got != seed {
		t.Fatalf("UncommittedBytes after rejected append = %d, want unchanged %d", got, seed)
	}
}
