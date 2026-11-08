package walspool

import (
	"testing"
	"time"
)

// TestBugCommitOveradvance verifies that Commit validates the target offset
// against the log tail. Committing an offset that does not name an existing
// record must either be rejected or clamp the checkpoint to the record count;
// it must never advance the checkpoint past the log, because then records
// appended afterwards receive offsets below the checkpoint, are never returned
// by ReadBatch, and are silently skipped by delivery.
func TestBugCommitOveradvance(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewFileStorageEngineWithOptions(dir, 100, FileStorageOptions{
		BufferSize: 4096,
		SyncPolicy: SyncBatchCommit,
	})
	if err != nil {
		t.Fatalf("failed to init engine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	fixedTime := time.Unix(0, 1_700_000_000_000_000_000)
	for i := 0; i < 3; i++ {
		if _, err := engine.Append(Record{
			ID:        uint64(i + 1),
			Timestamp: fixedTime,
			Topic:     "audit",
			Payload:   []byte("payload"),
		}); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Offset 99 names no record: the log only holds offsets 0, 1 and 2.
	commitErr := engine.Commit(99)

	pending, countErr := engine.UncommittedCount()
	if countErr != nil {
		t.Fatalf("UncommittedCount failed after Commit(99): %v", countErr)
	}

	// Contract: either Commit rejects the future offset (error) or it clamps the
	// checkpoint to the log tail (checkpoint = 3, keeping all 3 records pending).
	if commitErr == nil && pending == 0 {
		t.Fatalf("Commit(99) accepted a non-existent offset and advanced the checkpoint past the log: " +
			"Commit returned nil and UncommittedCount()=0; want a validation error or a clamped checkpoint " +
			"with 3 pending records")
	}
	if pending != 3 {
		t.Fatalf("Commit(99) changed the pending set: UncommittedCount()=%d after a 3-record log "+
			"(Commit err=%v); want 3 pending records", pending, commitErr)
	}

	// The concrete impact: a record appended after the over-advance must still be
	// observable by delivery (ReadBatch starts at the checkpoint).
	if _, err := engine.Append(Record{
		ID:        4,
		Timestamp: fixedTime,
		Topic:     "audit",
		Payload:   []byte("payload-after-overadvance"),
	}); err != nil {
		t.Fatalf("append after Commit(99) failed: %v", err)
	}

	batch, err := engine.ReadBatch(10)
	if err != nil {
		t.Fatalf("ReadBatch failed after Commit(99): %v", err)
	}
	if len(batch) == 0 {
		t.Fatalf("ReadBatch returned no records after Commit(99); the record appended at offset 3 is silently skipped")
	}
	if last := batch[len(batch)-1]; last.Offset != 3 {
		t.Fatalf("ReadBatch returned records through offset %d, want the appended record at offset 3", last.Offset)
	}
}
