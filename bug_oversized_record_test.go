package walspool

import (
	"testing"
	"time"
)

// TestBugOversizedRecord verifies the durability contract for a record whose
// payload is larger than maxRecoverableRecordSize (10 MB).
//
// Contract: either Append must reject the oversized record (so no durability
// promise is made), or an accepted record must survive a close/reopen cycle.
// On HEAD Append accepts it, but recovery silently truncates the frame because
// totalRecordLen > maxRecoverableRecordSize, so the acknowledged record is lost.
func TestBugOversizedRecord(t *testing.T) {
	const payloadSize = 10*1024*1024 + 1 // maxRecoverableRecordSize + 1

	dir := t.TempDir()

	engine, err := NewFileStorageEngine(dir, 1000)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	_, appendErr := engine.Append(Record{
		ID:        1,
		Timestamp: time.Now(),
		Topic:     "oversized",
		Payload:   payload,
	})

	if err := engine.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	reopened, err := NewFileStorageEngine(dir, 1000)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened storage: %v", err)
		}
	}()

	uncommitted, err := reopened.UncommittedCount()
	if err != nil {
		t.Fatalf("uncommitted count after reopen: %v", err)
	}

	if appendErr != nil {
		// Append refused the oversized record, so the contract is satisfied as
		// long as the refusal did not leave a phantom durable record behind.
		if uncommitted != 0 {
			t.Fatalf("Append rejected the oversized record (%v) but %d record(s) reappeared after close/reopen", appendErr, uncommitted)
		}
		return
	}

	if uncommitted != 1 {
		t.Fatalf("Append accepted a %d-byte record (limit is %d) but after close/reopen UncommittedCount() = %d, want 1: the acknowledged record was silently discarded on recovery",
			payloadSize, maxRecoverableRecordSize, uncommitted)
	}

	batch, err := reopened.ReadBatch(1)
	if err != nil {
		t.Fatalf("read batch after reopen: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("after reopen ReadBatch(1) returned %d record(s), want 1", len(batch))
	}
	if len(batch[0].Payload) != payloadSize {
		t.Fatalf("recovered payload length = %d, want %d", len(batch[0].Payload), payloadSize)
	}
}
