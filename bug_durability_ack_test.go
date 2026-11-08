package walspool

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBugDurabilityAck pins the documented acknowledgment contract:
// README.md:489 states records are acknowledged only after persisting to the local
// WAL, ports.go:11 calls Enqueue "durable storage", and README.md:57 claims crash
// durability (SIGKILL / OOM). Once Append returns nil, the record must therefore
// at least have been written through to the OS file (active.wal), so a process
// crash cannot lose an acknowledged record.
//
// Under SyncPolicy SyncInterval the implementation only copies the marshalled
// record into a userspace bufio.Writer and returns; the bytes stay in process
// memory until the background flusher tick (or an explicit Flush) runs.
func TestBugDurabilityAck(t *testing.T) {
	dir := t.TempDir()

	opts := FileStorageOptions{
		BufferSize:   128 * 1024,
		SyncPolicy:   SyncInterval,
		SyncInterval: time.Hour, // keep the periodic flusher out of the measurement window
	}

	engine, err := NewFileStorageEngineWithOptions(dir, 100, opts)
	if err != nil {
		t.Fatalf("init engine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	rec := Record{
		ID:        1,
		Timestamp: time.Now(),
		Topic:     "durability.ack",
		Payload:   []byte(`{"invoice_id":42}`),
	}

	if _, err := engine.Append(rec); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	walPath := filepath.Join(dir, "active.wal")

	// Append has just reported success: the record is now acknowledged to the caller.
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat %s: %v", walPath, err)
	}
	if info.Size() == 0 {
		// Diagnostic: prove the bytes were only sitting in the userspace buffer.
		flushErr := engine.Flush()
		after, statErr := os.Stat(walPath)
		afterSize := int64(-1)
		if statErr == nil {
			afterSize = after.Size()
		}
		t.Fatalf("durability contract violated: Append returned success (record acknowledged) "+
			"but active.wal is empty (size=%d bytes); after explicit Flush size=%d bytes (flushErr=%v). "+
			"The acknowledged record was still buffered in the userspace bufio.Writer and would be "+
			"lost on SIGKILL / process crash, contradicting the documented durable-ack contract.",
			info.Size(), afterSize, flushErr)
	}
}
