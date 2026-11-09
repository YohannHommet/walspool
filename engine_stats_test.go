package walspool

import (
	"context"
	"testing"
	"time"
)

func TestEngineStatsCounters(t *testing.T) {
	storage := NewMemoryStorageEngine(100)
	cfg := DefaultConfig()
	cfg.BatchSize = 1
	spool, err := New(cfg, storage, successSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	if err := spool.Enqueue(context.Background(), "audit", []byte("one")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for spool.Stats().CommittedRecords == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("record was not committed; stats=%+v", spool.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	stats := spool.Stats()
	if stats.Enqueued != 1 || stats.DeliveryAttempts != 1 || stats.CommittedRecords != 1 {
		t.Fatalf("stats = %+v, want enqueued=1 attempts=1 committed=1", stats)
	}
}

func TestEngineStatsSpoolFull(t *testing.T) {
	storage := NewMemoryStorageEngine(100)
	cfg := DefaultConfig()
	cfg.MaxCapacity = 1
	spool, err := New(cfg, storage, blockedSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	if err := spool.Enqueue(context.Background(), "audit", []byte("one")); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	if err := spool.Enqueue(context.Background(), "audit", []byte("two")); err == nil {
		t.Fatalf("second Enqueue unexpectedly succeeded")
	}
	if got := spool.Stats().SpoolFull; got != 1 {
		t.Fatalf("SpoolFull = %d, want 1", got)
	}
}

func TestFileStorageDiskBytesCache(t *testing.T) {
	storage, err := NewFileStorageEngineWithOptions(t.TempDir(), 100, FileStorageOptions{
		BufferSize: 1024,
		SyncPolicy: SyncEveryRecord,
	})
	if err != nil {
		t.Fatalf("NewFileStorageEngineWithOptions: %v", err)
	}
	defer storage.Close()

	if _, err := storage.Append(Record{ID: 1, Topic: "audit", Payload: []byte("one")}); err != nil {
		t.Fatalf("append one: %v", err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	first, err := storage.DiskBytes()
	if err != nil || first == 0 {
		t.Fatalf("first DiskBytes = %d, %v", first, err)
	}

	if _, err := storage.Append(Record{ID: 2, Topic: "audit", Payload: []byte("two")}); err != nil {
		t.Fatalf("append two: %v", err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	cached, err := storage.DiskBytes()
	if err != nil {
		t.Fatalf("cached DiskBytes: %v", err)
	}
	if cached != first {
		t.Fatalf("DiskBytes bypassed cache: first=%d cached=%d", first, cached)
	}

	storage.diskCacheMu.Lock()
	storage.diskCacheAt = time.Now().Add(-2 * diskBytesCacheTTL)
	storage.diskCacheMu.Unlock()
	refreshed, err := storage.DiskBytes()
	if err != nil {
		t.Fatalf("refreshed DiskBytes: %v", err)
	}
	if refreshed <= first {
		t.Fatalf("refreshed DiskBytes = %d, want > %d", refreshed, first)
	}
}
