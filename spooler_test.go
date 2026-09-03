package walspool_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"walspool"
)

// recordingSink is an in-memory test stand-in for the Sink outbound port.
type recordingSink struct {
	mu           sync.Mutex
	delivered    []walspool.Record
	failureCount int
	failErr      error
}

func (s *recordingSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failureCount > 0 {
		s.failureCount--
		return s.failErr
	}

	s.delivered = append(s.delivered, batch...)
	return nil
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.delivered)
}

// 1. Contract Preconditions Test (Meyer DbC)
func TestPreconditions(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &recordingSink{
		failureCount: 10,
		failErr:      walspool.ErrSinkUnavailable,
	}
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil)
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	defer spool.Close()

	ctx := context.Background()

	// Empty topic must fail
	if err := spool.Enqueue(ctx, "", []byte("payload")); !errors.Is(err, walspool.ErrPreconditionViolated) {
		t.Errorf("expected ErrPreconditionViolated for empty topic, got %v", err)
	}

	// Empty payload must fail
	if err := spool.Enqueue(ctx, "audit", nil); !errors.Is(err, walspool.ErrPreconditionViolated) {
		t.Errorf("expected ErrPreconditionViolated for empty payload, got %v", err)
	}

	// Payload exceeding limit must fail
	oversized := make([]byte, 1024*1024+1)
	if err := spool.Enqueue(ctx, "audit", oversized); !errors.Is(err, walspool.ErrPreconditionViolated) {
		t.Errorf("expected ErrPreconditionViolated for oversized payload, got %v", err)
	}
}

// 2. Behavioral Seam Test: Enqueue, Batch Drain & Flush
func TestEnqueueAndFlush_InMemory(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &recordingSink{}

	cfg := walspool.DefaultConfig()
	cfg.FlushInterval = 10 * time.Millisecond
	cfg.BatchSize = 10

	spool, err := walspool.New(cfg, storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	ctx := context.Background()
	const total = 25
	for i := 0; i < total; i++ {
		if err := spool.Enqueue(ctx, "telemetry.events", []byte("sensor-data")); err != nil {
			t.Fatalf("enqueue failed at %d: %v", i, err)
		}
	}

	// Flush barrier ensures all 25 records are delivered to sink
	flushCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := spool.Flush(flushCtx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if sink.count() != total {
		t.Errorf("expected %d delivered records, got %d", total, sink.count())
	}
}

// 3. Backpressure & Quota Rejection Test (Tier 2 Failure Contract)
func TestBackpressure_QuotaExhaustion(t *testing.T) {
	// Storage quota strictly limited to 5 uncommitted items
	storage := walspool.NewMemoryStorageEngine(5)
	// Sink that blocks delivery to trigger buffer buildup
	blockingSink := &recordingSink{
		failureCount: 100,
		failErr:      walspool.ErrSinkUnavailable,
	}

	spool, err := walspool.New(walspool.DefaultConfig(), storage, blockingSink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := spool.Enqueue(ctx, "audit", []byte("msg")); err != nil {
			t.Fatalf("unexpected error at item %d: %v", i, err)
		}
	}

	// 6th item must be rejected with ErrSpoolFull
	err = spool.Enqueue(ctx, "audit", []byte("rejected-msg"))
	if !errors.Is(err, walspool.ErrSpoolFull) {
		t.Errorf("expected ErrSpoolFull, got %v", err)
	}
}

// 4. Transient Fault Retry Test with Exponential Backoff
func TestTransientFault_AutomaticRetry(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &recordingSink{
		failureCount: 2, // Fail twice with Tier 3 transient fault, then succeed
		failErr:      walspool.ErrSinkUnavailable,
	}

	cfg := walspool.DefaultConfig()
	cfg.InitialBackoff = 5 * time.Millisecond
	cfg.FlushInterval = 5 * time.Millisecond

	spool, err := walspool.New(cfg, storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	ctx := context.Background()
	if err := spool.Enqueue(ctx, "security.alerts", []byte("auth_failure")); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Flush blocks until retry succeeds
	flushCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := spool.Flush(flushCtx); err != nil {
		t.Fatalf("flush failed after retry: %v", err)
	}

	if sink.count() != 1 {
		t.Errorf("expected 1 record delivered after retry, got %d", sink.count())
	}
}

// 5. Crash Resilience & Recovery with Disk-Backed WAL
func TestDiskWAL_CrashRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fileStorage, err := walspool.NewFileStorageEngine(tmpDir, 1000)
	if err != nil {
		t.Fatalf("failed to init file storage: %v", err)
	}

	// Step 1: Enqueue records but deliberately don't flush them to sink
	discardSink := &recordingSink{
		failureCount: 9999, // block delivery to leave records uncheckpointed in WAL
		failErr:      walspool.ErrSinkUnavailable,
	}

	spool1, err := walspool.New(walspool.DefaultConfig(), fileStorage, discardSink, nil)
	if err != nil {
		t.Fatalf("spool1 init error: %v", err)
	}

	ctx := context.Background()
	const recoveredTotal = 15
	for i := 0; i < recoveredTotal; i++ {
		if err := spool1.Enqueue(ctx, "billing.reconcile", []byte("uncommitted-transaction")); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	// Abruptly close spool1 (simulating process restart before delivery)
	_ = spool1.Close()

	// Step 2: Boot fresh Spooler instance on the same directory with a healthy sink
	reopenedStorage, err := walspool.NewFileStorageEngine(tmpDir, 1000)
	if err != nil {
		t.Fatalf("failed to reopen file storage: %v", err)
	}

	healthySink := &recordingSink{}
	spool2, err := walspool.New(walspool.DefaultConfig(), reopenedStorage, healthySink, nil)
	if err != nil {
		t.Fatalf("spool2 init error: %v", err)
	}
	defer spool2.Close()

	// Flush must drain all 15 records recovered from the WAL
	flushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := spool2.Flush(flushCtx); err != nil {
		t.Fatalf("flush on recovered spooler failed: %v", err)
	}

	if healthySink.count() != recoveredTotal {
		t.Errorf("expected %d recovered records, got %d", recoveredTotal, healthySink.count())
	}
}

// 6. CRC32 Bit-Rot / Corruption Detection
func TestDiskWAL_CRC32CorruptionDetection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_corrupt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := walspool.NewFileStorageEngine(tmpDir, 100)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	sink := &recordingSink{
		failureCount: 10,
		failErr:      walspool.ErrSinkUnavailable,
	}
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to create spooler: %v", err)
	}

	ctx := context.Background()
	if err := spool.Enqueue(ctx, "audit", []byte("vital-data")); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	_ = spool.Close()

	// Corrupt a byte in the active.wal payload area
	walPath := filepath.Join(tmpDir, "active.wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed to read wal: %v", err)
	}

	// Invert a bit in the payload byte
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("failed to write corrupted wal: %v", err)
	}

	// Reopen should fail during batch read due to CRC32 mismatch
	storageCorrupt, err := walspool.NewFileStorageEngine(tmpDir, 100)
	if err != nil {
		t.Fatalf("reopen storage failed: %v", err)
	}
	defer storageCorrupt.Close()

	_, readErr := storageCorrupt.ReadBatch(10)
	if !errors.Is(readErr, walspool.ErrStorageUnavailable) {
		t.Errorf("expected ErrStorageUnavailable on corrupted data, got %v", readErr)
	}
}

// 7. Concurrency Test: High-contention concurrent enqueuing
func TestConcurrentEnqueue_RaceFree(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(10000)
	sink := &recordingSink{}

	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 50
	cfg.FlushInterval = 5 * time.Millisecond

	spool, err := walspool.New(cfg, storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	const workers = 10
	const itemsPerWorker = 100
	var wg sync.WaitGroup

	ctx := context.Background()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				payload := []byte("concurrent-event")
				if err := spool.Enqueue(ctx, "telemetry.concurrent", payload); err != nil {
					t.Errorf("worker %d failed at %d: %v", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	flushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := spool.Flush(flushCtx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	const expectedTotal = workers * itemsPerWorker
	if sink.count() != expectedTotal {
		t.Errorf("expected %d total records, got %d", expectedTotal, sink.count())
	}
}

func BenchmarkSpoolerEnqueue_InMemory(b *testing.B) {
	storage := walspool.NewMemoryStorageEngine(b.N + 1000)
	sink := &recordingSink{}
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil)
	if err != nil {
		b.Fatalf("failed to init: %v", err)
	}
	defer spool.Close()

	ctx := context.Background()
	payload := []byte("benchmark-payload-data-sample")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = spool.Enqueue(ctx, "bench.events", payload)
	}
}
