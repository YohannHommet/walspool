package walspool_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
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

// 8. CRIT-04 / Bug #9: Concurrent Flush and Close must never deadlock
func TestConcurrentFlushAndClose_NoDeadlock(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(5000)
	sink := &recordingSink{}

	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = 5 * time.Millisecond

	spool, err := walspool.New(cfg, storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = spool.Enqueue(ctx, "telemetry.flushclose", []byte("sample-data"))
	}

	const flusherCount = 20
	var wg sync.WaitGroup
	wg.Add(flusherCount)

	for i := 0; i < flusherCount; i++ {
		go func() {
			defer wg.Done()
			flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := spool.Flush(flushCtx)
			if err != nil && !errors.Is(err, walspool.ErrSpoolerClosed) {
				t.Errorf("unexpected error from Flush: %v", err)
			}
		}()
	}

	// Stagger slightly and Close while flushes are actively pending
	time.Sleep(2 * time.Millisecond)
	if err := spool.Close(); err != nil {
		t.Fatalf("spool close failed: %v", err)
	}

	// Ensure all concurrent flushers return without deadlock within timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Succeeded: no deadlock
	case <-time.After(3 * time.Second):
		t.Fatal("deadlock detected: Flush concurrent with Close hung indefinitely")
	}

	// Postcondition: any subsequent Flush or Enqueue on closed spooler must return ErrSpoolerClosed
	if err := spool.Flush(ctx); !errors.Is(err, walspool.ErrSpoolerClosed) {
		t.Fatalf("expected ErrSpoolerClosed on closed spool Flush, got %v", err)
	}
	if err := spool.Enqueue(ctx, "test", []byte("data")); !errors.Is(err, walspool.ErrSpoolerClosed) {
		t.Fatalf("expected ErrSpoolerClosed on closed spool Enqueue, got %v", err)
	}
}

// 9. Meyer DbC & Concurrency: Enqueue respects ctx.Err() and handles Close() without race
func TestEnqueue_ContextAndConcurrentClose(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(1000)
	sink := &recordingSink{}

	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	// 1. Pre-canceled context must fail immediately with ctx.Err()
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = spool.Enqueue(canceledCtx, "telemetry.canceled", []byte("payload"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got %v", err)
	}

	// 2. High concurrency enqueue during Close to verify TOCTOU safety and atomic RLock
	const workers = 15
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				err := spool.Enqueue(context.Background(), "telemetry.race", []byte("val"))
				if err != nil && !errors.Is(err, walspool.ErrSpoolerClosed) {
					t.Errorf("unexpected enqueue error: %v", err)
				}
			}
		}()
	}

	time.Sleep(1 * time.Millisecond)
	_ = spool.Close()
	wg.Wait()
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

// 10. Phase 2: UnmarshalBinary with excess bytes / padding must succeed and ignore padding (MAJ-04)
func TestRecord_UnmarshalBinary_Padding(t *testing.T) {
	orig := walspool.Record{
		ID:        42,
		Timestamp: time.Unix(1700000000, 0),
		Topic:     "system.metrics",
		Payload:   []byte(`{"cpu":85.4,"mem":60.2}`),
	}

	data, err := orig.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Append trailing sector padding / excess bytes
	padding := []byte("EXTRA_TRAILING_SECTOR_PADDING_DATA_1234567890")
	dataWithPadding := append(data, padding...)

	var rec walspool.Record
	if err := rec.UnmarshalBinary(dataWithPadding); err != nil {
		t.Fatalf("UnmarshalBinary failed on padded buffer: %v", err)
	}

	if rec.ID != orig.ID {
		t.Errorf("expected ID %d, got %d", orig.ID, rec.ID)
	}
	if rec.Topic != orig.Topic {
		t.Errorf("expected Topic %q, got %q", orig.Topic, rec.Topic)
	}
	if string(rec.Payload) != string(orig.Payload) {
		t.Errorf("expected Payload %q, got %q", string(orig.Payload), string(rec.Payload))
	}
	if len(rec.Payload) != len(orig.Payload) {
		t.Errorf("expected Payload length %d, got %d (padding was not stripped)", len(orig.Payload), len(rec.Payload))
	}
}

// 11. Phase 2: UnmarshalBinary with truncated data must return ErrTruncatedData (MAJ-04)
func TestRecord_UnmarshalBinary_Truncated(t *testing.T) {
	orig := walspool.Record{
		ID:        100,
		Timestamp: time.Unix(1700000000, 0),
		Topic:     "audit.events",
		Payload:   []byte(`{"action":"login","user":"alice"}`),
	}

	data, err := orig.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// 1. Buffer smaller than headerSize (29 bytes)
	for l := 0; l < 29; l++ {
		var rec walspool.Record
		err := rec.UnmarshalBinary(data[:l])
		if !errors.Is(err, walspool.ErrTruncatedData) {
			t.Errorf("expected ErrTruncatedData for header len %d, got %v", l, err)
		}
	}

	// 2. Buffer with valid header but truncated body (topic or payload truncated)
	for l := 29; l < len(data); l++ {
		var rec walspool.Record
		err := rec.UnmarshalBinary(data[:l])
		if !errors.Is(err, walspool.ErrTruncatedData) {
			t.Errorf("expected ErrTruncatedData for truncated body len %d, got %v", l, err)
		}
	}
}

// 12. Phase 2: Defensive Copy Isolation test (MAJ-02)
func TestSpooler_DefensiveCopy_Isolation(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &recordingSink{
		failureCount: 100, // hold in storage so we can inspect storage engine directly
		failErr:      walspool.ErrSinkUnavailable,
	}

	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	// Caller creates a mutable byte buffer
	mutablePayload := []byte("original-unmutated-payload-bytes")

	if err := spool.Enqueue(context.Background(), "telemetry.defensive", mutablePayload); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Caller immediately mutates the underlying byte array post-enqueue
	for i := range mutablePayload {
		mutablePayload[i] = 'X'
	}

	// Verify storage engine retains the unmutated original bytes
	batch, err := storage.ReadBatch(1)
	if err != nil {
		t.Fatalf("ReadBatch failed: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record in storage, got %d", len(batch))
	}

	expected := "original-unmutated-payload-bytes"
	if string(batch[0].Payload) != expected {
		t.Fatalf("defensive copy violated! Storage record was mutated: got %q, expected %q", string(batch[0].Payload), expected)
	}
}

// 13. Phase 2: IngestionObserver Port notification test (MAJ-03)
type mockIngestionObserver struct {
	mu       sync.Mutex
	ingested []walspool.Record
}

func (m *mockIngestionObserver) OnIngested(rec walspool.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingested = append(m.ingested, rec)
}

func (m *mockIngestionObserver) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.ingested)
}

func (m *mockIngestionObserver) records() []walspool.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]walspool.Record, len(m.ingested))
	copy(copied, m.ingested)
	return copied
}

func TestSpooler_IngestionObserver_Notification(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &recordingSink{}
	mockObs := &mockIngestionObserver{}

	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil, walspool.WithObserver(mockObs))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	ctx := context.Background()
	testRecords := []struct {
		topic   string
		payload []byte
	}{
		{"audit.auth", []byte(`{"user":"admin","action":"login"}`)},
		{"audit.billing", []byte(`{"invoice":42,"amount":150}`)},
		{"audit.security", []byte(`{"ip":"10.0.0.1","threat":false}`)},
	}

	for _, tr := range testRecords {
		if err := spool.Enqueue(ctx, tr.topic, tr.payload); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	if mockObs.count() != 3 {
		t.Fatalf("expected 3 records notified to observer, got %d", mockObs.count())
	}

	recs := mockObs.records()
	for i, tr := range testRecords {
		if recs[i].Topic != tr.topic {
			t.Errorf("record %d topic mismatch: expected %s, got %s", i, tr.topic, recs[i].Topic)
		}
		if string(recs[i].Payload) != string(tr.payload) {
			t.Errorf("record %d payload mismatch: expected %s, got %s", i, string(tr.payload), string(recs[i].Payload))
		}
		if recs[i].Offset != walspool.Offset(i) {
			t.Errorf("record %d offset mismatch: expected %d, got %d", i, i, recs[i].Offset)
		}
		if recs[i].ID == 0 {
			t.Errorf("record %d ID must not be zero", i)
		}
	}
}
